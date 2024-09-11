import random
import string
import duckdb
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_asset import DataAsset


@asset("simple_asset.parquet")
def simple_asset():
    return [
        {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        {"count": 10, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
    ]


async def test_simple_asset():
    result = await simple_asset()
    assert result


async def test_when_data_asset_yields_another_data_asset():
    @asset("composed_asset.parquet")
    async def composed_asset():
        yield simple_asset()
        more_numbers = await simple_asset.query("SELECT count + 5 as count")
        yield more_numbers

    composed_asset_query = await composed_asset.query("SELECT COUNT(*) c")
    assert composed_asset_query

    count_query_result = composed_asset_query.fetchone()
    assert count_query_result

    # Because composed adds 5 as a new array, there should be 6 in the array total
    assert count_query_result[0] == 6


async def test_when_data_asset_takes_another_data_asset_as_parameter():
    @asset("asset_with_asset_parameter.{asset.name}.parquet")
    async def asset_with_param(asset: DataAsset):
        return asset

    await asset_with_param(simple_asset)


async def test_when_data_asset_yields_multiple_lists():
    @asset("multiple_lists_asset.parquet")
    async def multiple_lists_asset():
        yield [
            {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]
        yield [
            {"count": 10, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 15, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    multiple_lists_asset_query = await multiple_lists_asset.query("SELECT COUNT(*) c")
    assert multiple_lists_asset_query
    count_query_result = multiple_lists_asset_query.fetchone()
    assert count_query_result

    # The total count should be 4 since there are 4 rows in the output parquet
    assert count_query_result[0] == 4


async def test_when_data_asset_schema_evolution():
    @asset("schema_evolution_asset_1.parquet")
    async def schema_evolution_asset_1():
        yield [
            {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    @asset("schema_evolution_asset_2.parquet")
    async def schema_evolution_asset_2():
        yield [
            {
                "count": 10,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "broken": {"value": None},
            },
            {"count": 15, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {
                "count": 20,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "extra_field": "extra_value",
                "broken": {"value": "oh hellooo"},
            },
        ]

    @asset("schema_evolution_asset.parquet")
    async def schema_evolution_asset():
        result = await schema_evolution_asset_1()
        yield result
        result = await schema_evolution_asset_2()
        yield result

    schema_evolution_asset_query = await schema_evolution_asset.query(
        "SELECT COUNT(*) c"
    )
    assert schema_evolution_asset_query
    count_query_result = schema_evolution_asset_query.fetchone()

    assert count_query_result

    # The total count should be 5 since there are 5 rows in the output parquet
    assert count_query_result[0] == 5


async def test_when_data_asset_contains_empty_struct():
    @asset("empty_struct_asset.parquet")
    async def empty_struct_asset():
        yield [
            {
                "count": 1,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
            {
                "count": 5,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
            {
                "count": 10,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
            {
                "count": 15,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
        ]

    empty_struct_asset_query = await empty_struct_asset.query("SELECT COUNT(*) c")
    assert empty_struct_asset_query
    count_query_result = empty_struct_asset_query.fetchone()

    assert count_query_result

    # The total count should be 4 since there are 4 rows in the output parquet
    assert count_query_result[0] == 4


async def test_when_data_asset_yields_no_data():
    @asset("empty_asset.parquet")
    async def empty_asset():
        yield []

    empty_asset_query = await empty_asset.query("SELECT *")

    if not empty_asset_query:
        return

    count_query_result = duckdb.query("SELECT COUNT(*) c").fetchone()

    assert count_query_result

    # The total count should be 0 since the asset yields no data
    assert count_query_result[0] == 0


async def test_nested_structs_with_many_keys_should_not_cast_to_string():
    def generate_unique_dict(keys):
        return {f"{key}@somewhere.com": ["creator", "editor"] for key in keys}

    def generate_random_keys(count):
        return [
            "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
            for _ in range(count)
        ]

    def generate_rows(num_rows):
        rows = []
        for i in range(num_rows):
            keys = generate_random_keys(100)
            rows.append(
                {
                    "id": str(i + 1),
                    "data": generate_unique_dict(keys),
                }
            )
        return rows

    @asset("dict_array_asset_1.parquet")
    async def dict_array_asset_1():
        yield generate_rows(50)
        yield generate_rows(50)

    # Query the composed asset to check its contents
    composed_query = await dict_array_asset_1.query("SELECT *")

    assert composed_query

    # The second column is named data
    assert composed_query.description[1][0] == "data"

    # And it not a string type
    # duckdb GitHub Issue: https://github.com/duckdb/duckdb/issues/13734
    assert composed_query.description[1][1] != "STRING"

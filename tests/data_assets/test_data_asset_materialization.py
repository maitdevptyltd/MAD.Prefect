from decimal import Decimal
import random
import string
from uuid import UUID
import duckdb
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_asset import DataAsset
from datetime import datetime, date
import pandas as pd

from mad_prefect.data_assets.options import ReadJsonOptions


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


async def test_materialize_artifact_with_decimal():
    @asset("decimal_asset.parquet")
    async def decimal_asset():
        yield [
            {"count": Decimal(1.1), "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": Decimal(5.5), "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": Decimal(10.75), "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    decimal_asset_query = await decimal_asset.query("SELECT COUNT(*) c")
    assert decimal_asset_query
    count_query_result = decimal_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the decimal values are correctly stored and retrieved
    decimal_values_query = await decimal_asset.query("SELECT count")
    assert decimal_values_query
    decimal_values = [row[0] for row in decimal_values_query.fetchall()]
    assert decimal_values == [1.1, 5.5, 10.75]


async def test_materialize_artifact_with_datetime():
    @asset("datetime_asset.parquet")
    async def datetime_asset():
        yield [
            {
                "timestamp": datetime(2023, 1, 1, 12, 0, 0),
                "date": date(2023, 1, 1),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 2, 12, 0, 0),
                "date": date(2023, 2, 1),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 3, 12, 0, 0),
                "date": date(2023, 3, 1),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
        ]

    datetime_asset_query = await datetime_asset.query("SELECT COUNT(*) c")
    assert datetime_asset_query
    count_query_result = datetime_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the datetime values are correctly stored and retrieved
    datetime_values_query = await datetime_asset.query("SELECT timestamp, date")
    assert datetime_values_query
    datetime_values = [(row[0], row[1]) for row in datetime_values_query.fetchall()]
    assert datetime_values == [
        (datetime(2023, 1, 1, 12, 0, 0), date(2023, 1, 1)),
        (datetime(2023, 1, 2, 12, 0, 0), date(2023, 2, 1)),
        (datetime(2023, 1, 3, 12, 0, 0), date(2023, 3, 1)),
    ]


async def test_materialize_artifact_with_pandas_dataframe():
    @asset("pandas_dataframe_asset.parquet")
    async def pandas_dataframe_asset():
        data = {
            "count": [1, 5, 10],
            "id": [
                "951c58e4-b9a4-4478-883e-22760064e416",
                "951c58e4-b9a4-4478-883e-22760064e416",
                "951c58e4-b9a4-4478-883e-22760064e416",
            ],
        }
        df = pd.DataFrame(data)
        yield df

    pandas_dataframe_asset_query = await pandas_dataframe_asset.query(
        "SELECT COUNT(*) c"
    )
    assert pandas_dataframe_asset_query
    count_query_result = pandas_dataframe_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the dataframe values are correctly stored and retrieved
    dataframe_values_query = await pandas_dataframe_asset.query("SELECT count, id")
    assert dataframe_values_query
    dataframe_values = [(row[0], row[1]) for row in dataframe_values_query.fetchall()]
    expected_values = [
        (1, UUID("951c58e4-b9a4-4478-883e-22760064e416")),
        (5, UUID("951c58e4-b9a4-4478-883e-22760064e416")),
        (10, UUID("951c58e4-b9a4-4478-883e-22760064e416")),
    ]
    assert dataframe_values == expected_values


async def test_materialize_artifact_with_inner_struct():
    @asset(
        "inner_struct_asset.parquet",
        read_json_options=ReadJsonOptions(columns={"data": "MAP(STRING, STRING)"}),
    )
    async def inner_struct_asset():
        yield [
            {"id": "1", "data": {"field1": "value1", "field2": "value2"}},
            {"id": "2", "data": {"field1": "value3", "field2": "value4"}},
        ]

    inner_struct_asset_query = await inner_struct_asset.query("SELECT COUNT(*) c")

    assert inner_struct_asset_query

    count_query_result = inner_struct_asset_query.fetchone()
    assert count_query_result

    # The total count should be 2 since there are 2 rows in the output parquet
    assert count_query_result[0] == 2

    # Verify that the id and data columns exist and data is of type MAP(STRING, STRING)
    inner_struct_asset_query = await inner_struct_asset.query("SELECT *")
    assert inner_struct_asset_query

    # Check that the id column exists
    assert "id" in [desc[0] for desc in inner_struct_asset_query.description]

    assert inner_struct_asset_query.types[1] == "MAP(VARCHAR, VARCHAR)"


async def test_json_artifacts_with_different_timestamp_precisions_are_deserialized_as_timestamps():
    @asset("datetime_asset_precision.parquet", artifact_filetype="json")
    async def datetime_asset_precision():
        yield [
            {
                "timestamp": datetime(2023, 1, 1, 12, 5, 50, 99),
                "timestamp_ez": datetime(2023, 1, 1, 12),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 2, 12, 0, 0, 55),
                "timestamp_ez": datetime(2023, 1, 1, 11),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 3, 12, 0, 0, 33),
                "timestamp_ez": datetime(2023, 1, 1, 10),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
        ]

    datetime_asset_query = await datetime_asset_precision.query("SELECT COUNT(*) c")
    assert datetime_asset_query
    count_query_result = datetime_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the datetime values are correctly stored and retrieved
    datetime_values_query = await datetime_asset_precision.query(
        "SELECT timestamp, timestamp_ez"
    )

    assert datetime_values_query
    assert datetime_values_query.types[0] == "timestamp"
    assert datetime_values_query.types[1] == "timestamp"

    assert datetime_values_query
    datetime_values = [row[0] for row in datetime_values_query.fetchall()]
    assert datetime_values == [
        datetime(2023, 1, 1, 12, 5, 50, 99),
        datetime(2023, 1, 2, 12, 0, 0, 55),
        datetime(2023, 1, 3, 12, 0, 0, 33),
    ]

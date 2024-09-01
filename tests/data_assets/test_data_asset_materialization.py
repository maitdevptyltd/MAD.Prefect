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
        more_numbers = await simple_asset.query(
            "SELECT count + 5 as count FROM simple_asset"
        )
        yield more_numbers

    composed_asset_query = await composed_asset.query("SELECT * FROM composed_asset")
    count_query_result = duckdb.query(
        "SELECT COUNT(*) c FROM composed_asset_query"
    ).fetchone()

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

    multiple_lists_asset_query = await multiple_lists_asset.query(
        "SELECT * FROM multiple_lists_asset"
    )
    count_query_result = duckdb.query(
        "SELECT COUNT(*) c FROM multiple_lists_asset_query"
    ).fetchone()

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
        yield await schema_evolution_asset_1()
        yield await schema_evolution_asset_2()

    schema_evolution_asset_query = await schema_evolution_asset.query(
        "SELECT * FROM schema_evolution_asset"
    )
    count_query_result = duckdb.query(
        "SELECT COUNT(*) c FROM schema_evolution_asset_query"
    ).fetchone()

    assert count_query_result

    # The total count should be 5 since there are 5 rows in the output parquet
    assert count_query_result[0] == 5

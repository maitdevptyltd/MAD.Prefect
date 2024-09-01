import duckdb
from mad_prefect.data_assets import asset


@asset("simple_asset.parquet")
def simple_asset():
    return [{"count": 1}, {"count": 5}, {"count": 10}]


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

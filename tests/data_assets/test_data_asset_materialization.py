import duckdb
from mad_prefect.data_assets import asset


async def test_when_data_asset_yields_another_data_asset():
    @asset("simple_asset.parquet")
    def simple_asset():
        return [1, 2, 3]

    @asset("composed_asset.parquet")
    async def composed_asset():
        yield simple_asset()
        more_numbers = await simple_asset.query("SELECT 0 + 5 as num FROM simple_asset")
        yield more_numbers

    composed_asset_query = await composed_asset.query("SELECT * FROM composed_asset")
    count_query_result = duckdb.query(
        "SELECT COUNT(*) c FROM composed_asset_query"
    ).fetchone()

    assert count_query_result

    # Because composed adds 5 as a new array, there should be 6 in the array total
    assert count_query_result[0] == 6

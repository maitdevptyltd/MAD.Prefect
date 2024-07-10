import asyncio
import duckdb
from mad_prefect.data_assets import asset
from tests.sample_data.mock_api import get_api, ingest_endpoint
import mad_prefect.filesystems
import pandas as pd


# Test writing data from return function
@asset(f"bronze/orgs_returned/organisations_return.parquet")
async def asset_bronze_organisations_return():

    data = await get_api("organisations", {"limit": 3})
    return data["organisations"]


# Test writing data with yield, no artifact_dir, httpx.Response output
@asset(f"bronze/buildings_yielded/buildings_api-response.parquet")
async def asset_bronze_buildings_yielded_response():
    async for output in ingest_endpoint(
        endpoint="buildings", return_type="api_response"
    ):
        yield output


# Test writing data with yield, no artifact_dir, json output
@asset(f"bronze/orgs_yielded/organisations_json.parquet")
async def asset_bronze_organisations_yielded_json():
    async for output in ingest_endpoint():
        yield output["organisations"]


# Test writing data with yield, json output, specific artifact_dir
@asset(f"bronze/pels_yielded/pelicans_json.parquet", artifacts_dir="raw/pelicans")
async def asset_bronze_pelicans_yielded_json():
    async for output in ingest_endpoint(endpoint="pelicans"):
        yield output["pelicans"]


# Test writing data with return, DuckDBPyRelation output, no artifact_dir
@asset(f"bronze/buildings_unnested/buildings_unnested_query.parquet")
async def asset_bronze_buildings_unnested_query():
    nested_buildings = asset_bronze_buildings_yielded_response
    unnested_buildings_query = await nested_buildings.query(
        "SELECT UNNEST(buildings, max_depth:=2) FROM asset"
    )
    return unnested_buildings_query


# Test writing data with return, DuckDBPyRelation output, no artifact_dir
@asset(f"bronze/buildings_unnested/buildings_unnested_df.parquet")
async def asset_bronze_buildings_unnested_df():
    nested_buildings = asset_bronze_buildings_yielded_response
    unnested_buildings_query = await nested_buildings.query(
        "SELECT UNNEST(buildings, max_depth:=2) FROM asset"
    )
    return unnested_buildings_query.df()


async def run_tests():
    await asset_bronze_organisations_return()
    await asset_bronze_buildings_yielded_response()
    await asset_bronze_organisations_yielded_json()
    await asset_bronze_pelicans_yielded_json()
    await asset_bronze_buildings_unnested_query()
    await asset_bronze_buildings_unnested_df()


if __name__ == "__main__":
    asyncio.run(run_tests())

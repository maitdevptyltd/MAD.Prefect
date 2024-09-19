import asyncio
from datetime import timedelta
import os

import duckdb
from mad_prefect.data_assets import ASSET_METADATA_LOCATION, asset
from mad_prefect.filesystems import get_fs
from tests.data_assets.sample_data.mock_api import get_api


async def test_no_cache_expiration():
    fs = await get_fs()
    await fs.delete_path(ASSET_METADATA_LOCATION, recursive=True)

    @asset("caching/no_cache_expiration.parquet", name="no_cache")
    async def test_asset():
        return await get_api("organisations", {"limit": 3})

    first_run = test_asset.with_arguments()
    first_run_artifact = await first_run()
    first_run_materialized = first_run.asset_run.materialized

    # As data is materialized should output DuckDBPyRelation
    assert isinstance(first_run_artifact.data, duckdb.DuckDBPyRelation)

    second_run = test_asset.with_arguments()
    second_run_artifact = await second_run()
    second_run_last_materialized = second_run.last_materialized

    # As no cache_expiration is provided data should rematerialize
    assert isinstance(second_run_artifact.data, duckdb.DuckDBPyRelation)
    # Previous materialization timestamp in metadata should match timestamp from first run
    assert first_run_materialized == second_run_last_materialized


async def test_within_cache_expiration():
    fs = await get_fs()
    await fs.delete_path(ASSET_METADATA_LOCATION, recursive=True)

    @asset(
        "caching/within_cache_expiration.parquet",
        cache_expiration=timedelta(hours=2),
        name="within_cache",
    )
    async def test_asset():
        return await get_api("organisations", {"limit": 3})

    first_run = test_asset.with_arguments()
    first_run_artifact = await first_run()
    first_run_materialized = first_run.asset_run.materialized

    # As data is materialized should output DuckDBPyRelation
    assert isinstance(first_run_artifact.data, duckdb.DuckDBPyRelation)

    second_run = test_asset.with_arguments()
    second_run_artifact = await second_run()
    second_run_last_materialized = second_run.last_materialized

    # As cache_expiration is provided data should not rematerialize and artifact should be empty
    assert second_run_artifact.data is None
    # Previous materialization timestamp in metadata should match timestamp from first run
    assert first_run_materialized == second_run_last_materialized


async def test_outside_cache_expiration():
    fs = await get_fs()
    await fs.delete_path(ASSET_METADATA_LOCATION, recursive=True)

    @asset(
        "caching/outside_cache_expiration.parquet",
        name="outside_cache",
        cache_expiration=timedelta(minutes=1),
    )
    async def test_asset():
        return await get_api("organisations", {"limit": 3})

    first_run = test_asset.with_arguments()
    first_run_artifact = await first_run()
    first_run_materialized = first_run.asset_run.materialized

    # As data is materialized should output DuckDBPyRelation
    assert isinstance(first_run_artifact.data, duckdb.DuckDBPyRelation)

    await asyncio.sleep(80)

    second_run = test_asset.with_arguments()
    second_run_artifact = await second_run()
    second_run_last_materialized = second_run.last_materialized

    # As second run outside cache_expiration  data should rematerialize
    assert isinstance(second_run_artifact.data, duckdb.DuckDBPyRelation)
    # Previous materialization timestamp in metadata should match timestamp from first run
    assert first_run_materialized == second_run_last_materialized

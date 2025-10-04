import os
from datetime import datetime, timezone

import duckdb
import pytest

from mad_prefect import filesystems as mad_filesystems
from mad_prefect.data_assets.asset_metadata import get_asset_metadata
from mad_prefect.data_assets.data_asset_run import DataAssetRun


@pytest.fixture()
async def isolated_filesystem(tmp_path, monkeypatch):
    """Ensure each test runs against a fresh MAD filesystem."""
    import mad_prefect.duckdb as mad_duckdb

    original_fs = mad_filesystems._get_fs_result
    original_url = mad_filesystems.FILESYSTEM_URL
    original_block_name = mad_filesystems.FILESYSTEM_BLOCK_NAME
    original_env_url = os.environ.get("FILESYSTEM_URL")
    original_env_block = os.environ.get("FILESYSTEM_BLOCK_NAME")
    original_mad_fs_ref = mad_duckdb._mad_filesystem_ref

    mad_filesystems.FILESYSTEM_URL = f"file://{tmp_path}"
    mad_filesystems.FILESYSTEM_BLOCK_NAME = None
    monkeypatch.setenv("FILESYSTEM_URL", mad_filesystems.FILESYSTEM_URL)
    monkeypatch.delenv("FILESYSTEM_BLOCK_NAME", raising=False)
    mad_filesystems._get_fs_result = None
    mad_duckdb._mad_filesystem_ref = None

    if duckdb.filesystem_is_registered("mad"):
        duckdb.unregister_filesystem("mad")

    yield tmp_path

    if duckdb.filesystem_is_registered("mad"):
        duckdb.unregister_filesystem("mad")

    mad_filesystems._get_fs_result = original_fs
    mad_filesystems.FILESYSTEM_URL = original_url
    mad_filesystems.FILESYSTEM_BLOCK_NAME = original_block_name
    mad_duckdb._mad_filesystem_ref = original_mad_fs_ref

    if original_env_url is not None:
        monkeypatch.setenv("FILESYSTEM_URL", original_env_url)
    else:
        monkeypatch.delenv("FILESYSTEM_URL", raising=False)

    if original_env_block is not None:
        monkeypatch.setenv("FILESYSTEM_BLOCK_NAME", original_env_block)
    else:
        monkeypatch.delenv("FILESYSTEM_BLOCK_NAME", raising=False)


async def test_get_asset_metadata_without_files(isolated_filesystem):
    result = await get_asset_metadata("asset.name", "asset-id")
    assert result is None


async def test_get_asset_metadata_reads_available_files(isolated_filesystem):
    asset_name = "asset.name"
    asset_id = "asset-id"

    first_run = DataAssetRun(
        id="run-1",
        runtime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        materialized=datetime(2024, 1, 1, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    second_run = DataAssetRun(
        id="run-2",
        runtime=datetime(2024, 1, 2, tzinfo=timezone.utc),
        materialized=datetime(2024, 1, 2, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    await first_run.persist()
    await second_run.persist()

    relation = await get_asset_metadata(asset_name, asset_id)
    assert relation is not None
    assert isinstance(relation, duckdb.DuckDBPyRelation)

    rows = relation.fetchall()
    assert rows

    materialized_values = [row[2] for row in rows if row[2] is not None]
    assert materialized_values
    assert max(materialized_values) == datetime(2024, 1, 2)

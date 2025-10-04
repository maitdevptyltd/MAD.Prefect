import os
from datetime import datetime, timezone

import duckdb
import pytest

from mad_prefect import filesystems as mad_filesystems
from mad_prefect.data_assets.asset_metadata import (
    AssetManifest,
    AssetManifestRun,
    ManifestRunStatus,
    get_asset_metadata,
    load_asset_manifest,
)
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.data_asset_run import DataAssetRun
from mad_prefect.data_assets.data_asset_callable import DataAssetCallable
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_asset_options import DataAssetOptions


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


def test_asset_manifest_defaults_are_timezone_aware():
    manifest = AssetManifest(
        asset_name="asset.name",
        asset_id="asset-id",
        updated_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    assert manifest.manifest_version == "1"
    assert manifest.last_status is ManifestRunStatus.UNKNOWN
    assert manifest.last_artifacts == ()
    assert manifest.updated_at.tzinfo is not None


def test_asset_manifest_run_requires_timezone():
    run = AssetManifestRun(
        id="run-1",
        metadata_path="path.json",
        materialized=datetime(2024, 1, 2, tzinfo=timezone.utc),
    )

    manifest = AssetManifest(
        asset_name="asset.name",
        asset_id="asset-id",
    ).with_run(run, status=ManifestRunStatus.SUCCESS)

    assert manifest.last_run == run
    assert manifest.last_materialized == datetime(2024, 1, 2, tzinfo=timezone.utc)


def test_asset_manifest_run_rejects_naive_datetime():
    with pytest.raises(ValueError):
        AssetManifestRun(
            id="run-1",
            metadata_path="path.json",
            materialized=datetime(2024, 1, 2),
        )


async def test_data_asset_run_persist_updates_manifest(isolated_filesystem):
    asset_name = "asset.name"
    asset_id = "asset-id"

    run = DataAssetRun(
        id="run-1",
        runtime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    await run.persist(asset_signature="signature-1")

    manifest = await load_asset_manifest(asset_name, asset_id)
    assert manifest is not None
    assert manifest.last_status is ManifestRunStatus.UNKNOWN
    assert manifest.last_run is not None
    assert manifest.last_run.id == "run-1"
    assert manifest.last_run.materialized is None

    run.materialized = datetime(2024, 1, 2, tzinfo=timezone.utc)
    run.duration_miliseconds = 1_000

    await run.persist(
        artifact_paths=("results/data.parquet",),
        asset_signature="signature-1",
        status=ManifestRunStatus.SUCCESS,
    )

    manifest = await load_asset_manifest(asset_name, asset_id)
    assert manifest is not None
    assert manifest.last_status is ManifestRunStatus.SUCCESS
    assert manifest.last_materialized == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert manifest.last_artifacts == ("results/data.parquet",)
    assert manifest.last_run is not None
    assert manifest.last_run.metadata_path.endswith(
        "/asset_run_id=run-1/metadata.json"
    )


async def test_manifest_created_when_only_legacy_metadata_exists(isolated_filesystem):
    asset_name = "asset.name"
    asset_id = "asset-id"

    fs = await mad_filesystems.get_fs()

    legacy_run = DataAssetRun(
        id="run-legacy",
        runtime=datetime(2023, 12, 31, tzinfo=timezone.utc),
        materialized=datetime(2023, 12, 31, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    legacy_path = (
        f"{ASSET_METADATA_LOCATION}/asset_name={asset_name}/asset_id={asset_id}/"
        "asset_run_id=run-legacy/metadata.json"
    )
    await fs.write_data(legacy_path, legacy_run.model_dump(mode="json"))

    manifest = await load_asset_manifest(asset_name, asset_id)
    assert manifest is None

    new_run = DataAssetRun(
        id="run-new",
        runtime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        materialized=datetime(2024, 1, 1, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    await new_run.persist(
        artifact_paths=("results/data.parquet",),
        asset_signature="signature-1",
        status=ManifestRunStatus.SUCCESS,
    )

    manifest = await load_asset_manifest(asset_name, asset_id)
    assert manifest is not None
    assert manifest.last_run is not None
    assert manifest.last_run.id == "run-new"
    assert manifest.last_status is ManifestRunStatus.SUCCESS
    assert manifest.last_artifacts == ("results/data.parquet",)

    relation = await get_asset_metadata(asset_name, asset_id)
    assert relation is not None
    rows = relation.fetchall()
    assert any(row[0] == "run-new" for row in rows)


async def test_get_last_materialized_prefers_manifest(isolated_filesystem):
    asset_name = "asset.name"
    test_asset_callable = _create_test_callable(asset_name)
    asset_id = test_asset_callable.asset.id

    run = DataAssetRun(
        id="run-1",
        runtime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        materialized=datetime(2024, 1, 2, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    await run.persist(
        asset_signature=asset_id,
        status=ManifestRunStatus.SUCCESS,
    )

    manifest = await load_asset_manifest(asset_name, asset_id)
    assert manifest and manifest.last_materialized == datetime(2024, 1, 2, tzinfo=timezone.utc)

    last_materialized = await test_asset_callable._get_last_materialized(test_asset_callable.asset)
    assert last_materialized == datetime(2024, 1, 2, tzinfo=timezone.utc)


async def test_get_last_materialized_falls_back_to_metadata(isolated_filesystem):
    asset_name = "asset.name"
    test_asset_callable = _create_test_callable(asset_name)
    asset_id = test_asset_callable.asset.id

    run = DataAssetRun(
        id="run-legacy",
        runtime=datetime(2024, 1, 1, tzinfo=timezone.utc),
        materialized=datetime(2024, 1, 2, tzinfo=timezone.utc),
        asset_id=asset_id,
        asset_name=asset_name,
        asset_path="/tmp/asset",
    )

    await run.persist(
        asset_signature=asset_id,
        status=ManifestRunStatus.SUCCESS,
    )

    fs = await mad_filesystems.get_fs()
    manifest_path = (
        f"{ASSET_METADATA_LOCATION}/asset_name={asset_name}/asset_id={asset_id}/manifest.json"
    )
    await fs.delete_path(manifest_path, recursive=False)

    last_materialized = await test_asset_callable._get_last_materialized(test_asset_callable.asset)
    assert last_materialized == datetime(2024, 1, 2, tzinfo=timezone.utc)


def _create_test_callable(asset_name: str) -> DataAssetCallable:
    async def materialize():
        return []

    asset_instance = DataAsset(
        materialize,
        f"{asset_name}.parquet",
        asset_name,
        DataAssetOptions(),
    )

    return DataAssetCallable(asset_instance)

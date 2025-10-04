"""Helpers and models for asset metadata manifests."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
import logging
from typing import Iterable, Literal, Sequence

import duckdb
from pydantic import BaseModel, Field, model_validator

from mad_prefect.data_assets.asset_decorator import ASSET_METADATA_LOCATION
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs

logger = logging.getLogger(__name__)

__all__ = [
    "get_asset_metadata",
    "ManifestRunStatus",
    "AssetManifestRun",
    "AssetManifest",
    "load_asset_manifest",
    "persist_asset_manifest",
    "upsert_asset_manifest_from_run",
]


MANIFEST_FILENAME = "manifest.json"


def _ensure_tzaware(value: datetime | None) -> datetime | None:
    """Ensure datetime values are timezone-aware UTC."""

    if value is None:
        return None

    if value.tzinfo is None:
        raise ValueError("Datetime value must be timezone aware")

    return value.astimezone(timezone.utc)


class ManifestRunStatus(str, Enum):
    """Lifecycle status for the last observed run."""

    SUCCESS = "success"
    FAILED = "failed"
    UNKNOWN = "unknown"


class AssetManifestRun(BaseModel):
    """Summarises the latest run captured in the manifest."""

    id: str
    metadata_path: str
    materialized: datetime | None = None
    artifact_paths: tuple[str, ...] = Field(default_factory=tuple)

    @model_validator(mode="after")
    def _validate_datetimes(self) -> "AssetManifestRun":
        self.materialized = _ensure_tzaware(self.materialized)
        return self

    def with_artifacts(self, artifacts: Iterable[str]) -> "AssetManifestRun":
        return self.model_copy(update={"artifact_paths": tuple(artifacts)})


class AssetManifest(BaseModel):
    """Manifest describing the most recent run and metadata for an asset."""

    manifest_version: Literal["1"] = "1"
    asset_name: str
    asset_id: str
    asset_signature: str | None = None
    last_materialized: datetime | None = None
    last_run: AssetManifestRun | None = None
    last_status: ManifestRunStatus = ManifestRunStatus.UNKNOWN
    last_error: str | None = None
    last_artifacts: tuple[str, ...] = Field(default_factory=tuple)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @model_validator(mode="after")
    def _validate_datetimes(self) -> "AssetManifest":
        updated_at = _ensure_tzaware(self.updated_at) or datetime.now(timezone.utc)
        self.updated_at = updated_at
        self.last_materialized = _ensure_tzaware(self.last_materialized)
        if self.last_run and self.last_run.materialized and not self.last_materialized:
            self.last_materialized = self.last_run.materialized
        return self

    def with_run(
        self,
        run: AssetManifestRun,
        *,
        status: ManifestRunStatus = ManifestRunStatus.SUCCESS,
        error: str | None = None,
        artifacts: Sequence[str] | None = None,
    ) -> "AssetManifest":
        """Return a copy of the manifest updated with the provided run information."""

        updated_artifacts = tuple(artifacts) if artifacts is not None else tuple(self.last_artifacts)
        if artifacts is None and run.artifact_paths:
            updated_artifacts = run.artifact_paths

        return self.model_copy(
            update={
                "last_run": run,
                "last_status": status,
                "last_error": error,
                "last_materialized": run.materialized or self.last_materialized,
                "last_artifacts": updated_artifacts,
                "updated_at": datetime.now(timezone.utc),
            }
        )


def _manifest_path(asset_name: str, asset_id: str) -> str:
    return f"{ASSET_METADATA_LOCATION}/asset_name={asset_name}/asset_id={asset_id}/{MANIFEST_FILENAME}"


async def load_asset_manifest(asset_name: str, asset_id: str) -> AssetManifest | None:
    """Load the manifest for the given asset if it exists."""

    fs = await get_fs()
    path = _manifest_path(asset_name, asset_id)

    if not fs.exists(path):
        return None

    try:
        raw_manifest = await fs.read_data(path)
    except Exception:  # pragma: no cover - surfaced via log
        logger.exception(
            "Failed to read asset manifest",
            extra={"asset_name": asset_name, "asset_id": asset_id, "path": path},
        )
        return None

    try:
        return AssetManifest.model_validate(raw_manifest)
    except Exception:  # pragma: no cover - surfaced via log
        logger.exception(
            "Failed to parse asset manifest",
            extra={"asset_name": asset_name, "asset_id": asset_id, "path": path},
        )
        return None


async def persist_asset_manifest(manifest: AssetManifest) -> AssetManifest:
    """Persist the provided manifest to the filesystem."""

    fs = await get_fs()
    path = _manifest_path(manifest.asset_name, manifest.asset_id)
    await fs.write_data(path, manifest.model_dump(mode="json"))
    logger.debug(
        "Persisted asset manifest",
        extra={
            "asset_name": manifest.asset_name,
            "asset_id": manifest.asset_id,
            "path": path,
        },
    )
    return manifest


async def upsert_asset_manifest_from_run(
    *,
    asset_name: str,
    asset_id: str,
    run_id: str,
    metadata_path: str,
    materialized: datetime | None,
    status: ManifestRunStatus,
    artifact_paths: Sequence[str] | None = None,
    asset_signature: str | None = None,
    error: str | None = None,
) -> AssetManifest:
    """Create or update the manifest using the latest run information."""

    manifest = await load_asset_manifest(asset_name, asset_id)

    run = AssetManifestRun(
        id=run_id,
        metadata_path=metadata_path,
        materialized=materialized,
        artifact_paths=tuple(artifact_paths or ()),
    )

    if manifest is None:
        manifest = AssetManifest(
            asset_name=asset_name,
            asset_id=asset_id,
            asset_signature=asset_signature,
        )
    else:
        updates: dict[str, object] = {}
        if asset_signature and manifest.asset_signature != asset_signature:
            updates["asset_signature"] = asset_signature
        if updates:
            manifest = manifest.model_copy(update=updates)

    manifest = manifest.with_run(
        run,
        status=status,
        error=error,
        artifacts=artifact_paths,
    )

    return await persist_asset_manifest(manifest)


async def get_asset_metadata(
    asset_name: str,
    asset_id: str,
) -> duckdb.DuckDBPyRelation | None:
    """Return asset metadata for the provided asset if available."""

    await register_mad_protocol()
    fs = await get_fs()

    metadata_glob = (
        f"{ASSET_METADATA_LOCATION}/asset_name={asset_name}/asset_id={asset_id}/**/*.json"
    )
    logger.debug("Searching for asset metadata with glob: %s", metadata_glob)

    if fs.glob(metadata_glob):
        return duckdb.query(
            f"SELECT UNNEST(data, max_depth:=2) FROM read_json('mad://{metadata_glob}')"
        )

    return None

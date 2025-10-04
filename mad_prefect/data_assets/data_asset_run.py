import datetime
import logging
from typing import Sequence
from pydantic import BaseModel
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.asset_metadata import (
    ManifestRunStatus,
    upsert_asset_manifest_from_run,
)
from mad_prefect.filesystems import get_fs

logger = logging.getLogger(__name__)


class DataAssetRun(BaseModel):
    id: str | None = None
    runtime: datetime.datetime | None = None
    materialized: datetime.datetime | None = None
    duration_miliseconds: int | None = None

    asset_id: str | None = None
    asset_name: str | None = None
    asset_path: str | None = None
    parameters: str | None = None

    async def persist(
        self,
        *,
        status: ManifestRunStatus | None = None,
        error: str | None = None,
        artifact_paths: Sequence[str] | None = None,
        asset_signature: str | None = None,
        update_manifest: bool = True,
    ):
        fs = await get_fs()
        path = f"{ASSET_METADATA_LOCATION}/asset_name={self.asset_name}/asset_id={self.asset_id}/asset_run_id={self.id}/metadata.json"
        logger.debug(
            "Persisting asset run metadata",
            extra={"run_id": self.id, "path": path},
        )
        await fs.write_data(
            path,
            self,
        )

        if not update_manifest:
            return

        if not (self.asset_name and self.asset_id and self.id):
            return

        manifest_status = status or (
            ManifestRunStatus.SUCCESS if self.materialized else ManifestRunStatus.UNKNOWN
        )

        await upsert_asset_manifest_from_run(
            asset_name=self.asset_name,
            asset_id=self.asset_id,
            run_id=self.id,
            metadata_path=path,
            materialized=self.materialized,
            status=manifest_status,
            artifact_paths=artifact_paths,
            asset_signature=asset_signature,
            error=error,
        )

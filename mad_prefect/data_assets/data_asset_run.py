import datetime
import logging
from pydantic import BaseModel
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
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

    async def persist(self):
        fs = await get_fs()
        path = f"{ASSET_METADATA_LOCATION}/asset_name={self.asset_name}/asset_id={self.asset_id}/asset_run_id={self.id}/metadata.json"
        logger.debug("Persisting asset run metadata", run_id=self.id, path=path)
        await fs.write_data(
            path,
            self,
        )

import datetime
from pydantic import BaseModel


class DataAssetRun(BaseModel):
    id: str | None = None
    runtime: datetime.datetime | None = None
    duration_miliseconds: int | None = None

    asset_id: str | None = None
    asset_name: str | None = None
    asset_path: str | None = None
    parameters: str | None = None

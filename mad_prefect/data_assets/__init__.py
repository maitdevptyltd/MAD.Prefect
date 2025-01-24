import datetime
import os
from typing import Callable, Literal

from mad_prefect.data_assets.options import ReadJsonOptions, ReadCSVOptions

ASSET_METADATA_LOCATION = os.getenv("ASSET_METADATA_LOCATION", "_asset_metadata")
ARTIFACT_FILE_TYPES = Literal["parquet", "json", "csv"]


def asset(
    path: str,
    artifacts_dir: str = "",
    name: str | None = None,
    snapshot_artifacts: bool = False,
    artifact_filetype: ARTIFACT_FILE_TYPES = "json",
    read_json_options: ReadJsonOptions | None = None,
    read_csv_options: ReadCSVOptions | None = None,
    cache_expiration: datetime.timedelta | None = None,
):
    # Prevent a circular reference as it references the env variable
    from mad_prefect.data_assets.data_asset import DataAsset

    def decorator(fn: Callable):
        return DataAsset(
            fn,
            path,
            artifacts_dir,
            name,
            snapshot_artifacts,
            artifact_filetype,
            read_json_options,
            read_csv_options,
            cache_expiration,
        )

    return decorator

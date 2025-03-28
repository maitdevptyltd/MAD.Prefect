from dataclasses import dataclass
from datetime import timedelta
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions


@dataclass
class DataAssetOptions:
    artifacts_dir: str = ""
    snapshot_artifacts: bool = False
    artifact_filetype: ARTIFACT_FILE_TYPES = "json"
    read_json_options: ReadJsonOptions | None = None
    read_csv_options: ReadCSVOptions | None = None
    cache_expiration: timedelta | None = None

import datetime
from inspect import isclass
import os
from typing import Callable, Literal, TypeVar
from mad_prefect.data_assets.options import (
    ContextFactoryType,
)
from mad_prefect.data_assets.options import (
    DataHydraOptions,
    ReadJsonOptions,
    ReadCSVOptions,
)

ASSET_METADATA_LOCATION = os.getenv("ASSET_METADATA_LOCATION", "_asset_metadata")
ARTIFACT_FILE_TYPES = Literal["parquet", "json", "csv"]

T = TypeVar("T")


class AssetDecorator:
    def __call__(
        self,
        path: str,
        artifacts_dir: str = "",
        name: str | None = None,
        snapshot_artifacts: bool = False,
        artifact_filetype: ARTIFACT_FILE_TYPES = "json",
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
        cache_expiration: datetime.timedelta | None = None,
        context_factory: ContextFactoryType = None,
        max_concurrency: int = 5,
    ):
        # Prevent a circular reference as it references the env variable
        from mad_prefect.data_assets.data_asset import DataAsset
        from mad_prefect.data_assets.data_hydra import DataHydra

        def decorator(fn):
            # If fn is a cls, it will be a DataHydra
            if isinstance(fn, type):
                return DataHydra(
                    fn,
                    DataHydraOptions(
                        path=path,
                        max_concurrency=max_concurrency,
                        name=name,
                        context_factory=context_factory,
                    ),
                )

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

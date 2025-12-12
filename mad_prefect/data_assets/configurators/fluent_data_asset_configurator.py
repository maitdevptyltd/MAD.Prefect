import logging
from datetime import timedelta
from functools import partial
from typing import Generic, ParamSpec, TypeVar, overload
from mad_prefect.data_assets.asset_decorator import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.data_asset import (
    DataAsset,
    CACHE_FIRST_CACHE_EXPIRATION,
)
from mad_prefect.data_assets.data_asset_options import DataAssetOptions
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")

logger = logging.getLogger(__name__)

class Unset: pass
UNSET = Unset()

def resolve(value: T | Unset, default: T) -> T:
    if isinstance(value, Unset):
        return default
    return value

class FluentDataAssetConfigurator(Generic[P, R]):
    def __init__(self, asset: DataAsset[P, R]):
        self.asset = asset

    @overload
    def with_arguments(self, *args: P.args, **kwargs: P.kwargs) -> DataAsset[P, R]: ...

    @overload
    def with_arguments(self, *args, **kwargs) -> DataAsset[P, R]: ...

    def with_arguments(self, *args, **kwargs):
        logger.debug(
            f"Configuring asset '{self.asset.name}' with new arguments. Args: {args}, Kwargs: {kwargs}"
        )
        # Create a partial function which has the arguments bound
        new_fn = partial(self.asset._fn, *args, **kwargs)

        # Return a new asset based on the bound arguments
        asset = DataAsset[P, R](
            new_fn,
            self.asset.template_path,
            self.asset.template_name,
            self.asset.base_options,
        )
        return asset

    def with_options(
        self,
        path: str | Unset = UNSET,
        artifacts_dir: str | Unset = UNSET,
        name: str | Unset = UNSET,
        snapshot_artifacts: bool | Unset = UNSET,
        artifact_filetype: ARTIFACT_FILE_TYPES | Unset = UNSET,
        read_json_options: ReadJsonOptions | None | Unset = UNSET,
        read_csv_options: ReadCSVOptions | None | Unset = UNSET,
        cache_expiration: timedelta | None | Unset = UNSET,
    ):
        logger.debug(f"Configuring asset '{self.asset.name}' with new options.")

        # Resolve defaults for any options that remain unset by with_options
        options = DataAssetOptions(
            artifacts_dir=resolve(artifacts_dir, self.asset.base_options.artifacts_dir),
            snapshot_artifacts=resolve(snapshot_artifacts, self.asset.base_options.snapshot_artifacts),
            artifact_filetype=resolve(artifact_filetype, self.asset.base_options.artifact_filetype),
            read_json_options=resolve(read_json_options, self.asset.base_options.read_json_options),
            read_csv_options=resolve(read_csv_options, self.asset.base_options.read_csv_options),
            cache_expiration=resolve(cache_expiration, self.asset.base_options.cache_expiration),
        )

        final_path = resolve(path, self.asset.template_path)
        final_name = resolve(name, self.asset.template_name)
        
        asset = DataAsset(
            self.asset._fn,
            final_path,
            final_name,
            options=options,
        )

        return asset

    def cache_first(self, expiration: timedelta | None = None) -> DataAsset[P, R]:
        logger.debug(
            "Configuring asset '%s' to prioritize cached materializations.",
            self.asset.name,
        )

        ttl = expiration if expiration is not None else CACHE_FIRST_CACHE_EXPIRATION
        return self.with_options(cache_expiration=ttl)

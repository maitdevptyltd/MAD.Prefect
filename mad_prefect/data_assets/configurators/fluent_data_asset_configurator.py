from datetime import timedelta
from functools import partial
from typing import Generic, ParamSpec, TypeVar, overload
from mad_prefect.data_assets.asset_decorator import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_asset_options import DataAssetOptions
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions

P = ParamSpec("P")
R = TypeVar("R")


class FluentDataAssetConfigurator(Generic[P, R]):
    def __init__(self, asset: DataAsset[P, R]):
        self.asset = asset

    @overload
    def with_arguments(self, *args: P.args, **kwargs: P.kwargs): ...

    @overload
    def with_arguments(self, *args, **kwargs) -> DataAsset[P, R]: ...

    def with_arguments(self, *args, **kwargs):
        # Create a partial function which has the arguments bound
        new_fn = partial(self.asset._fn, *args, **kwargs)

        # Return a new asset based on the bound arguments
        asset = DataAsset[P, R](
            new_fn,
            self.asset.path,
            self.asset.name,
            self.asset.options,
        )
        return asset

    def with_options(
        self,
        path: str | None = None,
        artifacts_dir: str | None = None,
        name: str | None = None,
        snapshot_artifacts: bool | None = None,
        artifact_filetype: ARTIFACT_FILE_TYPES | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
        cache_expiration: timedelta | None = None,
    ):
        # Default to the current asset's options for any None values
        options = DataAssetOptions(
            artifacts_dir=artifacts_dir or self.asset.options.artifacts_dir,
            snapshot_artifacts=snapshot_artifacts
            or self.asset.options.snapshot_artifacts,
            artifact_filetype=artifact_filetype or self.asset.options.artifact_filetype,
            read_json_options=read_json_options or self.asset.options.read_json_options,
            read_csv_options=read_csv_options or self.asset.options.read_csv_options,
            cache_expiration=cache_expiration or self.asset.options.cache_expiration,
        )
        asset = DataAsset(
            self.asset._fn,
            path or self.asset.path,
            name or self.asset.name,
            options=options,
        )

        return asset

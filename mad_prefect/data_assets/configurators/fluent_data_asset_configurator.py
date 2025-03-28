from datetime import timedelta
from typing import ParamSpec, TypeVar
from mad_prefect.data_assets.asset_decorator import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_asset_options import DataAssetOptions
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions

P = ParamSpec("P")
R = TypeVar("R")


class FluentDataAssetConfigurator:
    def __init__(self, asset: DataAsset[P, R]):
        self.asset = asset

    def with_arguments(self, *args, **kwargs):
        asset = DataAsset(
            self.asset._fn,
            self.asset.path,
            self.asset.name,
            self.asset.options,
        )

        asset._bind_arguments(*args, **kwargs)
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
            artifacts_dir=artifacts_dir or self.asset.artifacts_dir,
            snapshot_artifacts=snapshot_artifacts or self.asset.snapshot_artifacts,
            artifact_filetype=artifact_filetype or self.asset.artifact_filetype,
            read_json_options=read_json_options or self.asset.read_json_options,
            read_csv_options=read_csv_options or self.asset.read_csv_options,
            cache_expiration=cache_expiration or self.asset.cache_expiration,
        )
        asset = DataAsset(
            self.asset._fn,
            path or self.asset.path,
            name or self.asset.name,
            options=options,
        )

        # Ensure we're also passing through any bound arguments if we have them
        if self.asset._bound_arguments:
            asset._bind_arguments(
                *self.asset._bound_arguments.args,
                **self.asset._bound_arguments.kwargs,
            )

        return asset

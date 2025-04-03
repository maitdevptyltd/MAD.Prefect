from functools import cached_property
import hashlib
import re
from typing import Callable, Generic, ParamSpec, TypeVar, overload
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery
from mad_prefect.data_assets.data_asset_options import DataAssetOptions

P = ParamSpec("P")
R = TypeVar("R")


class DataAsset(Generic[P, R]):
    def __init__(
        self,
        fn: Callable[P, R],
        path: str,
        name: str,
        options: DataAssetOptions,
    ):
        from .data_asset_callable import DataAssetCallable
        from .configurators import (
            FluentDataAssetConfigurator,
        )

        self.name = self._sanitize_name(name)
        self.path = path
        self.options = options
        self._fn = fn

        # Expose the fluent configurator api
        self._configurator = configurator = FluentDataAssetConfigurator(self)
        self.with_arguments = configurator.with_arguments
        self.with_options = configurator.with_options

        self._callable = DataAssetCallable(self)

    @overload
    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> DataArtifact: ...

    @overload
    async def __call__(self, *args, **kwargs) -> DataArtifact: ...

    async def __call__(self, *args: P.args, **kwargs: P.kwargs):
        return await self._callable(*args, **kwargs)

    async def query(self, query_str: str | None = None):
        result_artifact = await self()
        artifact_query = DataArtifactQuery([result_artifact])

        return await artifact_query.query(query_str)

    @cached_property
    def id(self):
        hash_input = f"{self.name}:{self.path}:{self.options.artifacts_dir}:{str(self._callable.args)}{str(self._callable.keywords)}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _sanitize_name(self, name: str) -> str:
        # Replace any character that's not alphanumeric or a '.', underscore, or hyphen with an underscore
        return re.sub(r"[^A-Za-z0-9_.\-]", "_", name)

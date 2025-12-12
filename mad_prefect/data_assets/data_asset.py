from datetime import timedelta
from functools import cached_property
import hashlib
import logging
import re
from copy import deepcopy
from typing import Callable, Generic, ParamSpec, TypeVar, overload
from mad_prefect.data_assets.asset_template_formatter import AssetTemplateFormatter
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery
from mad_prefect.data_assets.data_asset_options import DataAssetOptions

P = ParamSpec("P")
R = TypeVar("R")

logger = logging.getLogger(__name__)

CACHE_FIRST_CACHE_EXPIRATION = timedelta(days=9999)


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

        # Deep copy options object to ensure each instance gets a clean copy
        self.base_options = options
        self.options = deepcopy(options)

        # Set up template values prior to formatting with args
        self.template_name = name
        self.template_path = path
        self.template_artifacts_dir = self.options.artifacts_dir

        # Expose the fluent configurator api
        self._configurator = configurator = FluentDataAssetConfigurator(self)
        self.with_arguments = configurator.with_arguments
        self.with_options = configurator.with_options
        self.cache_first = configurator.cache_first

        self._fn = fn
        self._callable = DataAssetCallable(self)

        # Lazily format relevant fields if arguments are bound
        bound_args = self._callable.get_bound_arguments()
        
        if bound_args.arguments:
            formatter = AssetTemplateFormatter(self._callable.args, bound_args)
            self.path = (
                formatter.format(self.template_path, allow_partial=True)
                or self.template_path
            )
            formatted_name = formatter.format(self.template_name, allow_partial=True)
            self.name = self._sanitize_name(formatted_name or self.template_name)
            self.options.artifacts_dir = (
                formatter.format(self.template_artifacts_dir, allow_partial=True) or ""
            )
        else:
            self.path = self.template_path
            self.name = self._sanitize_name(self.template_name)

        logger.info(f"DataAsset '{self.name}' initialized for path '{self.path}'.")

        logger.debug(
            f"DataAsset '{self.name}' initialized with bound args {bound_args.arguments}, "
            f"path: '{self.path}', artifacts_dir: '{self.options.artifacts_dir}'"
        )

    @overload
    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> DataArtifact: ...

    @overload
    async def __call__(self, *args, **kwargs) -> DataArtifact: ...

    async def __call__(self, *args: P.args, **kwargs: P.kwargs):
        logger.debug(
            f"DataAsset '{self.name}' called with args: {args}, kwargs: {kwargs}"
        )
        return await self._callable(*args, **kwargs)

    async def query(self, query_str: str | None = None, params: object | None = None):
        logger.info(f"Querying data asset '{self.name}' with query: '{query_str}'")

        result_artifact = await self()
        artifact_query = DataArtifactQuery(
            [result_artifact],
            self.options.read_json_options,
            self.options.read_csv_options,
        )

        return await artifact_query.query(query_str, params=params)

    @cached_property
    def id(self):
        hash_input = f"{self.name}:{self.path}:{self.options.artifacts_dir}:{str(self._callable.args)}{str(self._callable.keywords)}"
        asset_id = hashlib.md5(hash_input.encode()).hexdigest()
        logger.debug(
            f"Generated asset ID for '{self.name}': {asset_id} from hash input: '{hash_input}'"
        )
        return asset_id

    def _sanitize_name(self, name: str) -> str:
        # Replace any character that's not alphanumeric or a '.', underscore, or hyphen with an underscore
        return re.sub(r"[^A-Za-z0-9_.\-{}]", "_", name)

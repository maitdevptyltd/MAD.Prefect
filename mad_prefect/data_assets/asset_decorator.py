import datetime
from inspect import isclass
import os
from types import FunctionType, BuiltinFunctionType
from typing import Any, Callable, Literal, ParamSpec, TypeVar, cast, overload
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

T = TypeVar("T", bound=Any)
P = ParamSpec("P")


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

        # Use overloads to track IDEs and type checkers that the return type is a DataHydra or DataAsset
        # otherwise they will not be able to determine the return type and return a union type

        @overload
        def decorator(fn: type[T]) -> DataHydra[T]: ...
        @overload
        def decorator(fn: Callable[P, T]) -> DataAsset[P, T]: ...

        def decorator(fn: type[T] | Callable[P, T]) -> DataHydra[T] | DataAsset[P, T]:
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

            # If fn is a cls, it will be a DataHydra
            elif callable(fn):
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

            raise ValueError(
                f"AssetDecorator cannot resolve attribute {fn} because the attribute is not a DataAsset"
            )

        return decorator

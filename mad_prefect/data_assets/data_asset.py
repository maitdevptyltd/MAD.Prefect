from datetime import datetime, UTC, timedelta, timezone
import hashlib
import inspect
import json
from pathlib import Path
import re
from typing import Callable, List
import duckdb
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_artifact_collector import DataArtifactCollector
from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery
from mad_prefect.data_assets.data_asset_run import DataAssetRun
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs
import os
import sys


class DataAsset:
    def __init__(
        self,
        fn: Callable,
        path: str,
        artifacts_dir: str = "",
        name: str | None = None,
        snapshot_artifacts: bool = False,
        artifact_filetype: ARTIFACT_FILE_TYPES = "json",
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
        cache_expiration: timedelta | None = None,
    ):
        self._fn: Callable = fn
        self._fn_signature: inspect.Signature = inspect.signature(fn)
        self._bound_arguments: inspect.BoundArguments | None = None

        self.name: str = name if name else f"{fn.__module__}.{fn.__name__}"
        self.path: str = path
        self.artifacts_dir: str = artifacts_dir
        self.snapshot_artifacts: bool = snapshot_artifacts

        self.artifact_filetype: ARTIFACT_FILE_TYPES = artifact_filetype
        self.cache_expiration: timedelta = cache_expiration or timedelta(0)

        self.id = self._generate_asset_guid()

        self.asset_run = DataAssetRun()
        self.asset_run.id = self._generate_asset_iteration_guid()
        self.asset_run.asset_name = self.name
        self.asset_run.asset_path = self.path

        self.read_json_options = read_json_options or ReadJsonOptions()
        self.read_csv_options = read_csv_options or ReadCSVOptions()

        # If the function has no parameters, bind empty arguments immediately
        if not self._fn_signature.parameters:
            self._bind_arguments()

    def with_arguments(self, *args, **kwargs):
        asset = DataAsset(
            self._fn,
            self.path,
            self.artifacts_dir,
            self.name,
            self.snapshot_artifacts,
            self.artifact_filetype,
            self.read_json_options,
            self.read_csv_options,
            self.cache_expiration,
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
        asset = DataAsset(
            self._fn,
            path=path or self.path,
            artifacts_dir=artifacts_dir or self.artifacts_dir,
            name=name or self.name,
            snapshot_artifacts=snapshot_artifacts or self.snapshot_artifacts,
            artifact_filetype=artifact_filetype or self.artifact_filetype,
            read_json_options=read_json_options or self.read_json_options,
            read_csv_options=read_csv_options or self.read_csv_options,
            cache_expiration=cache_expiration or self.cache_expiration,
        )

        # Ensure we're also passing through any bound arguments if we have them
        if self._bound_arguments:
            asset._bind_arguments(
                *self._bound_arguments.args, **self._bound_arguments.kwargs
            )

        return asset

    def _bind_arguments(self, *args, **kwargs):
        self._bound_arguments = self._fn_signature.bind(*args, **kwargs)
        self._bound_arguments.apply_defaults()

        args = self._bound_arguments
        args_dict = dict(args.arguments)

        # Resolve (path, artifacts_dir, name) to insert template values
        resolved_path = self._resolve_attribute(self.path)
        resolved_name = self._resolve_attribute(self.name)
        resolved_artifacts_dir = self._resolve_attribute(self.artifacts_dir) or ""

        if not resolved_path:
            raise ValueError(
                f"Unable to resolve data asset path: {self.path} with {args_dict}."
            )

        if not resolved_name:
            raise ValueError(
                f"Unable to resolve data asset name: {self.name} with {args_dict}."
            )

        self.path = resolved_path
        self.name = self._sanitize_name(resolved_name)
        self.artifacts_dir = resolved_artifacts_dir

        def _handle_unknown_types(data):
            if isinstance(data, DataAsset):
                return {"name": self.name, "fn": self._fn}

        # Recalculate the ids in case the parameters have changed
        self.id = self.asset_run.asset_id = self._generate_asset_guid()
        self.asset_run.id = self._generate_asset_iteration_guid()
        self.asset_run.asset_path = self.path
        self.asset_run.asset_name = self.name
        self.asset_run.parameters = json.dumps(args_dict, default=_handle_unknown_types)

        return self

    async def __call__(self, *args, **kwargs):
        if args or kwargs:
            # If arguments are passed in, create a new instance with bound arguments
            asset_with_arguments = self.with_arguments(*args, **kwargs)
            return await asset_with_arguments()
        else:
            # No arguments passed in
            if not self._bound_arguments:
                # If the function expects no parameters, bind empty arguments
                if not self._fn_signature.parameters:
                    self._bind_arguments()
                else:
                    raise TypeError(
                        f"{self.name}() missing required positional arguments"
                    )

        assert self._bound_arguments

        # Extract and set filetypes for result artifacts
        self.result_artifact_filetypes = self.get_result_artifact_filetypes()

        # There may be multiple result artifacts if following syntax is used "bronze/customers.parquet|csv"
        self.result_artifacts = self._create_result_artifacts()

        # Prevent asset from being rematerialized inside a single session
        if self.asset_run and (materialized := self.asset_run.materialized):
            return self.result_artifacts[0]

        self.asset_run.runtime = datetime.now(UTC)
        self.last_materialized = await self._get_last_materialized()

        # If data has been materialized within cache_expiration period return empty result_artifact
        if await self._cached_result(self.result_artifacts[0], self.asset_run.runtime):
            return self.result_artifacts[0]

        # Regenerate asset_run_id as runtime has now been set.
        self.asset_run.id = self._generate_asset_iteration_guid()

        print(
            f"Running operations for asset_run_id: {self.asset_run.id}, on asset_id: {self.id}, on asset: {self.name}"
        )

        # Write metadata before processing result for troubleshooting purposes
        await self.asset_run.persist()

        # For each fragment in the data batch, we create a new artifact
        base_artifact_path = self._get_artifact_base_path()

        # Clean up the old directory and delete it if we're not snapshotting
        if not self.snapshot_artifacts:
            fs = await get_fs()
            await fs.delete_path(base_artifact_path, recursive=True)

        collector = DataArtifactCollector(
            self._fn(*self._bound_arguments.args, **self._bound_arguments.kwargs),
            base_artifact_path,
            self.artifact_filetype,
            read_json_options=self.read_json_options,
            read_csv_options=self.read_csv_options,
        )

        # Collect the artifacts yielded from the materialization fn
        collector_artifacts = await collector.collect()

        # Query each of the artifacts [filepath1, filepath2, etc] with duckdb
        artifact_query = DataArtifactQuery(
            artifacts=collector_artifacts,
            read_json_options=self.read_json_options,
            read_csv_options=self.read_csv_options,
        )

        # The result is all the artifacts unioned
        result_artifact_data = await artifact_query.query()

        for result_artifact in self.result_artifacts:
            result_artifact.data = result_artifact_data

            # Persist the result artifact to storage, fully materialize it
            await result_artifact.persist()

        # Release reference to data
        result_artifact_data = None

        # Record information about the run
        self.asset_run.materialized = datetime.now(UTC)
        duration = self.asset_run.materialized - self.asset_run.runtime
        self.asset_run.duration_miliseconds = int(duration.total_seconds() * 1000)

        await self.asset_run.persist()

        print(
            f"Completed operations for asset_run_id: {self.asset_run.id}, on asset_id: {self.id}, on asset: {self.name}"
        )

        return self.result_artifacts[0]

    def _create_result_artifacts(self) -> List[DataArtifact]:
        path = Path(self.path)
        result_artifacts = []

        for filetype in self.result_artifact_filetypes:
            result_artifacts.append(
                DataArtifact(
                    path.with_suffix(filetype).as_posix(),
                    read_json_options=self.read_json_options,
                    read_csv_options=self.read_csv_options,
                )
            )

        return result_artifacts

    async def query(self, query_str: str | None = None):
        result_artifact = await self()

        artifact_query = DataArtifactQuery([result_artifact])
        return await artifact_query.query(query_str)

    def _get_artifact_base_path(self):
        partition = ""

        # If we snapshot artifacts, encapsulate the file in a directory with the runtime= parameter
        # so you can view changes over time
        if self.snapshot_artifacts and self.asset_run.runtime:
            runtime_str = str(self.asset_run.runtime.isoformat()).replace(":", "_")
            partition = (
                f"year={self.asset_run.runtime.year}/month={self.asset_run.runtime.month}/day={self.asset_run.runtime.day}/runtime={runtime_str}/"
                if self.snapshot_artifacts
                else ""
            )

        # Extract folder path for folder set up
        folder_path = os.path.dirname(self.path)

        # Set up the base path for artifact storage
        if not self.artifacts_dir:
            base_path: str = f"{folder_path}/_artifacts/asset={self.name}/{partition}"
        else:
            base_path: str = f"{self.artifacts_dir}/{partition}"

        return base_path

    def _resolve_attribute(self, input_str: str | None = None):
        if not input_str or not self._bound_arguments:
            return input_str

        input_str = input_str.format(**self._bound_arguments.arguments)
        return input_str

    def _sanitize_name(self, name: str) -> str:
        # Replace any character that's not alphanumeric or a '.', underscore, or hyphen with an underscore
        return re.sub(r"[^A-Za-z0-9_.\-]", "_", name)

    def _generate_asset_guid(self):
        hash_input = f"{self.name}:{self.path}:{self.artifacts_dir}:{str(self._bound_arguments.arguments) if self._bound_arguments else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _generate_asset_iteration_guid(self):
        hash_input = f"{self.name}:{self.path}:{self.artifacts_dir}:{self.asset_run.runtime.isoformat() if self.asset_run.runtime else ''}:{str(self._bound_arguments.arguments) if self._bound_arguments else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    async def _get_asset_metadata(self):
        await register_mad_protocol()
        fs = await get_fs()

        metadata_glob = f"{ASSET_METADATA_LOCATION}/asset_name={self.name}/asset_id={self.id}/**/*.json"

        if fs.glob(metadata_glob):
            return duckdb.query(
                f"SELECT UNNEST(data, max_depth:=2) FROM read_json('mad://{metadata_glob}')"
            )

    async def _get_last_materialized(self):
        asset_metadata = await self._get_asset_metadata()

        if not asset_metadata:
            return

        last_materialized_query = duckdb.query(
            "SELECT max(strptime(materialized, '%Y-%m-%dT%H:%M:%S.%fZ')) FROM asset_metadata"
        ).fetchone()

        if last_materialized_query and last_materialized_query[0]:
            # Convert DuckDB timestamp to Python datetime
            last_materialized = datetime.fromisoformat(str(last_materialized_query[0]))
            # Ensure it's UTC
            return last_materialized.replace(tzinfo=timezone.utc)

    async def _cached_result(self, result_artifact: DataArtifact, runtime: datetime):
        # Check if data has been materialized within cache_expiration period
        if (
            self.last_materialized
            and (self.last_materialized > runtime - self.cache_expiration)
            and await result_artifact.exists()
        ):
            print(
                f"Retrieving cached result_artifact for asset_id: {self.id} | asset: {self.name}"
            )
            return True

    def get_result_artifact_filetypes(self) -> List[str]:
        path = Path(self.path)
        filetypes_part = path.suffix

        return [f if f.startswith(".") else f".{f}" for f in filetypes_part.split("|")]

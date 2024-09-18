from datetime import datetime, UTC
import hashlib
import inspect
import json
from typing import Callable
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_artifact_collector import DataArtifactCollector
from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery
from mad_prefect.data_assets.data_asset_run import DataAssetRun
from mad_prefect.data_assets.options import ReadJsonOptions
from mad_prefect.filesystems import get_fs
import os


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
    ):
        self._fn: Callable = fn
        self._fn_signature: inspect.Signature = inspect.signature(fn)
        self._bound_arguments: inspect.BoundArguments | None = None

        self.name: str = name if name else fn.__name__
        self.path: str = path
        self.artifacts_dir: str = artifacts_dir
        self.snapshot_artifacts: bool = snapshot_artifacts

        self.artifact_filetype: ARTIFACT_FILE_TYPES = artifact_filetype

        self.id = self._generate_asset_guid()

        self.asset_run = DataAssetRun()
        self.asset_run.id = self._generate_asset_iteration_guid()
        self.asset_run.asset_name = self.name
        self.asset_run.asset_path = self.path

        self.read_json_options = read_json_options or ReadJsonOptions()

    def with_arguments(self, *args, **kwargs):
        asset = DataAsset(
            self._fn,
            self.path,
            self.artifacts_dir,
            self.name,
            self.snapshot_artifacts,
            self.artifact_filetype,
            self.read_json_options,
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
        self.name = resolved_name
        self.artifacts_dir = resolved_artifacts_dir

        def _handle_unknown_types(data):
            if isinstance(data, DataAsset):
                return {"name": self.name, "fn": self._fn}

        # Recalculate the ids incase the parameters have changed
        self.id = self.asset_run.asset_id = self._generate_asset_guid()
        self.asset_run.id = self._generate_asset_iteration_guid()
        self.asset_run.asset_path = self.path
        self.asset_run.asset_name = self.name
        self.asset_run.parameters = json.dumps(args_dict, default=_handle_unknown_types)

        return self

    async def __call__(self, *args, **kwargs):
        if not self._bound_arguments:
            # For now, if there are no bound arguments, then we will create a new instance of a DataAsset
            # which will prevent collision with same referenced assets with different parameters
            # called directly through DataAsset(args, kwargs)
            asset_with_arguments = self.with_arguments(*args, **kwargs)
            return await asset_with_arguments()

        assert self._bound_arguments
        result_artifact = self._create_result_artifact()

        if self.asset_run and (materialized := self.asset_run.materialized):
            # TODO: implement some sort of thoughtful caching. At the moment
            # this will just prevent the asset from rematerializing during the same session
            return result_artifact

        self.asset_run.runtime = datetime.now(UTC)

        print(
            f"Running operations for asset_run_id: {self.asset_run.id}, on asset_id: {self.id}, on asset: {self.name}"
        )

        # Write metadata before processing result for troubleshooting purposes
        await self.asset_run.persist()

        # TODO: in future set up caching that reads from path
        # Instead of running self.__fn if data
        # Has been created within cache period

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
        )
        result_artifact.data = await collector.collect()

        # Persist the result artifact to storage, fully materialize it
        await result_artifact.persist()

        # Record information about the run
        self.asset_run.materialized = datetime.now(UTC)
        self.asset_run.duration_miliseconds = int(
            (self.asset_run.materialized - self.asset_run.runtime).microseconds / 1000
        )
        await self.asset_run.persist()

        print(
            f"Completed operations for asset_run_id: {self.asset_run.id}, on asset_id: {self.id}, on asset: {self.name}"
        )

        return result_artifact

    def _create_result_artifact(self):
        return DataArtifact(self.path, read_json_options=self.read_json_options)

    async def query(self, query_str: str | None = None):
        result_artifact = await self()

        artifact_query = DataArtifactQuery([result_artifact])
        return await artifact_query.query(query_str)

    def _get_artifact_base_path(self):
        partition = ""

        # If we snapshot artifacts, encapsulate the file in a directory with the runtime= parameter
        # so you can view changes over time
        if self.snapshot_artifacts and self.asset_run.runtime:
            partition = (
                f"year={self.asset_run.runtime.year}/month={self.asset_run.runtime.month}/day={self.asset_run.runtime.day}/runtime={self.asset_run.runtime.isoformat()}/"
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

    def _generate_asset_guid(self):
        hash_input = f"{self.name}:{self.path}:{self.artifacts_dir}:{str(self._bound_arguments.arguments) if self._bound_arguments else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _generate_asset_iteration_guid(self):
        hash_input = f"{self.name}:{self.path}:{self.artifacts_dir}:{self.asset_run.runtime.isoformat() if self.asset_run.runtime else ''}:{str(self._bound_arguments.arguments) if self._bound_arguments else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

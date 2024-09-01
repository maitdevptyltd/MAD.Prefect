from datetime import datetime, UTC
import hashlib
import inspect
import json
from typing import Callable
import duckdb
import httpx
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_asset_run import DataAssetRun
from mad_prefect.data_assets.utils import (
    yield_data_batches,
)
from mad_prefect.filesystems import get_fs
from mad_prefect.duckdb import register_mad_protocol
import os


class DataAsset:
    __fn: Callable
    __fn_signature: inspect.Signature
    __bound_arguments: inspect.BoundArguments | None = None

    id: str
    path: str
    artifacts_dir: str
    name: str
    snapshot_artifacts: bool = False
    asset_run: DataAssetRun = DataAssetRun()

    def __init__(
        self,
        fn: Callable,
        path: str,
        artifacts_dir: str = "",
        name: str | None = None,
        snapshot_artifacts: bool = False,
    ):
        self.__fn = fn
        self.__fn_signature = inspect.signature(fn)

        self.name = name if name else fn.__name__
        self.path = path
        self.artifacts_dir = artifacts_dir
        self.snapshot_artifacts = snapshot_artifacts

        self.id = self._generate_asset_guid()

        self.asset_run.id = self._generate_asset_iteration_guid()
        self.asset_run.asset_name = self.name
        self.asset_run.asset_path = self.path

    def with_arguments(self, *args, **kwargs):
        asset = DataAsset(
            self.__fn,
            self.path,
            self.artifacts_dir,
            self.name,
            self.snapshot_artifacts,
        )

        asset._bind_arguments(*args, **kwargs)
        return asset

    def _bind_arguments(self, *args, **kwargs):
        self.__bound_arguments = self.__fn_signature.bind(*args, **kwargs)
        self.__bound_arguments.apply_defaults()

        args = self.__bound_arguments
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
                return {"name": self.name, "fn": self.__fn}

        # Recalculate the ids incase the parameters have changed
        self.id = self.asset_run.asset_id = self._generate_asset_guid()
        self.asset_run.id = self._generate_asset_iteration_guid()
        self.asset_run.asset_path = self.path
        self.asset_run.asset_name = self.name
        self.asset_run.parameters = json.dumps(args_dict, default=_handle_unknown_types)

        return self

    async def __call__(self, *args, **kwargs):
        result_artifact = self._create_result_artifact()

        if self.asset_run and (materialized := self.asset_run.runtime):
            # TODO: implement some sort of thoughtful caching. At the moment
            # this will just prevent the asset from rematerializing during the same session
            return result_artifact

        self.asset_run.runtime = datetime.now(UTC)

        # TODO: upon the first time a data asset binds its arguments, should it create a new instance of a
        # data asset? This will prevent 5 different assets which get injected parameters from overriding
        # different valeus of each other. Not an issue now but be a potential race condition
        if not self.__bound_arguments:
            self._bind_arguments(*args, **kwargs)

        assert self.__bound_arguments

        print(
            f"Running operations for asset_run_id: {self.asset_run.id}, on asset_id: {self.id}"
        )

        # Write metadata before processing result for troubleshooting purposes
        await self.__save_run_metadata()

        # TODO: in future set up caching that reads from path
        # Instead of running self.__fn if data
        # Has been created within cache period

        # For each fragment in the data batch, we create a new artifact
        base_artifact_path = self._get_artifact_base_path()

        # Clean up the old directory and delete it if we're not snapshotting
        if not self.snapshot_artifacts:
            fs = await get_fs()
            await fs.delete_path(base_artifact_path, recursive=True)

        fragment_num = 0
        artifacts: list[DataArtifact] = []

        async for fragment in yield_data_batches(
            self.__fn(*self.__bound_arguments.args, **self.__bound_arguments.kwargs)
        ):
            # If the output isn't a DataAssetArtifact manually set the params & base_path
            # and initialize the output as a DataAssetArtifact
            params = (
                dict(fragment.request.url.params)
                if isinstance(fragment, httpx.Response) and fragment.request.url.params
                else None
            )

            path = self._build_artifact_path(base_artifact_path, params, fragment_num)
            fragment_artifact = DataArtifact(path, fragment)
            await fragment_artifact.persist()

            artifacts.append(fragment_artifact)
            fragment_num += 1

        globs = [f"mad://{a.path.strip('/')}" for a in artifacts]

        # create the artifact for the data asset by glob querying all the artifacts together
        result_artifact.data = (
            duckdb.query(
                f"SELECT * FROM read_json_auto({globs}, hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)"
            )
            if globs
            else None
        )

        await result_artifact.persist()
        await self.__save_run_metadata()

        return result_artifact

    def _create_result_artifact(self):
        return DataArtifact(self.path)

    async def query(self, query_str: str | None = None):
        await self()

        # Set up filesystem abstraction
        fs = await get_fs()
        await register_mad_protocol()

        # # If asset has been created query the file
        if fs.glob(self.path):
            return self._get_self_query(query_str)

        # TODO: let's throw an error for now, as self() executed and there should be data
        # at the very least we should have an empty file?
        raise ValueError(f"No data found for asset_id: {self.id}")

    def _get_self_query(self, query_str: str | None = None):
        asset_query = duckdb.query(f"SELECT * FROM 'mad://{self.path}'")
        duckdb.register(f"{self.name}_{self.id}", asset_query)

        if not query_str:
            return duckdb.query(f"SELECT * FROM {self.name}_{self.id}")

        directed_string = query_str.replace(self.name, f"{self.name}_{self.id}")

        return duckdb.query(directed_string)

    def _build_artifact_path(
        self,
        base_path: str,
        params: dict | None = None,
        fragment_number: int | None = None,
    ):
        prefix = ""

        # If we snapshot artifacts, encapsulate the file in a directory with the runtime= parameter
        # so you can view changes over time
        if self.snapshot_artifacts and self.asset_run.runtime:
            prefix = (
                f"year={self.asset_run.runtime.year}/month={self.asset_run.runtime.month}/day={self.asset_run.runtime.day}/runtime={self.asset_run.runtime.isoformat()}/"
                if self.snapshot_artifacts
                else ""
            )

        if params is None and fragment_number is None:
            filename = self._get_filename()
            return f"{base_path}/{prefix}{filename}.json"

        if params is None:
            return f"{base_path}/{prefix}fragment={fragment_number}.json"

        params_path = "/".join(f"{key}={value}" for key, value in params.items())

        return f"{base_path}/{prefix}{params_path}.json"

    def _get_artifact_base_path(self):
        # Extract folder path for folder set up
        folder_path = os.path.dirname(self.path)

        # Set up the base path for artifact storage
        if not self.artifacts_dir:
            base_path: str = f"{folder_path}/_artifacts/asset={self.name}"
        else:
            base_path: str = self.artifacts_dir

        return base_path

    def _get_filename(self):
        return os.path.splitext(os.path.basename(self.path))[0]

    def _resolve_attribute(self, input_str: str | None = None):
        if not input_str or not self.__bound_arguments:
            return input_str

        input_str = input_str.format(**self.__bound_arguments.arguments)
        return input_str

    def _generate_asset_guid(self):
        hash_input = f"{self.name}:{self.path}:{self.artifacts_dir}:{str(self.__bound_arguments.arguments) if self.__bound_arguments else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _generate_asset_iteration_guid(self):
        hash_input = f"{self.name}:{self.path}:{self.artifacts_dir}:{self.asset_run.runtime.isoformat() if self.asset_run.runtime else ''}:{str(self.__bound_arguments.arguments) if self.__bound_arguments else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    async def __save_run_metadata(self):
        fs = await get_fs()

        await fs.write_data(
            f"{ASSET_METADATA_LOCATION}/asset_name={self.name}/asset_id={self.id}/asset_run_id={self.asset_run.id}/metadata.json",
            self.asset_run,
        )

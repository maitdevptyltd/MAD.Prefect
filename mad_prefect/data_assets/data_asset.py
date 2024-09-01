from datetime import datetime, UTC
import hashlib
import inspect
import json
from typing import Callable, cast
import duckdb
import httpx
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.utils import (
    yield_data_batches,
)
from mad_prefect.filesystems import get_fs
from mad_prefect.duckdb import register_mad_protocol
import os


class DataAsset:

    def __init__(
        self,
        fn: Callable,
        path: str,
        artifacts_dir: str | None = "",
        name: str | None = None,
        snapshot_artifacts: bool = False,
    ):
        self.__fn = fn
        self.path = path
        self.artifacts_dir = artifacts_dir
        self.name = name if name else fn.__name__
        self.snapshot_artifacts = snapshot_artifacts
        self.fn_name = fn.__name__
        self.fn_signature = inspect.signature(fn)
        self.runtime_str = None
        self.last_materialized = None

        self.bound_arguments = cast(inspect.BoundArguments | None, None)

    def with_arguments(self, *args, **kwargs):
        asset = DataAsset(
            self.__fn,
            self.path,
            self.artifacts_dir,
            self.name,
            self.snapshot_artifacts,
        )
        asset.bound_arguments = asset.fn_signature.bind(*args, **kwargs)
        asset.bound_arguments.apply_defaults()

        return asset

    async def __call__(self, *args, **kwargs):
        # Set runtime
        self.runtime = datetime.now(UTC)
        self.runtime_str = self.runtime.isoformat().replace(":", "_")

        # Wrap args into dictionary
        self.args = self.bound_arguments or self.fn_signature.bind(*args, **kwargs)
        self.args.apply_defaults()
        self.args_dict = dict(self.args.arguments)

        # Resolve (path, artifacts_dir, name) to insert template values
        resolved_path = self._resolve_attribute(self.path)
        resolved_name = self._resolve_attribute(self.name)
        resolved_artifacts_dir = self._resolve_attribute(self.artifacts_dir)

        if not resolved_path:
            raise ValueError(
                f"Unable to resolve data asset path: {self.path} with {self.args_dict}."
            )

        if not resolved_name:
            raise ValueError(
                f"Unable to resolve data asset name: {self.name} with {self.args_dict}."
            )

        self.resolved_path = resolved_path
        self.resolved_name = resolved_name
        self.resolved_artifacts_dir = resolved_artifacts_dir

        # Generate identifiers
        self.id = self._generate_asset_guid()
        self.run_id = self._generate_asset_iteration_guid()

        print(
            f"Running operations for asset_run_id: {self.run_id}, on asset_id: {self.id}"
        )

        # Set default values for attributes
        self.data_written: bool = False
        self.artifact_glob = None

        # TODO: set up function that examines metadata to extract
        # last_created timestamp
        # For now use runtime as placeholder
        self.last_created = self.runtime

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
            self.__fn(*self.args.args, **self.args.kwargs)
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
        result_artifact = DataArtifact(
            self.resolved_path,
            (
                duckdb.query(
                    f"SELECT * FROM read_json_auto({globs}, hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)"
                )
                if globs
                else None
            ),
        )

        await result_artifact.persist()
        await self.__save_run_metadata()

        return result_artifact

    async def query(self, query_str: str | None = None):
        await self()

        # Set up filesystem abstraction
        fs = await get_fs()
        await register_mad_protocol()

        # # If asset has been created query the file
        if fs.glob(self.resolved_path):
            return self._get_self_query(query_str)

        # TODO: let's throw an error for now, as self() executed and there should be data
        # at the very least we should have an empty file?
        raise ValueError(f"No data found for asset_id: {self.id}")

    def _get_self_query(self, query_str: str | None = None):
        asset_query = duckdb.query(f"SELECT * FROM 'mad://{self.resolved_path}'")
        duckdb.register(f"{self.resolved_name}_{self.id}", asset_query)

        if not query_str:
            return duckdb.query(f"SELECT * FROM {self.resolved_name}_{self.id}")

        directed_string = query_str.replace(
            self.resolved_name, f"{self.resolved_name}_{self.id}"
        )

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
        if self.snapshot_artifacts:
            prefix = (
                f"year={self.runtime.year}/month={self.runtime.month}/day={self.runtime.day}/runtime={self.runtime_str}/"
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
        folder_path = os.path.dirname(self.resolved_path)

        # Set up the base path for artifact storage
        if not self.resolved_artifacts_dir:
            base_path: str = f"{folder_path}/_artifacts/asset={self.name}"
        else:
            base_path: str = self.resolved_artifacts_dir

        return base_path

    def _get_filename(self):
        return os.path.splitext(os.path.basename(self.resolved_path))[0]

    def _resolve_attribute(self, input_str: str | None = None):
        if not input_str or not self.args_dict:
            return input_str

        param_names = [(f"{{{key}}}", key) for key in list(self.args_dict.keys())]
        for key_str, key in param_names:
            param_value = str(self.args_dict[f"{key}"])
            if param_value:
                input_str = input_str.replace(key_str, param_value)

        return input_str

    def _generate_asset_guid(self):
        hash_input = f"{self.resolved_name}:{self.resolved_path}:{self.resolved_artifacts_dir}:{str(self.args)}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def _generate_asset_iteration_guid(self):
        hash_input = f"{self.resolved_name}:{self.resolved_path}:{self.resolved_artifacts_dir}:{self.runtime_str}:{str(self.args)}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    async def __save_run_metadata(self):
        def _handle_unknown_types(data):
            if isinstance(data, DataAsset):
                return {"name": self.name, "fn": self.__fn}

        fs = await get_fs()
        asset_metadata = {
            "asset_run_id": self.run_id,
            "asset_id": self.id,
            "asset_name": self.resolved_name,
            "fn_name": self.fn_name,
            "parameters": json.dumps(self.args_dict, default=_handle_unknown_types),
            "output_path": self.resolved_path,
            "runtime": self.runtime_str,
            "data_written": self.data_written,
            "artifact_glob": self.artifact_glob,
        }

        await fs.write_data(
            f"{ASSET_METADATA_LOCATION}/asset_name={self.resolved_name}/asset_id={self.id}/asset_run_id={self.run_id}/metadata.json",
            asset_metadata,
        )


async def get_asset_metadata():
    fs = await get_fs()
    await register_mad_protocol()
    if fs.glob(f"{ASSET_METADATA_LOCATION}/**/*.json"):
        metadata = duckdb.query(
            f"""
                SELECT * 
                FROM read_json_auto('mad://{ASSET_METADATA_LOCATION}/**/*.json')
                ORDER BY
                    runtime DESC,
                    asset_id ASC
            """
        )
        duckdb.query(
            f"COPY (SELECT * FROM metadata) TO 'mad://{ASSET_METADATA_LOCATION}/metadata_binding.parquet' (use_tmp_file false)"
        )
        return metadata


async def get_data_by_asset_name(asset_name: str):
    await register_mad_protocol()
    metadata = await get_asset_metadata()
    if metadata:
        ranked_asset_query = duckdb.query(
            f"""
            SELECT
                asset_id,
                artifact_glob,
                runtime,
                ROW_NUMBER() OVER(PARTITION BY asset_id ORDER BY runtime DESC) as rn
            FROM 'mad://{ASSET_METADATA_LOCATION}/metadata_binding.parquet'
            WHERE asset_name = '{asset_name}' 
                AND data_written = 'true'
                AND artifact_glob IS NOT NULL
            """
        )
        row_tuples = duckdb.query(
            """
            SELECT 
                artifact_glob 
            FROM ranked_asset_query
            WHERE rn = 1
            """
        ).fetchall()

        artifact_globs = [f"mad://{artifact_glob[0]}" for artifact_glob in row_tuples]
    else:
        artifact_globs = None
        print("No metadata found.")

    if artifact_globs:
        full_data_set = duckdb.query(
            f"SELECT * FROM read_json_auto({artifact_globs}, union_by_name = true, maximum_object_size = 33554432)"
        )
        return full_data_set
    else:
        print(f"No artifact globs found for {asset_name}")

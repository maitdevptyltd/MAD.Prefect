from datetime import datetime, UTC
import hashlib
import inspect
from typing import Callable, cast
import duckdb
import httpx
import pandas
from mad_prefect.filesystems import get_fs
from mad_prefect.duckdb import register_mad_protocol
import re
import os

ASSET_METADATA_LOCATION = os.getenv("ASSET_METADATA_LOCATION", ".asset_metadata")


class DataAssetArtifact:
    def __init__(
        self,
        artifact: object,
        dir: str,
        asset_id: str | None = None,
        asset_run_id: str | None = None,
        asset_name: str | None = None,
        path: str | None = None,
        data_written: bool = False,
    ):
        self.artifact = artifact
        self.dir = dir
        self.asset_id = asset_id
        self.asset_run_id = asset_run_id
        self.asset_name = asset_name
        self.path = path
        self.data_written = data_written
        pass

    def _get_params(self):
        params = (
            dict(self.artifact.request.url.params)
            if isinstance(self.artifact, httpx.Response)
            and self.artifact.request.url.params
            else None
        )
        return params

    def _get_base_path(self):
        return self.dir

    def _generate_artifact_guid(self):
        hash_input = f"{self.asset_id}:{self.asset_run_id}:{self.path}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    async def _write_artifact_metadata(self):
        fs = await get_fs()
        artifact_id = self._generate_artifact_guid()
        artifact_metadata = {
            "artifact_id": artifact_id,
            "asset_run_id": self.asset_run_id,
            "asset_id": self.asset_id,
            "artifact_path": self.path,
            "artifact_written": self.data_written,
        }

        await fs.write_data(
            f"{ASSET_METADATA_LOCATION}/asset_name={self.asset_name}/asset_id={self.asset_id}/asset_run_id={self.asset_run_id}/_artifacts/artifact_id={artifact_id}/metadata.json",
            artifact_metadata,
        )

    async def _run_artifact(
        self, asset_id: str, asset_run_id: str, asset_name: str, artifact_path: str
    ):
        self.asset_id = asset_id
        self.asset_run_id = asset_run_id
        self.asset_name = asset_name
        self.path = artifact_path

        artifact_id = self._generate_artifact_guid
        await self._write_artifact_metadata()

        if _output_has_data(self.artifact):
            try:
                await _write_asset_data(self.path, self.artifact)
                self.data_written
            except:
                raise ValueError(
                    f"Artifact write operation failed for artifact_id: {artifact_id}"
                )

        await self._write_artifact_metadata()


class DataAsset:

    def __init__(
        self,
        fn: Callable,
        path: str,
        artifacts_dir: str | None = "",
        name: str | None = None,
        snapshot_artifacts: bool = True,
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
        asset = DataAsset(self.__fn, self.path, self.artifacts_dir, self.name)
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
        self.resolved_path = self._resolve_attribute(self.path)
        self.resolved_artifacts_dir = self._resolve_attribute(self.artifacts_dir)
        self.resolved_name = self._resolve_attribute(self.name)

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

        # TODO: in future set up caching that reads from path
        # Instead of running self.__fn if data
        # Has been created within cache period

        # Handle the fn differently depending on whether or not it yields
        is_generator_fn = inspect.isasyncgenfunction(
            self.__fn
        ) or inspect.isgeneratorfunction(self.__fn)

        if is_generator_fn:
            await self._handle_yield(*self.args.args, **self.args.kwargs)
        else:
            await self._handle_return(*self.args.args, **self.args.kwargs)

        # Write metadata
        await self.__register_asset()

    async def query(self, query_str: str | None = None):
        await self()

        # Set up filesystem abstraction
        fs = await get_fs()
        await register_mad_protocol()

        # # If asset has been created query the file
        if fs.glob(self.resolved_path):
            asset_query = duckdb.query(f"SELECT * FROM 'mad://{self.resolved_path}'")
            duckdb.register(f"{self.resolved_name}_{self.id}", asset_query)

            if not query_str:
                return duckdb.query(f"SELECT * FROM {self.resolved_name}_{self.id}")
            else:
                directed_string = query_str.replace(
                    self.resolved_name, f"{self.resolved_name}_{self.id}"
                )

            return duckdb.query(directed_string)
        else:
            return None

    async def _handle_yield(self, *args, **kwargs):
        artifact_glob = await self._create_yield_artifacts(*args, **kwargs)
        await self._create_yield_output(artifact_glob)

    async def _create_yield_artifacts(self, *args, **kwargs):
        # Set up starting artifact fragment_number
        fragment_number = 1
        artifact_glob: list[str] = []

        async for output in self.__fn(*args, **kwargs):
            artifact = await self._handle_artifact(output, fragment_number)

            if artifact.data_written:
                print(
                    f"An artifact for asset_id: {self.id} was written to path: \n {artifact.path}\n\n   "
                )

            # Update fragment_number if data is written & it is used in path
            if artifact.data_written and "fragment=" in str(artifact.path):
                fragment_number = +1

            artifact_glob.append(f"mad://{artifact.path}")

        self.artifact_glob = artifact_glob
        return artifact_glob

    async def _create_yield_output(self, artifact_glob: str):
        # Set up filesystem abstraction
        fs = await get_fs()
        await register_mad_protocol()

        if fs.glob(artifact_glob):
            duckdb.query("SET temp_directory = './.tmp/duckdb/'")
            folder_path = os.path.dirname(self.resolved_path)
            fs.mkdirs(folder_path, exist_ok=True)
            duckdb.query(
                f"""
                    COPY (
                    SELECT *
                    FROM read_json_auto({artifact_glob}, hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)
                    ) TO 'mad://{self.resolved_path}' (use_tmp_file false)

                """
            )
            self.data_written = True

        else:
            print(
                f"No artifacts have been found for asset_id: {self.id}\n When using artifact glob: {artifact_glob}\n While attempting to write to {self.resolved_path}"
            )

    async def _handle_return(self, *args, **kwargs):
        # Call function to recieve output
        output = await self.__fn(*args, **kwargs)

        # If nothing is return do not perform write operation
        if _output_has_data(output):
            # Write output file to provided path
            await _write_asset_data(self.resolved_path, output)

            # TODO: Normalize artifact write process to align with _handle_yield
            # Write raw json file to appropriate location
            artifact = await self._handle_artifact(output)
            if artifact.data_written:
                self.data_written = True
                self.artifact_glob = artifact.path

    async def _handle_artifact(
        self, output: object, fragment_number: int | None = None
    ):
        if isinstance(output, DataAssetArtifact):
            # If the output is already a DataAssetArtifact
            # use the input artifact_dir as base_path & httpx.Response params as params
            artifact = output
            params = output._get_params()
            base_path = output._get_base_path()
        else:
            # If the output isn't a DataAssetArtifact manually set the params & base_path
            # and initialize the output as a DataAssetArtifact
            params = (
                dict(output.request.url.params)
                if isinstance(output, httpx.Response) and output.request.url.params
                else None
            )
            base_path = self._get_artifact_base_path()
            artifact = DataAssetArtifact(artifact=output, dir=base_path)

        path = self._build_artifact_path(base_path, params, fragment_number)

        # This function will record artifact metadata and write the artifact to path
        await artifact._run_artifact(
            asset_id=self.id,
            asset_run_id=self.run_id,
            asset_name=self.name,
            artifact_path=path,
        )

        return artifact

    def _build_artifact_path(
        self,
        base_path: str,
        params: dict | None = None,
        fragment_number: int | None = None,
    ):
        asset_runtime_str = (
            f"runtime={self.runtime_str}/" if self.snapshot_artifacts else ""
        )

        if params is None and fragment_number is None:
            filename = self._get_filename()
            return f"{base_path}/{asset_runtime_str}{filename}.json"

        if params is None:
            return f"{base_path}/{asset_runtime_str}fragment={fragment_number}.json"

        params_path = "/".join(f"{key}={value}" for key, value in params.items())

        return f"{base_path}/{asset_runtime_str}{params_path}.json"

    def _get_artifact_base_path(self):
        # Extract folder path for folder set up
        folder_path = os.path.dirname(self.resolved_path)

        # Set up the base path for artifact storage
        if not self.resolved_artifacts_dir:
            base_path: str = f"{folder_path}/_artifacts"
        else:
            base_path: str = self.resolved_artifacts_dir

        return base_path

    def _get_filename(self):
        return os.path.splitext(os.path.basename(self.resolved_path))[0]

    def _resolve_attribute(self, input_str: str = ""):
        if not input_str:
            return input_str
        if not self.args_dict:
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

    async def __register_asset(self):
        fs = await get_fs()
        asset_metadata = {
            "asset_run_id": self.run_id,
            "asset_id": self.id,
            "asset_name": self.resolved_name,
            "fn_name": self.fn_name,
            "parameters": self.args_dict,
            "output_path": self.resolved_path,
            "runtime": self.runtime_str,
            "data_written": self.data_written,
            "artifact_glob": self.artifact_glob,
        }

        await fs.write_data(
            f"{ASSET_METADATA_LOCATION}/asset_name={self.resolved_name}/asset_id={self.id}/asset_run_id={self.run_id}/metadata.json",
            asset_metadata,
        )

    def __getstate__(self):
        pass

    def __setstate__(self, query: str):
        pass


def _output_has_data(data: object):
    if isinstance(data, httpx.Response):
        # TODO: Create checking mechanism for raw text responses
        check_result = True if data.json() else False

    elif isinstance(data, duckdb.DuckDBPyRelation):
        check_result = True if len(data.df()) else False

    elif isinstance(data, pandas.DataFrame):
        check_result = True if len(data) else False

    else:
        check_result = True if data else False

    return check_result


async def _write_asset_data(path: str, data: object):
    fs = await get_fs()
    await register_mad_protocol()

    if isinstance(data, (duckdb.DuckDBPyRelation, pandas.DataFrame)):
        # Before using COPY TO statement ensure directory exists
        folder_path = os.path.dirname(path)
        fs.mkdirs(folder_path, exist_ok=True)

        json_format = "FORMAT JSON, ARRAY true," if ".json" in path else ""
        duckdb.query("SET temp_directory = './.tmp/duckdb/'")
        duckdb.query(
            f"""
                COPY(
                    SELECT * FROM data
                ) TO 'mad://{path}' ({json_format} use_tmp_file false)
            """
        )
    elif isinstance(data, httpx.Response):
        # TODO: Find way to process raw text responses
        await fs.write_data(path, data.json())
    else:
        await fs.write_data(path, data)


async def get_asset_metadata():
    fs = await get_fs()
    await register_mad_protocol()
    ASSET_METADATA_LOCATION = os.getenv("ASSET_METADATA_LOCATION", ".asset_metadata")
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
        ASSET_METADATA_LOCATION = os.getenv(
            "ASSET_METADATA_LOCATION", ".asset_metadata"
        )
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

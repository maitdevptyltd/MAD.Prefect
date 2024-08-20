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


class DataAsset:
    ASSET_METADATA_LOCATION = os.getenv("ASSET_METADATA_LOCATION", ".asset_metadata")

    def __init__(
        self,
        fn: Callable,
        path: str,
        artifacts_dir: str | None = "",
        name: str | None = None,
    ):
        self.__fn = fn
        self.path = path
        self.artifacts_dir = artifacts_dir
        self.name = name if name else fn.__name__
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
        artifact_glob_pattern = await self._create_yield_artifacts(*args, **kwargs)
        await self._create_yield_output(artifact_glob_pattern)

    async def _create_yield_artifacts(self, *args, **kwargs):
        # Set up starting artifact fragment_number
        fragment_number = 1

        # Extract artifact base path
        base_path = await self._get_artifact_base_path()

        async for output in self.__fn(*args, **kwargs):
            if self._output_has_data(output):

                params = (
                    dict(output.request.url.params)
                    if isinstance(output, httpx.Response) and output.request.url.params
                    else None
                )

                path = self._build_artifact_path(base_path, params, fragment_number)
                await self._write_operation(path, output)

                print(
                    f"An artifact for asset_id: {self.id} was written to path: \n {path}\n\n   "
                )

                # Update fragment_number if used in path
                fragment_number = (
                    (fragment_number + 1) if "fragment=" in path else fragment_number
                )

        artifact_glob_pattern = (
            f"{base_path}/_artifacts/runtime={self.runtime_str}/**/*.json"
        )
        self.artifact_glob = artifact_glob_pattern
        return artifact_glob_pattern

    async def _create_yield_output(self, glob_pattern: str):
        # Set up filesystem abstraction
        fs = await get_fs()
        await register_mad_protocol()

        if fs.glob(glob_pattern):
            duckdb.query("SET temp_directory = './.tmp/duckdb/'")
            folder_path = await self._get_folder_path(self.resolved_path)
            fs.mkdirs(folder_path, exist_ok=True)
            duckdb.query(
                f"""
                    COPY (
                    SELECT *
                    FROM read_json_auto('mad://{glob_pattern}', hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)
                    ) TO 'mad://{self.resolved_path}' (use_tmp_file false)

                """
            )
            self.data_written = True

        else:
            print(
                f"No artifacts have been found for asset_id: {self.id}\n When using glob pattern: {glob_pattern}\n While attempting to write to {self.resolved_path}"
            )

    async def _handle_return(self, *args, **kwargs):
        # Call function to recieve output
        output = await self.__fn(*args, **kwargs)

        # If nothing is return do not perform write operation
        if self._output_has_data(output):
            # Write output file to provided path
            await self._write_operation(self.resolved_path, output)

            # Write raw json file to appropriate location
            if ".json" not in self.resolved_path:
                base_path = await self._get_artifact_base_path()
                file_name = await self._get_file_name()
                artifact_path = (
                    f"{base_path}/_artifact/runtime={self.runtime_str}/{file_name}.json"
                )
                await self._write_operation(artifact_path, output)

            self.data_written = True

    def _build_artifact_path(
        self,
        base_path: str,
        params: dict | None = None,
        fragment_number: int | None = None,
    ):

        if params is None:
            return f"{base_path}/_artifacts/runtime={self.runtime_str}/fragment={fragment_number}.json"

        params_path = "/".join(f"{key}={value}" for key, value in params.items())

        return f"{base_path}/_artifacts/runtime={self.runtime_str}/{params_path}.json"

    async def _get_artifact_base_path(self):
        # Extract folder path for folder set up
        folder_path = await self._get_folder_path(self.resolved_path)

        # Set up the base path for artifact storage
        if not self.resolved_artifacts_dir:
            base_path = folder_path
        else:
            base_path = self.resolved_artifacts_dir

        return base_path

    async def _write_operation(self, path: str, data: object):
        fs = await get_fs()
        await register_mad_protocol()

        if isinstance(data, (duckdb.DuckDBPyRelation, pandas.DataFrame)):
            # Before using COPY TO statement ensure directory exists
            folder_path = await self._get_folder_path(path)
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

    async def _get_folder_path(self, path: str):
        fs = await get_fs()
        folder_path = re.sub(r"[^/\\]+$", "", path)

        return folder_path

    async def _get_file_name(self):
        return re.search(r"([^\\/]+)(?=\.[^\\/\.]+$)", self.resolved_path).group(1)

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

    def _output_has_data(self, data: object):
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
            f"{self.ASSET_METADATA_LOCATION}/asset_name={self.resolved_name}/asset_id={self.id}/{self.run_id}.json",
            asset_metadata,
        )

    def __getstate__(self):
        pass

    def __setstate__(self, query: str):
        pass


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
    asset_meta_query = await get_asset_metadata()
    print(asset_meta_query)
    ranked_asset_query = duckdb.query(
        f"""
        SELECT
            asset_id,
            artifact_glob,
            runtime,
            ROW_NUMBER() OVER(PARTITION BY asset_id ORDER BY runtime DESC) as rn
        FROM asset_meta_query
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

    full_data_set = duckdb.query(
        f"SELECT * FROM read_json_auto({artifact_globs}, union_by_name = true, maximum_object_size = 33554432)"
    )
    return full_data_set

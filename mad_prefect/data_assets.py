from datetime import datetime
import inspect
from typing import Callable
import duckdb
import httpx
import pandas
from mad_prefect.filesystems import get_fs
from mad_prefect.duckdb import register_mad_protocol
import re
import mad_prefect.filesystems

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests/sample_data"


class DataAsset:
    def __init__(self, fn: Callable, path: str, artifacts_dir: str | None = None):
        self.__fn = fn
        self.path = path
        self.artifacts_dir = artifacts_dir
        self.name = fn.__name__
        self.last_refreshed = None

        pass

    async def __call__(self, *args, **kwargs):

        if inspect.isasyncgen(self.__fn(*args, **kwargs)):
            await self._handle_yield(*args, **kwargs)
        else:
            await self._handle_return(*args, **kwargs)
        self.last_refreshed = datetime.utcnow()

    async def _handle_yield(self, *args, **kwargs):
        # Set up filesystem abstraction
        fs = await get_fs()
        await register_mad_protocol()

        # Set up starting fragment_number
        fragment_number = 1

        # Extract root path for folder set up
        root_path = await self.get_root_path(self.path)

        # Set up the base path for artifact storage
        if not self.artifacts_dir:
            base_path = root_path
        else:
            base_path = self.artifacts_dir

        async for output in self.__fn(*args, **kwargs):
            if isinstance(output, httpx.Response):

                # If API response has params use these in filepath for hive partitioning
                if output.request.url.params:
                    params_path = self._build_params_path(
                        dict(output.request.url.params)
                    )
                    path = f"{base_path}/_artifacts/{params_path}.json"

                # If there aren't params use fragment number for hive partitioning
                else:
                    path = f"{base_path}/_artifacts/fragment={fragment_number}.json"

            # If output is not httpx.Response use fragment number for hive partitioning
            else:
                path = f"{base_path}/_artifacts/fragment={fragment_number}.json"

            await self._write_operation(path, output)

            # Update fragment_number if used in path
            fragment_number = (
                (fragment_number + 1) if "fragment=" in path else fragment_number
            )

        duckdb.query("SET temp_directory = './.tmp/duckdb/'")
        duckdb.query(
            f"""
                COPY (
                SELECT *
                FROM read_json_auto('mad://{base_path}/_artifacts/**/*.json', hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)
                ) TO 'mad://{self.path}' (use_tmp_file false)

            """
        )

    async def _handle_return(self, *args, **kwargs):
        output = await self.__fn(*args, **kwargs)

        # TODO: insert logic for artifact/raw data write
        await self._write_operation(self.path, output)

    def _build_params_path(
        self,
        params: dict | None = None,
    ):

        params_path = (
            "/".join(f"{key}={value}" for key, value in params.items())
            if params
            else ""
        )
        return params_path

    def _build_artifact_path(
        self,
        base_path: str,
        params: dict | None = None,
        fragment_number: int | None = None,
    ):
        params_path = (
            "/".join(f"{key}={value}" for key, value in params.items())
            if params
            else ""
        )

        if params_path:
            path = f"{base_path}/_artifacts/{params_path}.json"
        else:
            path = f"{base_path}/_artifacts/fragment={fragment_number}.json"

        return path

    async def _write_operation(self, path, data):
        fs = await get_fs()
        await register_mad_protocol()
        await self.get_root_path(path)

        if isinstance(data, (duckdb.DuckDBPyRelation, pandas.DataFrame)):
            duckdb.query("SET temp_directory = './.tmp/duckdb/'")
            duckdb.query(
                f"""
                    COPY(
                        SELECT * FROM data
                    ) TO 'mad://{path}' (use_tmp_file false)
                """
            )
        elif isinstance(data, httpx.Response):
            output = data.json() if data.json() else data.text
            await fs.write_data(path, output)
        else:
            await fs.write_data(path, data)

    async def query(self, query_str: str | None = None):
        # TODO: query shouldn't __call__ every time to materialize the asset for querying, in the future develop
        # a way to rematerialize an asset only if its required (doesn't exist) or has expired.
        await self()

        await register_mad_protocol()

        asset_query = duckdb.query(f"SELECT * FROM 'mad://{self.path}'")
        duckdb.register("asset", asset_query)

        if not query_str:
            return duckdb.query("SELECT * FROM asset")

        return duckdb.query(query_str)

    async def get_root_path(self, path):
        fs = await get_fs()
        root_path = re.sub(r"[^/\\]+$", "", path)
        fs.mkdirs(root_path, exist_ok=True)

        return root_path

    def __getstate__(self):
        pass

    def __setstate__(self, query: str):
        pass


def asset(path: str, artifacts_dir: str | None = None):
    def decorator(fn: Callable):
        return DataAsset(fn, path, artifacts_dir)

    return decorator

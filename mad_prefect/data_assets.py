from datetime import datetime
import inspect
from typing import Callable
import duckdb
import httpx
from mad_prefect.filesystems import get_fs
from mad_prefect.duckdb import register_mad_protocol, write_duckdb


class DataAsset:
    def __init__(self, fn: Callable, path: str):
        self.__fn = fn
        self.path = path
        self.name = fn.__name__
        self.last_refreshed = None

        pass

    async def __call__(self, *args, **kwargs):
        # Call the fn to materialize the asset
        result = await self.__fn(*args, **kwargs)
        self.last_refreshed = datetime.utcnow()

        if inspect.isasyncgen(result):
            self._handle_yield(result)
        else:
            self._handle_return(result)

    async def _handle_yield(self, generator):
        fs = await get_fs()
        await register_mad_protocol
        fragment_number = 1

        async for output in generator:
            if isinstance(output, httpx.Response):
                params = dict(output.request.url.params)
                path = self._build_path(params)
                data = output.json() if output.json() else output.text
                await fs.write_data(path, data)
            else:
                path = f"{self.path}/_artifacts/fragment{fragment_number}.json"
                await fs.write_data(path, output)
                fragment_number += 1

        duckdb.query("SET temp_directory = './.tmp/duckdb/'")
        duckdb.query(
            f"""
                COPY (
                SELECT *
                FROM read_json_auto('mad://{self.path}/_artifacts/**/*.json', union_by_name = true, maximum_object_size = 33554432)
                ) TO 'mad://{self.path}' (use_tmp_file false)

            """
        )

    async def _handle_return(self, output):
        fs = await get_fs()
        await register_mad_protocol
        if isinstance(output, httpx.Response):
            params = dict(output.request.url.params)
            path = self._build_path(params)
            data = output.json() if output.json() else output.text
            await fs.write_data(path, data)
        # TODO: write code for DuckDB Relation outputs
        else:
            await fs.write_data(self.path, output)

    def _build_path(self, params: dict | None = None):
        params_path = (
            "/".join(f"{key}={value}" for key, value in params.items())
            if params
            else ""
        )
        if params_path:
            # TODO: need to remove file name from path here
            path = f"{self.path}/_artifacts/{params_path}.json"

        return path

    async def query(self):
        # TODO: query shouldn't __call__ every time to materialize the asset for querying, in the future develop
        # a way to rematerialize an asset only if its required (doesn't exist) or has expired.
        await self()

        await register_mad_protocol()

        return duckdb.query(f"SELECT * FROM 'mad://{self.path}'")

    def __getstate__(self):
        pass

    def __setstate__(self, query: str):
        pass


def asset(path: str):
    def decorator(fn: Callable):
        return DataAsset(fn, path)

    return decorator

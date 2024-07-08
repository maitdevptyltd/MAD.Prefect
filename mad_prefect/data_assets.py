from datetime import datetime
import inspect
from typing import Callable
import duckdb
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
            chunk_number = 1
            async for chunk in result:
                await self._write_chunk(chunk, chunk_number)
                chunk_number += 1
                yield chunk
            await self._process_chunks()
        else:
            await self._write_output(result)
            return result

    async def _write_output(self, data):
        fs = await get_fs()
        await register_mad_protocol()
        if isinstance(data, (duckdb.DuckDBPyConnection, duckdb.DuckDBPyRelation)):
            await write_duckdb(self.path, data)
        else:
            await fs.write_data(self.path, data)

    async def _write_chunk(self, chunk, chunk_number):
        fs = await get_fs()
        await register_mad_protocol()

        if isinstance(chunk, (duckdb.DuckDBPyConnection, duckdb.DuckDBPyRelation)):
            await write_duckdb(f"{self.path}/artifacts/{chunk_number}.parquet", chunk)
        else:
            await fs.write_data(f"{self.path}/artifacts/{chunk_number}.parquet", chunk)

    async def _process_chunks(self):
        fs = await get_fs()
        await register_mad_protocol()
        paths = fs.glob(f"{self.path}/artifacts/**/*.parquet")
        first_chunk = True

        for path in paths:
            chunk = duckdb.query(f"SELECT * FROM 'mad://{path}'")
            if first_chunk:
                duckdb.query(f"COPY chunk TO '{self.path}' (FORMAT PARQUET)")
            else:
                duckdb.query(
                    f"COPY chunk TO '{self.path}' (FORMAT PARQUET, APPEND TRUE)"
                )
            first_chunk = False
            yield chunk

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

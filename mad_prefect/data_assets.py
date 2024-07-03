from typing import Callable
import duckdb
from mad_prefect.filesystems import get_fs
from mad_prefect.duckdb import register_mad_protocol


class DataAsset:
    def __init__(self, fn: Callable, path: str):
        self.__fn = fn
        self.path = path
        pass

    async def __call__(self, *args, **kwargs):
        # Call the fn to materialize the asset
        result = await self.__fn(*args, **kwargs)

        # Write the result to the filesystem at path
        fs = await get_fs()
        await fs.write_data(self.path, result)

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

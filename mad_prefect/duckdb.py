from typing import cast
import duckdb
import fsspec
from mad_prefect.filesystems import get_fs
from fsspec.implementations.dirfs import DirFileSystem


class MadFileSystem(DirFileSystem):
    protocol = "mad"

    def __init__(self, basepath: str, storage_options: dict | None = None, **kwargs):
        options = storage_options or kwargs
        fs, fs_url = cast(
            tuple[fsspec.AbstractFileSystem, str],
            fsspec.core.url_to_fs(basepath, **options),
        )

        super().__init__(path=fs_url.rstrip("/"), fs=fs)


async def register_mad_protocol(connection: duckdb.DuckDBPyConnection | None = None):
    if connection and connection.filesystem_is_registered("mad"):
        return
    elif not connection and duckdb.filesystem_is_registered("mad"):
        return

    fs = await get_fs()
    mad_fs = MadFileSystem(fs.basepath, fs.storage_options.get_secret_value())

    if connection:
        connection.register_filesystem(cast(str, mad_fs))
    else:
        duckdb.register_filesystem(cast(str, mad_fs))

from typing import cast
import duckdb
import fsspec
from mad_prefect.filesystems import get_fs
from fsspec.implementations.dirfs import DirFileSystem
import weakref

_global_registered_filesystem_ids: set[int] = set()
_connection_registered_filesystems: "weakref.WeakKeyDictionary[duckdb.DuckDBPyConnection, set[int]]" = (
    weakref.WeakKeyDictionary()
)


def register_fsspec_filesystem(
    filesystem: fsspec.AbstractFileSystem,
    connection: duckdb.DuckDBPyConnection | None = None,
) -> None:
    """Register a fsspec filesystem with DuckDB only once per process/connection."""

    filesystem_id = id(filesystem)

    if connection:
        registered = _connection_registered_filesystems.setdefault(connection, set())

        if filesystem_id in registered:
            return

        connection.register_filesystem(filesystem)
        registered.add(filesystem_id)
        return

    if filesystem_id in _global_registered_filesystem_ids:
        return

    duckdb.register_filesystem(filesystem)
    _global_registered_filesystem_ids.add(filesystem_id)


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
        register_fsspec_filesystem(mad_fs, connection)
    else:
        register_fsspec_filesystem(mad_fs)

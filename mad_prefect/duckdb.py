from typing import cast
import duckdb
import fsspec
from mad_prefect.filesystems import FILESYSTEM_URL, get_fs
import warnings


class MadFileSystem(fsspec.AbstractFileSystem):
    protocol = "mad"

    def __init__(self, basepath: str, storage_options: dict | None = None, **kwargs):
        options = storage_options or kwargs
        fs, fs_url = fsspec.core.url_to_fs(basepath, **options)

        self._fs: fsspec.AbstractFileSystem = fs
        self._fs_url: str = fs_url

        super().__init__(**options)

    def glob(self, path: str, **kwargs):
        return self._fs.glob(self.fix_path(path), **kwargs)

    def info(self, path: str, **kwargs):
        return self._fs.info(self.fix_path(path), **kwargs)

    def _open(self, path: str, **kwargs):
        return self._fs._open(self.fix_path(path), **kwargs)

    def rm(self, path: str, **kwargs):
        return self._fs.rm(self.fix_path(path), **kwargs)

    def mv(self, path1: str, path2: str, **kwargs):
        return self._fs.mv(self.fix_path(path1), self.fix_path(path2), **kwargs)

    def fix_path(self, path: str):
        # Remove protocol from the path if it exists
        if path.startswith(f"{self.protocol}://"):
            path = path[len(f"{self.protocol}://") :]

        # If path is incomplete, prefix the path with the fs_url
        if not path.startswith(self._fs_url):
            path = f"{self._fs_url.rstrip('/')}/{path}"

        return path


def register_mad_filesystem(connection: duckdb.DuckDBPyConnection | None = None):
    warnings.warn(
        "register_mad_filesystem is deprecated. Use register_mad_protocol instead.",
        DeprecationWarning,
    )
    fs = MadFileSystem(FILESYSTEM_URL)

    if connection:
        connection.register_filesystem(cast(str, fs))
    else:
        duckdb.register_filesystem(cast(str, fs))


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

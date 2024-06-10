from typing import Any, cast
import duckdb
import fsspec
from mad_prefect.filesystems import FILESYSTEM_URL


class MadFileSystem(fsspec.AbstractFileSystem):
    protocol = "mad"

    def __init__(self, basepath: str, storage_options=None, **kwargs):
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
            path = f"{self._fs_url}/{path}"

        return path


def register_mad_filesystem(connection: duckdb.DuckDBPyConnection | None = None):
    fs = MadFileSystem(FILESYSTEM_URL)

    if connection:
        connection.register_filesystem(fs)
    else:
        duckdb.register_filesystem(fs)

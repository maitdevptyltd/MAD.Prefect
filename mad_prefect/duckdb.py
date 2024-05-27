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


if __name__ == "__main__":
    # Override the filesystem url for testing purposes
    FILESYSTEM_URL = "file://./tests"
    register_mad_filesystem()

    # Execute the SQL query to select all data from the specified file
    test = duckdb.sql("SELECT * FROM 'mad://sample1.parquet'")

    # Assert that the query result is not empty
    assert test

    # Execute a query to count the number of rows in the 'column0'
    count_result = duckdb.sql(
        "SELECT COUNT(*) AS count FROM 'mad://sample1.parquet'"
    ).fetchone()

    assert count_result

    # Extract the count value from the result
    count_value = count_result[0]

    # Assert the count value (or use it as needed)
    assert count_value
    print("Count of column0:", count_value)

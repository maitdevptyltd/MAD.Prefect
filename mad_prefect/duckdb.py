from typing import Any, cast
import duckdb
import fsspec
from mad_prefect.filesystems import FILESYSTEM_URL, get_fs
import warnings


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
    warnings.warn(
        "register_mad_filesystem is deprecated. Use register_mad_protocol instead.",
        DeprecationWarning,
    )
    fs = MadFileSystem(FILESYSTEM_URL)

    if connection:
        connection.register_filesystem(fs)
    else:
        duckdb.register_filesystem(fs)


async def register_mad_protocol(connection: duckdb.DuckDBPyConnection | None = None):
    fs = await get_fs()
    mad_fs = MadFileSystem(fs.basepath, fs.storage_options)

    if connection:
        connection.register_filesystem(mad_fs)
    else:
        duckdb.register_filesystem(mad_fs)


def register_query(query: str | duckdb.DuckDBPyRelation) -> duckdb.DuckDBPyRelation:
    """
    Registers a query or DuckDBPyRelation as a temporary table "tmp_tbl" in DuckDB.

    Addresses the transient nature of DuckDBPyRelation objects by storing the input
    in a temporary table, allowing persistence between function calls.

    Args:
        query: SQL query string or DuckDBPyRelation object to register.

    Returns:
        duckdb.DuckDBPyRelation: Represents the queried temporary table.

    Note:
        Overwrites any existing "tmp_tbl" on each call.
    """
    if isinstance(query, str):
        query = duckdb.query(query)

    query.create("tmp_tbl")

    output = duckdb.query("SELECT * FROM tmp_tbl")

    return output


def get_tables(connection: duckdb.DuckDBPyConnection):
    return [table[0] for table in (connection.query("SHOW TABLES").fetchall())]


async def write_duckdb(
    path: str,
    duckdb_data: duckdb.DuckDBPyConnection | duckdb.DuckDBPyRelation,
    source_table: str | None = None,
):

    # Register filesystem
    fs = await get_fs()
    await register_mad_protocol()

    # If virtual table is passed with no source_table use input as source data
    if isinstance(duckdb_data, duckdb.DuckDBPyRelation) and source_table is None:
        query = duckdb_data

    # If DuckDBPyConnection is passed query tables for source data
    elif isinstance(duckdb_data, duckdb.DuckDBPyConnection):
        # If no source table is provided set to default
        if not source_table:
            source_table = "tmp_tbl"

        tables = get_tables(duckdb_data)
        if source_table not in tables:
            raise ValueError(
                "Selected source_table does not exist in current DuckDBPyConnection"
            )

        query = duckdb.query(f"SELECT * FROM {source_table}")

    # If DuckDBPyRelation is passed with a separate source table - raise error
    else:
        raise ValueError(
            "To access source_table ensure valid DuckDBPyConnection is provided"
        )

    # Ensure destination path exists (excluding file_name)
    fs.mkdirs(path.rsplit("/", 1)[0], exist_ok=True)

    # Write to path
    query.to_parquet(f"mad://{path}")
    duckdb.sql

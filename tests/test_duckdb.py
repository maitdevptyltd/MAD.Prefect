import duckdb
import pytest

from mad_prefect import filesystems as mad_filesystems
from mad_prefect.duckdb import register_mad_protocol


@pytest.fixture()
async def sample_parquet(tmp_path, monkeypatch):
    """Provide a fresh MAD filesystem populated with sample1.parquet."""
    original_fs = mad_filesystems._get_fs_result
    original_url = mad_filesystems.FILESYSTEM_URL
    original_block_name = mad_filesystems.FILESYSTEM_BLOCK_NAME

    mad_filesystems.FILESYSTEM_URL = f"file://{tmp_path}"
    mad_filesystems.FILESYSTEM_BLOCK_NAME = None
    monkeypatch.setenv("FILESYSTEM_URL", mad_filesystems.FILESYSTEM_URL)
    monkeypatch.delenv("FILESYSTEM_BLOCK_NAME", raising=False)
    mad_filesystems._get_fs_result = None

    if duckdb.filesystem_is_registered("mad"):
        duckdb.unregister_filesystem("mad")

    await register_mad_protocol()

    sample_path = tmp_path / "sample1.parquet"
    if sample_path.exists():
        sample_path.unlink()

    duckdb.sql(
        """
        COPY (
            SELECT *
            FROM (VALUES (1), (2), (3)) AS data(column0)
        ) TO 'mad://sample1.parquet' (FORMAT PARQUET)
        """
    )

    yield tmp_path

    if duckdb.filesystem_is_registered("mad"):
        duckdb.unregister_filesystem("mad")

    mad_filesystems._get_fs_result = original_fs
    mad_filesystems.FILESYSTEM_URL = original_url
    mad_filesystems.FILESYSTEM_BLOCK_NAME = original_block_name


async def test_mad_filesystem_queries_file(sample_parquet):
    await register_mad_protocol()

    result_relation = duckdb.sql("SELECT * FROM 'mad://sample1.parquet'")
    rows = result_relation.fetchall()
    assert rows

    count_row = duckdb.sql(
        "SELECT COUNT(*) AS count FROM 'mad://sample1.parquet'"
    ).fetchone()
    assert count_row is not None
    count_value = count_row[0]

    assert count_value == 3


async def test_overwriting_existing_file(sample_parquet):
    await register_mad_protocol()

    duckdb.sql(
        """
        COPY (
            SELECT * FROM 'mad://sample1.parquet'
        ) TO 'mad://sample2.parquet' (FORMAT PARQUET)
        """
    )

    copied_row = duckdb.sql(
        "SELECT COUNT(*) AS count FROM 'mad://sample2.parquet'"
    ).fetchone()
    assert copied_row is not None
    copied_count = copied_row[0]

    assert copied_count == 3

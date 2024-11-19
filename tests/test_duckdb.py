import asyncio
import duckdb
import mad_prefect.filesystems
import pytest
import duckdb
import json

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests"
from mad_prefect.duckdb import register_mad_protocol


async def test_mad_filesystem_queries_file():
    await register_mad_protocol()

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


async def test_overwriting_existing_file():
    await register_mad_protocol()

    sample1 = duckdb.query("SELECT * FROM 'mad://sample1.parquet'")

    # Overwrite sample2
    duckdb.execute("COPY sample1 TO 'mad://sample2.parquet' (FORMAT PARQUET)")

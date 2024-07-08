import asyncio
import duckdb
import mad_prefect.filesystems
import pytest
import duckdb
import json

from mad_prefect.duckdb import register_query
from tests.sample_data.data_gen import get_api

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests"
from mad_prefect.duckdb import register_mad_filesystem, register_mad_protocol


def test_mad_filesystem_queries_file():
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


def test_overwriting_existing_file():
    register_mad_filesystem()

    sample1 = duckdb.query("SELECT * FROM 'mad://sample1.parquet'")

    # Overwrite sample2
    duckdb.execute("COPY sample1 TO 'mad://sample2.parquet' (FORMAT PARQUET)")


async def test_register_query():
    # read sample data
    con = await register_mad_protocol()
    query = duckdb.query("SELECT * FROM 'mad://sample_data/raw/organisations.json'")
    input_data = query.fetchall()
    # Register the query
    result = register_query(query)

    # Verify the result
    assert isinstance(result, duckdb.DuckDBPyRelation)

    # Check if the result contains data
    output_data = result.fetchall()
    assert len(output_data) > 0

    # Verify that the number of rows matches our input
    assert len(output_data) == len(input_data)

    # Optionally, verify some of the content
    print(result.columns)
    assert (
        "organisations" in result.columns
    )  # Assuming 'name' is a field in your organisation data

    tables = con.query("SHOW TABLES")


if __name__ == "__main__":
    asyncio.run(test_register_query())

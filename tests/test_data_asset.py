import asyncio
from datetime import datetime
import duckdb

from mad_prefect.data_assets import asset
import mad_prefect.data_assets
from mad_prefect.duckdb import register_mad_protocol
from tests.sample_data.mock_api import get_api, ingest_endpoint
from mad_prefect.filesystems import get_fs
import mad_prefect.filesystems
import pandas as pd
import os
import pytest

# Set up pytest
pytest.register_assert_rewrite("pytest_asyncio")
pytestmark = pytest.mark.asyncio


# Set session timestamp as path prefix for test files
@pytest.fixture(scope="session", autouse=True)
def settest_filesystem_url():
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    base_url = os.getenv("FILESYSTEM_URL", "file://./.tmp/storage")
    mad_prefect.filesystems.FILESYSTEM_URL = f"{base_url}/test_{timestamp}"
    mad_prefect.data_assets.FILESYSTEM_URL = f"{base_url}/test_{timestamp}"


## FIXTURES ##


# Fixture 1
# Purpose: Test writing data from basic return function with json output
# Function Name: asset_bronze_organisations_return
# Output Method: Return
# Data Type: json
# Params Used: N/A - json
# Artifact Storage: Default
@asset(f"fixture_1/bronze/orgs_returned/organisations_return.parquet")
async def asset_bronze_organisations_return():
    data = await get_api("organisations", {"limit": 3})
    return data["organisations"]


@pytest.fixture(scope="session")
def fixture_1():
    asyncio.run(asset_bronze_organisations_return())


# Fixture 2
# Purpose: Test writing data with yield, to params based hive partition
# Function Name: asset_bronze_buildings_yielded_response
# Output Method: Yield
# Data Type: httpx.Response
# Params Used: Yes
# Artifact Storage: Default
@asset(f"fixture_2/bronze/buildings_yielded/buildings_api-response.parquet")
async def asset_bronze_buildings_yielded_response():
    async for output in ingest_endpoint(
        endpoint="buildings", return_type="api_response"
    ):
        yield output


@pytest.fixture(scope="session")
def fixture_2():
    asyncio.run(asset_bronze_buildings_yielded_response())


# Fixture 3
# Purpose: Test writing data with yield, for httpx.Response output with no params
# Function Name: asset_bronze_plants_yielded_response
# Output Method: Yield
# Data Type: httpx.Response
# Params Used: No
# Artifact Storage: Default
@asset(f"fixture_3/bronze/plants_yielded/plants_api-response.parquet")
async def asset_bronze_plants_yielded_response():
    yield await get_api("plants", return_type="api_response")
    yield await get_api("plants", return_type="api_response")
    yield await get_api("plants", return_type="api_response")


@pytest.fixture(scope="session")
def fixture_3():
    asyncio.run(asset_bronze_plants_yielded_response())


# Fixture 4
# Purpose: Test writing data with yield, with json output
# Function Name: asset_bronze_organisations_yielded_json
# Output Method: Yield
# Data Type: json
# Params Used: N/A - json
# Artifact Storage: Default
@asset(f"fixture_4/bronze/orgs_yielded/organisations_json.parquet")
async def asset_bronze_organisations_yielded_json():
    async for output in ingest_endpoint():
        yield output["organisations"]


@pytest.fixture(scope="session")
def fixture_4():
    asyncio.run(asset_bronze_organisations_yielded_json())


# Fixture 5
# Purpose: Test writing data with yield, with custom artifact directory
# Function Name: asset_bronze_pelicans_yielded_json
# Output Method: Yield
# Data Type: json
# Params Used: N/A - json
# Artifact Storage: Custom
@asset(
    f"fixture_5/bronze/pels_yielded/pelicans_json.parquet", artifacts_dir="raw/pelicans"
)
async def asset_bronze_pelicans_yielded_json():
    async for output in ingest_endpoint(endpoint="pelicans"):
        yield output["pelicans"]


@pytest.fixture(scope="session")
def fixture_5():
    asyncio.run(asset_bronze_pelicans_yielded_json())


# Fixture 6
# Purpose: Test .query() method for accessing asset data, and write operations for DuckDBPyRelation output
# Function Name: asset_bronze_buildings_unnested_query
# Output Method: Return
# Data Type: duckdb.DuckDBPyRelation
# Params Used: N/A - Other
# Artifact Storage: Default
@asset(f"fixture_6/bronze/buildings_unnested/buildings_unnested_query.parquet")
async def asset_bronze_buildings_unnested_query():
    nested_buildings = asset_bronze_buildings_yielded_response

    # .query() method creates DuckDB view named 'asset'
    # When using query_str use 'asset' in FROM clause as below
    unnested_buildings_query = await nested_buildings.query(
        "SELECT UNNEST(buildings, max_depth:=2) FROM asset"
    )
    return unnested_buildings_query


@pytest.fixture(scope="session")
def fixture_6():
    asyncio.run(asset_bronze_buildings_unnested_query())


# Fixture 7
# Purpose: Test write operations for pd.DataFrame output and custom artifact directory for return functions
# Function Name: asset_bronze_plants_unnested_df
# Output Method: Return
# Data Type: pd.DataFrame
# Params Used: N/A - Other
# Artifact Storage: Custom
@asset(
    f"fixture_7/bronze/plants_unnested/plants_unnested_df.parquet",
    artifacts_dir="raw/plants_unnested",
)
async def asset_bronze_plants_unnested_df():
    nested_plants = asset_bronze_plants_yielded_response

    # .query() method creates DuckDB view named 'asset'
    # When using query_str use 'asset' in FROM clause as below
    unnested_plants_query = await nested_plants.query(
        "SELECT UNNEST(plants, max_depth:=2) FROM asset"
    )
    return unnested_plants_query.df()


@pytest.fixture(scope="session")
def fixture_7():
    asyncio.run(asset_bronze_plants_unnested_df())


## TESTS ##


# Test 1
async def test_return_json_artifact(fixture_1):
    """
    Tests existence of artifact file at default path & ensures no data loss.

    Fixture Purpose: Test writing data from basic return function with json output

    Acceptance Criteria:
    1. Artifact file exists
    2. All three records are present

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_1/**/*.json")
    expected_path = "fixture_1/bronze/orgs_returned/_artifact/organisations_return.json"
    expected_records = 3

    assert (
        expected_path in json_paths
    ), f"Expected artifact path: {expected_path} not found in glob: {json_paths}"

    artifact = await fs.read_data(expected_path)

    assert len(artifact) == expected_records, "Artifact has incorrect number of records"


# Test 2
async def test_return_json_output(fixture_1):
    """
    Tests existence of output file at specified path & ensures no data loss.

    Fixture Purpose: Test writing data from basic return function with json output

    Acceptance Criteria:
    1. Output file exists
    2. Has correct number of rows
    3. Has correct columns

    """
    fs = await get_fs()
    await register_mad_protocol()
    parquet_paths = fs.glob("fixture_1/**/*.parquet")
    expected_path = "fixture_1/bronze/orgs_returned/organisations_return.parquet"
    expected_row_count = 3
    expected_columns = ["organisation_id", "users", "products", "orders", "reviews"]

    assert (
        expected_path in parquet_paths
    ), f"Expected output path: {expected_path} \n Not found in glob: {parquet_paths}"

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    row_count = duckdb.query("SELECT COUNT(*) FROM output").fetchone()[0]
    columns = output.columns

    assert (
        row_count == expected_row_count
    ), f"Output row_count: {row_count}, did not match expected {expected_row_count}"

    assert (
        columns == expected_columns
    ), f"Output columns: {columns} \n Did not match expected columns: {expected_columns}"


# Test 3
# Fixture: 2
# Function Name: test_yielded_buildings_artifacts_path
# Purpose: Ensure params based hive partitioned file paths have been created
# Acceptance Criteria:
# 1. Confirm file paths are as below:
async def test_yield_response_with_params_artifacts(fixture_2):
    """
    Tests artifacts have been successfully created with params based file paths

    Fixture Purpose: Test writing httpx.Response's with yield, to params based file paths

    Acceptance Criteria:
    1. Correct file paths have been created
    2. Last artifact has the correct number of records

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_2/**/*.json")
    expected_paths = [
        "fixture_2/bronze/buildings_yielded/_artifacts/limit=100/offset=0.json",
        "fixture_2/bronze/buildings_yielded/_artifacts/limit=100/offset=100.json",
        "fixture_2/bronze/buildings_yielded/_artifacts/limit=100/offset=200.json",
    ]
    expected_records = 67

    assert (
        expected_paths == json_paths
    ), f"Expected aritfact paths: \n {expected_paths} \n Did not match glob: \n {json_paths}"

    last_artifact = await fs.read_data(expected_paths[2])
    record_count = len(last_artifact["buildings"])

    assert (
        record_count == expected_records
    ), f"Record count for third artifact: {record_count} \n Did not match expected record count: {expected_records}"


# Test 4
# Fixture: 2
# Function Name: test_yielded_buildings_artifact
# Purpose: Ensure no artifact data loss
# Acceptance Criteria:
# 1. Ensure ____ JSON key is accessible in first artifact

# Test 5
# Fixture: 2
# Function Name: test_yielded_buildings_output
# Purpose: Test existence of output at specified path, ensure no data loss & confirm partition columns
# Acceptance Criteria:
# 1. Read file and confirm row_count =
# 2. Confirm "offset" and "limit" column exist in output parquet

# Test 6
# Fixture: 3
# Function Name: test_yielded_plants_artifacts_path
# Purpose: Ensure fragment based hive partitioned file paths have been created
# Acceptance Criteria:
# 1. Confirm file paths are as below:

# Test 7
# Fixture: 3
# Function Name: test_yielded_plants_artifact
# Purpose: Ensure no artifact data loss
# Acceptance Criteria:
# 1. Ensure ____ JSON key is accessible in last artifact

# Test 8
# Fixture: 3
# Function Name: test_yielded_plants_output
# Purpose: Test existence of output at specified path, ensure no data loss & confirm partition column
# Acceptance Criteria:
# 1. Read file and confirm row_count =
# 2. Confirm "fragment" column exists in output parquet

# Test 9
# Fixture: 4
# Function Name: test_yielded_orgs_artifacts_path
# Purpose: Ensure fragment based hive partitioned file paths have been created
# Acceptance Criteria:
# 1. Confirm file paths are as below:

# Test 10
# Fixture: 4
# Function Name: test_yielded_orgs_output
# Purpose: Test existence of output at specified path, ensure no data loss & confirm partition columns
# Acceptance Criteria:
# 1. Read file and confirm row_count =
# 2. Confirm "fragment" column exists in output parquet

# Test 11
# Fixture: 5
# Function Name: test_yielded_pelicans_artifacts_path
# Purpose: Ensure fragment based hive partitioned file paths have been created in custom directory
# Acceptance Criteria:
# 1. Confirm file paths are as below:

# Test 12
# Fixture: 5
# Function Name: test_yielded_pelicans_output
# Purpose: Test existence of output at specified path, ensure no data loss & confirm partition column
# Acceptance Criteria:
# 1. Read file and confirm row_count =
# 2. Confirm "fragment" column exists in output parquet

# Test 13
# Fixture: 6
# Function Name: test_unnest_buildings_artifact
# Purpose: Test existence of artifact at default path & ensure no data loss
# Acceptance Criteria:
# 1. Ensure ____ JSON key is accessible

# Test 14
# Fixture: 6
# Function Name: test_unnest_buildings_output
# Purpose: Test existence of output at specified path, ensure no data loss & column structure
# Acceptance Criteria:
# 1. Read file and confirm row_count =
# 2. Confirm column structure is as below:

# Test 15
# Fixture: 7
# Function Name: test_unnest_plants_artifact
# Purpose: Test existence of artifact at custom path & ensure no data loss
# Acceptance Criteria:
# 1. Ensure ____ JSON key is accessible

# Test 16
# Fixture: 7
# Function Name: test_unnest_plants_output
# Purpose: Test existence of output at specified path, ensure no data loss & column structure
# Acceptance Criteria:
# 1. Read file and confirm row_count =
# 2. Confirm column structure is as below:


# if __name__ == "__main__":
#     asyncio.run(asset_bronze_buildings_yielded_response())

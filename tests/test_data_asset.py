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


@asset(f"fixture_1/bronze/orgs_returned/organisations_return.parquet")
async def asset_bronze_organisations_return():
    """
    Fixture 1
    Purpose: Test writing data from basic return function with json output
    Function Name: asset_bronze_organisations_return
    Output Method: Return
    Data Type: json
    Params Used: N/A - json
    Artifact Storage: Default
    """
    data = await get_api("organisations", {"limit": 3})
    return data["organisations"]


@pytest.fixture(scope="session")
def fixture_1():
    asyncio.run(asset_bronze_organisations_return())


@asset(f"fixture_2/bronze/buildings_yielded/buildings_api-response.parquet")
async def asset_bronze_buildings_yielded_response():
    """
    Fixture 2
    Purpose: Test writing data with yield, to params based hive partition
    Function Name: asset_bronze_buildings_yielded_response
    Output Method: Yield
    Data Type: httpx.Response
    Params Used: Yes
    Artifact Storage: Default
    """
    async for output in ingest_endpoint(
        endpoint="buildings", return_type="api_response"
    ):
        yield output


@pytest.fixture(scope="session")
def fixture_2():
    asyncio.run(asset_bronze_buildings_yielded_response())


@asset(f"fixture_3/bronze/plants_yielded/plants_api-response.parquet")
async def asset_bronze_plants_yielded_response():
    """
    Fixture 3
    Purpose: Test writing data with yield, for httpx.Response output with no params
    Function Name: asset_bronze_plants_yielded_response
    Output Method: Yield
    Data Type: httpx.Response
    Params Used: No
    Artifact Storage: Default
    """
    yield await get_api("plants", return_type="api_response")
    yield await get_api("plants", return_type="api_response")
    yield await get_api("plants", return_type="api_response")


@pytest.fixture(scope="session")
def fixture_3():
    asyncio.run(asset_bronze_plants_yielded_response())


@asset(f"fixture_4/bronze/orgs_yielded/organisations_json.parquet")
async def asset_bronze_organisations_yielded_json():
    """
    Fixture 4
    Purpose: Test writing data with yield, with json output
    Function Name: asset_bronze_organisations_yielded_json
    Output Method: Yield
    Data Type: json
    Params Used: N/A - json
    Artifact Storage: Default
    """
    async for output in ingest_endpoint():
        yield output["organisations"]


@pytest.fixture(scope="session")
def fixture_4():
    asyncio.run(asset_bronze_organisations_yielded_json())


@asset(
    f"fixture_5/bronze/pels_yielded/pelicans_json.parquet",
    artifacts_dir="fixture_5/raw/pelicans",
)
async def asset_bronze_pelicans_yielded_json():
    """
    Fixture 5
    Purpose: Test writing data with yield, with custom artifact directory
    Function Name: asset_bronze_pelicans_yielded_json
    Output Method: Yield
    Data Type: json
    Params Used: N/A - json
    Artifact Storage: Custom
    """
    async for output in ingest_endpoint(endpoint="pelicans"):
        yield output["pelicans"]


@pytest.fixture(scope="session")
def fixture_5():
    asyncio.run(asset_bronze_pelicans_yielded_json())


@asset(f"fixture_6/bronze/buildings_unnested/buildings_unnested_query.parquet")
async def asset_bronze_buildings_unnested_query():
    """
    Fixture 6
    Purpose: Test .query() method for accessing asset data, and write operations for DuckDBPyRelation output
    Function Name: asset_bronze_buildings_unnested_query
    Output Method: Return
    Data Type: duckdb.DuckDBPyRelation
    Params Used: N/A - Other
    Artifact Storage: Default
    """
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


@asset(
    f"fixture_7/bronze/plants_unnested/plants_unnested_df.parquet",
    artifacts_dir="fixture_7/raw/plants_unnested",
)
async def asset_bronze_plants_unnested_df():
    """
    Fixture 7
    Purpose: Test write operations for pd.DataFrame output and custom artifact directory for return functions
    Function Name: asset_bronze_plants_unnested_df
    Output Method: Return
    Data Type: pd.DataFrame
    Params Used: N/A - Other
    Artifact Storage: Custom
    """
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

    assert expected_path in json_paths

    artifact = await fs.read_data(expected_path)

    assert len(artifact) == expected_records


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

    assert expected_path in parquet_paths

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    row_count = duckdb.query("SELECT COUNT(*) FROM output").fetchone()[0]
    columns = output.columns

    assert row_count == expected_row_count

    assert columns == expected_columns


# Test 3
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

    assert expected_paths == json_paths

    last_artifact = await fs.read_data(expected_paths[2])
    record_count = len(last_artifact["buildings"])

    assert record_count == expected_records


# Test 4
async def test_yield_response_with_params_output(fixture_2):
    """
    Tests output have been successfully constructed with hive partition columns

    Fixture Purpose: Test writing httpx.Response's with yield, to params based file paths

    Acceptance Criteria:
    1. Output file exists
    2. Has correct columns
    3. Has correct record count

    """
    fs = await get_fs()
    await register_mad_protocol()
    parquet_paths = fs.glob("fixture_2/**/*.parquet")
    expected_path = "fixture_2/bronze/buildings_yielded/buildings_api-response.parquet"
    expected_record_count = 267
    expected_columns = [
        "api_version",
        "timestamp",
        "record_count",
        "buildings",
        "limit",
        "offset",
    ]

    assert expected_path in parquet_paths

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    columns = output.columns

    assert columns == expected_columns

    record_count = duckdb.query("SELECT SUM(record_count) FROM output").fetchone()[0]

    assert record_count == expected_record_count


# Test 5
async def test_yield_response_fragment_artifacts(fixture_3):
    """
    Tests artifacts have been successfully created with fragment based file paths

    Fixture Purpose: Test writing httpx.Response's with yield, when no params are provided

    Acceptance Criteria:
    1. Correct file paths have been created
    2. Last artifact has the correct number of records

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_3/**/*.json")
    expected_paths = [
        "fixture_3/bronze/plants_yielded/_artifacts/fragment=1.json",
        "fixture_3/bronze/plants_yielded/_artifacts/fragment=2.json",
        "fixture_3/bronze/plants_yielded/_artifacts/fragment=3.json",
    ]
    expected_record_count = 10

    assert expected_paths == json_paths

    last_artifact = await fs.read_data(expected_paths[2])
    record_count = len(last_artifact["plants"])

    assert record_count == expected_record_count


# Test 6
async def test_yield_response_fragment_output(fixture_3):
    """
    Tests output have been successfully constructed with hive partition fragment column

    Fixture Purpose: Test writing httpx.Response's with yield, when no params are provided

    Acceptance Criteria:
    1. Output file exists
    2. Has correct columns
    3. Has correct record count

    """
    fs = await get_fs()
    await register_mad_protocol()
    parquet_paths = fs.glob("fixture_3/**/*.parquet")
    expected_path = "fixture_3/bronze/plants_yielded/plants_api-response.parquet"
    expected_record_count = 30
    expected_columns = [
        "api_version",
        "timestamp",
        "record_count",
        "plants",
        "fragment",
    ]

    assert expected_path in parquet_paths

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    columns = output.columns

    assert columns == expected_columns

    record_count = duckdb.query("SELECT SUM(record_count) FROM output").fetchone()[0]

    assert record_count == expected_record_count


# Test 7
async def test_yield_json_fragment_artifacts(fixture_4):
    """
    Tests artifacts have been successfully created with fragment based file paths

    Fixture Purpose: Test writing data with yield, with json output

    Acceptance Criteria:
    1. Correct file paths have been created
    2. Last artifact has the correct number of records

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_4/**/*.json")
    expected_paths = [
        "fixture_4/bronze/orgs_yielded/_artifacts/fragment=1.json",
        "fixture_4/bronze/orgs_yielded/_artifacts/fragment=2.json",
        "fixture_4/bronze/orgs_yielded/_artifacts/fragment=3.json",
    ]
    expected_record_count = 67

    assert expected_paths == json_paths

    last_artifact = await fs.read_data(expected_paths[2])
    record_count = len(last_artifact)

    assert record_count == expected_record_count


# Test 8
async def test_yield_json_fragment_output(fixture_4):
    """
    Tests output have been successfully constructed with hive partition fragment column

    Fixture Purpose: Test writing data with yield, with json output

    Acceptance Criteria:
    1. Output file exists
    2. Has correct columns
    3. Has correct record count

    """
    fs = await get_fs()
    await register_mad_protocol()
    parquet_paths = fs.glob("fixture_4/**/*.parquet")
    expected_path = "fixture_4/bronze/orgs_yielded/organisations_json.parquet"
    expected_record_count = 267
    expected_columns = [
        "organisation_id",
        "users",
        "products",
        "orders",
        "reviews",
        "fragment",
    ]

    assert expected_path in parquet_paths

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    columns = output.columns

    assert columns == expected_columns

    record_count = duckdb.query("SELECT COUNT(*) FROM output").fetchone()[0]

    assert record_count == expected_record_count


# Test 9
async def test_yield_artifacts_custom_dir(fixture_5):
    """
    Tests artifacts have been successfully created at at specified artifact_dir

    Fixture Purpose: Test writing data with yield, with custom artifact directory

    Acceptance Criteria:
    1. Correct file paths have been created
    2. Last artifact has the correct number of records

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_5/**/*.json")
    expected_paths = [
        "fixture_5/raw/pelicans/_artifacts/fragment=1.json",
        "fixture_5/raw/pelicans/_artifacts/fragment=2.json",
        "fixture_5/raw/pelicans/_artifacts/fragment=3.json",
    ]
    expected_record_count = 67

    assert expected_paths == json_paths

    last_artifact = await fs.read_data(expected_paths[2])
    record_count = len(last_artifact)

    assert record_count == expected_record_count


# Test 10
async def test_duckdbpyrelation_artifact(fixture_6):
    """
    Tests DuckDBPyRelation can be written to json with _handle_return function

    Fixture Purpose: Test .query() method for accessing asset data, and write operations for DuckDBPyRelation output

    Acceptance Criteria:
    1. Confirm file has been created
    2. File has correct number of records

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_6/**/*.json")
    expected_path = (
        "fixture_6/bronze/buildings_unnested/_artifact/buildings_unnested_query.json"
    )
    expected_record_count = 267

    assert expected_path in json_paths

    artifact = await fs.read_data(expected_path)
    record_count = len(artifact)

    assert record_count == expected_record_count


# Test 11
async def test_duckdbpyrelation_output(fixture_6):
    """
    Tests output have been successfully created from DuckDBPyRelation

    Fixture Purpose: Test .query() method for accessing asset data, and write operations for DuckDBPyRelation output

    Acceptance Criteria:
    1. Output file exists
    2. Has correct columns
    3. Has correct record count

    """
    fs = await get_fs()
    await register_mad_protocol()
    parquet_paths = fs.glob("fixture_6/**/*.parquet")
    expected_path = (
        "fixture_6/bronze/buildings_unnested/buildings_unnested_query.parquet"
    )
    expected_record_count = 267
    expected_columns = [
        "building_id",
        "users",
        "products",
        "orders",
        "reviews",
    ]

    assert expected_path in parquet_paths

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    columns = output.columns

    assert columns == expected_columns

    record_count = duckdb.query("SELECT COUNT(*) FROM output").fetchone()[0]

    assert record_count == expected_record_count


# Test 12
async def test_pd_dataframe_artifact(fixture_7):
    """
    Tests pd.DataFrame can be written to json with _handle_return function

    Fixture Purpose: Test write operations for pd.DataFrame output, with custom artifact directory

    Acceptance Criteria:
    1. Confirm file has been created
    2. File has correct number of records

    """
    fs = await get_fs()
    json_paths = fs.glob("fixture_7/**/*.json")
    expected_path = "fixture_7/raw/plants_unnested/_artifact/plants_unnested_df.json"
    expected_record_count = 30

    assert expected_path in json_paths

    artifact = await fs.read_data(expected_path)
    record_count = len(artifact)

    assert record_count == expected_record_count


# Test 13
async def test_pd_dataframe_output(fixture_7):
    """
    Tests output have been successfully created from pd.DataFrame

    Fixture Purpose: Test write operations for pd.DataFrame output & custom artifact directory for return functions

    Acceptance Criteria:
    1. Output file exists
    2. Has correct columns
    3. Has correct record count

    """
    fs = await get_fs()
    await register_mad_protocol()
    parquet_paths = fs.glob("fixture_7/**/*.parquet")
    expected_path = "fixture_7/bronze/plants_unnested/plants_unnested_df.parquet"
    expected_record_count = 30
    expected_columns = [
        "plant_id",
        "users",
        "products",
        "orders",
        "reviews",
    ]

    assert expected_path in parquet_paths

    output = duckdb.query(f"SELECT * FROM 'mad://{expected_path}'")
    columns = output.columns

    assert columns == expected_columns

    record_count = duckdb.query("SELECT COUNT(*) FROM output").fetchone()[0]

    assert record_count == expected_record_count

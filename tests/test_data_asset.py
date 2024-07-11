import asyncio
import duckdb
from mad_prefect.data_assets import asset
from tests.sample_data.mock_api import get_api, ingest_endpoint
from mad_prefect.filesystems import get_fs
import mad_prefect.filesystems
import pandas as pd
import os


FILESYSTEM_URL = os.getenv("FILESYSTEM_URL", "file://./.tmp/storage")

## FIXTURES ##


# Fixture 1
# Purpose: Test writing data from return function
# Function Name: asset_bronze_organisations_return
# Output Method: Return
# Data Type: json
# Params Used: N/A - json
# Artifact Storage: Default
@asset(f"bronze/orgs_returned/organisations_return.parquet")
async def asset_bronze_organisations_return():
    data = await get_api("organisations", {"limit": 3})
    return data["organisations"]


# Fixture 2
# Purpose: Test writing data with yield, to params based hive partition
# Function Name: asset_bronze_buildings_yielded_response
# Output Method: Yield
# Data Type: httpx.Response
# Params Used: Yes
# Artifact Storage: Default
@asset(f"bronze/buildings_yielded/buildings_api-response.parquet")
async def asset_bronze_buildings_yielded_response():
    async for output in ingest_endpoint(
        endpoint="buildings", return_type="api_response"
    ):
        yield output


# Fixture 3
# Purpose: Test writing data with yield, for httpx.Response output with no params
# Function Name: asset_bronze_plants_yielded_response
# Output Method: Yield
# Data Type: httpx.Response
# Params Used: No
# Artifact Storage: Default
@asset(f"bronze/plants_yielded/plants_api-response.parquet")
async def asset_bronze_plants_yielded_response():
    yield await get_api("plants", return_type="api_response")
    yield await get_api("plants", return_type="api_response")
    yield await get_api("plants", return_type="api_response")


# Fixture 4
# Purpose: Test writing data with yield, with json output
# Function Name: asset_bronze_organisations_yielded_json
# Output Method: Yield
# Data Type: json
# Params Used: N/A - json
# Artifact Storage: Default
@asset(f"bronze/orgs_yielded/organisations_json.parquet")
async def asset_bronze_organisations_yielded_json():
    async for output in ingest_endpoint():
        yield output["organisations"]


# Fixture 5
# Purpose: Test writing data with yield, with custom artifact directory
# Function Name: asset_bronze_pelicans_yielded_json
# Output Method: Yield
# Data Type: json
# Params Used: N/A - json
# Artifact Storage: Custom
@asset(f"bronze/pels_yielded/pelicans_json.parquet", artifacts_dir="raw/pelicans")
async def asset_bronze_pelicans_yielded_json():
    async for output in ingest_endpoint(endpoint="pelicans"):
        yield output["pelicans"]


# Fixture 6
# Purpose: Test .query() method for accessing asset data, and write operations for DuckDBPyRelation output
# Function Name: asset_bronze_buildings_unnested_query
# Output Method: Return
# Data Type: duckdb.DuckDBPyRelation
# Params Used: N/A - Other
# Artifact Storage: Default
@asset(f"bronze/buildings_unnested/buildings_unnested_query.parquet")
async def asset_bronze_buildings_unnested_query():
    nested_buildings = asset_bronze_buildings_yielded_response

    # .query() method creates DuckDB view named 'asset'
    # When using query_str use 'asset' in FROM clause as below
    unnested_buildings_query = await nested_buildings.query(
        "SELECT UNNEST(buildings, max_depth:=2) FROM asset"
    )
    return unnested_buildings_query


# Fixture 7
# Purpose: Test write operations for pd.DataFrame output and custom artifact directory for return functions
# Function Name: asset_bronze_plants_unnested_df
# Output Method: Return
# Data Type: pd.DataFrame
# Params Used: N/A - Other
# Artifact Storage: Custom
@asset(
    f"bronze/plants_unnested/plants_unnested_df.parquet",
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


## TESTS ##


# Test 1
# Fixture: 1
# Function Name: test_returned_orgs_artifact
# Purpose: Test existence of artifact at default path & ensure no data loss
# Acceptance Criteria:
# 1. Ensure ____ JSON key is accessible
async def test_returned_orgs_artifact():
    fs = await get_fs()
    artifact = await fs.read_data("path")


# Test 2
# Fixture: 1
# Function Name: test_returned_orgs_output
# Purpose: Test existence of output at specified path & ensure no data loss
# Acceptance Criteria:
# 1. Read file and confirm row_count =

# Test 3
# Fixture: 2
# Function Name: test_yielded_buildings_artifacts_path
# Purpose: Ensure params based hive partitioned file paths have been created
# Acceptance Criteria:
# 1. Confirm file paths are as below:

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


async def test_run_fixtures():
    await asset_bronze_organisations_return()
    await asset_bronze_buildings_yielded_response()
    await asset_bronze_plants_yielded_response()
    await asset_bronze_organisations_yielded_json()
    await asset_bronze_pelicans_yielded_json()
    await asset_bronze_buildings_unnested_query()
    await asset_bronze_plants_unnested_df()


if __name__ == "__main__":
    asyncio.run(run_fixtures())

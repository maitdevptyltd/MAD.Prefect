import asyncio
import inspect

import duckdb
from mad_prefect.data_assets import asset
from mad_prefect.duckdb import register_query
from tests.sample_data.mock_api import get_api
import mad_prefect.filesystems
import pandas as pd

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests/sample_data"


# Initialize data asset using decorator
@asset(f"bronze/organisations.parquet")
async def bronze_organisations():
    from tests.sample_data.mock_api import get_api

    data = await get_api("organisations", {"limit": 3})
    return data["organisations"]


# Materialize data by calling
@asset(f"bronze/org_users_1.parquet")
async def bronze_org_users_1():

    # Reference the instance of the class
    orgs = bronze_organisations

    # Materialize the asset by accessing its __call__ function
    # This also pulls into memory the data returned by the original function
    preserialization_data = await orgs()

    org_users_nested = [
        {"org_id": org["organisation_id"], "users": org["users"]}
        for org in preserialization_data
    ]

    return org_users_nested


# Testing query materialization
# And the ability to write duckdb objects using DataAsset
@asset(f"bronze/org_users_2.parquet")
async def bronze_org_users_2():

    # Reference the instance of the class
    orgs = bronze_organisations

    # If instance hasn't been written to filesystem
    await orgs()

    orgs_query = await orgs.query()

    org_users_unnested = duckdb.query(
        f"SELECT organisation_id AS org_id, UNNEST(users, max_depth:=2) AS users FROM orgs_query"
    )

    result = org_users_unnested.create(org_users_unnested)

    print(type(result))

    return result


if __name__ == "__main__":
    asyncio.run(bronze_org_users_2())

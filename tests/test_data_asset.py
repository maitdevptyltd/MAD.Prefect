import asyncio
from mad_prefect.data_assets import asset
from tests.synthetic_data.data_gen import get_api
import mad_prefect.filesystems
import pandas as pd

# Override the environment variable before importing register_mad_filesystem
mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests/synthetic_data"


@asset(f"bronze/organisations.parquet")
async def bronze_organisations():
    from tests.synthetic_data.data_gen import get_api

    data = await get_api("organisations", {"limit": 3})
    return data["organisations"]


if __name__ == "__main__":
    asyncio.run(bronze_organisations())

import asyncio
import duckdb
from mad_prefect.data_assets import asset
from tests.sample_data.mock_api import get_api, ingest_endpoint
import mad_prefect.filesystems
import pandas as pd

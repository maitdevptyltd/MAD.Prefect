"""Helpers for reading asset metadata records from storage."""

from __future__ import annotations

import logging
import duckdb

from mad_prefect.data_assets.asset_decorator import ASSET_METADATA_LOCATION
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs

logger = logging.getLogger(__name__)


async def get_asset_metadata(
    asset_name: str,
    asset_id: str,
) -> duckdb.DuckDBPyRelation | None:
    """Return asset metadata for the provided asset if available.

    This is a direct extraction of the logic originally embedded in
    ``DataAssetCallable``; the behavior remains unchanged.
    """

    await register_mad_protocol()
    fs = await get_fs()

    metadata_glob = (
        f"{ASSET_METADATA_LOCATION}/asset_name={asset_name}/asset_id={asset_id}/**/*.json"
    )
    logger.debug("Searching for asset metadata with glob: %s", metadata_glob)

    if fs.glob(metadata_glob):
        return duckdb.query(
            f"SELECT UNNEST(data, max_depth:=2) FROM read_json('mad://{metadata_glob}')"
        )

    return None

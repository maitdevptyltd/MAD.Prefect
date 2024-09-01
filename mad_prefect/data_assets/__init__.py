import os
from typing import Callable

ASSET_METADATA_LOCATION = os.getenv("ASSET_METADATA_LOCATION", ".asset_metadata")


def asset(
    path: str,
    artifacts_dir: str | None = None,
    name: str | None = None,
    snapshot_artifacts: bool = False,
):
    # Prevent a circular reference as it references the env variable
    from mad_prefect.data_assets.data_asset import DataAsset

    def decorator(fn: Callable):
        return DataAsset(fn, path, artifacts_dir, name, snapshot_artifacts)

    return decorator

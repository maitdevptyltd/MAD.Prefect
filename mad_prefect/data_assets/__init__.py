from typing import Callable
from mad_prefect.data_assets.data_asset import DataAsset


def asset(
    path: str,
    artifacts_dir: str | None = None,
    name: str | None = None,
    snapshot_artifacts: bool = True,
):
    def decorator(fn: Callable):
        return DataAsset(fn, path, artifacts_dir, name, snapshot_artifacts)

    return decorator

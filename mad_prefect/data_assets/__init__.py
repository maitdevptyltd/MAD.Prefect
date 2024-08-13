from typing import Callable
from mad_prefect.data_assets.data_asset import DataAsset


def asset(path: str, artifacts_dir: str | None = None, name: str | None = None):
    def decorator(fn: Callable):
        return DataAsset(fn, path, artifacts_dir, name)

    return decorator

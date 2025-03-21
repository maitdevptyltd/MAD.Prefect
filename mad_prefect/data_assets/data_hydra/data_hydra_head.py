from typing import Any
from mad_prefect.data_assets.data_hydra import DataHydra
from mad_prefect.data_assets.data_asset import DataAsset


class DataHydraHead:

    hydra: DataHydra
    asset: DataAsset

    def __init__(self, hydra: DataHydra, asset: DataAsset):
        self.hydra = hydra
        self.asset = asset

    async def __call__(self, *args: Any, **kwds: Any) -> Any:
        return await self.asset(self.hydra)

from functools import partial
from typing import Protocol, dataclass_transform
import pytest
from pydantic import BaseModel, ConfigDict
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_hydra import (
    DataAsset,
    DataHydra,
)
from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck
from mad_prefect.data_assets.options import DataHydraOptions


@asset(
    "tenant_id={tenant_id}",
    context_factory=[
        {"tenant_id": "abc123"},
        {"tenant_id": "123abc"},
    ],
)
class TenantAsset:
    tenant_id: str

    @asset("work_orders.parquet")
    async def work_orders(self):
        assert self.tenant_id
        return [1, 2, 3]


async def test_data_hydra_proof_of_concept():
    # Ensure the tenant_asset_hydra is camouflaged as the TenantAsset (intellisense for work_orders)
    hydra_work_orders = TenantAsset.hydra()
    assert isinstance(hydra_work_orders, DataHydraNeck)

    # When materializing a hydra's data assets, it should probably? return a DataAsset?
    # although the DataAsset it returns won't represent a single asset but instead the
    # cartesian product of all the data assets in the hydra
    result = await hydra_work_orders()
    assert isinstance(result, DataArtifact)


async def test_hydra_tenant_id_exception(tenant_asset_hydra):
    with pytest.raises(Exception) as e:
        assert tenant_asset_hydra.tenant_id

    print(f"Expected exception: {e.value}")

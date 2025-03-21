import pytest
from pydantic import BaseModel, ConfigDict
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_hydra import (
    DataAsset,
    DataHydra,
)
from mad_prefect.data_assets.data_hydra.types import DataHydraOptions
from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck


class TenantAsset(BaseModel):
    tenant_id: str

    @asset("tenant_id={tenant_id}/work_orders.parquet")
    async def work_orders(self):
        assert self.tenant_id
        return [1, 2, 3]

    # TODO: This is required in order to define a DataAsset as a function as seen above
    # TODO: figure out how to hide this away so the user doesnt have to add this
    model_config = ConfigDict(
        ignored_types=(DataAsset,),
        arbitrary_types_allowed=True,
    )


@pytest.fixture
def tenant_asset_hydra():
    return DataHydra(
        TenantAsset,
        DataHydraOptions(
            context_factory={"tenant_id": "abcdef_123", "other_prop": True},
        ),
    )


async def test_data_hydra_materializes_single(tenant_asset_hydra):
    # Ensure the tenant_asset_hydra is camouflaged as the TenantAsset (intellisense for work_orders)
    hydra_work_orders = tenant_asset_hydra.work_orders
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

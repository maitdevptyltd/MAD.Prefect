import pytest
from pydantic import BaseModel, ConfigDict
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_hydra import (
    DataAsset,
    DataHydra,
    DataHydraOptions,
    DataHydraHead,
)


class TenantAsset(BaseModel):
    tenant_id: str

    @asset("tenant_id=1/work_orders.parquet")
    async def work_orders():
        return [1, 2, 3]

    # TODO: This is required in order to define a DataAsset as a function as seen above
    # TODO: figure out how to hide this away so the user doesnt have to add this
    model_config = ConfigDict(
        ignored_types=(DataAsset,),
        arbitrary_types_allowed=True,
    )


tenant_asset_hydra = DataHydra(
    TenantAsset,
    DataHydraOptions(
        context_factory={"tenant_id": "abcdef_123", "other_prop": True},
    ),
)


async def test_hydra_dogs():
    hydra_dogs = tenant_asset_hydra.work_orders
    assert isinstance(hydra_dogs, DataHydraHead)
    assert await hydra_dogs()


async def test_hydra_tenant_id_exception():
    with pytest.raises(Exception) as e:
        assert tenant_asset_hydra.tenant_id

    print(f"Expected exception: {e.value}")

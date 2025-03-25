import pytest
from pydantic import BaseModel, ConfigDict
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_hydra import (
    DataAsset,
    DataHydra,
)
from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck


@asset(
    "tenant_id={tenant_id}",
    context_factory=[
        {"tenant_id": "abc123"},
        {"tenant_id": "123abc"},
    ],
)
class TenantAsset(BaseModel):
    tenant_id: str

    @asset("work_orders.parquet")
    async def work_orders(self):
        assert self.tenant_id
        return [1, 2, 3]

    # TODO: This is required in order to define a DataAsset as a function as seen above
    # TODO: figure out how to hide this away so the user doesnt have to add this
    model_config = ConfigDict(
        ignored_types=(DataAsset,),
        arbitrary_types_allowed=True,
    )


@asset("asset_with_params.parquet")
async def asset_with_params(a: str, b: str):
    return f"{a} {b}"


@pytest.fixture
def tenant_asset_hydra():
    return TenantAsset


async def test_asset_with_params_has_intellisense():
    # These should just have an intellisense error, not runtime error
    a = await asset_with_params(
        "a",
        "b",
    )

    b = asset_with_params.with_arguments(a="a", b="b")
    assert await b()


async def test_data_hydra_proof_of_concept(tenant_asset_hydra):
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

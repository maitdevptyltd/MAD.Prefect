import pytest
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_hydra.data_hydra_runner import (
    DataHydraRun,
)


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

    @asset("purchase_orders.parquet")
    async def purchase_orders(self):
        return [5, 6, 7]


@asset("unnested_asset.parquet")
async def unnested_asset(self):
    pass


async def test_running_a_full_hydra():
    # A hydra can be ran just by simply calling the hydra, it will begin the run
    run = TenantAsset()
    assert isinstance(run, DataHydraRun)

    # We can await the run to allow the system to process through entirely
    await run


async def test_running_a_single_hydra_asset():
    # We should be able to create an asset runner for a specific asset within the DataHydra
    hydra_run = TenantAsset(TenantAsset.cls.work_orders)
    await hydra_run

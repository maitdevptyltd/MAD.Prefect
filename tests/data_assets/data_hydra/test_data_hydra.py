from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_hydra.runner.data_hydra_run import (
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


Context = TenantAsset.cls


@asset("unrelated_asset_with_context.parquet")
async def unrelated_asset_with_context(context: Context):
    assert context.tenant_id


@asset("unrelated_asset_with_context.parquet")
async def unrelated_asset_with_injection(tenant_id: str):
    assert tenant_id


async def test_running_a_full_hydra():
    # A hydra can be ran just by simply calling the hydra, it will begin the run
    run = TenantAsset()
    assert isinstance(run, DataHydraRun)

    # We can await the run to allow the system to process through entirely
    result = await run
    assert len(result.heads) == 4


async def test_running_a_single_hydra_asset():
    # We should be able to create an asset runner for a specific asset within the DataHydra
    hydra_run = TenantAsset(TenantAsset.cls.work_orders)

    result = await hydra_run
    assert len(result.heads) == 2


async def test_running_an_unrelated_asset_with_hydra_context():
    # We should be able to associate unrelated assets with the hydra
    hydra_run = TenantAsset(unrelated_asset_with_context)

    result = await hydra_run
    # There should be a head for each context record (two) * each asset
    assert len(result.heads) == 2


async def test_running_many_hydra_assets():
    hydra_run = TenantAsset(
        TenantAsset.cls.work_orders,
        TenantAsset.cls.purchase_orders,
        unrelated_asset_with_context,
    )

    result = await hydra_run
    assert len(result.heads) == 6

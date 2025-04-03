from mad_prefect.data_assets import asset
from mad_prefect.data_assets.configurators.fluent_data_asset_configurator import (
    FluentDataAssetConfigurator,
)


@asset(
    "tests/configurations/test_fluent_data_asset_configurator/tenant_id={tenant_id}/project_id={project_id}/{endpoint}.parquet"
)
async def generic_asset(tenant_id: str, project_id: str, endpoint: str):
    return [{"tenant_id": tenant_id, "project_id": project_id, f"{endpoint}_id": 1}]


async def test_with_arguments_partial():
    configurator = FluentDataAssetConfigurator(generic_asset)
    work_orders = configurator.with_arguments(endpoint="work_orders")

    tenant_id = "big_tenant_man"
    project_id = "glory_for_rome"

    result = await work_orders(tenant_id, project_id)

    assert (
        result.path
        == f"tests/configurations/test_fluent_data_asset_configurator/tenant_id={tenant_id}/project_id={project_id}/work_orders.parquet"
    )

from typing import Any, Callable, cast
from prefect import Task, task
from prefect.client.schemas import TaskRun, State
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


@asset(
    "tests/configurations/test_fluent_data_asset_configurator/{path}.parquet",
    name="{path}",
)
async def generic_asset_with_name(path: str):
    return [{"path": path}]


async def test_with_arguments_partial_with_name():
    configurator = FluentDataAssetConfigurator(generic_asset_with_name)
    test_asset = configurator.with_arguments(path="work_orders")

    # At this point, the asset will have substitution due to initialization formatting
    assert test_asset.name == "work_orders"

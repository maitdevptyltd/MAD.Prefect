import inspect

import pytest

from mad_prefect.data_assets import asset
from mad_prefect.data_assets.asset_template_formatter import AssetTemplateFormatter
from mad_prefect.data_assets.data_asset import DataAsset


@asset("base/{value}.parquet")
async def base_asset(value: str):
    return value


@asset("dependent/{dependency}")
async def dependent_asset(dependency: DataAsset):
    return dependency


def test_formatter_collects_nested_asset_arguments():
    configured_base = base_asset.with_arguments("nested")
    configured_dependency = dependent_asset.with_arguments(dependency=configured_base)

    nested_callable = configured_dependency._callable
    bound_args = nested_callable.get_bound_arguments()

    formatter = AssetTemplateFormatter(nested_callable.args, bound_args)

    # Nested asset argument should be surfaced for formatting
    assert formatter.format_kwargs["value"] == "nested"
    assert formatter.format("dependent/{value}") == "dependent/nested"


def test_formatter_resolves_nested_placeholders():
    def fn(path: str, suffix: str):
        return path, suffix

    bound_args = inspect.signature(fn).bind_partial(path="{suffix}", suffix="final")
    formatter = AssetTemplateFormatter(tuple(), bound_args)

    assert formatter.format("{path}") == "final"


def test_formatter_raises_descriptive_key_error():
    def fn(path: str):
        return path

    bound_args = inspect.signature(fn).bind_partial(path="initial")
    formatter = AssetTemplateFormatter(tuple(), bound_args)

    with pytest.raises(KeyError) as exc:
        formatter.format("missing/{unknown}")

    message = str(exc.value)
    assert "Missing format key 'unknown'" in message
    assert "Available keys: path" in message


def test_formatter_handles_none_template():
    def fn():
        return None

    bound_args = inspect.signature(fn).bind_partial()
    formatter = AssetTemplateFormatter(tuple(), bound_args)

    assert formatter.format(None) is None

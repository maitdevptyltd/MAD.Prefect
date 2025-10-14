from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_asset import CACHE_FIRST_CACHE_EXPIRATION, DataAsset


async def test_cache_first_reuses_cached_materialization():
    call_count = 0
    asset_path = f"tests/cache_first/{uuid4().hex}.json"

    @asset(asset_path)
    async def cacheable_asset():
        nonlocal call_count
        call_count += 1
        yield [{"call_count": call_count}]

    # First run materialises the asset.
    await cacheable_asset()

    cached_asset = cacheable_asset.cache_first()

    # Re-running the cache-first variant should not execute the asset again.
    await cached_asset()

    assert call_count == 1


async def test_cache_first_accepts_custom_expiration():
    asset_path = f"tests/cache_first/{uuid4().hex}.json"

    @asset(asset_path)
    async def cacheable_asset():
        yield [{"value": 1}]

    custom_ttl = timedelta(minutes=5)
    cached_asset = cacheable_asset.cache_first(expiration=custom_ttl)

    assert isinstance(cached_asset, DataAsset)
    assert cached_asset.options.cache_expiration == custom_ttl

    default_cached = cacheable_asset.cache_first()
    assert default_cached.options.cache_expiration == CACHE_FIRST_CACHE_EXPIRATION

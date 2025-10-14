# Cache-First Assets

## Summary
- Introduces `DataAsset.cache_first()` and the matching fluent configurator helper to bias toward previously materialized artifacts.
- Establishes a default long-lived TTL (`timedelta(days=9999)`) that mirrors manual `with_options(cache_expiration=...)` usage while keeping an escape hatch for custom durations.
- Adds focused regression coverage to ensure cache-first paths do not rematerialize the asset.

## Usage
```python
from datetime import timedelta
from mad_prefect.data_assets import asset

@asset("analytics/daily_sales.json")
async def daily_sales():
    yield [{"store_id": 1, "total": 1200}]

# Prefer the cached artifact for ad-hoc analysis.
cached_sales = daily_sales.cache_first()
sales_view = await cached_sales.query("SELECT * FROM data")

# Opt into a smaller TTL when the cached view should refresh sooner.
fresh_cache = daily_sales.cache_first(expiration=timedelta(hours=1))
```

## Progress
- [x] Implemented `cache_first` helper on `DataAsset` and the fluent configurator.
- [x] Added regression tests covering default and custom TTL behaviours.
- [x] Updated README examples to highlight cache-first usage.

## Next Steps
- Collect feedback on the default TTL and adjust if teams need a shorter global default.
- Explore exposing cache hit metrics so interactive tooling can signal when a rematerialization occurs.

## Blockers & Risks
- None identified; helper defers to existing caching machinery so behaviour matches `with_options(cache_expiration=...)`.

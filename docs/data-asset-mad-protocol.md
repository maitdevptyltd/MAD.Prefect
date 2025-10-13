# MAD Protocol Registration

## Summary
- Data asset executions now register the `mad://` DuckDB filesystem automatically at runtime.
- Developers no longer need to call `register_mad_protocol()` inside asset materialization functions.
- Integration coverage (`test_asset_materialization_does_not_require_manual_protocol_registration`) ensures the implicit registration path stays intact.

## Usage
- Define assets without manual protocol setup; `DataAssetCallable` registers the protocol just before the user-defined coroutine runs.

```python
from mad_prefect.data_assets import asset

@asset("salesforce/opportunities.parquet", artifact_filetype="parquet")
async def opportunities():
    return duckdb.query(
        """
        SELECT *
        FROM 'mad://salesforce/opportunities_source.parquet'
        """
    )
```

- Existing assets that still call `register_mad_protocol()` remain compatible; the double registration is a no-op.

## Progress
- [x] Implicit protocol registration implemented in `DataAssetCallable`.
- [x] Regression test covering assets that rely on `mad://` reads without manual registration.
- [x] Documentation updated with examples and operational notes.

## Next Steps
- Monitor downstream usage for any remaining manual protocol registrations that can be removed.
- Evaluate whether other internal helpers (e.g. standalone utilities) need similar implicit registration hooks.

## Blockers & Risks
- None identified; DuckDB's filesystem registry treats repeated registrations as idempotent, so no behavioural regressions observed.

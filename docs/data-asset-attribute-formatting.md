# Attribute Formatting Lifecycle

## Summary
- Today, `DataAsset.with_arguments()` mutates a shared `DataAssetOptions` reference and only formats attribute templates when the asset is awaited, which causes sibling derivatives to bleed state and delays format errors until runtime.
- We want attribute formatting in two passes: a **partial** formatting step during asset initialization (format only the provided arguments, never raise) and a **strict** formatting step inside `DataAssetCallable.__call__` (all placeholders must resolve, otherwise raise immediately).
- We also need targeted regression coverage to prove option isolation, template overrides, partial derivatives, and call-time validation all behave as expected. Concrete derivatives (created via `with_arguments`) should sync updates when re-invoked, but the factory asset must retain its template values.

## Usage (planned)
```python
from mad_prefect.data_assets import asset

@asset(
    path="{customer}/{endpoint}.parquet",
    artifacts_dir="bronze/{customer}",
    name="{customer}-{endpoint}",
)
async def interactions(endpoint: str, customer: str):
    return [{"customer": customer, "endpoint": endpoint}]

# Partial formatting during initialization.
acme = interactions.with_arguments(customer="acme")
assert acme.path == "acme/{endpoint}.parquet"

# Full formatting during call; missing placeholders should raise here.
await acme(endpoint="orders")
```

## Progress
- [ ] Introduce `base_options` + `deepcopy` during `DataAsset` construction so each derivative begins with a clean template.
- [ ] Teach `AssetTemplateFormatter` (or a wrapper) to support `allow_partial=True` for initialization.
- [ ] Reapply strict formatting on `DataAssetCallable.__call__`, merging previously bound args with new call-time args.
- [ ] Add focused tests (`tests/data_assets/test_asset_template_formatting_on_initialization.py`) that demonstrate partial/strict behavior, `with_options` template overrides, and option isolation.

## Next Steps
- Prototype the formatter split and ensure partial formatting leaves unbound placeholders untouched.
- Wire strict formatting back into the callable path and confirm missing placeholders surface as `KeyError`s with clear context.
- Validate via `pytest test_asset_template_formatting_on_initialization.py` once the new coverage lands; expand to the full suite afterward.
- Update other docs (e.g., cache-first, MAD protocol) only if their examples rely on the old formatting semantics.

## Blockers & Risks
- Failing to deepcopy `DataAssetOptions` will continue leaking state between derivatives.
- Strict formatting must respect nested assets (e.g., `{listing_asset.name}`) and configurator overrides; missing a dependency should fail fast with actionable errors.

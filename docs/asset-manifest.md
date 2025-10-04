# Asset Metadata Manifest

## Summary
- Manifests live at `_asset_metadata/asset_name=<name>/asset_id=<id>/manifest.json`.
- They track the most recent run, last materialized timestamp, artifacts produced, and status for each `DataAsset` configuration (including argument-bound variants).
- The manifest schema is backed by `AssetManifest`/`AssetManifestRun` Pydantic models in `mad_prefect/data_assets/asset_metadata.py`.

## Usage
- `DataAssetRun.persist()` now writes run metadata and upserts the manifest with the run's status and materialization time.
- Call `mad_prefect.data_assets.asset_metadata.load_asset_manifest(name, id)` to read the manifest, or `upsert_asset_manifest_from_run()` when integrating new asset workflows.
- When accessing metadata, prefer the manifest before globbing to locate run files; fall back to globbing if the manifest is unavailable.

## Field Reference
- `manifest_version`: literal `"1"` to support schema migrations.
- `asset_name`, `asset_id`: identifiers deriving from the sanitized asset name and configuration hash.
- `asset_signature`: optional signature to detect configuration drift.
- `last_materialized`: latest successful materialization timestamp (UTC).
- `last_status`: enum (`success`, `failed`, `unknown`) capturing the run outcome.
- `last_run`: nested object containing `id`, `metadata_path`, `materialized`, and `artifact_paths` for the most recent run.
- `last_artifacts`: cached tuple of artifact paths so readers can skip expensive filesystem globs.
- `updated_at`: UTC timestamp recording when the manifest was last refreshed.
- `last_error`: optional string for failure diagnostics.

## Example Manifest
```json
{
  "manifest_version": "1",
  "asset_name": "my.module.asset",
  "asset_id": "c0ffee",
  "asset_signature": "c0ffee",
  "last_materialized": "2024-01-02T00:00:00+00:00",
  "last_status": "success",
  "last_error": null,
  "last_artifacts": [
    "bronze/my.module.asset.parquet"
  ],
  "updated_at": "2024-01-02T00:00:01.500000+00:00",
  "last_run": {
    "id": "run-123",
    "metadata_path": "_asset_metadata/asset_name=my.module.asset/asset_id=c0ffee/asset_run_id=run-123/metadata.json",
    "materialized": "2024-01-02T00:00:00+00:00",
    "artifact_paths": [
      "bronze/my.module.asset.parquet"
    ]
  }
}
```

## Progress
- [x] Pydantic models for manifests and run summaries implemented.
- [x] Manifests automatically upserted from `DataAssetRun.persist()` using run metadata.
- [x] Unit tests verifying manifest validation, persistence, and metadata queries.

## Next Steps
- Integrate manifest-aware lookup into `_get_last_materialized` to eliminate globbing entirely.
- Extend manifests with failure details when runs raise exceptions.
- Capture produced artifact metadata (e.g. schema hash) for richer cache validation.

## Blockers & Risks
- None identified.

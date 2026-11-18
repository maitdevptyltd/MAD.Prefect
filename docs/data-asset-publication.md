# Replacing a published asset

```python
@asset("bronze/logs.parquet")
async def logs():
    return duckdb.sql("SELECT * FROM 'mad://bronze/partitions/*.parquet'")
```

For local and Azure Blob Storage destinations, DuckDB relation results are written
completely to a temporary local file before replacing the published file. A failed
query or serialization during publication leaves the existing output untouched.
Local outputs use `os.replace` on the same filesystem. Azure protocols `abfs`,
`abfss` and `az` use `put_file`: the block-blob upload commits the replacement after
uploading its blocks, avoiding DuckDB's delete-then-copy replacement.

Other remote destinations, including SSH/SFTP, keep the previous DuckDB write
process through their registered filesystem. They do not use the new direct upload.

This protection starts at publication. With `snapshot_artifacts=False`, asset
execution first deletes its artifact directory recursively. Keep the published
file outside a custom `artifacts_dir`: for example, `artifacts_dir="bronze"`
would delete `bronze/logs.parquet` before publication begins. The default artifact
directory is separate from the published file in the example above.

This preserves the reason for the earlier workaround: DuckDB's temporary-path
handling fails when overwriting through the `mad://` wrapper. Previously the
publisher bypassed that wrapper using the underlying filesystem protocol;
local staging bypasses it too. `test_published_relation_can_be_overwritten`
checks two successive publications while reading the input and output via `mad://`.

Local staging files are removed after success or failure. Failed writes propagate
their error and do not mark the artifact as persisted or release its data.
The temporary file ends in `.tmp`, with its format supplied explicitly to DuckDB,
so recursive `*.parquet`, `*.csv` and `*.json` searches exclude unfinished output.

## Progress

- Publication refuses a DuckDB table or view named `_d` visible on the current search path
  (case-insensitive), which would otherwise override the supplied Python relation.
  Rename the conflicting object before retrying; publication leaves any existing output intact.
- Local and Azure publication stage a complete file before replacing the output.
- The original `_d` relation name is retained; an existing `_relation` table or view does not block publication.
- COPY uses an escaped filename literal for compatibility with DuckDB 0.9.
- Focused tests cover export failure, upload failure, local replacement failure,
  successful replacement and temporary-file cleanup.

## Next Steps

- Review and release the change, then update the consuming application's lock.
- Repeat the isolated upload checks with the consuming application's pinned driver versions.

## Direct-write comparison

Azurite testing on 16 November 2026 used DuckDB 1.4.1, fsspec 2025.9.0 and
adlfs 2026.8.0 with synthetic data only. `USE_TMP_FILE=false` successfully
published 20,000 rows, but a conversion error at row 10,000 closed the stream
and committed an incomplete, unreadable Parquet file over the previous blob.
This occurred with both 64 KiB and 50 MiB stream buffers.

Local staging preserved the previous blob byte-for-byte after the same query
failure and after injected block-upload and block-list commit failures. Both
approaches produced identical complete files when successful. The emulator
does not establish production performance or every storage provider's behaviour.

## Blockers & Risks

- For local and Azure destinations, one complete output file must fit on temporary disk; local replacement stages
  beside its destination so it stays on the same filesystem.
- Remote overwrite guarantees belong to the storage driver: this is not a
  cross-provider atomicity guarantee or a concurrent-writer lock.
- This change covers DuckDB relation publication, not direct JSON/CSV/Parquet
  fragment writers or multi-file transactions.

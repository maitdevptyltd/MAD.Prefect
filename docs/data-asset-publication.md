# Replacing a published asset

```python
@asset("bronze/logs.parquet")
async def logs():
    return duckdb.sql("SELECT * FROM 'mad://bronze/partitions/*.parquet'")
```

DuckDB relation results are written completely to a temporary local file before
replacing the published file. A failed query or serialization leaves the old
output untouched. Local outputs use `os.replace` on the same filesystem. Remote
outputs use the configured filesystem's `put_file` without deleting the old file
first. Azure's block-blob upload commits the replacement after uploading its
blocks, avoiding DuckDB's delete-then-copy replacement.

This preserves the reason for the earlier workaround: DuckDB's temporary-path
handling fails when overwriting through the `mad://` wrapper. Previously the
publisher bypassed that wrapper using the underlying filesystem protocol;
local staging bypasses it too. `test_published_relation_can_be_overwritten`
checks two successive publications while reading the input and output via `mad://`.

Temporary files are removed after success or failure. Failed writes propagate
their error and do not mark the artifact as persisted or release its data.
The temporary file ends in `.tmp`, with its format supplied explicitly to DuckDB,
so recursive `*.parquet`, `*.csv` and `*.json` searches exclude unfinished output.

## Progress

- Relation publication stages a complete file before replacing the output.
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

- One complete output file must fit on temporary disk; local replacement stages
  beside its destination so it stays on the same filesystem.
- Remote overwrite guarantees belong to the storage driver: this is not a
  cross-provider atomicity guarantee or a concurrent-writer lock.
- This change covers DuckDB relation publication, not direct JSON/CSV/Parquet
  fragment writers or multi-file transactions.

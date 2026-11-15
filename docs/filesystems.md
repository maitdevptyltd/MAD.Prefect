# Filesystem paths

```python
from mad_prefect.filesystems import FsspecFileSystem

filesystem = FsspecFileSystem(basepath="file://./data/")
for path in filesystem.glob("invoice/*.parquet"):
    assert filesystem.exists(path)
```

`glob()` returns paths relative to `basepath`. A returned path can be passed
directly to `exists()`, `read_path()` or another filesystem method. A trailing
slash in the configured URL does not change those relative paths.

The configured URL is retained, including root URLs such as `file:///` and
`memory://`. Storage drivers parse that URL into their own root representation.
`glob()` uses the existing file-access resolver to identify the leading prefix
to remove. Matching text inside the remaining path is preserved.

## Progress

- [x] File listings and file access use the same resolved root.
- [x] Regression tests cover trailing slashes, local and cloud-style roots,
  repeated prefix text, and empty listings.

## Next Steps

- Use the existing path resolver when adding filesystem methods.
- Follow the [release flow](release-flow.md), then update consumer lock files
  and rebuild their images to adopt the fix.

## Blockers & Risks

- Pyright reports two existing errors for `basepath` and `storage_options` in
  the parent constructor. These are Pydantic model fields, but Prefect's parent
  signature does not expose them to the type checker.
- The regression tests isolate the wrapper with mocked storage drivers. Cloud
  authentication and permissions still require validation in the consuming
  environment.

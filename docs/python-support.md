# Python Support

## Summary

MAD.Prefect supports Python 3.10 through 3.14, matching the interpreter range
supported by Prefect 3.8.4. The package metadata expresses this as
`>=3.10,<3.15`.

## Usage

Install MAD.Prefect with any supported CPython minor:

```bash
python -m pip install mad-prefect
```

Production workloads should still pin one reviewed Python, Prefect, and
MAD.Prefect combination. Supporting a range allows consumers to upgrade at
their own release boundary; it does not make interpreter upgrades automatic.

## Compatibility Contract

- GitHub Actions runs the complete test suite on Python 3.10, 3.11, 3.12,
  3.13, and 3.14.
- The Poetry lock must resolve installable artifacts across the complete range.
- The published wheel must retain `Requires-Python: >=3.10,<3.15`.
- A future Prefect release may change its Python range. MAD.Prefect changes its
  range only after the package and its dependency lock pass the corresponding
  test matrix.

## Progress

- [x] Package metadata covers Python 3.10 through 3.14.
- [x] UTC timestamps use the Python-3.10-compatible `timezone.utc` spelling.
- [x] Pull requests and protected branch pushes run the complete Python matrix.

## Next Steps

- Keep the matrix and package metadata aligned when the pinned Prefect baseline
  changes.

## Blockers & Risks

- MAD.Prefect allows Prefect 2.18 through 3.x for existing consumers, while the
  current compatibility matrix resolves Prefect 3.8.4 from the lock. Consumers
  that intentionally remain on Prefect 2 must validate their own Python and
  Prefect combination.

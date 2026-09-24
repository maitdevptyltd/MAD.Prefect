# Environment-backed Prefect blocks

## Developer API

Subclass `EnvBlock`, declare credential fields as Pydantic model fields, and use an
optional `prefix` model field when the environment-variable prefix should differ
from the class name.

```python
from mad_prefect.envblock import EnvBlock


class XeroCredentials(EnvBlock):
    prefix: str | None = "XERO"
    client_id: str
    client_secret: str
    scopes: str


credentials = await XeroCredentials.from_env()
```

This example reads `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET`, and `XERO_SCOPES`.
Without an explicit prefix, `EnvBlock` uses the uppercased subclass name. For
example, `XeroCredentials` reads `XEROCREDENTIALS_CLIENT_ID`.

If `<PREFIX>_CREDENTIAL_BLOCK_NAME` is set, `from_env()` loads that saved Prefect
block instead of constructing one from individual environment variables. Required
fields remain required in either declaration style.

Declare overrides as `str | None`, matching the base model field:

```python
class ServiceCredentials(EnvBlock):
    prefix: str | None = "SERVICE"
    token: str
```

Using a `ClassVar` removes `prefix` from Pydantic's model fields and breaks prefix
resolution. Using a narrower `str` annotation conflicts with the optional base
field under Pyright.

## Progress

- [x] Document the model-field declaration and consumer override convention.
- [x] Preserve explicit-prefix and class-name fallback environment loading.
- [x] Preserve `<PREFIX>_CREDENTIAL_BLOCK_NAME` loading.
- [x] Cover required-field validation and Pydantic schema/serialization behavior.

## Next Steps

- Release the framework change and update consumers to declare explicit prefixes
  as `str | None`.

## Blockers & Risks

- Existing consumers that override `prefix` as `str` will continue to run, but
  should adopt `str | None` to satisfy Pyright without a suppression.

# Choose your own HTTP retries

```python
import httpx
from mad_prefect.rate_limit import ApiRateLimit, RateLimitedAsyncTransport

limit = ApiRateLimit(name="provider-account", requests_per_minute=40)
paced = RateLimitedAsyncTransport(limit)

async with httpx.AsyncClient(transport=paced) as client:
    response = await client.get("https://api.example.com/users")
    response.raise_for_status()
```

This client has shared rate limiting and no HTTP retries. MAD.Prefect supplies
the allowance and the small transport adapter. Your repository chooses whether
to retry, which failures qualify, and how long to wait.

The adapter creates a fresh `httpx.AsyncHTTPTransport()` by default. To supply
custom network settings or a test transport, pass
`RateLimitedAsyncTransport(limit, transport=network)`. Closing the adapter closes
either the default or supplied transport.

## Using httpx-retries

Install `httpx-retries` in the consuming repository, then wrap the paced transport:

```python
from httpx_retries import Retry, RetryTransport

# Reuse the limiter, but create a fresh adapter for each client.
paced = RateLimitedAsyncTransport(limit)
retrying = RetryTransport(
    transport=paced,
    retry=Retry(total=2, allowed_methods=["GET", "HEAD"], backoff_factor=0.5),
)
async with httpx.AsyncClient(transport=retrying) as client:
    response = await client.get("https://api.example.com/users")
    response.raise_for_status()
```

The order is client, retry transport, rate-limit transport, network transport.
When the retry transport tries again, it must pass through the limiter again.
Putting retries inside the paced transport would let those extra requests bypass
the limiter. Leave the network transport's own retries disabled. Apply the same
ordering to custom transports and any HTTPX mounts.

A request hook alone is insufficient for this arrangement: retries inside a
transport do not call the client's request hooks again. The transport adapter
puts the allowance check immediately before the network attempt instead.

## Other retry systems, including Prefect tasks

A caller can retry `client.get()` using another library, a small retry loop or a
Prefect task. Each call still reaches the paced transport. A retried Prefect task
reruns the whole task body, so choose a task boundary that is safe to repeat.
The rate limiter neither replaces Prefect tasks nor creates tasks of its own.

For clients other than HTTPX, call `await limit.acquire()` immediately before
each actual attempt. If the client retries internally, put acquisition inside
its retry callback or transport. Acquiring once before an entire retry loop
does not cover subsequent attempts.

## Two different failures

`ApiRateLimit.acquire()` retries temporary failures while contacting Prefect.
Those retries are bounded and logged by MAD.Prefect. If acquisition ultimately
fails, the transport raises `RateLimitAcquisitionError` and sends no API request.
The original error is its `__cause__`. This separate exception prevents HTTPX
retry policies from treating a Prefect connection failure as a provider failure.

The downstream retry system handles provider responses and network failures.
It should exclude `RateLimitAcquisitionError`; a broad retry-on-any-exception
policy would otherwise multiply the limiter's retries. Cancellation propagates.
The adapter returns provider responses and errors unchanged and closes the
wrapped network transport when the client closes.

Provider cooldowns such as `Retry-After` remain the downstream policy's concern.
Check the chosen retry library's delay cap and total time budget: never shorten
a provider's required wait merely to fit a backoff cap. KnowBe4 calls aretry_request directly, using a response hook for its five-minute
cooldown fallback and asyncio.timeout for its overall deadline.

## Migration from the feature branch's earlier client

`mad_prefect.http.RetryingAsyncClient` and `HttpRetryPolicy` have been removed.
Use an ordinary HTTPX client with the transport above, or your own retry client
around that transport. MAD.Prefect no longer depends on `httpx-retries`.
See [shared API rate limits](shared-api-rate-limit.md) for server configuration,
setup caching, logging and limitations.

## Progress

Implemented with tests for independent retry composition, failure separation,
redirects, cancellation and closing connections. KnowBe4 selects its own retries.

## Next Steps

Review the feature branches before release and authorized production rollout.

## Blockers & Risks

Every actual attempt must pass through the paced transport. Hidden retries or
HTTPX mounts that bypass it cannot be paced. Separate Prefect servers do not
share allowances. Provider cooldowns are not broadcast between processes.

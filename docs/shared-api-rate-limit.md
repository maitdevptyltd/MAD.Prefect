# Shared API rate limits

```python
from mad_prefect.rate_limit import ApiRateLimit

limiter = ApiRateLimit(name="knowbe4-account", requests_per_minute=40)
await limiter.acquire()
# Send one API request here.
```

Supply a name for the account's shared allowance and its requests per minute as
a positive integer. Zero and negative rates raise `ValueError` at construction.
`40` means requests are spaced about 1.5 seconds apart, across all
participating processes. It does not allow an immediate burst of 40 requests.
The implementation converts the rate to the per-second value Prefect expects.

The name identifies the **API account's allowance**, not a process or deployment.
All callers sharing that allowance must use the same name, rate and Prefect server.
If one caller specifies 40 and another specifies 50 under the same name, the
second caller fails when checking the existing limit. It never overwrites it.
Different Prefect servers do not coordinate, even with matching names.

For HTTPX, use [the transport adapter](http-retries.md) so every actual attempt,
including retries, acquires an allowance automatically. Other clients can call
`acquire()` immediately before each attempt. The caller chooses its HTTP retry
library and handles provider cooldowns such as Retry-After.

## Prefect connection recovery

Most callers only need the two arguments above. To change the number of attempts
to contact Prefect, use `max_attempts=3`. The default is four total attempts,
including the first. This setting never retries a downstream API request.

Temporary network failures, HTTP timeouts, protocol errors and HTTP
500/502/503/504 responses can be retried. Missing limits, incompatible settings,
authentication errors and cancellation stop the operation. Retry delays start at
one second, double up to ten seconds and include jitter to spread reconnecting pods.
These delay settings are internal rather than part of the constructor.

Prefect's HTTP client has its own connection/request timeouts. In the inspected
Prefect 3.2.15 source, the default HTTP request timeout is 60 seconds. This is not
a deadline for the whole acquire operation: Prefect may retry individual requests.

Waiting for an occupied allowance is different. Prefect's acquisition service
can repeat HTTP 423 responses without a time limit by default. MAD.Prefect passes
an internal 120-second allowance-wait limit so that condition eventually fails.
Downstream developers do not configure it. There is no additional overall timeout
around setup and recovery. If acquisition fails, no downstream request is sent.

Retries, recovery and exhaustion are logged through the Prefect run logger, or
Python logging outside a run. Logs include the limit name and error types, not
URLs, credentials or exception messages. No per-request Prefect tasks are created.

## Setup and reuse

Reuse the limiter instance across requests. On the first call it creates a missing
limit with capacity one, or checks an existing limit. Successful setup is cached;
later calls only acquire allowances. A lock prevents simultaneous first calls
from repeating setup. This is ordinary Python state, with no Pydantic model,
Field declarations or PrivateAttr bookkeeping in the limiter.

Stop consumers before changing the rate or server, then create fresh instances.
The instance does not poll for changes or recheck settings after successful setup.
A deleted limit fails strict acquisition rather than being silently recreated.
The feature requires Prefect 3.2.13 or later within this package's supported range.

## Updating an earlier feature-branch consumer

Replace `requests_per_second` with integer `requests_per_minute` (multiply the
old rate by 60). Remove `wait_timeout_seconds`, `retry_delay_seconds` and
`max_retry_delay_seconds` arguments. `max_attempts` remains optional.
Use the provider's documented allowance, allowing headroom for other integrations.

## Progress

Plain Python limiter, minute-based input, cached setup and Prefect recovery are
implemented. KnowBe4 uses 40 requests per minute. Tests use synthetic data.

## Next Steps

Review the feature branches before release and authorized production rollout.

## Blockers & Risks

This is request spacing, not a daily quota counter, in-flight concurrency limit
or account-wide cooldown. Integrations that do not use this limiter still consume
the provider's quota. A lost allowance response can consume another allowance
when retried, reducing throughput without permitting an unconfirmed API request.


## Reading acquisition and retries separately

`ApiRateLimit.acquire()` in `rate_limit.py` defines its acquisition steps in a
local `obtain_allowance()` function, then explicitly passes that operation to
`retry()`. The acquisition steps check setup and ask Prefect for an allowance.
Successful setup remains cached between attempts.

The separate `retry()` function handles attempts, delays and logging. It receives
an operation, an attempt limit, an error classifier and a logging description;
it does not know about the limiter or obtain allowances itself.

`is_retryable_prefect_error()` lives below the retry function and returns a boolean.
Neither function creates a Prefect task or retries a provider API request.
Each retry call has its own counter, including concurrent calls on one limiter.

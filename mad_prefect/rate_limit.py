"""Shared request pacing through Prefect's global concurrency limits."""

from __future__ import annotations

import asyncio
import logging
import random
import math
from collections.abc import Awaitable, Callable

from httpx import (
    AsyncBaseTransport,
    AsyncHTTPTransport,
    HTTPStatusError,
    NetworkError,
    RemoteProtocolError,
    TimeoutException,
    Request,
    Response,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate, GlobalConcurrencyLimitUpdate
from prefect.concurrency.asyncio import rate_limit
from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
from prefect.exceptions import MissingContextError, ObjectNotFound
from prefect.logging import get_run_logger
from prefect.settings import PREFECT_API_URL


class PrefectRateLimiter:
    """Share request spacing between processes using the same Prefect server.

    Use one non-secret name per provider account and reuse this instance across
    requests. Only successful setup is cached; each acquire still asks Prefect
    for a new allowance. HTTP retries and provider cooldowns belong to the caller.
    """

    name: str
    requests_per_minute: int
    # Total attempts to obtain an allowance from Prefect after temporary failures.
    # Includes the first attempt; does not retry downstream API requests.
    max_attempts: int

    _configuration_checked: bool
    _setup_lock: asyncio.Lock

    def __init__(self, name: str, requests_per_minute: int, *, max_attempts: int = 4) -> None:
        """Set the account's shared rate and the maximum Prefect connection attempts.

        For example, requests_per_minute=40 spaces requests about 1.5 seconds
        apart across all callers using this name on the same Prefect server.
        max_attempts includes the first attempt and affects only Prefect calls.
        """
        if requests_per_minute <= 0:
            raise ValueError("requests_per_minute must be greater than zero")

        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        self.name = name
        self.requests_per_minute = requests_per_minute
        self.max_attempts = max_attempts

        # Reuse successful setup. The lock stops concurrent first requests from
        # checking or creating the same limit more than once in this instance.
        self._configuration_checked = False
        self._setup_lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Check setup once, then ask Prefect for one rate-limit allowance.

        The local operation contains the acquisition steps. The explicit retry
        call below runs those steps and handles temporary failures using
        max_attempts. Successful setup remains cached between attempts.

        Return only when Prefect grants permission. No Prefect task is created
        and no allowance needs to be released. Cancellation stops immediately.
        """

        async def obtain_allowance() -> None:
            """Check setup and request one allowance."""
            if not PREFECT_API_URL.value():
                raise RuntimeError("Shared API rate limits require a configured PREFECT_API_URL")

            if not self._configuration_checked:
                # Concurrent callers share setup; failures leave it eligible for retry.
                async with self._setup_lock:
                    if not self._configuration_checked:
                        await self._create_or_check_prefect_limit()
                        self._configuration_checked = True

            # A successful allowance is consumed, not released. Prefect restores it
            # at the configured rate, which spaces requests across all participating pods.
            await rate_limit(
                self.name, occupy=1, strict=True,
                timeout_seconds=_PREFECT_ALLOWANCE_WAIT_SECONDS,
            )

        await retry(
            obtain_allowance,
            max_attempts=self.max_attempts,
            should_retry=is_retryable_prefect_error,
            description=f"Rate limit {self.name}",
        )

    async def _create_or_check_prefect_limit(self) -> None:
        """Read the named Prefect limit, creating it if it does not exist.

        Warn and update differing settings to an active limit with capacity one
        and the requested decay rate. The update applies to all shared callers.
        """
        # This client talks to the Prefect server, not the downstream API. Setup
        # happens once per limiter so ordinary requests only acquire an allowance.
        async with get_client() as client:
            try:
                limit = await client.read_global_concurrency_limit_by_name(self.name)
            except ObjectNotFound:
                # Capacity one prevents an initial burst. Decay controls how soon
                # another pod can consume the next allowance.
                try:
                    await client.create_global_concurrency_limit(
                        GlobalConcurrencyLimitCreate(
                            name=self.name,
                            limit=1,
                            active=True,
                            slot_decay_per_second=self.requests_per_minute / 60,
                        )
                    )
                except HTTPStatusError as error:
                    # Another pod can create the account's limit at the same time.
                    if error.response.status_code != 409:
                        raise
                limit = await client.read_global_concurrency_limit_by_name(self.name)

            # Make changes visible because this allowance is shared across callers.
            if (
                not limit.active
                or limit.limit != 1
                or not math.isclose(
                    limit.slot_decay_per_second, self.requests_per_minute / 60, rel_tol=1e-6
                )
            ):
                try:
                    logger = get_run_logger()
                except MissingContextError:
                    logger = logging.getLogger(__name__)
                logger.warning(
                    "Prefect rate limit %s has different settings; updating to active=True, "
                    "capacity=1, requests_per_minute=%d",
                    self.name, self.requests_per_minute,
                )
                await client.update_global_concurrency_limit(
                    self.name,
                    GlobalConcurrencyLimitUpdate(
                        active=True,
                        limit=1,
                        slot_decay_per_second=self.requests_per_minute / 60,
                    ),
                )


class RateLimitedAsyncTransport(AsyncBaseTransport):
    """Acquire an allowance immediately before the wrapped transport sends.

    Put retry transports outside this transport, and a non-retrying network
    transport inside it. Each retry will then come back here for a new allowance.
    Closing this transport also closes its wrapped transport.
    """

    def __init__(
        self, limiter: PrefectRateLimiter, transport: AsyncBaseTransport | None = None,
    ) -> None:
        """Use a fresh HTTPX network transport unless the caller supplies one."""
        self.limiter = limiter
        self.transport = transport if transport is not None else AsyncHTTPTransport()

    async def handle_async_request(self, request: Request) -> Response:
        """Obtain a Prefect allowance, then return the downstream HTTP response.

        Raise RateLimitAcquisitionError without sending if Prefect acquisition
        fails. Pass downstream HTTP errors through unchanged so the caller's
        retry system can handle them. Cancellation stops the operation.
        """
        try:
            await self.limiter.acquire()
        except Exception as error:
            # Keep Prefect failures separate from the HTTP errors returned by the
            # provider transport below. Cancellation is not caught here.
            raise RateLimitAcquisitionError(
                "Could not acquire a Prefect rate-limit allowance"
            ) from error

        return await self.transport.handle_async_request(request)

    async def aclose(self) -> None:
        """Release the network resources when the HTTP client closes."""
        await self.transport.aclose()


# Prefect's HTTP timeout handles unreachable servers. Its allowance service can
# otherwise repeat HTTP 423 responses forever while a limit is busy. Bound that
# wait internally so downstream developers only need to specify their API quota.
_PREFECT_ALLOWANCE_WAIT_SECONDS = 120


class RateLimitAcquisitionError(RuntimeError):
    """Prefect could not confirm an allowance; no provider request was sent.

    This is deliberately not an HTTPX network error. HTTP retry libraries must
    not mistake a Prefect outage for a failed provider request and multiply the
    limiter's bounded retries. The original exception remains available as cause.
    """


async def retry(
    operation: Callable[[], Awaitable[None]],
    *,
    max_attempts: int,
    should_retry: Callable[[Exception], bool],
    description: str,
) -> None:
    """Run an operation, retrying eligible failures up to max_attempts.

    Count the first call as an attempt. Between retries, wait with increasing
    jittered delays and log the failure. Log recovery or exhaustion as well.
    Raise the original error if it is not retryable or all attempts fail.
    Cancellation stops immediately.

    The caller supplies the operation and decides which failures are retryable.
    This function does not inspect a limiter, obtain allowances or send requests.
    Each call has its own retry counter, including concurrent calls.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")

    # Attach recovery messages to the flow when available. Standalone callers
    # still get the same messages through normal Python logging.
    try:
        logger = get_run_logger()
    except MissingContextError:
        logger = logging.getLogger(__name__)

    retry_delay_seconds = 1.0
    for attempt in range(1, max_attempts + 1):
        try:
            await operation()
        except Exception as error:
            # Log error types, not messages that might contain credentials.
            error_description = type(error).__name__
            if error.__cause__ is not None:
                error_description += f" caused by {type(error.__cause__).__name__}"

            if not should_retry(error) or attempt == max_attempts:
                logger.error(
                    "%s failed on attempt %d/%d (%s)",
                    description, attempt, max_attempts, error_description,
                )
                raise

            # Spread out reconnecting callers. Backoff starts at one second,
            # doubles after each failure and stops growing at ten seconds.
            wait_seconds = random.uniform(retry_delay_seconds / 2, retry_delay_seconds)
            logger.warning(
                "%s attempt %d/%d failed (%s); retrying in %.2fs",
                description, attempt, max_attempts, error_description, wait_seconds,
            )
            await asyncio.sleep(wait_seconds)
            retry_delay_seconds = min(retry_delay_seconds * 2, 10)
        else:
            if attempt > 1:
                logger.info("%s recovered on attempt %d/%d",
                            description, attempt, max_attempts)
            return


def is_retryable_prefect_error(error: Exception) -> bool:
    """Check whether a failed request to Prefect is worth retrying.

    Return True for network failures, timeouts, invalid HTTP responses,
    or HTTP status 500, 502, 503 or 504. These failures may be temporary.

    Return False for other errors, including missing limits, invalid settings
    and authentication failures.

    Inspect the original cause when Prefect wraps an error during rate-limit
    acquisition. This function only checks the error; it does not retry anything
    or handle requests to the downstream API.
    """
    # Prefect wraps both connection failures and missing-limit errors.
    # Inspect the original cause to distinguish between them. Repeating this
    # also handles nested wrappers without treating the wrapper as the cause.
    while isinstance(error, ConcurrencySlotAcquisitionError):
        original_error = error.__cause__

        if not isinstance(original_error, Exception):
            return False

        if original_error is error:
            return False

        error = original_error

    # These failures may resolve when we contact Prefect again.
    temporary_connection_errors = (
        NetworkError,
        TimeoutException,
        RemoteProtocolError,
    )
    if isinstance(error, temporary_connection_errors):
        return True

    # Only these server responses qualify for another attempt.
    if isinstance(error, HTTPStatusError):
        retryable_status_codes = (500, 502, 503, 504)
        return error.response.status_code in retryable_status_codes

    return False

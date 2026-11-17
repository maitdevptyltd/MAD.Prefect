"""Exercise retry policy without a limiter or a Prefect server."""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

import mad_prefect.rate_limit as module


async def test_retry_runs_an_unrelated_operation_until_it_recovers(monkeypatch):
    """The caller chooses the operation and what counts as temporary."""
    error = ValueError("synthetic temporary failure")
    operation = AsyncMock(side_effect=[error, None])
    classifier = Mock(return_value=True)
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, "sleep", sleep)

    await module.retry(
        operation, max_attempts=3, should_retry=classifier,
        description="Synthetic operation",
    )

    assert operation.await_count == 2
    classifier.assert_called_once_with(error)
    sleep.assert_awaited_once()


@pytest.mark.parametrize("retryable, expected_attempts", [(False, 1), (True, 3)])
async def test_retry_preserves_failure_and_honours_attempt_limit(
    monkeypatch, retryable, expected_attempts,
):
    error = ValueError("synthetic failure")
    operation = AsyncMock(side_effect=error)
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, "sleep", sleep)

    with pytest.raises(ValueError) as raised:
        await module.retry(
            operation, max_attempts=3, should_retry=lambda error: retryable,
            description="Synthetic operation",
        )

    assert raised.value is error
    assert operation.await_count == expected_attempts
    assert sleep.await_count == expected_attempts - 1


async def test_retry_does_not_classify_or_retry_cancellation(monkeypatch):
    operation = AsyncMock(side_effect=asyncio.CancelledError())
    classifier = Mock(return_value=True)
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, "sleep", sleep)

    with pytest.raises(asyncio.CancelledError):
        await module.retry(
            operation, max_attempts=3, should_retry=classifier,
            description="Synthetic operation",
        )

    operation.assert_awaited_once()
    classifier.assert_not_called()
    sleep.assert_not_awaited()


async def test_retry_rejects_invalid_attempt_limit_before_running():
    operation = AsyncMock()

    with pytest.raises(ValueError, match="max_attempts must be at least 1"):
        await module.retry(
            operation, max_attempts=0, should_retry=lambda error: True,
            description="Synthetic operation",
        )

    operation.assert_not_awaited()

import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

import mad_prefect.rate_limit as module
from mad_prefect.rate_limit import PrefectRateLimiter
from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
from test_rate_limit import backend


def wrapped(error):
    wrapper = ConcurrencySlotAcquisitionError('sensitive wrapper text')
    wrapper.__cause__ = error
    return wrapper


@pytest.mark.parametrize('phase', ['setup', 'acquisition'])
async def test_connection_recovers_and_logs_without_exception_payload(backend, monkeypatch, caplog, phase):
    client, acquire = backend
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    error = httpx.ConnectError('secret URL and credentials')
    if phase == 'setup':
        client.read_global_concurrency_limit_by_name.side_effect = [error, client.read_global_concurrency_limit_by_name.return_value]
    else:
        acquire.side_effect = [wrapped(error), None]
    with caplog.at_level(logging.INFO):
        rate = PrefectRateLimiter(name='test-account', requests_per_minute=40)
        await rate.acquire()
    assert sleep.await_count == 1
    assert 0.5 <= sleep.call_args.args[0] <= 1
    assert 'attempt 1/4' in caplog.text
    assert 'ConnectError' in caplog.text
    assert 'recovered on attempt 2/4' in caplog.text
    assert 'secret URL' not in caplog.text
    assert 'sensitive wrapper' not in caplog.text


async def test_exhaustion_is_bounded_and_backoff_is_capped(backend, monkeypatch, caplog):
    _client, acquire = backend
    error = wrapped(httpx.ConnectError('private'))
    acquire.side_effect = error
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    monkeypatch.setattr(module.random, 'uniform', lambda low, high: high)
    with pytest.raises(ConcurrencySlotAcquisitionError) as raised:
        await PrefectRateLimiter(name='test-account', requests_per_minute=40,
                           max_attempts=6).acquire()
    assert raised.value is error
    assert acquire.await_count == 6
    assert [call.args[0] for call in sleep.call_args_list] == [1, 2, 4, 8, 10]
    assert 'attempt 6/6' in caplog.text


@pytest.mark.parametrize('status', [401, 403, 404, 422, 429, 500, 502, 503, 504])
async def test_only_selected_server_errors_retry(backend, monkeypatch, status):
    _client, acquire = backend
    response = httpx.Response(status, request=httpx.Request('GET', 'https://synthetic.invalid'))
    error = httpx.HTTPStatusError('sensitive', request=response.request, response=response)
    acquire.side_effect = [wrapped(error), None]
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    rate = PrefectRateLimiter(name='test-account', requests_per_minute=40)
    if status >= 500:
        await rate.acquire()
        assert acquire.await_count == 2
    else:
        with pytest.raises(ConcurrencySlotAcquisitionError):
            await rate.acquire()
        assert acquire.await_count == 1


async def test_missing_limit_wrapper_is_not_retried(backend):
    _client, acquire = backend
    acquire.side_effect = ConcurrencySlotAcquisitionError('missing limit')
    with pytest.raises(ConcurrencySlotAcquisitionError):
        await PrefectRateLimiter(name='test-account', requests_per_minute=40).acquire()
    assert acquire.await_count == 1


async def test_prefect_connection_timeouts_stop_after_max_attempts(backend, monkeypatch):
    """Prefect's HTTP timeout becomes a failure, then retries remain bounded."""
    _client, acquire = backend
    acquire.side_effect = wrapped(httpx.ReadTimeout('synthetic Prefect timeout'))
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())

    with pytest.raises(ConcurrencySlotAcquisitionError):
        await PrefectRateLimiter(name='test-account', requests_per_minute=40, max_attempts=3).acquire()

    assert acquire.await_count == 3


async def test_cancellation_is_not_retried(backend):
    _client, acquire = backend
    acquire.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await PrefectRateLimiter(name='test-account', requests_per_minute=40).acquire()
    assert acquire.await_count == 1


async def test_logs_use_prefect_run_logger_when_available(backend, monkeypatch):
    _client, acquire = backend
    acquire.side_effect = [wrapped(httpx.ReadTimeout('private')), None]
    logger = Mock()
    monkeypatch.setattr(module, 'get_run_logger', lambda: logger)
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    await PrefectRateLimiter(name='test-account', requests_per_minute=40).acquire()
    logger.warning.assert_called_once()
    logger.info.assert_called_once()
    logger.error.assert_not_called()


@pytest.mark.parametrize('error, expected', [
    (httpx.ConnectError('synthetic'), True),
    (httpx.ReadTimeout('synthetic'), True),
    (httpx.RemoteProtocolError('synthetic'), True),
    (ValueError('invalid settings'), False),
    (ConcurrencySlotAcquisitionError('missing limit'), False),
    (wrapped(httpx.ConnectError('synthetic')), True),
    (wrapped(wrapped(httpx.ReadTimeout('synthetic'))), True),
    (wrapped(ValueError('invalid settings')), False),
])
def test_prefect_retry_check_returns_a_boolean(error, expected):
    """Classification answers yes or no without returning or raising an error."""
    assert module.is_retryable_prefect_error(error) is expected


def test_self_referencing_prefect_error_is_not_retryable():
    """A malformed cause must not leave classification looping forever."""
    error = ConcurrencySlotAcquisitionError('synthetic')
    error.__cause__ = error
    assert module.is_retryable_prefect_error(error) is False


async def test_concurrent_acquisitions_have_independent_retry_attempts(backend, monkeypatch):
    """Concurrent calls each get their own attempt budget, not a shared counter."""
    _client, acquire = backend
    attempts_by_task = {}

    async def fail_once_per_task(*args, **kwargs):
        current = asyncio.current_task()
        attempts_by_task[current] = attempts_by_task.get(current, 0) + 1
        if attempts_by_task[current] == 1:
            raise wrapped(httpx.ConnectError('synthetic'))

    acquire.side_effect = fail_once_per_task
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    limiter = PrefectRateLimiter(name='test-account', requests_per_minute=40, max_attempts=2)
    await asyncio.gather(limiter.acquire(), limiter.acquire())

    assert sorted(attempts_by_task.values()) == [2, 2]

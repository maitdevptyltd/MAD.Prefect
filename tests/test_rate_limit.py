import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from prefect.exceptions import ObjectNotFound

import mad_prefect.rate_limit as module
from mad_prefect.rate_limit import PrefectRateLimiter


@pytest.fixture
def backend(monkeypatch):
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.read_global_concurrency_limit_by_name.return_value = SimpleNamespace(
        active=True, limit=1, slot_decay_per_second=2 / 3
    )
    acquire = AsyncMock()
    monkeypatch.setattr(module, "get_client", lambda: client)
    monkeypatch.setattr(module, "rate_limit", acquire)
    monkeypatch.setattr(module, "PREFECT_API_URL", SimpleNamespace(value=lambda: "http://localhost/api"))
    return client, acquire


def limiter(**kwargs):
    return PrefectRateLimiter(name="test-account", requests_per_minute=40, **kwargs)


async def test_existing_limit_is_checked_once_per_instance(backend):
    client, acquire = backend
    rate = limiter()
    await rate.acquire()
    await rate.acquire()
    assert client.read_global_concurrency_limit_by_name.await_count == 1
    client.create_global_concurrency_limit.assert_not_called()
    assert acquire.await_count == 2
    acquire.assert_awaited_with("test-account", occupy=1, strict=True, timeout_seconds=120)


@pytest.mark.parametrize("race", [False, True])
async def test_creates_missing_limit_and_handles_another_pod_creating_it(backend, race):
    client, acquire = backend
    settings = client.read_global_concurrency_limit_by_name.return_value
    client.read_global_concurrency_limit_by_name.side_effect = [ObjectNotFound(Exception("missing")), settings]
    if race:
        response = httpx.Response(409, request=httpx.Request("POST", "http://local/limits"))
        client.create_global_concurrency_limit.side_effect = httpx.HTTPStatusError(
            "exists", request=response.request, response=response
        )
    await limiter().acquire()
    created = client.create_global_concurrency_limit.call_args.args[0]
    assert (created.name, created.limit, created.active, created.slot_decay_per_second) == (
        "test-account", 1, True, 2 / 3
    )
    acquire.assert_awaited_once()


@pytest.mark.parametrize("field,value", [("active", False), ("limit", 2), ("slot_decay_per_second", 2)])
async def test_mismatched_configuration_fails_without_acquiring_or_overwriting(backend, field, value):
    client, acquire = backend
    setattr(client.read_global_concurrency_limit_by_name.return_value, field, value)
    with pytest.raises(ValueError, match="incompatible"):
        await limiter().acquire()
    acquire.assert_not_called()
    client.update_global_concurrency_limit.assert_not_called()


async def test_backend_failure_prevents_acquisition(backend):
    client, acquire = backend
    client.read_global_concurrency_limit_by_name.side_effect = OSError("unavailable")
    with pytest.raises(OSError):
        await limiter().acquire()
    acquire.assert_not_called()


async def test_creation_failure_is_not_mistaken_for_a_race(backend):
    client, acquire = backend
    client.read_global_concurrency_limit_by_name.side_effect = ObjectNotFound(Exception("missing"))
    response = httpx.Response(403, request=httpx.Request("POST", "http://local/limits"))
    client.create_global_concurrency_limit.side_effect = httpx.HTTPStatusError(
        "forbidden", request=response.request, response=response
    )
    with pytest.raises(httpx.HTTPStatusError):
        await limiter().acquire()
    acquire.assert_not_called()


async def test_requires_shared_server(backend, monkeypatch):
    client, acquire = backend
    monkeypatch.setattr(module, "PREFECT_API_URL", SimpleNamespace(value=lambda: None))
    with pytest.raises(RuntimeError, match="PREFECT_API_URL"):
        await limiter().acquire()
    client.read_global_concurrency_limit_by_name.assert_not_called()
    acquire.assert_not_called()


async def test_concurrent_first_calls_share_validation(backend):
    client, acquire = backend
    entered = asyncio.Event()
    release = asyncio.Event()
    settings = client.read_global_concurrency_limit_by_name.return_value
    async def read(name):
        entered.set()
        await release.wait()
        return settings
    client.read_global_concurrency_limit_by_name.side_effect = read
    rate = limiter()
    first = asyncio.create_task(rate.acquire())
    await entered.wait()
    second = asyncio.create_task(rate.acquire())
    await asyncio.sleep(0)
    assert client.read_global_concurrency_limit_by_name.await_count == 1
    release.set()
    await asyncio.gather(first, second)
    assert client.read_global_concurrency_limit_by_name.await_count == 1
    assert acquire.await_count == 2


async def test_failed_setup_is_not_cached(backend):
    client, acquire = backend
    settings = client.read_global_concurrency_limit_by_name.return_value
    client.read_global_concurrency_limit_by_name.side_effect = [OSError('unavailable'), settings]
    rate = limiter()
    with pytest.raises(OSError):
        await rate.acquire()
    await rate.acquire()
    assert client.read_global_concurrency_limit_by_name.await_count == 2
    acquire.assert_awaited_once()


async def test_acquisition_retries_do_not_repeat_successful_setup(backend, monkeypatch):
    from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
    client, acquire = backend
    error = ConcurrencySlotAcquisitionError('synthetic')
    error.__cause__ = httpx.ConnectError('synthetic')
    acquire.side_effect = [error, None, None]
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    rate = limiter()
    await rate.acquire()
    await rate.acquire()
    client.read_global_concurrency_limit_by_name.assert_awaited_once()
    assert acquire.await_count == 3


async def test_instances_validate_independently(backend):
    client, acquire = backend
    await limiter().acquire()
    await limiter().acquire()
    assert client.read_global_concurrency_limit_by_name.await_count == 2
    assert acquire.await_count == 2


async def test_same_name_with_a_different_rate_fails_without_overwriting(backend):
    """A second caller cannot silently change the shared account's rate."""
    client, acquire = backend
    await PrefectRateLimiter(name="test-account", requests_per_minute=40).acquire()

    with pytest.raises(ValueError, match="incompatible"):
        await PrefectRateLimiter(name="test-account", requests_per_minute=50).acquire()

    acquire.assert_awaited_once()
    client.update_global_concurrency_limit.assert_not_called()


@pytest.mark.parametrize("requests_per_minute", [0, -1])
def test_rejects_non_positive_rate(requests_per_minute):
    with pytest.raises(ValueError, match="requests_per_minute must be greater than zero"):
        PrefectRateLimiter(name="test-account", requests_per_minute=requests_per_minute)

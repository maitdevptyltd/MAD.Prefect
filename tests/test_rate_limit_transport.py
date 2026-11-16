"""The shared transport paces attempts without choosing an HTTP retry policy."""

import asyncio
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from mad_prefect.rate_limit import RateLimitedAsyncTransport, RateLimitAcquisitionError


async def test_default_transport_is_created_per_adapter_and_closed(monkeypatch):
    networks = [AsyncMock(spec=httpx.AsyncBaseTransport) for _ in range(2)]
    factory = Mock(side_effect=networks)
    monkeypatch.setattr('mad_prefect.rate_limit.AsyncHTTPTransport', factory)

    first = RateLimitedAsyncTransport(AsyncMock())
    second = RateLimitedAsyncTransport(AsyncMock())
    assert first.transport is networks[0]
    assert second.transport is networks[1]
    await first.aclose()
    await second.aclose()

    assert factory.call_count == 2
    factory.assert_called_with()
    for network in networks:
        network.aclose.assert_awaited_once()


def test_custom_transport_does_not_create_default_transport(monkeypatch):
    factory = Mock()
    monkeypatch.setattr('mad_prefect.rate_limit.AsyncHTTPTransport', factory)
    network = AsyncMock(spec=httpx.AsyncBaseTransport)

    transport = RateLimitedAsyncTransport(AsyncMock(), network)

    assert transport.transport is network
    factory.assert_not_called()


async def test_plain_client_acquires_before_sending_and_does_not_retry():
    """A 503 remains the caller's decision, after exactly one paced send."""
    events = []
    limiter = AsyncMock()
    limiter.acquire.side_effect = lambda: events.append('allowance')

    def respond(request):
        events.append('send')
        return httpx.Response(503)

    transport = RateLimitedAsyncTransport(limiter, httpx.MockTransport(respond))
    async with httpx.AsyncClient(transport=transport) as client:
        response = await client.get('https://synthetic.invalid')

    assert response.status_code == 503
    assert events == ['allowance', 'send']


async def test_independent_retry_transport_acquires_for_each_attempt():
    """Any outer retry transport can call back through the pacing transport."""
    limiter = AsyncMock()
    statuses = iter([503, 200])
    network = httpx.MockTransport(lambda request: httpx.Response(next(statuses)))
    paced = RateLimitedAsyncTransport(limiter, network)

    class RetryOnce(httpx.AsyncBaseTransport):
        """A small fake proves composition without installing a retry library."""

        async def handle_async_request(self, request):
            response = await paced.handle_async_request(request)
            if response.status_code == 503:
                await response.aclose()
                response = await paced.handle_async_request(request)
            return response

        async def aclose(self):
            await paced.aclose()

    async with httpx.AsyncClient(transport=RetryOnce()) as client:
        response = await client.get('https://synthetic.invalid')

    assert response.status_code == 200
    assert limiter.acquire.await_count == 2


async def test_failed_allowance_never_sends_and_preserves_cause():
    """A Prefect outage cannot be mistaken for a provider network error."""
    limiter = AsyncMock()
    failure = httpx.ConnectError('synthetic Prefect failure')
    limiter.acquire.side_effect = failure
    network = AsyncMock(spec=httpx.AsyncBaseTransport)
    transport = RateLimitedAsyncTransport(limiter, network)

    with pytest.raises(RateLimitAcquisitionError) as raised:
        await transport.handle_async_request(httpx.Request('GET', 'https://synthetic.invalid'))

    assert raised.value.__cause__ is failure
    assert not isinstance(raised.value, httpx.TransportError)
    limiter.acquire.assert_awaited_once()
    network.handle_async_request.assert_not_awaited()


async def test_provider_network_failure_passes_through_unchanged():
    """The downstream retry system still sees the original provider error."""
    limiter = AsyncMock()
    network = AsyncMock(spec=httpx.AsyncBaseTransport)
    failure = httpx.ReadTimeout('synthetic provider failure')
    network.handle_async_request.side_effect = failure
    transport = RateLimitedAsyncTransport(limiter, network)

    with pytest.raises(httpx.ReadTimeout) as raised:
        await transport.handle_async_request(httpx.Request('GET', 'https://synthetic.invalid'))

    assert raised.value is failure
    limiter.acquire.assert_awaited_once()


async def test_cancellation_does_not_send_or_get_wrapped():
    """Stopping a flow must stop allowance waiting immediately."""
    limiter = AsyncMock()
    limiter.acquire.side_effect = asyncio.CancelledError()
    network = AsyncMock(spec=httpx.AsyncBaseTransport)
    transport = RateLimitedAsyncTransport(limiter, network)

    with pytest.raises(asyncio.CancelledError):
        await transport.handle_async_request(httpx.Request('GET', 'https://synthetic.invalid'))

    network.handle_async_request.assert_not_awaited()


async def test_client_close_closes_wrapped_transport():
    """The adapter must not leave network connections open."""
    network = AsyncMock(spec=httpx.AsyncBaseTransport)
    async with httpx.AsyncClient(transport=RateLimitedAsyncTransport(AsyncMock(), network)):
        pass
    network.aclose.assert_awaited_once()


async def test_redirect_and_caller_retry_each_acquire_allowance():
    """Retries outside HTTPX and HTTPX's redirects both return through pacing."""
    limiter = AsyncMock()

    def respond(request):
        if request.url.path == '/':
            return httpx.Response(302, headers={'Location': '/next'})
        return httpx.Response(200)

    transport = RateLimitedAsyncTransport(limiter, httpx.MockTransport(respond))
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        for _ in range(2):
            assert (await client.get('https://synthetic.invalid')).status_code == 200

    assert limiter.acquire.await_count == 4

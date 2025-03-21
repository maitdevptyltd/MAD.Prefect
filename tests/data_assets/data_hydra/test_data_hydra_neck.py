import pytest
import inspect
import asyncio
from typing import AsyncGenerator, Generator, Awaitable
from unittest.mock import MagicMock

# Assuming these are in the same module where _context_to_head is defined
# Otherwise, adjust the import paths accordingly.
from mad_prefect.data_assets.data_hydra.data_hydra_neck import (
    DataHydraNeck,
)
from mad_prefect.data_assets.data_hydra.utils import _yield_context_dicts


@pytest.fixture
def dummy_neck():
    return MagicMock(spec=DataHydraNeck)


async def test_none_context(dummy_neck):
    """
    If context is None, _context_to_head should yield exactly one DataHydraHead
    with no explicit context.
    """
    results = [head async for head in _yield_context_dicts(None)]
    assert len(results) == 1
    assert results[0] is None
    # By default, you'd expect no custom context
    assert not hasattr(results[0], "context") or results[0] is None


async def test_single_dict_context(dummy_neck):
    """
    If context is a dict, _context_to_head should yield exactly one DataHydraHead
    with that dict as context.
    """
    context = {"a": 1, "b": 2}
    results = [head async for head in _yield_context_dicts(context)]
    assert len(results) == 1
    assert isinstance(results[0], dict)
    assert results[0] == context


async def test_list_of_dicts_context(dummy_neck):
    """
    If context is a list of dicts, _context_to_head should yield a head for each dict.
    """
    context_list = [{"a": 1}, {"b": 2}, {"c": 3}]
    results = [head async for head in _yield_context_dicts(context_list)]
    assert len(results) == 3
    for i, result in enumerate(results):
        assert isinstance(result, dict)
        assert result == context_list[i]


async def test_list_nested(dummy_neck):
    """
    If context is a nested list of dicts/lists,
    _context_to_head should recursively yield heads for all dicts.
    """
    context_nested = [
        {"x": 1},
        [
            {"y": 2},
            {"z": 3},
        ],
        None,  # Should yield a single head with no context
    ]
    results = [head async for head in _yield_context_dicts(context_nested)]
    assert len(results) == 4
    # Checking each head
    assert results[0] == {"x": 1}
    assert results[1] == {"y": 2}
    assert results[2] == {"z": 3}
    # The 'None' item yields a single head with no context
    assert results[3] is None


async def test_callable_returns_dict(dummy_neck):
    """
    If context is a callable returning a dict, it should yield exactly one DataHydraHead.
    """

    def context_callable():
        return {"test": "callable_returns_dict"}

    results = [head async for head in _yield_context_dicts(context_callable)]
    assert len(results) == 1
    assert isinstance(results[0], dict)
    assert results[0] == {"test": "callable_returns_dict"}


async def test_callable_returns_awaitable_dict(dummy_neck):
    """
    If context is a callable returning an awaitable of a dict,
    it should yield exactly one DataHydraHead after awaiting.
    """

    async def async_dict():
        await asyncio.sleep(0.01)
        return {"test": "awaitable_dict"}

    def context_callable():
        return async_dict()  # returns a coroutine

    results = [head async for head in _yield_context_dicts(context_callable)]
    assert len(results) == 1
    assert isinstance(results[0], dict)
    assert results[0] == {"test": "awaitable_dict"}


async def test_callable_returns_sync_generator(dummy_neck):
    """
    If context is a callable returning a synchronous generator of dicts,
    _context_to_head should yield a head for each dict in the generator.
    """

    def sync_generator():
        yield {"one": 1}
        yield {"two": 2}

    def context_callable():
        return sync_generator()

    results = [head async for head in _yield_context_dicts(context_callable)]
    assert len(results) == 2
    assert results[0] == {"one": 1}
    assert results[1] == {"two": 2}


async def test_callable_returns_async_generator(dummy_neck):
    """
    If context is a callable returning an async generator of dicts,
    _context_to_head should yield a head for each dict from the async generator.
    """

    async def async_gen():
        yield {"alpha": "a"}
        yield {"beta": "b"}

    def context_callable():
        return async_gen()

    results = [head async for head in _yield_context_dicts(context_callable)]
    assert len(results) == 2
    assert results[0] == {"alpha": "a"}
    assert results[1] == {"beta": "b"}


async def test_callable_returns_unsupported_type(dummy_neck):
    """
    If the callable returns something that's not dict, generator, async generator, or awaitable,
    it should raise a TypeError.
    """

    def context_callable():
        return 123  # Unsupported type

    with pytest.raises(TypeError, match="Callable returned an unsupported type"):
        _ = [head async for head in _yield_context_dicts(context_callable)]  # type: ignore


async def test_unsupported_context_type(dummy_neck):
    """
    If the context itself is not None, dict, list, or callable (e.g. an integer),
    _context_to_head should raise a TypeError immediately.
    """
    with pytest.raises(TypeError, match="Unsupported context type"):
        _ = [head async for head in _yield_context_dicts(42)]  # type: ignore


async def test_callable_returns_awaitable_list_of_dicts(dummy_neck):
    """
    If the context is a callable, and when invoked it returns an awaitable
    that resolves to a list of dicts, we should yield one DataHydraHead
    per dict.
    """

    async def async_list_of_dicts():
        await asyncio.sleep(0.01)
        return [{"key1": "val1"}, {"key2": "val2"}]

    def context_callable():
        # Returns the coroutine (awaitable) that eventually yields a list of dicts
        return async_list_of_dicts()

    # Collect all heads from the async generator
    results = [head async for head in _yield_context_dicts(context_callable)]

    assert len(results) == 2, "Expected two heads for two dicts in the returned list"
    assert isinstance(results[0], dict)
    assert isinstance(results[1], dict)

    # Verify the contexts
    assert results[0] == {"key1": "val1"}
    assert results[1] == {"key2": "val2"}

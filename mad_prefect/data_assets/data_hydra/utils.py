import inspect
from typing import AsyncIterable, TypeVar
from injector import Injector
from mad_prefect.data_assets.options import ContextFactoryType


async def _yield_context_dicts(
    context: ContextFactoryType,
    scope: Injector | None = None,
):
    from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

    """
    Recursively handle `ContextFactoryType` and yield DataHydraHead instances.
    """
    # 1. None -> yield an empty context
    if context is None:
        yield None
        return

    # 2. Direct dictionary -> yield one DataHydraHead
    if isinstance(context, dict):
        yield context
        return

    # 3. A list -> iterate each item, recursively handle
    if isinstance(context, list):
        for item in context:
            async for ctx in _yield_context_dicts(item):
                yield ctx

        return

    # 4. A callable -> figure out its return type
    if callable(context):
        result = scope.call_with_injection(context) if scope else context()

        # If result is awaitable, await it
        if inspect.isawaitable(result):
            result = await result

        # After awaiting, result might be:
        #   - dict
        #   - generator of dict
        #   - async generator of dict
        #   - something else
        if isinstance(result, dict):
            # Just yield a single head
            yield result
        elif inspect.isgenerator(result):
            # Synchronous generator
            for ctx_item in result:
                yield ctx_item

        elif inspect.isasyncgen(result):
            # Asynchronous generator
            async for ctx_item in result:
                yield ctx_item

        elif isinstance(result, list):
            # If the awaited value is a list, handle each item recursively
            for item in result:
                async for sub_head in _yield_context_dicts(item):
                    yield sub_head
        else:
            # If it's not dict / (a)sync generator,
            # we either handle more types or raise an error
            raise TypeError(f"Callable returned an unsupported type: {type(result)!r}")
        return

    # 5. If none of the above matched, it's an error
    raise TypeError(f"Unsupported context type: {type(context)!r}")


T = TypeVar("T")


async def _batched(async_iterable: AsyncIterable[T], batch_size: int):
    batch: list[T] = []

    async for item in async_iterable:
        batch.append(item)

        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch

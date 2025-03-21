import asyncio
import inspect
from typing import Any, AsyncIterable, TypeVar
from mad_prefect.data_assets.data_hydra import DataHydra
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.types import ContextFactoryType
from .types import ContextFactoryType


class DataHydraNeck:
    def __init__(self, hydra: DataHydra, asset: DataAsset):
        self.hydra = hydra
        self.asset = asset

        # We are scoped to the DataAsset now
        self._scope = hydra._scope.create_child_injector()
        self._scope.binder.bind(DataAsset, to=asset)
        self._scope.binder.bind(DataHydraNeck, to=self)

    async def __call__(self, *args: Any, **kwds: Any) -> Any:
        """
        The DataHydraNeck organizes the DataHydraHead instances into a batched
        pipeline, materializes the data assets, and then returns a DataArtifact
        representing the cartesian product of all.
        """
        max_concurrency = self.hydra._options.max_concurrency

        async for batch in _batched(self.yield_hydra_heads(), max_concurrency):
            result = await asyncio.gather(*[h.materialize() for h in batch])

    async def yield_hydra_heads(self):
        from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

        """
        Yields DataHydraHeads based on the hydra's context_factory.
        The context_factory may be:
          - None
          - A single dict
          - A callable returning a dict, generator, async generator, or awaitable of a dict
          - A list of any of the above
        """
        context_factory = self.hydra._options.context_factory

        # Otherwise, delegate logic to our helper
        async for head in _context_to_head(self, context_factory):
            yield head


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


async def _context_to_head(neck: DataHydraNeck, context: ContextFactoryType):
    from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

    """
    Recursively handle `ContextFactoryType` and yield DataHydraHead instances.
    """
    # 1. None -> yield an empty context
    if context is None:
        yield DataHydraHead(neck)
        return

    # 2. Direct dictionary -> yield one DataHydraHead
    if isinstance(context, dict):
        yield DataHydraHead(neck, context)
        return

    # 3. A list -> iterate each item, recursively handle
    if isinstance(context, list):
        for item in context:
            async for head in _context_to_head(neck, item):
                yield head
        return

    # 4. A callable -> figure out its return type
    if callable(context):
        result = context(neck)  # call with `self` if needed

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
            yield DataHydraHead(neck, result)
        elif inspect.isgenerator(result):
            # Synchronous generator
            for ctx_item in result:
                yield DataHydraHead(neck, ctx_item)
        elif inspect.isasyncgen(result):
            # Asynchronous generator
            async for ctx_item in result:
                yield DataHydraHead(neck, ctx_item)
        elif isinstance(result, list):
            # If the awaited value is a list, handle each item recursively
            for item in result:
                async for sub_head in _context_to_head(neck, item):
                    yield sub_head
        else:
            # If it's not dict / (a)sync generator,
            # we either handle more types or raise an error
            raise TypeError(f"Callable returned an unsupported type: {type(result)!r}")
        return

    # 5. If none of the above matched, it's an error
    raise TypeError(f"Unsupported context type: {type(context)!r}")

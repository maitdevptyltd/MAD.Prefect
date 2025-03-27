from dataclasses import dataclass
import inspect
from typing import Protocol
from injector import Injector, inject
from mad_prefect.data_assets.options import ContextFactoryType, DataHydraOptions


@inject
@dataclass
class DataHydraContextFactory:
    options: DataHydraOptions
    scope: Injector

    async def yield_contexts(self):
        """
        Yields DataHydraHeads based on the hydra's context_factory.
        The context_factory may be:
          - None
          - A single dict
          - A callable returning a dict, generator, async generator, or awaitable of a dict
          - A list of any of the above
        """
        context_factory = self.options.context_factory

        async for ctx in _yield_context_dicts(context_factory, self.scope):
            yield ctx


async def _yield_context_dicts(
    context: ContextFactoryType,
    scope: Injector | None = None,
):
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

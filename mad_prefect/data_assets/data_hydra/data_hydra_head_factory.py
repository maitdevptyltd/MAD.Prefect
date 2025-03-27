from dataclasses import dataclass
from typing import Protocol
from injector import Injector, inject
from mad_prefect.data_assets.data_asset import DataAsset


@inject
@dataclass
class DataHydraHeadFactory:
    from .data_hydra_context_factory import (
        DataHydraContextFactory,
    )

    context_factory: DataHydraContextFactory

    async def create_hydra_heads(self, assets: list[DataAsset], scope: Injector):
        from .data_hydra_head import DataHydraHead

        """
        Yields DataHydraHeads based on the hydra's context_factory.
        The context_factory may be:
          - None
          - A single dict
          - A callable returning a dict, generator, async generator, or awaitable of a dict
          - A list of any of the above
        """
        async for ctx in self.context_factory.yield_contexts():
            # For each context we will generate a head, and each head needs an asset
            # Find all the asset properties within the cls
            for asset in assets:
                # Create a head for each asset
                # head = self.scope.get(DataHydraHead)
                head = scope.create_object(
                    DataHydraHead,
                    additional_kwargs={"context": ctx, "asset": asset},
                )

                yield head

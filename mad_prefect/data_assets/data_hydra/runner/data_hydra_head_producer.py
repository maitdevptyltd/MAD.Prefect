from dataclasses import dataclass
from typing import Protocol
from injector import Injector
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.utils import _yield_context_dicts
from mad_prefect.data_assets.options import DataHydraOptions


@dataclass(kw_only=True)
class DataHydraHeadProducer(Protocol):
    from mad_prefect.data_assets.data_hydra import DataHydra
    from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

    options: DataHydraOptions
    scope: Injector
    hydra: DataHydra
    heads = list[DataHydraHead]()

    async def produce_hydra_heads(self):
        from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

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
            # For each context we will generate a head, and each head needs an asset
            # Find all the asset properties within the cls
            for asset in self.hydra.assets:
                # Create a head for each asset
                # head = self.scope.get(DataHydraHead)
                head = self.scope.create_object(
                    DataHydraHead,
                    additional_kwargs={"context": ctx, "asset": asset},
                )
                self.heads.append(head)
                yield head

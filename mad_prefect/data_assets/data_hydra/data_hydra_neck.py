import asyncio
from typing import Any
from mad_prefect.data_assets.data_hydra import DataHydra
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.utils import _batched, _yield_context_dicts
from ..options import DataHydraOptions


class DataHydraNeck:
    def __init__(self, hydra: DataHydra, asset: DataAsset, options: DataHydraOptions):
        self.hydra = hydra
        self.asset = asset
        self.options = options

        # We are scoped to the DataAsset now
        self._scope = hydra._scope.create_child_injector()
        self._scope.binder.bind(DataAsset, to=asset)
        self._scope.binder.bind(DataHydraNeck, to=self)

    async def __call__(self, *args: Any, **kwds: Any) -> Any:
        from mad_prefect.data_assets.data_hydra.data_hydra_artifact import (
            DataHydraArtifact,
        )

        """
        The DataHydraNeck organizes the DataHydraHead instances into a batched
        pipeline, materializes the data assets, and then returns a DataArtifact
        representing the cartesian product of all.
        """
        max_concurrency = self.options.max_concurrency
        hydra_heads = []

        async for batch in _batched(self.yield_hydra_heads(), max_concurrency):
            await asyncio.gather(*[h.materialize() for h in batch])
            hydra_heads.extend(batch)

        return DataHydraArtifact(self.options.path, hydra_heads=hydra_heads)

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
        context_factory = self.options.context_factory

        # Otherwise, delegate logic to our helper
        async for ctx in _yield_context_dicts(context_factory):
            yield DataHydraHead(self, ctx)

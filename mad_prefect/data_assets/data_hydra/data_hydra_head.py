from asyncio import Queue
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Protocol
from injector import Injector, inject
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra import DataHydra
from mad_prefect.data_assets.data_hydra.utils import _yield_context_dicts
from mad_prefect.data_assets.options import DataHydraOptions


@inject
@dataclass(kw_only=True)
class DataHydraHead:
    asset: DataAsset
    scope: Injector
    hydra: DataHydra
    options: DataHydraOptions
    context: dict = field(default_factory=dict)

    def __post_init__(self):
        self.scope = self.scope.create_child_injector()
        self.scope.binder.bind(DataHydraHead, to=self)
        self.scope.binder.bind(dict, to=self.context)

        # The path in options comes from the root DataHydra (the class)
        # so it should be considered the base path
        all_params = {
            **self.cls_instance.__dict__,
            **self.context,
            "context": self.context,
        }

        absolute_path = Path(self.options.path) / Path(self.asset.path)
        absolute_path = absolute_path.as_posix()
        absolute_path = absolute_path.format(**all_params)

        # Override the DataHydraHead's path with the absolute path
        self.asset = self.asset.with_options(path=absolute_path)
        self.scope.binder.bind(DataAsset, to=self.asset)

    @cached_property
    def cls_instance(self):
        # The DataHydra represents a class, and injects its dependencies.
        # We use cached_property to only inject the dependencies once, lazily.
        return self.scope.call_with_injection(self.hydra.cls, kwargs=self.context)

    async def materialize(self):
        # Get the instance to the original Hydra class instance
        instance = self.cls_instance

        # Ensure to pass through instance for the self argument
        result: DataArtifact = await self.asset(instance)
        self._artifact = result
        return result


@dataclass(kw_only=True)
class DataHydraHeadProducer(Protocol):
    options: DataHydraOptions
    scope: Injector
    hydra: DataHydra

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
            for _, attr in self.hydra.cls.__dict__.items():
                if isinstance(attr, DataAsset):
                    # Create a head for each asset
                    # head = self.scope.get(DataHydraHead)
                    head = self.scope.create_object(
                        DataHydraHead,
                        additional_kwargs={"context": ctx, "asset": attr},
                    )
                    yield head

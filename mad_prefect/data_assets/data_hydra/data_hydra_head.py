from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from injector import Injector, inject
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.options import DataHydraOptions


@inject
@dataclass
class DataHydraHead:
    scope: Injector
    cls: type
    options: DataHydraOptions

    asset: DataAsset
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
        return self.scope.create_object(self.cls, additional_kwargs=self.context)

    async def materialize(self):
        # Get the instance to the original Hydra class instance
        instance = self.cls_instance

        # Ensure to pass through instance for the self argument
        result: DataArtifact = await self.asset(instance)
        self._artifact = result
        return result

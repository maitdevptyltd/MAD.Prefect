from functools import cached_property
from mad_prefect.data_assets.data_asset import DataAsset
from injector import Injector, inject
import asyncio
from dataclasses import dataclass
from typing import Literal, Protocol


@dataclass
class DataHydraRunOptions:
    assets: list[DataAsset] | None = None


@inject
@dataclass(kw_only=True)
class DataHydraRun:
    options: DataHydraRunOptions
    scope: Injector

    def __post_init__(self):
        self.scope = self.scope.create_child_injector()
        self.scope.binder.bind(DataHydraRun, to=self)

        self.state: Literal["new", "running", "complete", "error"] = "new"
        self._future = asyncio.Future[DataHydraRun]()

    def __await__(self):
        return self._future.__await__()


@inject
@dataclass
class DataHydraRunFactory(Protocol):
    scope: Injector

    @cached_property
    def runner(self):
        from .data_hydra_runner import (
            DataHydraRunner,
        )

        # We have one instance of DataHydraRunner per DataHydra
        return self.scope.get(DataHydraRunner)

    def __call__(self, *assets: DataAsset):
        run_options = DataHydraRunOptions(assets=list(assets))
        return self.runner.run(run_options)

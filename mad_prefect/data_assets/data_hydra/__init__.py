from functools import cached_property
from typing import TypeVar
from injector import Injector
from typing import Generic

T = TypeVar("T")


class DataHydra(Generic[T]):
    from mad_prefect.data_assets.options import DataHydraOptions

    def __init__(self, cls: type[T], options: DataHydraOptions):
        from mad_prefect.data_assets.data_hydra.data_hydra_module import DataHydraModule

        self.cls = cls
        self.options = options
        self.scope = Injector(DataHydraModule(self))

    @cached_property
    def runner(self):
        from .data_hydra_runner import (
            DataHydraRunner,
        )

        # We have one instance of DataHydraRunner per DataHydra
        return self.scope.get(DataHydraRunner)

    def __call__(self):
        return self.runner.run()

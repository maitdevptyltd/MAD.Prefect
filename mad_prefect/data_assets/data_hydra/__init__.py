from typing import TypeVar
from injector import Injector
from typing import Generic
from .data_hydra_run import DataHydraRunFactory

T = TypeVar("T")


class DataHydra(Generic[T], DataHydraRunFactory):
    from mad_prefect.data_assets.options import DataHydraOptions

    def __init__(self, cls: type[T], options: DataHydraOptions):
        from mad_prefect.data_assets.data_hydra.data_hydra_module import DataHydraModule

        self.cls = cls
        self.options = options
        self.scope = Injector(DataHydraModule(self))

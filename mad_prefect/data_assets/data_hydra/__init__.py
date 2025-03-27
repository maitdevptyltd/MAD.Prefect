from functools import cached_property
from typing import TypeVar
from injector import Injector
from typing import Generic
from .data_hydra_run import DataHydraRunFactory

T = TypeVar("T")


class DataHydra(DataHydraRunFactory, Generic[T]):
    from mad_prefect.data_assets.options import DataHydraOptions

    def __init__(self, cls: type[T], options: DataHydraOptions):
        from mad_prefect.data_assets.data_hydra.data_hydra_module import DataHydraModule

        self.cls = cls
        self.options = options
        self.scope = Injector(DataHydraModule(self))

    @cached_property
    def assets(self):
        from mad_prefect.data_assets.data_asset import DataAsset

        result = list[DataAsset]()

        for _, attr in self.cls.__dict__.items():
            if isinstance(attr, DataAsset):
                yield attr

        return result

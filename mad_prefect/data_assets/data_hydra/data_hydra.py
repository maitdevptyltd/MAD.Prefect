from dataclasses import dataclass
from functools import cached_property
from typing import TypeVar
from injector import (
    Binder,
    ClassProvider,
    InstanceProvider,
    inject,
    singleton,
)
from typing import Generic

from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.data_hydra_head_factory import (
    DataHydraHeadFactory,
)

from .data_hydra_context_factory import (
    DataHydraContextFactory,
)
from .runner.data_hydra_run_factory import DataHydraRunFactory
from mad_prefect.data_assets.options import DataHydraOptions

T = TypeVar("T")


@inject
@singleton
@dataclass
class DataHydra(
    DataHydraRunFactory,
    DataHydraContextFactory,
    DataHydraHeadFactory,
    Generic[T],
):
    cls: type[T]
    options: DataHydraOptions
    binder: Binder

    def __post_init__(self):
        # Wrap in a dataclass so all the declares props become kwargs in its __init__
        self.binder.bind(type, to=InstanceProvider(dataclass(self.cls)))

        # Register the cls class so that we can inject it later
        # which will allow us to inject dependencies into the instance of the cls
        self.binder.bind(self.cls, to=ClassProvider(self.cls))

        # Make it so object can be used as an alias to inject the instance
        self.binder.bind(object, to=ClassProvider(self.cls))

        self.binder.multibind(
            list[DataAsset],
            to=self.assets,
        )

    @cached_property
    def assets(self):
        result = list[DataAsset]()

        for _, attr in self.cls.__dict__.items():
            if isinstance(attr, DataAsset):
                result.append(attr)

        return result

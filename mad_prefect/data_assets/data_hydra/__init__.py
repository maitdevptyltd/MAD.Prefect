from typing import TypeVar
from injector import (
    Injector,
)


from mad_prefect.data_assets.data_hydra.data_hydra import DataHydra
from mad_prefect.data_assets.options import DataHydraOptions

T = TypeVar("T")


def hydra(cls: type[T], options: DataHydraOptions) -> DataHydra[T]:
    # Create the root scope which has the DataHydra module register
    scope = Injector()
    binder = scope.binder
    binder.bind(DataHydraOptions, to=options)

    # Create an instance, inject missing kwargs
    return scope.create_object(
        DataHydra,
        additional_kwargs={"cls": cls},
    )

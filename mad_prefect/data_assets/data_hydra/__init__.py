from functools import cached_property
from inspect import get_annotations
from typing import Generic, Self, TypeVar, Union, cast
from pydantic import BaseModel
from injector import ClassProvider, Injector
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.types import DataHydraOptions


T = TypeVar("T")


class DataHydra(Generic[T]):
    # Make DataHydra's instance signature match T, purely for intellisense
    def __new__(cls, asset_cls: type[T], options: DataHydraOptions) -> Union[T, Self]:
        instance = super().__new__(cls)
        return cast(T, instance)

    def __init__(self, asset_cls: type[T], options: DataHydraOptions):
        self._asset_cls = asset_cls
        self._options = options
        self._scope = Injector()

        # Register the asset_cls class so that we can inject it later
        # which will allow us to inject the dependencies of the asset_cls
        self._scope.binder.bind(
            self._asset_cls,
            to=ClassProvider(self._asset_cls),
        )

        # Register the DataHydra so we can inject it later
        self._scope.binder.bind(DataHydra, to=self)

        # Provide default options that will be overridden potentially
        # from the user via the asset decorator
        self._scope.binder.bind(DataHydraOptions, to=options)

    def __getattr__(self, name: str):
        if name == "__scope__":
            return

        from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck

        # If the attr we're trying to get is a DataAsset, then we spawn a DataHydraNeck
        val, type = self.get_value_and_type(name)

        # TODO: check if we've already grown a neck for this asset
        if val and issubclass(type, DataAsset):
            return DataHydraNeck(self, val, self._scope.get(DataHydraOptions))

        raise ValueError(
            f"DataHydra cannot resolve attribute {name} in {self._asset_cls} because the attribute is not a DataAsset"
        )

    def get_value_and_type(self, attr: str):
        attr_val = getattr(self._asset_cls, attr, None)

        if isinstance(attr_val, DataAsset):
            return attr_val, DataAsset

        if not attr_val:
            # If the attribute is part of the asset_cls, we need to ensure it is a DataAsset
            if issubclass(self._asset_cls, BaseModel):
                annot = self._asset_cls.model_fields.get(attr)
                annot = annot.annotation if annot else None
        else:
            annot = get_annotations(attr_val)

        return attr_val, cast(type, annot)

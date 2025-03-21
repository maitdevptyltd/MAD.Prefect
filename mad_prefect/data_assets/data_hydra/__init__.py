import asyncio
from inspect import get_annotations
from typing import Callable, Generic, TypeVar, cast
from pydantic import BaseModel, ConfigDict
from typing import Callable, AsyncGenerator, Generator, Union
from injector import ClassProvider, inject, Injector, BoundKey
import pytest

from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_asset import DataAsset

ContextFactoryType = Union[
    dict,
    Callable[..., dict],
    Callable[..., Generator[dict, None, None]],
    Callable[..., AsyncGenerator[dict, None]],
    list[dict],
    list[Callable[..., dict]],
    list[Callable[..., Generator[dict, None, None]]],
    list[Callable[..., AsyncGenerator[dict, None]]],
    None,
]


class DataHydraOptions(BaseModel):
    context_factory: ContextFactoryType = None


T = TypeVar("T")


class DataHydra(Generic[T]):
    # Make DataHydra's instance signature match T, purely for intellisense
    def __new__(cls, asset_cls: type[T], options: DataHydraOptions) -> T:
        instance = super().__new__(cls)
        return cast(T, instance)

    def __init__(self, asset_cls: type[T], options: DataHydraOptions):
        self._asset_cls = asset_cls
        self._options = options
        self._injector = Injector()

    def __getattr__(self, name: str):
        from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

        # If the attr we're trying to get is a DataAsset, then we spawn a DataHydraHead
        val, type = self.get_value_and_type(name)

        if val and issubclass(type, DataAsset):
            return DataHydraHead(hydra=self, asset=val)

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

        return attr_val, annot

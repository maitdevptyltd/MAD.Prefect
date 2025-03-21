from typing import AsyncGenerator, Awaitable, Callable, Generator, Union
from pydantic import BaseModel


ContextFactoryResultType = dict | list[dict]
ContextFactoryType = Union[
    ContextFactoryResultType,
    Callable[..., ContextFactoryResultType],
    Callable[..., Generator[ContextFactoryResultType, None, None]],
    Callable[..., AsyncGenerator[ContextFactoryResultType, None]],
    Callable[..., Awaitable[ContextFactoryResultType]],
    list[Callable[..., ContextFactoryResultType]],
    list[Callable[..., Generator[ContextFactoryResultType, None, None]]],
    list[Callable[..., AsyncGenerator[ContextFactoryResultType, None]]],
    list[Callable[..., Awaitable[ContextFactoryResultType]]],
    None,
]


class DataHydraOptions(BaseModel):
    context_factory: ContextFactoryType = None
    max_concurrency: int = 5

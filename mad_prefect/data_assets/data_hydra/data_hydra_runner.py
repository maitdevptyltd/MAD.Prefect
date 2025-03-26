from asyncio import Queue
import asyncio
from dataclasses import dataclass
from typing import TypeVar
from injector import Injector, inject
from mad_prefect.data_assets.data_hydra.data_hydra_head import (
    DataHydraHead,
    DataHydraHeadProducer,
)

T = TypeVar("T")


@inject
@dataclass
class DataHydraRunner:
    scope: Injector

    def __post_init__(self):
        self.scope = self.scope.create_child_injector()
        self.scope.binder.bind(DataHydraRunner, to=self)

    def run(
        self,
    ):
        return self.scope.get(DataHydraRun)


@inject
@dataclass(kw_only=True)
class DataHydraRun(DataHydraHeadProducer):
    __queue = Queue[DataHydraHead | None]()

    scope: Injector
    heads = list[DataHydraHead]()

    def __post_init__(self):
        self.scope = self.scope.create_child_injector()
        self.scope.binder.bind(DataHydraRun, to=self)

    async def run(self):
        # As part of the run process, we have to discover the Hydra Heads.
        generator = self.produce_hydra_heads()

        # At the same time, we want to consume the heads and materialize the assets
        async with asyncio.TaskGroup() as group:
            # Spin up a group of workers to process the heads as they come in concurrently
            count_workers = range(self.options.max_concurrency - 1)
            [group.create_task(self.__worker__()) for _ in count_workers]

            # Begin discovering the heads
            async for head in generator:
                # Keep track of it
                self.heads.append(head)
                self.__queue.put_nowait(head)

            # Tell the workers to turn off, no more is coming
            [self.__queue.put_nowait(None) for _ in count_workers]

        return self

    async def __worker__(self):
        while True:
            head = await self.__queue.get()
            try:
                if not head:
                    return

                await head.materialize()

            finally:
                self.__queue.task_done()

    def __await__(self):
        yield from self.run().__await__()
        return self

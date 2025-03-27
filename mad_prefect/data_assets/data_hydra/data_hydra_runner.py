from asyncio import Queue
import asyncio
from dataclasses import dataclass
from typing import TypeVar
from injector import Injector, inject
from mad_prefect.data_assets.data_hydra.data_hydra_run import DataHydraRun
from mad_prefect.data_assets.data_hydra.data_hydra_head import (
    DataHydraHead,
)
from mad_prefect.data_assets.options import DataHydraOptions

T = TypeVar("T")


@inject
@dataclass
class DataHydraRunner:
    __queue = Queue[DataHydraHead | DataHydraRun | None]()

    scope: Injector
    options: DataHydraOptions

    def __post_init__(self):
        self.scope = self.scope.create_child_injector()
        self.scope.binder.bind(DataHydraRunner, to=self)

        # Spin up a group of workers to process the heads as they come in concurrently
        asyncio.gather(
            *[
                asyncio.create_task(self.__worker__())
                for _ in range(self.options.max_concurrency - 1)
            ]
        )

    def run(self):
        run = self.scope.get(DataHydraRun)
        self.__queue.put_nowait(run)

        return run

    def stop(self):
        # Tell the workers to turn off, no more is coming
        [self.__queue.put_nowait(None) for _ in range(self.options.max_concurrency - 1)]

    async def __worker__(self):
        while True:
            head_or_run = await self.__queue.get()
            try:
                if not head_or_run:
                    return

                # If its a run, enqueue its heads
                if isinstance(head_or_run, DataHydraRun):

                    if head_or_run.state == "new":
                        head_or_run.state = "running"
                        # Queue the heads for processing
                        async for h in head_or_run.produce_hydra_heads():
                            self.__queue.put_nowait(h)
                    else:
                        if all([x._artifact for x in head_or_run.heads]):
                            head_or_run.state = "complete"
                            head_or_run._future.set_result(head_or_run)
                            continue

                    self.__queue.put_nowait(head_or_run)
                    continue

                await head_or_run.materialize()

            finally:
                self.__queue.task_done()

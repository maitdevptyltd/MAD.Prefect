from asyncio import Queue
import asyncio
from dataclasses import dataclass
from typing import TypeVar
from injector import Injector, inject, singleton
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.data_hydra_head_factory import (
    DataHydraHeadFactory,
)
from mad_prefect.data_assets.options import DataHydraOptions

T = TypeVar("T")


@singleton
@inject
@dataclass
class DataHydraRunner:

    from .data_hydra_run import DataHydraRun, DataHydraRunOptions
    from ..data_hydra_head import DataHydraHead

    scope: Injector
    options: DataHydraOptions
    assets: list[DataAsset]
    head_factory: DataHydraHeadFactory

    _queue = Queue[DataHydraHead | DataHydraRun | None]()

    def __post_init__(self):
        # Spin up a group of workers to process the heads as they come in concurrently
        asyncio.gather(
            *[
                asyncio.create_task(self.__worker__())
                for _ in range(self.options.max_concurrency - 1)
            ]
        )

    def run(self, run: DataHydraRun):
        self._queue.put_nowait(run)
        return run

    def stop(self):
        # Tell the workers to turn off, no more is coming
        [self._queue.put_nowait(None) for _ in range(self.options.max_concurrency - 1)]

    async def __worker__(self):
        from .data_hydra_run import DataHydraRun

        while True:
            head_or_run = await self._queue.get()
            try:
                if not head_or_run:
                    return

                # If its a run, enqueue its heads
                if isinstance(head_or_run, DataHydraRun):
                    run = head_or_run

                    if run.state == "new":
                        run.state = "running"

                        heads = self.head_factory.create_hydra_heads(
                            run.options.assets or self.assets,
                            run.scope,
                        )

                        # Queue the heads for processing
                        async for h in heads:
                            self._queue.put_nowait(h)
                            run.result.heads.append(h)
                    else:
                        if all([x._artifact for x in run.result.heads]):
                            run.state = "complete"
                            run._future.set_result(run.result)
                            continue

                    self._queue.put_nowait(head_or_run)
                    continue

                head = head_or_run
                await head.materialize()

            finally:
                self._queue.task_done()

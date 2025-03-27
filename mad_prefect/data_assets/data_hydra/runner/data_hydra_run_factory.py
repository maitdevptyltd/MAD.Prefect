from dataclasses import dataclass
from functools import cached_property
from injector import Injector, inject
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.data_assets.data_hydra.runner.data_hydra_run import (
    DataHydraRun,
    DataHydraRunOptions,
)


@inject
@dataclass
class DataHydraRunFactory:
    scope: Injector

    @cached_property
    def runner(self):
        # This is a cached_property so that the DataHydraRunner is loaded lazily
        # and thus the list[DataAssets] have been provided by the hydra
        from . import (
            DataHydraRunner,
        )

        # We have one instance of DataHydraRunner per DataHydra
        return self.scope.get(DataHydraRunner)

    def __call__(self, *assets: DataAsset):
        run_options = DataHydraRunOptions(assets=list(assets))
        run = self.scope.create_object(
            DataHydraRun,
            additional_kwargs={"options": run_options},
        )

        return self.runner.run(run)

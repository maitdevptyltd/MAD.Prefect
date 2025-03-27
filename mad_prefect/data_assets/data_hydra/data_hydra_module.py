from injector import Binder, ClassProvider, Module


class DataHydraModule(Module):
    from mad_prefect.data_assets.data_hydra import DataHydra

    def __init__(self, hydra: DataHydra):
        self.hydra = hydra

    def configure(self, binder: Binder):
        from mad_prefect.data_assets.data_hydra import DataHydra
        from .data_hydra_head import DataHydraHead
        from .data_hydra_run import DataHydraRun
        from .data_hydra_runner import (
            DataHydraRunner,
        )
        from mad_prefect.data_assets.options import DataHydraOptions

        # Register the cls class so that we can inject it later
        # which will allow us to inject dependencies into the instance of the cls
        binder.bind(
            self.hydra.cls,
            to=ClassProvider(self.hydra.cls),
        )

        # Make it so object can be used as an alias to inject the instance
        binder.bind(object, to=ClassProvider(self.hydra.cls))

        # Register the DataHydra so we can inject it later
        binder.bind(DataHydra, to=self.hydra)
        binder.bind(DataHydraHead)
        binder.bind(DataHydraRunner)
        binder.bind(DataHydraRun)

        # Provide default options that will be overridden potentially
        # from the user via the asset decorator
        binder.bind(DataHydraOptions, to=self.hydra.options)

from mad_prefect.data_assets.options import DataHydraOptions
from typing import TypeVar
from injector import ClassProvider, Injector
from typing import Generic

T = TypeVar("T")


class DataHydra(Generic[T]):
    def __init__(self, cls: type[T], options: DataHydraOptions):
        from .data_hydra_head import DataHydraHead
        from .data_hydra_runner import (
            DataHydraRun,
            DataHydraRunner,
        )

        self.cls = cls
        self._options = options
        self._scope = Injector()

        # Register the cls class so that we can inject it later
        # which will allow us to inject dependencies into the instance of the cls
        self._scope.binder.bind(
            self.cls,
            to=ClassProvider(self.cls),
        )

        # Make it so object can be used as an alias to inject the instance
        self._scope.binder.bind(object, to=ClassProvider(self.cls))

        # Register the DataHydra so we can inject it later
        self._scope.binder.bind(DataHydra, to=self)
        self._scope.binder.bind(DataHydraHead)
        self._scope.binder.bind(DataHydraRunner)
        self._scope.binder.bind(DataHydraRun)

        # Provide default options that will be overridden potentially
        # from the user via the asset decorator
        self._scope.binder.bind(DataHydraOptions, to=options)

    def to_runner(self):
        from .data_hydra_runner import (
            DataHydraRunner,
        )

        return self._scope.get(DataHydraRunner)

    def __call__(self):
        return self.to_runner().run()

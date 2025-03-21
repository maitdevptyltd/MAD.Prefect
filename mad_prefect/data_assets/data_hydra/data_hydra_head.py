from functools import cached_property
from pathlib import Path
from mad_prefect.data_assets.options import DataHydraOptions


class DataHydraHead:
    from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck

    def __init__(self, neck: DataHydraNeck, context: dict | None = None):
        self.neck = neck
        self.context = context or {}

        self._scope = neck.hydra._scope.create_child_injector()
        self._scope.binder.bind(neck.hydra._asset_cls)
        self._scope.binder.bind(DataHydraHead, to=self)
        self._scope.binder.bind(dict, to=context)

        self.options = self._scope.get(DataHydraOptions)

        # The path in options comes from the root DataHydra (the class)
        # so it should be considered the base path.
        all_params = {
            **self.asset_cls_instance.__dict__,
            **self.context,
            "context": context,
        }

        absolute_path = Path(self.options.path) / Path(self.neck.asset.path)
        absolute_path = absolute_path.as_posix()
        absolute_path = absolute_path.format(**all_params)

        # Override the DataHydraHead's path with the absolute path
        self.asset = self.neck.asset.with_options(path=absolute_path)

    @cached_property
    def asset_cls_instance(self):
        # The DataHydra represents a class, and injects its dependencies.
        # We use cached_property to only inject the dependencies once, lazily.
        return self._scope.call_with_injection(
            self.neck.hydra._asset_cls, kwargs=self.context
        )

    async def materialize(self):
        # Get the instance to the original Hydra class instance
        instance = self.asset_cls_instance

        # Ensure to pass through instance for the self argument
        result = await self.asset(instance)
        return result

from functools import cached_property
from injector import inject


class DataHydraHead:
    from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck

    def __init__(self, neck: DataHydraNeck, context: dict | None = None):
        self.neck = neck
        self.context = context or {}

        self._scope = neck.hydra._scope.create_child_injector()
        self._scope.binder.bind(neck.hydra._asset_cls)
        self._scope.binder.bind(DataHydraHead, to=self)
        self._scope.binder.bind(dict, to=context)

    @cached_property
    def asset_cls_instance(self):
        # The DataHydra represents a class, and injects its dependencies.
        # We use cached_property to only inject the dependencies once, lazily.
        return self._scope.call_with_injection(
            self.neck.hydra._asset_cls, kwargs=self.context
        )

    async def materialize(self):
        # We need to modify the asset's _fn and inject the context
        # as well as the dependencies of the asset
        instance = self.asset_cls_instance
        asset = self.neck.asset
        # asset._bound_arguments = asset._fn_signature.bind_partial(**self.context)

        result = await asset()
        return result

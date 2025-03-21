class DataHydraHead:
    from mad_prefect.data_assets.data_hydra.data_hydra_neck import DataHydraNeck

    def __init__(self, neck: DataHydraNeck, context: dict | None = None):
        self.neck = neck
        self.context = context

        self._scope = neck.hydra._scope.create_child_injector()
        self._scope.binder.bind(DataHydraHead, to=self)
        self._scope.binder.bind(dict, to=context)

    async def materialize(self):
        asset = self.neck.asset

        # Now we just need to ensure the asset materializes with the context
        # and dependency injection
        await self._scope.call_with_injection(asset)

class DataHydraRunResult:
    from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

    heads = list[DataHydraHead]()

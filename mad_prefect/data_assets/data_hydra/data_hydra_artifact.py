from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions


class DataHydraArtifact(DataArtifact):
    from mad_prefect.data_assets.data_hydra.data_hydra_head import DataHydraHead

    def __init__(
        self,
        path: str,
        hydra_heads: list[DataHydraHead],
        data: object | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        self.hydra_heads = hydra_heads

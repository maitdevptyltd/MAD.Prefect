from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions
from mad_prefect.data_assets.utils import yield_data_batches
from mad_prefect.data_assets.data_artifact import DataArtifact


class DataArtifactCollector:

    def __init__(
        self,
        collector: object,
        dir: str,
        filetype: ARTIFACT_FILE_TYPES = "json",
        artifacts: list[DataArtifact] | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        self.collector = collector
        self.dir = dir
        self.filetype = filetype
        self.artifacts = artifacts or []
        self.read_json_options = read_json_options or ReadJsonOptions()
        self.read_csv_options = read_csv_options or ReadCSVOptions()

    async def collect(self):
        fragment_num = 0

        async for fragment in yield_data_batches(self.collector):
            # If the output isn't a DataArtifact manually set the params & base_path
            # and initialize the output as a DataArtifact
            if isinstance(fragment, DataArtifact):
                fragment_artifact = fragment
            else:
                path = self._build_artifact_path(self.dir, fragment_number=fragment_num)
                fragment_artifact = DataArtifact(
                    path,
                    fragment,
                    self.read_json_options,
                    self.read_csv_options,
                )

            if await fragment_artifact.persist():
                self.artifacts.append(fragment_artifact)
                fragment_num += 1

        return self.artifacts

    def _build_artifact_path(
        self,
        base_path: str,
        params: dict | None = None,
        fragment_number: int | None = None,
    ):
        filetype = self.filetype
        base_path = base_path.rstrip("/")

        if params is None:
            return f"{base_path}/fragment={fragment_number}.{filetype}"

        params_path = "/".join(f"{key}={value}" for key, value in params.items())

        return f"{base_path}/{params_path}.{filetype}"

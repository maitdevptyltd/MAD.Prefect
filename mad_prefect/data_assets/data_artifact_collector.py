import duckdb
import httpx
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.utils import yield_data_batches


class DataArtifactCollector:

    collector: object
    dir: str
    artifacts: list[DataArtifact]
    filetype: ARTIFACT_FILE_TYPES

    def __init__(
        self,
        collector: object,
        dir: str,
        filetype: ARTIFACT_FILE_TYPES = "json",
        artifacts: list[DataArtifact] | None = None,
    ):
        self.collector = collector
        self.dir = dir
        self.filetype = filetype
        self.artifacts = artifacts or []

    async def collect(self):
        fragment_num = 0

        async for fragment in yield_data_batches(self.collector):
            # If the output isn't a DataAssetArtifact manually set the params & base_path
            # and initialize the output as a DataAssetArtifact
            params = (
                dict(fragment.request.url.params)
                if isinstance(fragment, httpx.Response) and fragment.request.url.params
                else None
            )

            path = self._build_artifact_path(self.dir, params, fragment_num)
            fragment_artifact = DataArtifact(path, fragment)

            if await fragment_artifact.persist():
                self.artifacts.append(fragment_artifact)
                fragment_num += 1

        globs = [f"mad://{a.path.strip('/')}" for a in self.artifacts]

        if not globs:
            return

        return (
            duckdb.query(
                f"SELECT * FROM read_json_auto({globs}, hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)"
            )
            if self.filetype == "json"
            else duckdb.query(
                f"SELECT * FROM read_parquet({globs}, hive_partitioning = true, union_by_name = true)"
            )
        )

    def _build_artifact_path(
        self,
        base_path: str,
        params: dict | None = None,
        fragment_number: int | None = None,
    ):
        filetype = self.filetype

        if params is None:
            return f"{base_path}/fragment={fragment_number}.{filetype}"

        params_path = "/".join(f"{key}={value}" for key, value in params.items())

        return f"{base_path}/{params_path}.{filetype}"

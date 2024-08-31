import httpx
import hashlib
from mad_prefect.data_assets import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.utils import _output_has_data, _write_asset_data
from mad_prefect.filesystems import get_fs


class DataArtifact:
    def __init__(
        self,
        artifact: object,
        dir: str,
        asset_id: str | None = None,
        asset_run_id: str | None = None,
        asset_name: str | None = None,
        path: str | None = None,
        data_written: bool = False,
    ):
        self.artifact = artifact
        self.dir = dir
        self.asset_id = asset_id
        self.asset_run_id = asset_run_id
        self.asset_name = asset_name
        self.path = path
        self.data_written = data_written
        pass

    def _get_params(self):
        params = (
            dict(self.artifact.request.url.params)
            if isinstance(self.artifact, httpx.Response)
            and self.artifact.request.url.params
            else None
        )
        return params

    def _get_base_path(self):
        return self.dir

    def _generate_artifact_guid(self):
        hash_input = f"{self.asset_id}:{self.asset_run_id}:{self.path}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    async def _write_artifact_metadata(self):
        fs = await get_fs()
        artifact_id = self._generate_artifact_guid()
        artifact_metadata = {
            "artifact_id": artifact_id,
            "asset_run_id": self.asset_run_id,
            "asset_id": self.asset_id,
            "artifact_path": self.path,
            "artifact_written": self.data_written,
        }

        await fs.write_data(
            f"{ASSET_METADATA_LOCATION}/asset_name={self.asset_name}/asset_id={self.asset_id}/asset_run_id={self.asset_run_id}/_artifacts/artifact_id={artifact_id}/metadata.json",
            artifact_metadata,
        )

    async def _run_artifact(
        self, asset_id: str, asset_run_id: str, asset_name: str, artifact_path: str
    ):
        self.asset_id = asset_id
        self.asset_run_id = asset_run_id
        self.asset_name = asset_name
        self.path = artifact_path

        artifact_id = self._generate_artifact_guid()
        await self._write_artifact_metadata()

        if _output_has_data(self.artifact):
            try:
                await _write_asset_data(self.path, self.artifact)
                self.data_written = True
            except:
                raise ValueError(
                    f"Artifact write operation failed for artifact_id: {artifact_id}"
                )
        else:
            print("No data written due to empty artifact")

        await self._write_artifact_metadata()

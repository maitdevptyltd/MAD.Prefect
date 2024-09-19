from mad_prefect.data_assets import asset
from mad_prefect.filesystems import get_fs


async def test_default_artifact_path():
    @asset("test_default_artifact_path.parquet")
    def test_asset():
        return [{"test": "abc"}]

    base_path = await test_asset._get_artifact_base_path()
    assert test_asset._build_artifact_path(base_path).startswith("/_artifacts")


async def test_provided_artifact_path():
    @asset("test_default_artifact_path.parquet", artifacts_dir="/my_artifacts_dir")
    async def test_asset():
        return [{"test": "abc"}]

    await test_asset()

    base_path = await test_asset._get_artifact_base_path()
    path = test_asset._build_artifact_path(base_path)

    assert path.startswith("/my_artifacts_dir")
    assert not path.startswith("/my_artifacts_dir/_artifacts")

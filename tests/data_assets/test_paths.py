from pydantic import BaseModel
from mad_prefect.data_assets import asset


async def test_bug_result_artifact_path_with_periods():
    @asset(path="path.with.periods/{asset_number}/people.parquet")
    async def list_people(asset_number):
        return [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

    lp = list_people.with_arguments(1)
    lp.result_artifact_filetypes = lp.get_result_artifact_filetypes()
    ra = lp._create_result_artifacts()

    assert ra[0].path == "path.with.periods/1/people.parquet"


async def test_path_with_multiple_result_formats():
    @asset(
        path="test_multiple_result_artifacts.parquet|csv|json",
    )
    async def multi_format_asset():
        # Yield a single batch of data
        yield [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

    # Call the assets
    primary_result_artifact = await multi_format_asset()
    assert primary_result_artifact

    # Access the list of result artifacts via the attribute
    result_artifacts = multi_format_asset.result_artifacts
    assert result_artifacts
    assert len(result_artifacts) == 3

    # Directly assert the paths for each artifact
    assert result_artifacts[0].path == "test_multiple_result_artifacts.parquet"
    assert result_artifacts[1].path == "test_multiple_result_artifacts.csv"
    assert result_artifacts[2].path == "test_multiple_result_artifacts.json"

    # Query the primary result artifact
    primary_query = await primary_result_artifact.query("SELECT COUNT(*) c")
    assert primary_query

    count_result = primary_query.fetchone()
    assert count_result[0] == 2


async def test_filetype_resolution():
    class Asset(BaseModel):
        path: str

    @asset(path="{asset.path}")
    async def path_resolution_asset(asset: Asset):
        return [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

    path_asset_class_obj = Asset(path="path_resolution_asset.parquet|csv")
    path_asset = path_resolution_asset.with_arguments(path_asset_class_obj)

    return await path_asset()

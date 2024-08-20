from mad_prefect.data_assets import asset
from mad_prefect.filesystems import get_fs


async def test_bound_arguments():
    @asset("bound_arguments/{value}.parquet")
    async def test_asset(value: str):
        return [{"test": value}]

    test_asset_a = test_asset.with_arguments("a")
    test_asset_b = test_asset.with_arguments("b")
    test_asset_c = test_asset.with_arguments("c")

    await test_asset_a()
    await test_asset_b()
    await test_asset_c()

    fs = await get_fs()
    files = fs.glob("bound_arguments/*.parquet")

    assert len(files) == 3
    assert files[0] == "bound_arguments/a.parquet"
    assert files[1] == "bound_arguments/b.parquet"
    assert files[2] == "bound_arguments/c.parquet"

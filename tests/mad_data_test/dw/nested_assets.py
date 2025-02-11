from mad_prefect.data_assets import asset


def nested_assets_func():
    @asset(
        path="nested_asset_1_module_asset_name_testing.parquet",
    )
    async def nested_asset_1():
        return [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

    @asset(
        path="nested_asset_2_module_asset_name_testing.parquet",
    )
    async def nested_asset_2():
        return [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ]

    return nested_asset_1, nested_asset_2

    # async def _materialize():
    #     asset_1 = await nested_asset_1()
    #     asset_2 = await nested_asset_2()
    #     return asset_1, asset_2

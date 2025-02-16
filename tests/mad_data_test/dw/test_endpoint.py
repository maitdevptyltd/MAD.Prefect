from mad_prefect.data_assets import asset


@asset(path="test_asset_name_endpoint.parquet")
async def modular_name_asset_function():
    return [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ]


dynamic_elephants = modular_name_asset_function.with_arguments()

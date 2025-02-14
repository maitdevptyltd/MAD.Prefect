from tests.mad_data_test.dw import modular_name_asset_function


ferocious_penguins_other_mod = modular_name_asset_function.with_arguments()


async def call_other_mod_asset():
    await ferocious_penguins_other_mod()

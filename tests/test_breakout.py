import asyncio
from mad_prefect.utils.nested_structures import extract_nested_tables
import mad_prefect.filesystems
from pandas import DataFrame

mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests"


async def test_nested_structures_file():
    fs = await mad_prefect.filesystems.get_fs()

    # Test ability to unpack nested STRUCT
    await extract_nested_tables(
        table_name="sample3",
        folder="nested_structures",
        break_out_fields=["address", "hobbies", "contact_details"],
        depth=1,
    )

    address = DataFrame(await fs.read_data("bronze/nested_structures/address.parquet"))
    address_row_count = len(address)
    address_columns = address.columns.to_list()

    assert address_row_count == 12
    assert address_columns == ["sample3_id", "city", "street", "zipcode"]

    hobbies = DataFrame(await fs.read_data("bronze/nested_structures/hobbies.parquet"))
    hobbies_row_count = len(hobbies)
    hobbies_columns = hobbies.columns.to_list()

    assert hobbies_row_count == 29
    assert hobbies_columns == ["sample3_id", "hobbies"]


if __name__ == "__main__":
    asyncio.run(test_nested_structures_file())

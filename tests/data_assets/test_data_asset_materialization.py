import asyncio
from decimal import Decimal
import json
import random
import string
from uuid import UUID
import duckdb
from pydantic import BaseModel
from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_asset import DataAsset
from datetime import datetime, date
import pandas as pd
from pydantic import BaseModel
from mad_prefect.data_assets.options import ReadJsonOptions
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs


@asset("simple_asset.parquet")
def simple_asset():
    return [
        {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        {"count": 10, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
    ]


async def test_simple_asset():
    result = await simple_asset()
    assert result


async def test_when_data_asset_yields_another_data_asset():
    @asset("composed_asset.parquet")
    async def composed_asset():
        yield simple_asset()
        more_numbers = await simple_asset.query("SELECT count + 5 as count")
        yield more_numbers

    composed_asset_query = await composed_asset.query("SELECT COUNT(*) c")
    assert composed_asset_query

    count_query_result = composed_asset_query.fetchone()
    assert count_query_result

    # Because composed adds 5 as a new array, there should be 6 in the array total
    assert count_query_result[0] == 6


async def test_when_data_asset_takes_another_data_asset_as_parameter():
    @asset("asset_with_asset_parameter.{asset.name}.parquet")
    async def asset_with_param(asset: DataAsset):
        return asset

    await asset_with_param(simple_asset)


async def test_when_data_asset_yields_multiple_lists():
    @asset("multiple_lists_asset.parquet")
    async def multiple_lists_asset():
        yield [
            {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]
        yield [
            {"count": 10, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 15, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    multiple_lists_asset_query = await multiple_lists_asset.query("SELECT COUNT(*) c")
    assert multiple_lists_asset_query
    count_query_result = multiple_lists_asset_query.fetchone()
    assert count_query_result

    # The total count should be 4 since there are 4 rows in the output parquet
    assert count_query_result[0] == 4


async def test_when_data_asset_schema_evolution():
    @asset("schema_evolution_asset_1.parquet")
    async def schema_evolution_asset_1():
        yield [
            {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    @asset("schema_evolution_asset_2.parquet")
    async def schema_evolution_asset_2():
        yield [
            {
                "count": 10,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "broken": {"value": None},
            },
            {"count": 15, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {
                "count": 20,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "extra_field": "extra_value",
                "broken": {"value": "oh hellooo"},
            },
        ]

    @asset("schema_evolution_asset.parquet")
    async def schema_evolution_asset():
        result = await schema_evolution_asset_1()
        yield result
        result = await schema_evolution_asset_2()
        yield result

    schema_evolution_asset_query = await schema_evolution_asset.query(
        "SELECT COUNT(*) c"
    )
    assert schema_evolution_asset_query
    count_query_result = schema_evolution_asset_query.fetchone()

    assert count_query_result

    # The total count should be 5 since there are 5 rows in the output parquet
    assert count_query_result[0] == 5


async def test_when_data_asset_contains_empty_struct():
    @asset("empty_struct_asset.parquet")
    async def empty_struct_asset():
        yield [
            {
                "count": 1,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
            {
                "count": 5,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
            {
                "count": 10,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
            {
                "count": 15,
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
                "empty": {},
                "empty2": {"empty3": {}},
            },
        ]

    empty_struct_asset_query = await empty_struct_asset.query("SELECT COUNT(*) c")
    assert empty_struct_asset_query
    count_query_result = empty_struct_asset_query.fetchone()

    assert count_query_result

    # The total count should be 4 since there are 4 rows in the output parquet
    assert count_query_result[0] == 4


async def test_when_data_asset_yields_no_data():
    @asset("empty_asset.parquet")
    async def empty_asset():
        yield []

    empty_asset_query = await empty_asset.query("SELECT *")

    if not empty_asset_query:
        return

    count_query_result = duckdb.query("SELECT COUNT(*) c").fetchone()

    assert count_query_result

    # The total count should be 0 since the asset yields no data
    assert count_query_result[0] == 0


async def test_nested_structs_with_many_keys_should_not_cast_to_string():
    def generate_unique_dict(keys):
        return {f"{key}@somewhere.com": ["creator", "editor"] for key in keys}

    def generate_random_keys(count):
        return [
            "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
            for _ in range(count)
        ]

    def generate_rows(num_rows):
        rows = []
        for i in range(num_rows):
            keys = generate_random_keys(100)
            rows.append(
                {
                    "id": str(i + 1),
                    "data": generate_unique_dict(keys),
                }
            )
        return rows

    @asset("dict_array_asset_1.parquet")
    async def dict_array_asset_1():
        yield generate_rows(50)
        yield generate_rows(50)

    # Query the composed asset to check its contents
    composed_query = await dict_array_asset_1.query("SELECT *")

    assert composed_query

    # The second column is named data
    assert composed_query.description[1][0] == "data"

    # And it not a string type
    # duckdb GitHub Issue: https://github.com/duckdb/duckdb/issues/13734
    assert composed_query.description[1][1] != "STRING"


async def test_materialize_artifact_with_decimal():
    @asset("decimal_asset.parquet")
    async def decimal_asset():
        yield [
            {"count": Decimal(1.1), "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": Decimal(5.5), "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": Decimal(10.75), "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    decimal_asset_query = await decimal_asset.query("SELECT COUNT(*) c")
    assert decimal_asset_query
    count_query_result = decimal_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the decimal values are correctly stored and retrieved
    decimal_values_query = await decimal_asset.query("SELECT count")
    assert decimal_values_query
    decimal_values = [row[0] for row in decimal_values_query.fetchall()]
    assert decimal_values == [1.1, 5.5, 10.75]


async def test_materialize_artifact_with_datetime():
    @asset("datetime_asset.parquet")
    async def datetime_asset():
        yield [
            {
                "timestamp": datetime(2023, 1, 1, 12, 0, 0),
                "date": date(2023, 1, 1),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 2, 12, 0, 0),
                "date": date(2023, 2, 1),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 3, 12, 0, 0),
                "date": date(2023, 3, 1),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
        ]

    datetime_asset_query = await datetime_asset.query("SELECT COUNT(*) c")
    assert datetime_asset_query
    count_query_result = datetime_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the datetime values are correctly stored and retrieved
    datetime_values_query = await datetime_asset.query("SELECT timestamp, date")
    assert datetime_values_query
    datetime_values = [(row[0], row[1]) for row in datetime_values_query.fetchall()]
    assert datetime_values == [
        (datetime(2023, 1, 1, 12, 0, 0), date(2023, 1, 1)),
        (datetime(2023, 1, 2, 12, 0, 0), date(2023, 2, 1)),
        (datetime(2023, 1, 3, 12, 0, 0), date(2023, 3, 1)),
    ]


async def test_materialize_artifact_with_pandas_dataframe():
    @asset("pandas_dataframe_asset.parquet")
    async def pandas_dataframe_asset():
        data = {
            "count": [1, 5, 10],
            "id": [
                "951c58e4-b9a4-4478-883e-22760064e416",
                "951c58e4-b9a4-4478-883e-22760064e416",
                "951c58e4-b9a4-4478-883e-22760064e416",
            ],
        }
        df = pd.DataFrame(data)
        yield df

    pandas_dataframe_asset_query = await pandas_dataframe_asset.query(
        "SELECT COUNT(*) c"
    )
    assert pandas_dataframe_asset_query
    count_query_result = pandas_dataframe_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the dataframe values are correctly stored and retrieved
    dataframe_values_query = await pandas_dataframe_asset.query("SELECT count, id")
    assert dataframe_values_query
    dataframe_values = [(row[0], row[1]) for row in dataframe_values_query.fetchall()]
    expected_values = [
        (1, UUID("951c58e4-b9a4-4478-883e-22760064e416")),
        (5, UUID("951c58e4-b9a4-4478-883e-22760064e416")),
        (10, UUID("951c58e4-b9a4-4478-883e-22760064e416")),
    ]
    assert dataframe_values == expected_values


async def test_materialize_artifact_with_inner_struct():
    @asset(
        "inner_struct_asset.parquet",
        read_json_options=ReadJsonOptions(columns={"data": "MAP(STRING, STRING)"}),
    )
    async def inner_struct_asset():
        yield [
            {"id": "1", "data": {"field1": "value1", "field2": "value2"}},
            {"id": "2", "data": {"field1": "value3", "field2": "value4"}},
        ]

    inner_struct_asset_query = await inner_struct_asset.query("SELECT COUNT(*) c")

    assert inner_struct_asset_query

    count_query_result = inner_struct_asset_query.fetchone()
    assert count_query_result

    # The total count should be 2 since there are 2 rows in the output parquet
    assert count_query_result[0] == 2

    # Verify that the id and data columns exist and data is of type MAP(STRING, STRING)
    inner_struct_asset_query = await inner_struct_asset.query("SELECT *")
    assert inner_struct_asset_query

    # Check that the id column exists
    assert "id" in [desc[0] for desc in inner_struct_asset_query.description]

    assert inner_struct_asset_query.types[1] == "MAP(VARCHAR, VARCHAR)"


async def test_json_artifacts_with_different_timestamp_precisions_are_deserialized_as_timestamps():
    @asset("datetime_asset_precision.parquet", artifact_filetype="json")
    async def datetime_asset_precision():
        yield [
            {
                "timestamp": datetime(2023, 1, 1, 12, 5, 50, 99),
                "timestamp_ez": datetime(2023, 1, 1, 12),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 2, 12, 0, 0, 55),
                "timestamp_ez": datetime(2023, 1, 1, 11),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
            {
                "timestamp": datetime(2023, 1, 3, 12, 0, 0, 33),
                "timestamp_ez": datetime(2023, 1, 1, 10),
                "id": "951c58e4-b9a4-4478-883e-22760064e416",
            },
        ]

    datetime_asset_query = await datetime_asset_precision.query("SELECT COUNT(*) c")
    assert datetime_asset_query
    count_query_result = datetime_asset_query.fetchone()
    assert count_query_result

    # The total count should be 3 since there are 3 rows in the output parquet
    assert count_query_result[0] == 3

    # Verify that the datetime values are correctly stored and retrieved
    datetime_values_query = await datetime_asset_precision.query(
        "SELECT timestamp, timestamp_ez"
    )

    assert datetime_values_query
    assert datetime_values_query.types[0] == "timestamp"
    assert datetime_values_query.types[1] == "timestamp"

    assert datetime_values_query
    datetime_values = [row[0] for row in datetime_values_query.fetchall()]
    assert datetime_values == [
        datetime(2023, 1, 1, 12, 5, 50, 99),
        datetime(2023, 1, 2, 12, 0, 0, 55),
        datetime(2023, 1, 3, 12, 0, 0, 33),
    ]


FETCHMANY_SAMPLE_DATA = [
    {"id": "e2b8033a-59bb-4d47-a761-8b754ed48e2e", "name": "John Smith"},
    {"id": "606a7a69-476a-488a-a563-ebe728f20f61", "name": "Alice Johnson"},
    {"id": "91d0ff16-236f-46b2-ba2e-a24d912a2322", "name": "Bob Williams"},
    {"id": "944c75be-7ff2-4354-bf22-67e3627c2f5e", "name": "Carol Brown"},
    {"id": "d7919b94-4027-4db9-95ee-69cba672af0e", "name": "Dave Jones"},
    {"id": "ff6b54d5-7aa3-4455-a541-4ce6dd0385cb", "name": "Emily Miller"},
    {"id": "bfcc7074-032f-4b1a-9bd2-615adba4098c", "name": "Frank Davis"},
    {"id": "6e1eaa3c-f033-4375-baf6-c6c9d0ad21ef", "name": "Grace Garcia"},
    {"id": "b75adee9-e785-4d5f-8f6b-495a9f688e68", "name": "Henry Rodriguez"},
    {"id": "65136fe2-4983-41c7-992b-1db54f9027fd", "name": "Ivy Wilson"},
    {"id": "ed0f5c77-713f-47b8-b0ab-33ef3d69fe3c", "name": "Jack Martinez"},
    {"id": "0b06d36f-dcc9-4d64-a520-5bb15f7ac80a", "name": "Kate Anderson"},
    {"id": "b7bea19c-9448-4fd8-bec2-9a708f01264d", "name": "Leo Taylor"},
    {"id": "68388ada-e48b-4ef0-9812-8d0334cdaf22", "name": "Mia Thomas"},
    {"id": "492ab6ae-2659-48d2-ab0a-c59b67cd9c23", "name": "Nancy Hernandez"},
    {"id": "dcedf173-a4e6-4c6a-a053-e3cc7fadd0fd", "name": "Oscar Moore"},
    {"id": "2b32c6c5-9452-48d9-b7b4-d51e3e7c5861", "name": "Paul Martin"},
    {"id": "88a26168-66aa-456c-a1c2-25adf2d2f6fd", "name": "Queen Jackson"},
    {"id": "8f3ceb9c-195f-4b3f-9d56-f8beb7e82b84", "name": "Ray Thompson"},
    {"id": "d94ed1a6-b190-40d7-a336-cc03573e4acf", "name": "Sarah White"},
    {"id": "d3c25b2f-2a9e-4a9e-b0ec-21ae22c79b3d", "name": "Tom Lopez"},
    {"id": "fbd099cf-d39c-400f-8b75-99ed5075f23b", "name": "Uma Lee"},
    {"id": "a4bf19e9-a469-4aa3-b6d1-28303b33bed3", "name": "Violet Gonzalez"},
    {"id": "709652e2-cf75-4f67-9967-5c6c00858f9b", "name": "Will Harris"},
    {"id": "ea6c5091-8b77-472f-bbb3-7c79c2814d41", "name": "Xena Clark"},
    {"id": "c2a753ae-cc24-4ce1-b4d9-7934f1a6aeb7", "name": "Yvonne Lewis"},
    {"id": "97a578d3-7634-4c1a-818f-3da589e7dcf1", "name": "Zack Robinson"},
    {"id": "bd7af370-322d-4555-8694-4346bb3db96e", "name": "John Walker"},
    {"id": "6dd3e3bc-372b-478b-bd49-bc427b6a6dd1", "name": "Jane Perez"},
    {"id": "49363354-8951-41cb-a903-fde541fdf350", "name": "Alice Hall"},
]


async def test_nested_assets_fetchmany():

    @asset(path="nested_fetchmany_list_asset_{endpoint}.parquet", name="{endpoint}")
    async def listing_asset(endpoint: str):
        results = []

        for record in FETCHMANY_SAMPLE_DATA:
            record["endpoint"] = endpoint
            results.append(record)

        return results

    # Set up details asset
    @asset(
        path="nested_fetchmany_details_{listing_asset.name}.parquet",
        name="{listing_asset.name}_details",
    )
    async def details_asset(listing_asset: DataAsset, id_column: str = "id"):

        id_query = await listing_asset.query(f"SELECT DISTINCT {id_column}")

        # Call execute to resolve the id_query into a DuckDbPyConnection (which is a cursor)
        # as the id_query is a DuckDbPyRelation which doesn't behave like a cursor
        qry = id_query.execute()

        if id_query is None:
            raise ValueError("fetchmany_id_list_asset is not generated properly.")

        BATCH_SIZE = 10

        async def basic_async_func(id: str):
            return {"id": id}

        results = []

        while True:
            b = qry.fetchmany(BATCH_SIZE)

            if not b:
                break

            async_tasks = [basic_async_func(row[0]) for row in b]
            batch_results = await asyncio.gather(*async_tasks, return_exceptions=True)

            results.extend(batch_results)

        return results

    # Set up listing assets
    beavers_listing = listing_asset.with_arguments("beavers")
    peacocks_listing = listing_asset.with_arguments("peacocks")

    # Set up details assets
    beavers_details = details_asset.with_arguments(beavers_listing)
    peacocks_details = details_asset.with_arguments(peacocks_listing)

    # Materialize details assets
    await asyncio.gather(
        beavers_details(),
        peacocks_details(),
        return_exceptions=True,
    )

    # Test the listing assets for distinct ids
    beavers_listing_distinct_ids = await beavers_listing.query(
        "SELECT COUNT(DISTINCT id)"
    )
    beavers_listing_distinct_id_count: tuple = beavers_listing_distinct_ids.fetchone()  # type: ignore

    peacocks_listing_distinct_ids = await peacocks_listing.query(
        "SELECT COUNT(DISTINCT id)"
    )
    peacocks_listing_distinct_id_count: tuple = peacocks_listing_distinct_ids.fetchone()  # type: ignore

    assert beavers_listing_distinct_id_count[0] == len(FETCHMANY_SAMPLE_DATA)
    assert peacocks_listing_distinct_id_count[0] == len(FETCHMANY_SAMPLE_DATA)

    # Test the details assets for row count
    beavers_details_count_query = await beavers_details.query("SELECT COUNT(*)")
    beavers_details_count: tuple = beavers_details_count_query.fetchone()  # type: ignore

    peacocks_details_count_query = await peacocks_details.query("SELECT COUNT(*)")
    peacocks_details_count: tuple = peacocks_details_count_query.fetchone()  # type: ignore

    print(f"beavers: {beavers_details_count[0]}")
    print(f"peacocks: {peacocks_details_count[0]}")

    assert beavers_details_count[0] == len(FETCHMANY_SAMPLE_DATA)
    assert peacocks_details_count[0] == len(FETCHMANY_SAMPLE_DATA)


async def test_listing_asset_fetchmany():

    @asset(path="listing_asset_fetchmany_{endpoint}.parquet")
    async def base_asset(endpoint: str):
        return FETCHMANY_SAMPLE_DATA

    async def fetchmany_function(base_asset: DataAsset):
        query = await base_asset.query("SELECT id")
        base_asset_name = base_asset._bound_arguments.arguments["endpoint"]

        if not query:
            return

        BATCH_SIZE = 10

        async def basic_async_func(id: str, endpoint: str):
            return {"id": id, "endpoint": endpoint}

        results = []

        while True:
            async_tasks = [
                basic_async_func(str(row[0]), base_asset_name)
                for row in query.fetchmany(BATCH_SIZE)
            ]

            if not async_tasks:
                break

            batch_results = await asyncio.gather(*async_tasks, return_exceptions=True)

            results.extend(batch_results)

        fs = await get_fs()

        await fs.write_data(f"non_asset_fetchmany_{base_asset_name}.json", results)

    beavers = base_asset.with_arguments("beavers")
    peacocks = base_asset.with_arguments("peacocks")

    await fetchmany_function(beavers)
    await fetchmany_function(peacocks)

    fs = await get_fs()

    beavers_data = await fs.read_data("non_asset_fetchmany_beavers.json")
    peacocks_data = await fs.read_data("non_asset_fetchmany_peacocks.json")

    assert len(beavers_data) == len(FETCHMANY_SAMPLE_DATA)
    assert len(peacocks_data) == len(FETCHMANY_SAMPLE_DATA)


async def test_details_asset_fetchmany():

    async def write_base_data(endpoint: str):
        results = []
        for record in FETCHMANY_SAMPLE_DATA:
            record["endpoint"] = endpoint
            results.append(record)

        fs = await get_fs()
        await fs.write_data(f"non_asset_fetchmany_base_{endpoint}.parquet", results)

    @asset(path="details_fetchmany_asset.parquet")
    async def fetchmany_function(endpoint: str):
        await register_mad_protocol()
        query = duckdb.query(
            f"SELECT DISTINCT id FROM 'mad://non_asset_fetchmany_base_{endpoint}.parquet'"
        )

        if not query:
            return

        BATCH_SIZE = 10

        async def basic_async_func(id: str, endpoint: str):
            return {"id": id, "endpoint": endpoint}

        results = []

        while True:
            async_tasks = [
                basic_async_func(str(row[0]), endpoint)
                for row in query.fetchmany(BATCH_SIZE)
            ]

            if not async_tasks:
                break

            batch_results = await asyncio.gather(*async_tasks, return_exceptions=True)

            results.extend(batch_results)

        return results

    await write_base_data("beavers")
    await write_base_data("peacocks")

    beavers_details = fetchmany_function.with_arguments("beavers")
    peacocks_details = fetchmany_function.with_arguments("peacocks")

    await beavers_details()
    await peacocks_details()

    # Test the details assets for row count
    beavers_details_count_query = await beavers_details.query("SELECT COUNT(*)")
    beavers_details_count: tuple = beavers_details_count_query.fetchone()  # type: ignore

    peacocks_details_count_query = await peacocks_details.query("SELECT COUNT(*)")
    peacocks_details_count: tuple = peacocks_details_count_query.fetchone()  # type: ignore

    print(f"beavers: {beavers_details_count[0]}")
    print(f"peacocks: {peacocks_details_count[0]}")

    assert beavers_details_count[0] == len(FETCHMANY_SAMPLE_DATA)
    assert peacocks_details_count[0] == len(FETCHMANY_SAMPLE_DATA)


async def test_materialize_artifact_csv():
    @asset("test_csv_asset.csv")
    async def csv_asset():
        # Yield first batch
        yield [
            {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]
        # Yield second batch
        yield [
            {"count": 10, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 15, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

    # Materialize the CSV file
    csv_artifact = await csv_asset()
    assert csv_artifact

    # Use the artifact's query method to count rows (DuckDB can handle CSV)
    csv_artifact_query = await csv_artifact.query("SELECT COUNT(*) c")
    assert csv_artifact_query

    count_query_result = csv_artifact_query.fetchone()
    assert count_query_result

    # We expect 4 total rows from the two yields above
    assert count_query_result[0] == 4


async def test_csv_artifacts_with_hive_partitions():
    @asset(
        path="test_csv_artifacts_with_hive_partitions.csv",
        artifact_filetype="csv",
        artifacts_dir="raw/month=12/year=2024",
    )
    async def csv_asset():
        # Yield first batch
        yield [
            {"count": 1, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 5, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]
        # Yield second batch
        yield [
            {"count": 10, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
            {"count": 15, "id": "951c58e4-b9a4-4478-883e-22760064e416"},
        ]

        # Materialize the CSV file

    csv_artifact = await csv_asset()
    assert csv_artifact

    # Use the artifact's query method to count rows (DuckDB can handle CSV)
    count_csv_artifact_query = await csv_artifact.query("SELECT COUNT(*) c")
    assert count_csv_artifact_query

    count_query_result = count_csv_artifact_query.fetchone()
    assert count_query_result

    # We expect 4 total rows from the two yields above
    assert count_query_result[0] == 4

    csv_artifact_query = await csv_artifact.query()

    csv_columns = csv_artifact_query.columns if csv_artifact_query else []

    assert "year" in csv_columns
    assert "month" in csv_columns


async def test_multiple_result_artifacts():
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


async def test_pydantic_model_asset():
    class PedanticChild(BaseModel):
        child_name: str
        test_score: float

    class PedanticPydanticClass(BaseModel):
        id: int
        pendantic_levels: float
        description: str
        creation_date: datetime
        child: PedanticChild

    data = PedanticPydanticClass(
        id=666999555,
        pendantic_levels=0.89,
        description="Very pedantic",
        creation_date=datetime.now(),
        child=PedanticChild(child_name="John", test_score=0.6667),
    )

    @asset(path="pydantic_model_asset.{filetype}")
    async def pydantic_model_asset(model: BaseModel, filetype: str):
        return model

    filetypes = ["json", "parquet", "csv"]

    for filetype in filetypes:

        model_asset = pydantic_model_asset.with_arguments(data, filetype)

        # Test the outputs of the model
        parent_model = await model_asset.query()
        assert parent_model

        parent_model_data = parent_model.fetchone()
        assert parent_model_data

        assert (
            isinstance(parent_model_data[0], int) and parent_model_data[0] == 666999555
        )
        assert isinstance(parent_model_data[1], float) and parent_model_data[1] == 0.89
        assert (
            isinstance(parent_model_data[2], str)
            and parent_model_data[2] == "Very pedantic"
        )
        assert (
            isinstance(parent_model_data[3], datetime)
            and parent_model_data[3] == data.creation_date
        )

        if filetype == "csv":
            assert isinstance(json.loads(parent_model_data[4]), dict)
        else:
            assert isinstance(parent_model_data[4], dict)

        # Test the outputs of the nested model
        child_model = await model_asset.query("SELECT UNNEST(child)")

        assert child_model

        child_model_data = child_model.fetchone()
        assert child_model_data
        assert isinstance(child_model_data[0], str) and child_model_data[0] == "John"
        assert isinstance(child_model_data[1], float) and child_model_data[1] == 0.6667

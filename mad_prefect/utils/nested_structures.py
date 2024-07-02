import asyncio
import json
import re
import duckdb
import pandas as pd
from prefect import flow
from mad_prefect.duckdb import register_mad_protocol
import mad_prefect.filesystems
from mad_prefect.filesystems import get_fs
from typing import Callable

# mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests"


### Utility functions designed to extract metadata for parquet files
### To guide process for unpacking nested structures


def extract_json_columns(table_name: str, folder: str):
    """
    This function is designed to identify nested structures that have been serialized as VARCHAR or JSON.
    """
    # Query to get VARCHAR columns, excluding array types
    target_columns = (
        duckdb.query(
            f"""
                SELECT * 
                FROM (DESCRIBE SELECT * FROM 'mad://bronze/{folder}/{table_name}.parquet') 
                WHERE column_type = 'VARCHAR' OR column_type = 'JSON'
            """
        )
        .df()["column_name"]
        .tolist()
    )

    json_columns = []
    for column in target_columns:
        # Check if the column contains valid JSON based on first 100 rows
        col = f'"{column}"'
        json_check_query = duckdb.query(
            f"""
                SELECT
                    SUM(non_json)
                FROM
                    (SELECT 
                        CASE
                            WHEN {col} LIKE '{{%}}' OR {col} LIKE '[%]' THEN 0
                            ELSE 1
                        END non_json
                    FROM 'mad://bronze/{folder}/{table_name}.parquet'
                    WHERE {col} NOT NULL
                    LIMIT 100)
            """
        ).fetchone()[0]

        if json_check_query == 0:
            json_columns.append(column)

    return json_columns


async def extract_complex_columns(table_name: str, folder: str):
    """
    This function extracts and categorizes complex columns from a parquet file,
    including STRUCT, ARRAY, and JSON-like VARCHAR columns.
    """
    await register_mad_protocol()
    print(f"extracting columns for {table_name}")

    # Get table metadata
    metadata = duckdb.query(
        f"DESCRIBE SELECT * FROM 'mad://bronze/{folder}/{table_name}.parquet'"
    )
    print(metadata)

    # Identify JSON-like columns
    json_columns = extract_json_columns(table_name, folder)

    if json_columns:
        json_columns_str = ", ".join(f"'{col}'" for col in json_columns)
        json_insert_str = f"WHEN column_name IN ({json_columns_str}) THEN 'JSON'"
    else:
        json_insert_str = ""

    # Categorize columns based on their type
    columns_data_full = duckdb.query(
        f"""
        SELECT 
            column_name,
            CASE
                WHEN column_type LIKE '%STRUCT%' THEN 'STRUCT'
                WHEN column_type LIKE '%[]%' THEN 'ARRAY'
                {json_insert_str}
                ELSE 'OTHER'
            END AS column_type
        FROM metadata
    """
    )

    columns_data = duckdb.query(
        "SELECT * FROM columns_data_full WHERE column_type != 'OTHER'"
    ).df()

    return columns_data


### Utility functions designed to unpack different nested structures


def default_json_unpack(df: pd.DataFrame, json_column: str) -> pd.DataFrame:
    """
    Default JSON unpacking function with row count safety check.


    Args:
        df (pd.DataFrame): Input DataFrame containing the JSON column and parent_id.
        json_column (str): Name of the column containing JSON data.

    Returns:
        pd.DataFrame: DataFrame with unpacked JSON, maintaining the parent_id.
    """

    try:
        # Parse JSON strings
        parsed_json = df[json_column].apply(json.loads)
        object_types = set(map(type, parsed_json))

        # Check for consistency in object types
        if len(object_types) > 1:
            print(
                f"Inconsistent data types found in {json_column}. Custom json_unpack_func should be used"
            )
            print(
                f"Skipping {json_column}. Please review and provide a custom json_unpack_func if needed."
            )
            return pd.DataFrame()

        # Replace input column with parsed_json objects
        df[json_column] = parsed_json

        # Extract the parent_id column name (assuming it's the first column)
        parent_id_column = df.columns[0]

        # Normalize JSON while explicitly preserving all other columns
        df_unpacked = pd.json_normalize(
            df.to_dict(orient="records"),
            meta=[parent_id_column],
            errors="ignore",
            max_level=2,
        )

        df_unpacked.columns = [
            col.split(".")[-1] if "." in col else col for col in df_unpacked.columns
        ]

        # Drop the original JSON column if it exists in the unpacked DataFrame
        if json_column in df_unpacked.columns:
            df_unpacked = df_unpacked.drop(columns=[json_column])

        return df_unpacked

    except Exception as e:
        print(f"Error unpacking {json_column}: {str(e)}. Returning empty DataFrame.")
        return pd.DataFrame()  # Return an empty DataFrame


# This uses switch logic to apply the correct unpacking method to the structure
async def process_complex_columns(
    columns_data: pd.DataFrame,
    parent_id_alias: str,
    field: str,
    folder: str,
    table_name: str,
    json_unpack_func: Callable[[pd.DataFrame, str], pd.DataFrame],
    primary_key: str = "id",
):
    await register_mad_protocol()

    columns_query = duckdb.register("columns_query", columns_data)

    # Identify the type of nested structure to unpack
    field_type = duckdb.query(
        f"SELECT column_type FROM columns_query WHERE column_name = '{field}'"
    ).fetchone()[0]

    if field_type == "STRUCT":
        query = duckdb.query(
            f"""
                SELECT 
                    {parent_id_alias} AS {table_name}_id, 
                    UNNEST({field}, max_depth:=2) 
                FROM 'mad://bronze/{folder}/{table_name}.parquet'
            """
        )
    elif field_type == "ARRAY":
        query = duckdb.query(
            f"""
            SELECT 
                {parent_id_alias} AS {table_name}_id, 
                UNNEST({field}) AS {field}
            FROM 'mad://bronze/{folder}/{table_name}.parquet'
            """
        )
    elif field_type == "JSON":
        query_df = duckdb.query(
            f"""
            SELECT 
                {primary_key} AS {table_name}_id, 
                {field}
            FROM 'mad://bronze/{folder}/{table_name}.parquet'
            """
        ).df()
        unpacked_df: pd.DataFrame = json_unpack_func(query_df, field)
        if unpacked_df.empty:
            return None
        unpacked_df_table = duckdb.register("unpacked_df_table", unpacked_df)
        query = duckdb.query("SELECT * FROM unpacked_df_table")
    else:
        return
    return query


## Actual breakout function


@flow
async def extract_nested_tables(
    table_name: str,
    folder: str,
    break_out_fields: list = [],
    parent_id_alias: str = "id",
    primary_key: str = "id",
    depth: int = 0,
    prefix: str = "",
    json_unpack_func: Callable[[pd.DataFrame, str], pd.DataFrame] = default_json_unpack,
):
    """
    Extract and process nested tables from a parquet file.

    Args:
        table_name (str): Name of the table to process.
        folder (str): Folder containing the parquet file.
        break_out_fields (list, optional): Specific fields to process. Defaults to all complex fields.
        parent_id_alias (str, optional): Name of the parent ID column. Defaults to "id".
        depth (int, optional): Depth of recursion for nested structures. Defaults to 2.
        prefix (str, optional): Prefix for output file names. Defaults to "".
        json_unpack_func (Callable, optional): Function to unpack JSON columns.
            Should take a DataFrame and column name as input and return a DataFrame.
            Defaults to a basic JSON normalization function.

    The json_unpack_func should have the following signature:
    def custom_json_unpack(df: pd.DataFrame, json_column: str) -> pd.DataFrame:
        # Custom unpacking logic here
        return unpacked_df

    Returns:
        None
    """
    await register_mad_protocol()

    fs = await get_fs()

    # Extract the complex columns from the table
    columns_data = await extract_complex_columns(table_name, folder)
    column_list = columns_data["column_name"].tolist()

    # Determine which fields to process at this level
    current_level_fields = break_out_fields if break_out_fields else column_list

    next_level_fields = []

    # Process all first-level fields
    for field in current_level_fields:

        # Handle prefixes to avoid file duplication
        paths = fs.glob(f"bronze/{folder}/**/*_{field}.parquet")
        counter = len(paths)
        prefixed_field = f"{prefix}_{field}" if prefix else field

        # If the given field is a complex column in parquet
        if field in column_list:
            query = await process_complex_columns(
                columns_data,
                parent_id_alias,
                field,
                folder,
                table_name,
                json_unpack_func,
                primary_key=primary_key,
            )

            if query:
                print(duckdb.query("DESCRIBE SELECT * FROM query"))
                # data = query.df()
                # await fs.write_data(f"bronze/{folder}/{prefixed_field}.parquet", data)
                query.to_parquet(f"mad://bronze/{folder}/{prefixed_field}.parquet")
                next_level_fields.append(field)

    # Do not recurse if depth limit reached.
    if depth == 0:
        return

    # Recurse into the collected fields
    for new_table_name in next_level_fields:
        # On the next run re-alias parent id e.g. id becomes docket_id
        new_parent_id_alias = f"{table_name}_id"

        # Add a prefix in if second level structures an beyond
        new_prefix = f"{prefix}_{new_table_name}" if prefix else new_table_name

        # Rename the table if previous file written included a prefix
        new_table_name_with_prefix = (
            f"{prefix}_{new_table_name}" if prefix else new_table_name
        )

        # Run SQL query on new table and extract the complex columns
        new_column_data = await extract_complex_columns(
            new_table_name_with_prefix, folder
        )
        new_columns = new_column_data["column_name"].tolist()

        # For the next level, use all new complex columns if no break_out_fields specified
        next_level_break_out = break_out_fields if break_out_fields else new_columns

        # Recall the function with the new params
        await extract_nested_tables(
            folder=folder,
            table_name=new_table_name_with_prefix,
            break_out_fields=next_level_break_out,
            parent_id_alias=new_parent_id_alias,
            depth=depth - 1,
            prefix=new_prefix,
        )


if __name__ == "__main__":
    asyncio.run(
        extract_nested_tables(
            break_out_fields=[
                "Location",
                "CumulativeOperatingHours",
                "Distance",
                "FuelUsed",
                "FuelUsedLast24",
                "FuelRemaining",
                "CumulativeIdleNonOperatingHours",
                "DEFRemaining",
            ],
            folder="hitachi",
            table_name="equipment",
            parent_id_alias="PIN",
            depth=1,
        )
    )

import asyncio
import re
import duckdb
from mad_prefect.filesystems import get_fs, FsspecFileSystem
from prefect import flow
from mad_prefect.duckdb import register_mad_protocol
import pandas as pd


async def extract_complex_columns(table_name, folder):
    await register_mad_protocol()
    print(f"extracting columns for {table_name}")
    metadata = duckdb.query(
        f"DESCRIBE SELECT * FROM 'mad://bronze/{folder}/{table_name}.parquet'"
    )
    print(metadata)
    columns_data = duckdb.query(
        f"""
        SELECT 
            column_name,
            CASE
                WHEN column_type LIKE '%STRUCT%' THEN 'STRUCT'
                WHEN column_type LIKE '%[]%' THEN 'ARRAY'
                ELSE 'OTHER'
            END AS column_type
        FROM metadata
        WHERE column_type LIKE '%STRUCT%' OR column_type LIKE '%[]%'
        """
    ).df()
    return columns_data


async def process_complex_columns(
    columns_data: pd.DataFrame,
    parent_id_alias: str,
    field: str,
    folder: str,
    table_name: str,
):
    await register_mad_protocol()
    struct_list = columns_data[columns_data["column_type"] == "STRUCT"][
        "column_name"
    ].tolist()
    array_list = columns_data[columns_data["column_type"] == "ARRAY"][
        "column_name"
    ].tolist()
    if field in struct_list:
        query = duckdb.query(
            f"""
                SELECT 
                    {parent_id_alias} AS {table_name}_id, 
                    UNNEST({field}, max_depth:=2) 
                FROM 'mad://bronze/{folder}/{table_name}.parquet'
            """
        )
    elif field in array_list:
        query = duckdb.query(
            f"""
            SELECT 
                {parent_id_alias} AS {table_name}_id, 
                UNNEST({field}) AS {field}
            FROM 'mad://bronze/{folder}/{table_name}.parquet'
            """
        )

    else:
        return
    return query


@flow
async def extract_nested_tables(
    table_name: str,
    folder: str,
    break_out_fields: list = [],
    parent_id_alias: str = "id",
    depth: int = 2,
    prefix: str = "",
):
    await register_mad_protocol()

    if depth == 0:
        return

    fs = await get_fs()

    # Extract the STRUCT columns from the table
    columns_data = await extract_complex_columns(table_name, folder)
    column_list = columns_data["column_name"].tolist()

    # If no break out fields are passed perform on all columns
    if not break_out_fields:
        break_out_fields = column_list

    next_level_fields = []

    # Process all first-level fields
    for field in break_out_fields:
        print(f"Processing run: field = {field}, table_name = {table_name}")

        # Handle prefixes to avoid file duplication
        paths = fs.glob(f"bronze/{folder}/**/*_{field}.parquet")
        counter = len(paths)
        prefixed_field = f"{prefix}_{field}" if prefix else field

        # If the given field is a STRUCT column in parquet
        if field in column_list:
            query = await process_complex_columns(
                columns_data, parent_id_alias, field, folder, table_name
            )
            print(duckdb.query("DESCRIBE SELECT * FROM query"))
            query.to_parquet(f"mad://bronze/{folder}/{prefixed_field}.parquet")
            next_level_fields.append(field)

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

        # Run SQL query on new table and extract the STRUCT columns
        new_columns = await extract_complex_columns(new_table_name_with_prefix, folder)

        # Recall the function with the new params
        await extract_nested_tables(
            folder=folder,
            table_name=new_table_name_with_prefix,
            break_out_fields=new_columns,
            parent_id_alias=new_parent_id_alias,
            depth=depth - 1,
            prefix=new_prefix,
        )

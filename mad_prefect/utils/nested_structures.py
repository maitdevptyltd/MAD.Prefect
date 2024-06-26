import asyncio
import re
import duckdb
import pandas as pd
from prefect import flow
from mad_prefect.duckdb import register_mad_protocol
import mad_prefect.filesystems
from mad_prefect.filesystems import get_fs

mad_prefect.filesystems.FILESYSTEM_URL = "file://./tests"

### Utility functions designed to extract metadata for parquet files
### To guide process for unpacking nested structures


def extract_json_columns(table_name: str, folder: str):
    """
    This function is designed to identify nested structures that have been serialized as VARCHAR.
    """
    # Query to get VARCHAR columns, excluding array types
    varchar_columns = (
        duckdb.query(
            f"""
        SELECT * 
        FROM (DESCRIBE SELECT * FROM 'mad://bronze/{folder}/{table_name}.parquet') 
        WHERE column_type NOT LIKE '%[]%'
            AND column_type LIKE '%VARCHAR%'
        """
        )
        .df()["column_name"]
        .tolist()
    )

    json_columns = []
    for column in varchar_columns:
        # Check if the column contains valid JSON based on first 100 rows
        json_check = duckdb.query(
            f""" 
                SELECT COUNT(*) = 0
                FROM (
                    SELECT TRY_CAST({column} AS JSON) AS json_check
                    FROM 'mad://bronze/{folder}/{table_name}.parquet'
                    WHERE {column} IS NOT NULL 
                    LIMIT 100
                )
                WHERE json_check IS NULL
            """
        ).fetchone()[0]
        if json_check:
            json_columns.append(column)

    return json_columns


async def is_variable_json_structure(column_name: str, table_name: str, folder: str):
    await register_mad_protocol()
    sample_data = duckdb.query(
        f"""
            SELECT CAST({column_name} AS JSON) AS json_column
            FROM 'mad://bronze/{folder}/{table_name}.parquet'
            WHERE {column_name} IS NOT NULL
            LIMIT 1000
        """
    )
    json_keys = duckdb.query(
        """
            SELECT UNNEST(json_keys(json_column)) AS key
            FROM sample_data
        """
    )
    result = duckdb.query(
        """
        SELECT 
            COUNT(DISTINCT key) > 0.5 * COUNT(*) AS is_variable_structure
        FROM json_keys
        """
    ).fetchone()[0]

    return result


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

    # Categorize columns based on their type
    columns_data = duckdb.query(
        f"""
        SELECT 
            column_name,
            CASE
                WHEN column_type LIKE '%STRUCT%' THEN 'STRUCT'
                WHEN column_type LIKE '%[]%' THEN 'ARRAY'
                WHEN column_name IN ({', '.join(f"'{col}'" for col in json_columns)}) THEN 'JSON'
                ELSE 'OTHER'
            END AS column_type
        FROM metadata
    """
    ).df()

    return columns_data


### Utility functions designed to unpack different nested structures
async def process_complex_json_column(
    folder: str, table_name: str, json_column: str, parent_id_alias: str
):
    """
    Targets nested nested JSON structures with irregular formats. Such as all keys equal GUIDs
    Returns:
        DuckDB Query with keys expanded into new rows
    """

    casted_query = duckdb.query(
        f"""
            SELECT {parent_id_alias} AS {table_name}_id, CAST({json_column} AS JSON) AS json_data
            FROM 'mad://bronze/{folder}/{table_name}.parquet'
        """
    )
    key_query = duckdb.query(
        f"""
        SELECT 
            {table_name}_id,
            json_data,
            UNNEST(json_keys(json_data), max_depth:=2) AS key
	    FROM casted_query
        """
    )
    key_value_table = duckdb.query(
        f"""
        SELECT
            CAST({table_name}_id AS VARCHAR) AS {table_name}_id,
            key,
            json_extract(json_data, CONCAT('$.',key)) AS value
        FROM key_query  
        """
    ).df()

    extract_quoted_text = lambda x: (
        " ".join(re.findall(r'"(.*?)"', x)) if isinstance(x, str) else x
    )

    key_value_table["value"] = key_value_table["value"].apply(extract_quoted_text)
    query = duckdb.query(
        "SELECT * FROM key_value_table WHERE value IS NOT NULL AND value != ''"
    )

    return query


async def process_simple_json_column(
    folder: str, table_name: str, json_column: str, parent_id_column: str = "id"
):
    """
    Expands a JSON column into multiple columns.

    Returns:
        DuckDB Query with the JSON expanded into columns.
    """
    await register_mad_protocol()

    # Use json_transform to convert JSON to a struct
    query = f"""
    SELECT 
        {parent_id_column} as {table_name}_id,
        json_transform({json_column}, '$.%') AS json_struct
    FROM 'mad://bronze/{folder}/{table_name}.parquet'
    """

    df = duckdb.query(query).df()

    # Use pandas json_normalize to expand the struct into columns
    json_df = pd.json_normalize(df["json_struct"])

    # Add the parent_id back to the DataFrame
    json_df[parent_id_column] = df[parent_id_column]

    # Reorder columns to have parent_id first
    columns = [parent_id_column] + [
        col for col in json_df.columns if col != parent_id_column
    ]
    json_df = json_df[columns]
    query = duckdb.query("SELECT * FROM json_df")

    return query


# This uses switch logic to apply the correct unpacking method to the structure
async def process_complex_columns(
    columns_data: pd.DataFrame,
    parent_id_alias: str,
    field: str,
    folder: str,
    table_name: str,
):
    await register_mad_protocol()

    columns_query = duckdb.register("columns_query", columns_data)
    # columns_query = duckdb.query("SELECT * FROM columns_data")
    print(columns_query)

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
        if await is_variable_json_structure(field, table_name, folder):
            query = await process_complex_json_column(
                folder, table_name, field, parent_id_alias
            )
        else:
            query = await process_simple_json_column(
                folder, table_name, field, parent_id_alias
            )

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


if __name__ == "__main__":
    asyncio.run(
        extract_nested_tables(
            table_name="sample3",
            folder="nested_structures",
            break_out_fields=["address", "hobbies", "contact_details"],
            depth=1,
        )
    )

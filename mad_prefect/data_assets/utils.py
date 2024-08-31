import os
import duckdb
import httpx
import pandas
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs


def _output_has_data(data: object):
    if isinstance(data, httpx.Response):
        # TODO: Create checking mechanism for raw text responses
        check_result = True if data.json() else False

    elif isinstance(data, duckdb.DuckDBPyRelation):
        check_result = True if len(data.df()) else False

    elif isinstance(data, pandas.DataFrame):
        check_result = True if len(data) else False

    else:
        check_result = True if data else False

    return check_result


async def _write_asset_data(path: str, data: object):
    fs = await get_fs()
    await register_mad_protocol()

    if isinstance(data, (duckdb.DuckDBPyRelation, pandas.DataFrame)):
        # Before using COPY TO statement ensure directory exists
        folder_path = os.path.dirname(path)
        fs.mkdirs(folder_path, exist_ok=True)

        json_format = "FORMAT JSON, ARRAY true," if ".json" in path else ""
        duckdb.query("SET temp_directory = './.tmp/duckdb/'")
        duckdb.query(
            f"""
                COPY(
                    SELECT * FROM data
                ) TO 'mad://{path}' ({json_format} use_tmp_file false)
            """
        )
    elif isinstance(data, httpx.Response):
        # TODO: Find way to process raw text responses
        await fs.write_data(path, data.json())
    else:
        await fs.write_data(path, data)

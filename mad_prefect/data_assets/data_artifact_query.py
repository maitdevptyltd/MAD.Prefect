from typing import cast
import duckdb
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.data_assets.data_artifact import DataArtifact


class DataArtifactQuery:

    artifacts: list[DataArtifact]
    columns: dict[str, str]

    def __init__(
        self,
        artifacts: list[DataArtifact] | None = None,
        columns: dict[str, str] | None = None,
    ):
        self.artifacts = artifacts or []
        self.columns = columns or {}

    async def query(self, query_str: str | None = None):
        await register_mad_protocol()

        # Get the globs for any artifacts which exist
        existing_artifacts = [a for a in self.artifacts if await a.exists()]
        globs = [f"mad://{a.path.strip('/')}" for a in existing_artifacts]

        if not globs:
            return

        # Ensure each artifact is of the same filetype
        filetypes = set([a.filetype for a in existing_artifacts])

        if not filetypes or len(filetypes) > 1:
            raise ValueError("Cannot query artifacts of different filetypes")

        # Get the base query
        filetype: ARTIFACT_FILE_TYPES = cast(ARTIFACT_FILE_TYPES, filetypes.pop())
        artifact_query = self._create_query(globs, filetype)

        # Apply any additional query ontop
        if query_str:
            return duckdb.query(f"FROM artifact_query {query_str}")

        return artifact_query

    def _create_query(self, globs: list[str], filetype: ARTIFACT_FILE_TYPES):
        artifact_base_query: str

        if filetype == "json":
            artifact_base_query = f"SELECT * FROM read_json_auto({globs}, hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)"
        elif filetype == "parquet":
            # Parquet's columns cannot be provided while reading like json,
            # as parquet stores column data type metadata already
            return duckdb.query(
                f"SELECT * FROM read_parquet({globs}, hive_partitioning = true, union_by_name = true)"
            )
        else:
            raise ValueError(f"Unsupported file format {self.filetype}")

        # If the artifact has been provided columns, we override the default
        # auto-detected duckdb columns with any provided
        duckdb_columns_map = ""
        if self.columns:
            asset_query_describe = duckdb.query(f"DESCRIBE {artifact_base_query}")
            asset_query_schema = asset_query_describe.fetchall()

            # Convert the describe query into a dict of { column: {describe row} }
            asset_query_schema = {
                tuple[0]: dict(zip(asset_query_describe.columns, tuple))
                for tuple in asset_query_schema
            }

            # Loop through each supplied column and override the existing data type
            for col_name, new_col_type in self.columns.items():
                existing_col = asset_query_schema.get(col_name, None)

                if not existing_col:
                    raise ValueError(f"Column {col_name} not found in artifact schema")

                existing_col["column_type"] = new_col_type

            # Convert the new columns into a map format as duckdb expects
            duckdb_columns_dict = {
                col_name: col_type
                for col_name, col_data in asset_query_schema.items()
                for col_type in [col_data["column_type"]]
            }

            duckdb_columns_map = f"{duckdb_columns_dict}"

        # If we had a new map, let's insert that into our query
        if duckdb_columns_map:
            if filetype == "json":
                artifact_base_query = f"SELECT * FROM read_json({globs}, format='auto', columns={duckdb_columns_map}, hive_partitioning = true, union_by_name = true, maximum_object_size = 33554432)"
            else:
                raise ValueError(f"Unsupported file format {self.filetype}")

        artifact_query = duckdb.query(artifact_base_query)
        return artifact_query

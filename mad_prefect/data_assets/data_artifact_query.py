import logging
from typing import cast
import duckdb
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.data_assets.data_artifact import DataArtifact

logger = logging.getLogger(__name__)


class DataArtifactQuery:

    def __init__(
        self,
        artifacts: list[DataArtifact] | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        self.artifacts = artifacts or []
        self.read_json_options = read_json_options or ReadJsonOptions()
        self.read_csv_options = read_csv_options or ReadCSVOptions()

    async def query(self, query_str: str | None = None, params: object | None = None):
        await register_mad_protocol()

        # Skip redundant existence checks; rely on persist() marking artifacts as persisted.
        existing_artifacts = [a for a in self.artifacts if a.persisted]
        artifact_paths = [f"mad://{a.path.strip('/')}" for a in existing_artifacts]

        if not artifact_paths:
            logger.warning(
                "Query attempted on an artifact collection with no existing files. Returning None."
            )
            return

        logger.info(f"Starting query across {len(artifact_paths)} artifact paths.")
        logger.debug(f"Querying paths: {artifact_paths}")

        # Ensure each artifact is of the same filetype
        filetypes = set([a.filetype for a in existing_artifacts])

        if not filetypes or len(filetypes) > 1:
            raise ValueError("Cannot query artifacts of different filetypes")

        # Get the base query
        filetype: ARTIFACT_FILE_TYPES = cast(ARTIFACT_FILE_TYPES, filetypes.pop())
        logger.debug(f"Determined artifact filetype for query: {filetype}")

        if filetype == "json":
            artifact_query = self._create_query_json(artifact_paths)
        elif filetype == "parquet":
            artifact_query = self._create_query_parquet(artifact_paths)
        elif filetype == "csv":
            artifact_query = self._create_query_csv(artifact_paths)
        else:
            raise ValueError(f"Unsupported file format {filetype}")

        # Apply any additional query on top
        if query_str:
            final_query_string = f"FROM artifact_query {query_str}"
            logger.debug(f"Executing final query: {final_query_string}")
            return duckdb.query(final_query_string, params=params)

        logger.debug("Executing base artifact query.")
        return artifact_query

    def _create_query_json(self, artifact_paths: list[str]):
        # Prepare the globs string
        artifact_paths_str = ", ".join(f"'{g}'" for g in artifact_paths)
        artifact_paths_formatted = f"[{artifact_paths_str}]"
        logger.debug(
            f"Building JSON read query with options: {self.read_json_options.model_dump(exclude_none=True)}"
        )

        # Build the base options dict without 'columns'
        base_options = self.read_json_options.model_dump(
            exclude={"columns"},
            exclude_none=True,
        )
        options_str = self._format_options_dict(base_options)

        # Build the base query string without 'columns'
        base_query = (
            f"SELECT * FROM read_json({artifact_paths_formatted}, {options_str})"
            if options_str
            else f"SELECT * FROM read_json({artifact_paths_formatted})"
        )

        # Process columns after building the base query
        if self.read_json_options.columns:
            updated_columns = self._process_columns(
                base_query, self.read_json_options.columns
            )

            # Include 'columns' in options
            options_with_columns = base_options.copy()
            options_with_columns["columns"] = updated_columns
            options_str_with_columns = self._format_options_dict(options_with_columns)

            # Rebuild the query with 'columns'
            final_query = f"SELECT * FROM read_json({artifact_paths_formatted}, {options_str_with_columns})"
        else:
            final_query = base_query

        # Execute the query
        logger.debug(f"Generated DuckDB JSON query: {final_query}")
        artifact_query = duckdb.query(final_query)
        return artifact_query

    def _process_columns(
        self,
        base_query: str,
        columns: dict[str, str],
    ) -> dict[str, str]:
        # Describe the base query to get the schema
        logger.debug("Describing base query to determine schema for column processing.")
        schema_info = duckdb.query(f"DESCRIBE {base_query}").fetchall()
        schema_columns = {row[0]: row[1] for row in schema_info}
        logger.debug(f"Inferred schema columns: {schema_columns}")

        # Update column types based on provided columns
        updated_columns = {}
        for col_name, col_type in schema_columns.items():
            if col_name in columns:
                # Use the provided type
                updated_columns[col_name] = columns[col_name]
            else:
                # Use the existing type from the schema
                updated_columns[col_name] = col_type

        logger.debug(f"Final columns for query: {updated_columns}")
        return updated_columns

    def _create_query_parquet(self, artifact_paths: list[str]):
        # Prepare the globs string
        artifact_paths_str = ", ".join(f"'{g}'" for g in artifact_paths)
        artifact_paths_formatted = f"[{artifact_paths_str}]"

        # Include only relevant options
        options_dict = {"hive_partitioning": True, "union_by_name": True}
        options_str = self._format_options_dict(options_dict)

        # Build the query string
        artifact_base_query = (
            f"SELECT * FROM read_parquet({artifact_paths_formatted}, {options_str})"
        )

        # Execute the query
        logger.debug(f"Generated DuckDB Parquet query: {artifact_base_query}")
        artifact_query = duckdb.query(artifact_base_query)
        return artifact_query

    def _create_query_csv(self, artifact_paths: list[str]):
        # Convert each artifact path to a DuckDB-friendly string
        artifact_paths_str = ", ".join(f"'{g}'" for g in artifact_paths)
        artifact_paths_formatted = f"[{artifact_paths_str}]"
        logger.debug(
            f"Building CSV read query with options: {self.read_csv_options.model_dump(exclude_none=True)}"
        )

        # Build the base options dict without 'columns'
        base_options = self.read_csv_options.model_dump(
            exclude_none=True,
        )

        options_str = self._format_options_dict(base_options)

        # Build the base query string without 'columns'
        base_query = (
            f"SELECT * FROM read_csv({artifact_paths_formatted}, {options_str})"
            if options_str
            else f"SELECT * FROM read_csv({artifact_paths_formatted})"
        )

        # Execute the query
        logger.debug(f"Generated DuckDB CSV query: {base_query}")
        artifact_query = duckdb.query(base_query)
        return artifact_query

    def _format_options_dict(self, options_dict: dict) -> str:
        def format_value(key, value):
            if isinstance(value, bool):
                return "TRUE" if value else "FALSE"
            elif isinstance(value, str):
                return f"'{value}'"
            elif isinstance(value, dict):
                return f"{value}"
            else:
                return str(value)

        options_str = ", ".join(
            f"{key} = {format_value(key, value)}" for key, value in options_dict.items()
        )
        return options_str

import json
import os
from typing import BinaryIO, Sequence, cast, Any
import duckdb
import httpx
import jsonlines
import pandas as pd
from pydantic import TypeAdapter
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
from mad_prefect.data_assets.options import ReadCSVOptions, ReadJsonOptions
from mad_prefect.data_assets.utils import yield_data_batches
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pacsv
from mad_prefect.json.mad_json_encoder import MADJSONEncoder


class DataArtifact:
    def __init__(
        self,
        path: str,
        data: object | None = None,
        read_json_options: ReadJsonOptions | None = None,
        read_csv_options: ReadCSVOptions | None = None,
    ):
        self.path = path
        filetype = os.path.splitext(self.path)[1].lstrip(".")

        if filetype not in ["json", "parquet", "csv"]:
            raise ValueError(f"Unsupported file type: {filetype}")

        self.filetype: ARTIFACT_FILE_TYPES = cast(ARTIFACT_FILE_TYPES, filetype)
        self.data = data
        self.read_json_options = read_json_options or ReadJsonOptions()
        self.read_csv_options = read_csv_options or ReadCSVOptions()
        self.persisted = False

    async def persist(self):
        # If we've already persisted this artifact this session, don't do anything
        if self.persisted:
            return True

        if not self._truthy(self.data):
            return False

        await register_mad_protocol()

        # preserve_insertion_order to improve memory usage.
        # https://duckdb.org/docs/guides/performance/how_to_tune_workloads.html#the-preserve_insertion_order-option
        # duckdb.query("SET preserve_insertion_order = false;")

        if isinstance(self.data, duckdb.DuckDBPyRelation):
            # There is a bug with fsspec and duckdb see test: test_overwriting_existing_file
            # to work around the bug, instead of setting (use_tmp_file 0) in the query, we will directly reference
            # the wrapped filesystem
            fs = await get_fs()
            protocol = (
                fs._fs.protocol
                if isinstance(fs._fs.protocol, str)
                else fs._fs.protocol[0]
            )

            # Ensure the directory for the path exists
            fs.mkdirs(os.path.dirname(self.path), exist_ok=True)

            path = fs._resolve_path(self.path)
            d = self.data

            duckdb.register_filesystem(cast(str, fs._fs))
            duckdb.execute(f"COPY d TO '{protocol}://{path}'")
        else:
            if self.filetype == "json":
                await self._persist_json()
            elif self.filetype == "parquet":
                await self._persist_parquet()
            elif self.filetype == "csv":
                await self._persist_csv()
            else:
                raise ValueError(f"Unsupported file format {self.filetype}")

        self.persisted = await self.exists()

        # Release the reference to data to free up memory
        self.data = None

        return self.persisted

    async def _open(self):
        fs = await get_fs()
        return cast(BinaryIO, await fs.open(self.path, "wb", True))

    async def _persist_json(self):
        entities = self._yield_entities_to_persist()
        file: BinaryIO | None = None
        writer: jsonlines.Writer | None = None
        type_adapter: TypeAdapter = TypeAdapter(Any)

        try:
            next_entity = await anext(entities)

            while self._truthy(next_entity):
                # Use the first entity to determine the file's schema
                if not file or not writer:
                    file = await self._open()
                    writer = jsonlines.Writer(
                        file,
                        dumps=lambda obj: json.dumps(  # type: ignore
                            type_adapter.dump_python(obj), cls=MADJSONEncoder
                        ),
                    )

                table_or_batch: pa.RecordBatch | pa.Table = (
                    next_entity
                    if isinstance(next_entity, (pa.Table, pa.RecordBatch))
                    else None
                )

                if table_or_batch:
                    next_entity = table_or_batch.to_pylist()

                if not isinstance(next_entity, Sequence):
                    next_entity = [next_entity]

                writer.write_all(next_entity)
                next_entity = await anext(entities)
        except StopAsyncIteration as e:
            pass
        except Exception as e:
            raise
        finally:
            if writer:
                writer.close()

            if file:
                file.close()

            await entities.aclose()

    async def _persist_parquet(self):
        def __sanitize_data(data):
            """
            Recursively go through the data and replace any empty dictionaries
            with None or a dummy value to avoid Parquet serialization errors.
            """
            if isinstance(data, dict):
                if not data:  # it's an empty dict
                    return None  # or return {'dummy_field': None} to keep the key with a dummy field
                else:
                    return {key: __sanitize_data(value) for key, value in data.items()}
            elif isinstance(data, list):
                return [__sanitize_data(item) for item in data]
            else:
                return data

        entities = self._yield_entities_to_persist()
        file: BinaryIO | None = None
        writer: pq.ParquetWriter | None = None
        type_adapter = TypeAdapter(Any)

        try:
            next_entity = await anext(entities)

            while self._truthy(next_entity):
                b = __sanitize_data(next_entity)
                table_or_batch: pa.RecordBatch | pa.Table = (
                    b
                    if isinstance(b, (pa.Table, pa.RecordBatch))
                    else pa.RecordBatch.from_pylist(type_adapter.dump_python(b))
                )

                # Use the first entity to determine the file's schema
                if not file or not writer:
                    file = await self._open()
                    writer = pq.ParquetWriter(file, table_or_batch.schema)
                else:
                    # If schema has evolved, adjust the current RecordBatch
                    if not table_or_batch.schema.equals(writer.schema):
                        unified_schema = pa.unify_schemas(
                            [writer.schema, table_or_batch.schema],
                            promote_options="permissive",
                        )

                        # Align the RecordBatch with the unified schema
                        table_or_batch = table_or_batch.cast(unified_schema)

                        # Manually adjust the schema of the writer if needed
                        writer.schema = unified_schema

                writer.write(table_or_batch)
                next_entity = await anext(entities)
        except StopAsyncIteration as e:
            pass
        except Exception as e:
            raise
        finally:
            if writer:
                writer.close()

            if file:
                file.close()

            await entities.aclose()

    async def _persist_csv(self):
        entities = self._yield_entities_to_persist()
        file: BinaryIO | None = None
        type_adapter = TypeAdapter(Any)
        first_chunk = True  # Track whether we need to write CSV headers

        try:
            next_entity = await anext(entities)

            while self._truthy(next_entity):
                # If it's already a pa.Table or pa.RecordBatch, use it.
                # Otherwise, convert it to a Table from a list-of-dicts or list-of-rows.
                if not isinstance(next_entity, (pa.Table, pa.RecordBatch)):
                    next_entity = pa.Table.from_pylist(
                        type_adapter.dump_python(next_entity)
                    )

                # If the file isn't open yet, open it
                if not file:
                    file = await self._open()

                # Write the current batch to CSV
                pacsv.write_csv(
                    next_entity,
                    file,
                    write_options=pacsv.WriteOptions(include_header=first_chunk),
                )
                first_chunk = False

                next_entity = await anext(entities)

        except StopAsyncIteration:
            pass
        finally:
            if file:
                file.close()
            await entities.aclose()

    async def _yield_entities_to_persist(self):
        from mad_prefect.data_assets.data_asset import DataAsset

        async for batch_data in yield_data_batches(self.data):
            # If the data is an asset, execute it to get the result artifact
            if isinstance(batch_data, DataAsset):
                batch_data = await batch_data()

            # If the entity is a DataAsset, turn it into a DuckDbPyRelation, so it can be handled
            if isinstance(batch_data, DataArtifact):
                # An artifact may not exist for example when there were no results
                if not await batch_data.exists():
                    continue

                batch_data = await batch_data.query()

            if isinstance(batch_data, pd.DataFrame):
                batch_data = duckdb.from_df(batch_data)

            if isinstance(batch_data, (duckdb.DuckDBPyRelation)):
                # Convert duckdb into batches of arrow tables
                reader = batch_data.fetch_arrow_reader(1000)

                try:
                    while True:
                        try:
                            # this will yield a pyarrow RecordBatch
                            batch = reader.read_next_batch()
                            yield batch
                        except StopIteration as stop:
                            break
                finally:
                    reader.close()

            elif isinstance(batch_data, httpx.Response):
                yield batch_data.json()
            else:
                yield batch_data

    async def query(self, query_str: str | None = None):
        from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery

        artifact_query = DataArtifactQuery(
            [self], self.read_json_options, self.read_csv_options
        )
        return await artifact_query.query(query_str)

    async def exists(self):
        fs = await get_fs()
        self.persisted = fs.exists(self.path)
        return self.persisted

    def _truthy(self, data):
        if isinstance(data, pd.DataFrame):
            if data.empty:
                return False
        # duckdb hangs with the not self.data check, so make sure self.data isn't
        # a duckdb pyrelation before checking self.data
        elif isinstance(data, duckdb.DuckDBPyRelation):
            pass
        elif not data:
            return False

        return True

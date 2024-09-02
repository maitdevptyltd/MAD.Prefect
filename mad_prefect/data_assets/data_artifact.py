import datetime
import os
from typing import BinaryIO, Iterable, cast
import uuid
import duckdb
import httpx
import jsonlines
from mad_prefect.data_assets.utils import yield_data_batches
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs
import pyarrow as pa
import pyarrow.parquet as pq


class DataArtifact:
    def __init__(
        self,
        path: str,
        data: object | None = None,
    ):
        self.path = path
        self.data = data

    async def persist(self):
        if not self.data:
            return

        fs = await get_fs()
        await register_mad_protocol()
        duckdb.query("SET temp_directory = './.tmp/duckdb/'")

        (_, path_extension) = os.path.splitext(self.path)

        # Open the path, this is where we want to write the data
        with await fs.open(self.path, "wb", True) as f:
            f = cast(BinaryIO, f)

            if path_extension == ".json":
                await self._persist_json(f)
            elif path_extension == ".parquet":
                await self._persist_parquet(f)
            else:
                raise ValueError("Unsupported file format")

    async def _persist_json(self, file: BinaryIO):
        # The file can be written in batches for better memory management
        with jsonlines.Writer(file) as writer:
            async for b in self._yield_entities_to_persist():
                table_or_batch: pa.RecordBatch | pa.Table = (
                    b if isinstance(b, (pa.Table, pa.RecordBatch)) else None
                )

                if table_or_batch:
                    b = table_or_batch.to_pylist()

                    # json doesn't support datetime or UUID out the box, so sanitize it from pyarrow
                    def sanitze_data(data):
                        if isinstance(data, dict):
                            return {k: (sanitze_data(v)) for k, v in data.items()}
                        elif isinstance(data, list):
                            return [sanitze_data(item) for item in data]
                        elif isinstance(data, uuid.UUID):
                            return str(data)

                        # Parquet can handle dates, json doesn't by default
                        # TODO: how do we register the date data type with jsonl?
                        elif isinstance(data, datetime.datetime) or isinstance(
                            data, datetime.date
                        ):
                            return data.isoformat()
                        else:
                            return data

                    b = sanitze_data(b)

                if not isinstance(b, Iterable):
                    b = [b]

                writer.write_all(b)

    async def _persist_parquet(self, file: BinaryIO):
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

        writer: pq.ParquetWriter | None = None

        try:
            async for b in self._yield_entities_to_persist():
                b = __sanitize_data(b)
                table_or_batch: pa.RecordBatch | pa.Table = (
                    b
                    if isinstance(b, (pa.Table, pa.RecordBatch))
                    else pa.RecordBatch.from_pylist(b)
                )

                # Use the first entity to determine the file's schema
                if not writer:
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
        except Exception as e:
            raise
        finally:
            if writer:
                writer.close()

    async def _yield_entities_to_persist(self):
        from mad_prefect.data_assets.data_asset import DataAsset

        (_, path_extension) = os.path.splitext(self.path)

        async for batch_data in yield_data_batches(self.data):
            # If the data is an asset, execute it to get the result artifact
            if isinstance(batch_data, DataAsset):
                batch_data = await batch_data()

            # If the entity is a DataAsset, turn it into a DuckDbPyRelation, so it can be handled
            if isinstance(batch_data, DataArtifact):
                # An artifact may not exist for example when there were no results
                if not await batch_data.exists():
                    continue

                batch_data = batch_data.query()

            if isinstance(batch_data, (duckdb.DuckDBPyRelation)):
                # Convert duckdb into batches of arrow tables
                reader = batch_data.fetch_arrow_reader(1000)

                while True:
                    try:
                        # this will yield a pyarrow RecordBatch
                        batch = reader.read_next_batch()
                        yield batch
                    except StopIteration as stop:
                        break

            elif isinstance(batch_data, httpx.Response):
                yield batch_data.json()
            else:
                yield batch_data

    def query(self, query_str: str | None = None):
        asset_query = duckdb.query(f"SELECT * FROM 'mad://{self.path}'")

        if query_str:
            return duckdb.query(f"FROM asset_query {query_str}")

        return asset_query

    async def exists(self):
        fs = await get_fs()
        return fs.exists(self.path)

import os
from typing import BinaryIO, cast
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
                writer.write(b)

    async def _persist_parquet(self, file: BinaryIO):
        writer: pq.ParquetWriter | None = None

        async for b in self._yield_entities_to_persist():
            record_batch: pa.RecordBatch = pa.RecordBatch.from_pylist(b)

            # Use the first entity to determine the file's schema
            if not writer:
                writer = pq.ParquetWriter(file, record_batch.schema)

            writer.write_batch(record_batch)

    async def _yield_entities_to_persist(self):
        async for batch_data in yield_data_batches(self.data):
            # If the entity is a DataAsset, turn it into a DuckDbPyRelation, so it can be handled
            if isinstance(batch_data, DataArtifact):
                batch_data = batch_data.query()

            if isinstance(batch_data, (duckdb.DuckDBPyRelation)):

                while True:
                    # duckdb fetchmany returns a list of tuples
                    # turn it into a list of dicts
                    fetched_batch = batch_data.fetchmany(1000)
                    fetched_batch = [
                        dict(zip(batch_data.columns, row)) for row in fetched_batch
                    ]

                    if not fetched_batch:
                        break

                    yield fetched_batch

            elif isinstance(batch_data, httpx.Response):
                yield batch_data.json()
            else:
                yield batch_data

    def query(self, query_str: str | None = None):
        asset_query = duckdb.query(f"SELECT * FROM 'mad://{self.path}'")

        if query_str:
            return duckdb.query(f"FROM asset_query {query_str}")

        return asset_query

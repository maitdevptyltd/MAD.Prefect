import datetime
import os
from typing import BinaryIO, Sequence, cast
import uuid
import duckdb
import httpx
import jsonlines
from mad_prefect.data_assets import ARTIFACT_FILE_TYPES
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
        columns: dict[str, str] | None = None,
    ):
        self.path = path
        filetype = os.path.splitext(self.path)[1].lstrip(".")

        if filetype not in ["json", "parquet"]:
            raise ValueError(f"Unsupported file type: {filetype}")

        self.filetype: ARTIFACT_FILE_TYPES = cast(ARTIFACT_FILE_TYPES, filetype)
        self.data = data
        self.columns = columns or {}

    async def persist(self):
        if not self.data:
            return

        await register_mad_protocol()
        duckdb.query("SET temp_directory = './.tmp/duckdb/'")

        if self.filetype == "json":
            await self._persist_json()
        elif self.filetype == "parquet":
            await self._persist_parquet()
        else:
            raise ValueError(f"Unsupported file format {self.filetype}")

        return await self.exists()

    async def _open(self):
        fs = await get_fs()
        return cast(BinaryIO, await fs.open(self.path, "wb", True))

    async def _persist_json(self):
        # json doesn't support datetime or UUID out the box, so sanitize it from pyarrow
        def __sanitize_data(data):
            if isinstance(data, dict):
                return {k: (__sanitize_data(v)) for k, v in data.items()}
            elif isinstance(data, list):
                return [__sanitize_data(item) for item in data]
            elif isinstance(data, uuid.UUID):
                return str(data)

            # Parquet can handle dates, json doesn't by default
            # TODO: how do we register the date data type with jsonl?
            elif isinstance(data, datetime.datetime) or isinstance(data, datetime.date):
                return data.isoformat()
            else:
                return data

        entities = self._yield_entities_to_persist()
        file: BinaryIO | None = None
        writer: jsonlines.Writer | None = None

        try:
            next_entity = await anext(entities)

            while next_entity:
                # Use the first entity to determine the file's schema
                if not file or not writer:
                    file = await self._open()
                    writer = jsonlines.Writer(file)

                table_or_batch: pa.RecordBatch | pa.Table = (
                    next_entity
                    if isinstance(next_entity, (pa.Table, pa.RecordBatch))
                    else None
                )

                if table_or_batch:
                    next_entity = table_or_batch.to_pylist()
                    next_entity = __sanitize_data(next_entity)

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

        try:
            next_entity = await anext(entities)

            while next_entity:
                b = __sanitize_data(next_entity)
                table_or_batch: pa.RecordBatch | pa.Table = (
                    b
                    if isinstance(b, (pa.Table, pa.RecordBatch))
                    else pa.RecordBatch.from_pylist(b)
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

    async def query(self, query_str: str | None = None):
        from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery

        artifact_query = DataArtifactQuery([self], self.columns)
        return await artifact_query.query(query_str)

    async def exists(self):
        fs = await get_fs()
        return fs.exists(self.path)

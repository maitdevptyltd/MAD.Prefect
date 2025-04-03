from datetime import UTC, datetime, timedelta, timezone
from functools import partial
import hashlib
import inspect
import os
from pathlib import Path
from typing import Generic, ParamSpec, TypeVar, cast
import duckdb
from mad_prefect.data_assets.asset_decorator import ASSET_METADATA_LOCATION
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_artifact_collector import DataArtifactCollector
from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery
from mad_prefect.data_assets.data_asset_run import DataAssetRun
from mad_prefect.duckdb import register_mad_protocol
from mad_prefect.filesystems import get_fs
from mad_prefect.data_assets.data_asset import DataAsset

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


class DataAssetCallable(Generic[P, R]):
    def __init__(self, asset: DataAsset[P, R]):
        self.asset = asset
        self._fn = asset._fn

    @property
    def args(self):
        if isinstance(self._fn, partial):
            return self._fn.args

        return tuple()

    @property
    def keywords(self):
        if isinstance(self._fn, partial):
            return self._fn.keywords

        return {}

    @property
    def fn(self):
        if isinstance(self._fn, partial):
            return self._fn.func

        return self._fn

    def get_bound_arguments(self):
        bound_args = inspect.signature(self.fn).bind_partial(
            *self.args,
            **self.keywords,
        )
        bound_args.apply_defaults()

        return bound_args

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> DataArtifact:
        asset = self.asset

        if args or kwargs:
            # If arguments are passed in, create a new instance with bound arguments and call it
            asset = asset.with_arguments(*args, **kwargs)
            return await asset()

        bound_args = self.get_bound_arguments()

        asset.name = asset.name.format(*self.args, **bound_args.arguments)
        asset.path = asset.path.format(*self.args, **bound_args.arguments)
        asset.options.artifacts_dir = asset.options.artifacts_dir.format(
            *args,
            **kwargs,
        )

        self.asset_run = asset_run = DataAssetRun()
        asset_run.id = self._generate_asset_iteration_guid()
        asset_run.asset_id = asset.id
        asset_run.asset_name = asset.name
        asset_run.asset_path = asset.path

        # Extract and set filetypes for result artifacts
        self.result_artifact_filetypes = self.get_result_artifact_filetypes(asset)

        # There may be multiple result artifacts if following syntax is used "bronze/customers.parquet|csv"
        self.result_artifacts = self._create_result_artifacts(asset)

        # Prevent asset from being rematerialized inside a single session
        if asset_run and (materialized := asset_run.materialized):
            return self.result_artifacts[0]

        asset_run.runtime = datetime.now(UTC)
        self.last_materialized = await self._get_last_materialized(asset)

        # If data has been materialized within cache_expiration period return empty result_artifact
        if await self._cached_result(
            self.result_artifacts[0],
            asset_run.runtime,
            self.asset.options.cache_expiration or timedelta(0),
        ):
            return self.result_artifacts[0]

        # Regenerate asset_run_id as runtime has now been set.
        asset_run.id = self._generate_asset_iteration_guid()

        print(
            f"Running operations for asset_run_id: {asset_run.id}, on asset_id: {self.asset.id}, on asset: {asset.name}"
        )

        # Write metadata before processing result for troubleshooting purposes
        await asset_run.persist()

        # For each fragment in the data batch, we create a new artifact
        base_artifact_path = self._get_artifact_base_path()

        # Clean up the old directory and delete it if we're not snapshotting
        if not asset.options.snapshot_artifacts:
            fs = await get_fs()
            await fs.delete_path(base_artifact_path, recursive=True)

        collector = DataArtifactCollector(
            cast(partial, self._fn)(),
            base_artifact_path,
            asset.options.artifact_filetype,
            read_json_options=asset.options.read_json_options,
            read_csv_options=asset.options.read_csv_options,
        )

        # Collect the artifacts yielded from the materialization fn
        collector_artifacts = await collector.collect()

        # Query each of the artifacts [filepath1, filepath2, etc] with duckdb
        artifact_query = DataArtifactQuery(
            artifacts=collector_artifacts,
            read_json_options=asset.options.read_json_options,
            read_csv_options=asset.options.read_csv_options,
        )

        # The result is all the artifacts unioned
        result_artifact_data = await artifact_query.query()

        for result_artifact in self.result_artifacts:
            result_artifact.data = result_artifact_data

            # Persist the result artifact to storage, fully materialize it
            await result_artifact.persist()

        # Release reference to data
        result_artifact_data = None

        # Record information about the run
        asset_run.materialized = datetime.now(UTC)
        duration = asset_run.materialized - asset_run.runtime
        asset_run.duration_miliseconds = int(duration.total_seconds() * 1000)

        await asset_run.persist()

        print(
            f"Completed operations for asset_run_id: {asset_run.id}, on asset_id: {self.asset.id}, on asset: {self.asset.name}"
        )

        return self.result_artifacts[0]

    def _generate_asset_iteration_guid(self):
        hash_input = f"{self.asset.name}:{self.asset.path}:{self.asset.options.artifacts_dir}:{self.asset_run.runtime.isoformat() if self.asset_run.runtime else ''}:{str(self.args) if self.keywords else ''}"
        return hashlib.md5(hash_input.encode()).hexdigest()

    def get_result_artifact_filetypes(self, asset: DataAsset):
        path = Path(asset.path)
        filetypes_part = path.suffix

        return [f if f.startswith(".") else f".{f}" for f in filetypes_part.split("|")]

    def _create_result_artifacts(self, asset: DataAsset) -> list[DataArtifact]:
        path = Path(asset.path)
        result_artifacts = []
        filetypes = self.get_result_artifact_filetypes(asset)

        for filetype in filetypes:
            result_artifacts.append(
                DataArtifact(
                    path.with_suffix(filetype).as_posix(),
                    read_json_options=asset.options.read_json_options,
                    read_csv_options=asset.options.read_csv_options,
                )
            )

        return result_artifacts

    async def _get_last_materialized(self, asset: DataAsset):
        asset_metadata = await self._get_asset_metadata(asset)

        if not asset_metadata:
            return

        last_materialized_query = duckdb.query(
            "SELECT max(strptime(materialized, '%Y-%m-%dT%H:%M:%S.%fZ')) FROM asset_metadata"
        ).fetchone()

        if last_materialized_query and last_materialized_query[0]:
            # Convert DuckDB timestamp to Python datetime
            last_materialized = datetime.fromisoformat(str(last_materialized_query[0]))
            # Ensure it's UTC
            return last_materialized.replace(tzinfo=timezone.utc)

    async def _get_asset_metadata(self, asset: DataAsset):
        await register_mad_protocol()
        fs = await get_fs()

        metadata_glob = f"{ASSET_METADATA_LOCATION}/asset_name={asset.name}/asset_id={asset.id}/**/*.json"

        if fs.glob(metadata_glob):
            return duckdb.query(
                f"SELECT UNNEST(data, max_depth:=2) FROM read_json('mad://{metadata_glob}')"
            )

    async def _cached_result(
        self,
        result_artifact: DataArtifact,
        runtime: datetime,
        cache_expiration: timedelta,
    ):
        # Check if data has been materialized within cache_expiration period
        if (
            self.last_materialized
            and (self.last_materialized > runtime - cache_expiration)
            and await result_artifact.exists()
        ):
            print(
                f"Retrieving cached result_artifact for asset_id: {self.asset.id} | asset: {self.asset.name}"
            )
            return True

    def _get_artifact_base_path(self):
        partition = ""

        # If we snapshot artifacts, encapsulate the file in a directory with the runtime= parameter
        # so you can view changes over time
        if self.asset.options.snapshot_artifacts and self.asset_run.runtime:
            runtime_str = str(self.asset_run.runtime.isoformat()).replace(":", "_")
            partition = (
                f"year={self.asset_run.runtime.year}/month={self.asset_run.runtime.month}/day={self.asset_run.runtime.day}/runtime={runtime_str}/"
                if self.asset.options.snapshot_artifacts
                else ""
            )

        # Extract folder path for folder set up
        folder_path = os.path.dirname(self.asset.path)

        # Set up the base path for artifact storage
        if not self.asset.options.artifacts_dir:
            base_path = f"{folder_path}/_artifacts/asset={self.asset.name}/{partition}"
        else:
            base_path = f"{self.asset.options.artifacts_dir}/{partition}"

        return base_path

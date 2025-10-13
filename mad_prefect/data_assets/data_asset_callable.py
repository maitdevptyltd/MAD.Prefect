from datetime import UTC, datetime, timedelta, timezone
from functools import partial
import hashlib
import inspect
import logging
import os
from pathlib import Path
from typing import Generic, ParamSpec, TypeVar, cast
import duckdb
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.data_assets.data_artifact_collector import DataArtifactCollector
from mad_prefect.data_assets.data_artifact_query import DataArtifactQuery
from mad_prefect.data_assets.data_asset_run import DataAssetRun
from mad_prefect.data_assets.asset_template_formatter import AssetTemplateFormatter
from mad_prefect.data_assets.asset_metadata import (
    ManifestRunStatus,
    get_asset_metadata,
    load_asset_manifest,
)
from mad_prefect.data_assets.utils import safe_truthy
from mad_prefect.filesystems import get_fs
from mad_prefect.data_assets.data_asset import DataAsset
from mad_prefect.duckdb import register_mad_protocol

P = ParamSpec("P")
R = TypeVar("R", covariant=True)

logger = logging.getLogger(__name__)


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
            logger.debug(
                f"Asset '{asset.name}' called with arguments, creating new configured asset instance."
            )
            asset = asset.with_arguments(*args, **kwargs)
            return await asset()

        bound_args = self.get_bound_arguments()
        logger.debug(
            f"Bound arguments for asset '{asset.name}': {bound_args.arguments}"
        )

        formatter = AssetTemplateFormatter(self.args, bound_args)
        asset.name = formatter.format(asset.name) or ""
        asset.path = formatter.format(asset.path) or ""
        asset.options.artifacts_dir = (
            formatter.format(asset.options.artifacts_dir) or ""
        )
        logger.debug(f"Formatted asset name: '{asset.name}', path: '{asset.path}'")

        self.asset_run = asset_run = DataAssetRun()
        asset_run.id = self._generate_asset_iteration_guid()
        asset_run.asset_id = asset.id
        asset_run.asset_name = asset.name
        asset_run.asset_path = asset.path
        logger.debug(f"Initialized DataAssetRun with id: {asset_run.id}")

        # Extract and set filetypes for result artifacts
        self.result_artifact_filetypes = self.get_result_artifact_filetypes(asset)

        # There may be multiple result artifacts if following syntax is used "bronze/customers.parquet|csv"
        self.result_artifacts = self._create_result_artifacts(asset)
        logger.debug(
            f"Created {len(self.result_artifacts)} result artifact(s) for paths: {[a.path for a in self.result_artifacts]}"
        )

        # Prevent asset from being rematerialized inside a single session
        if asset_run and asset_run.materialized:
            logger.info(
                f"Asset '{asset.name}' already materialized in this session. Returning existing artifact."
            )
            return self.result_artifacts[0]

        asset_run.runtime = datetime.now(UTC)
        self.last_materialized = await self._get_last_materialized(asset)
        logger.debug(
            f"Last materialization time for asset '{asset.name}': {self.last_materialized}"
        )

        # If data has been materialized within cache_expiration period return empty result_artifact
        if await self._cached_result(
            self.result_artifacts[0],
            asset_run.runtime,
            self.asset.options.cache_expiration or timedelta(0),
        ):
            return self.result_artifacts[0]

        # Regenerate asset_run_id as runtime has now been set.
        asset_run.id = self._generate_asset_iteration_guid()
        logger.debug(f"Regenerated asset run ID with runtime: {asset_run.id}")

        logger.info(
            f"Executing asset '{asset.name}' (run_id: {asset_run.id}, asset_id: {self.asset.id})"
        )

        # Write metadata before processing result for troubleshooting purposes
        await asset_run.persist(
            asset_signature=asset.id,
            status=ManifestRunStatus.UNKNOWN,
            update_manifest=False,
        )

        # For each fragment in the data batch, we create a new artifact
        base_artifact_path = self._get_artifact_base_path()
        logger.debug(f"Base artifact path for fragments: {base_artifact_path}")

        # Clean up the old directory and delete it if we're not snapshotting
        if not asset.options.snapshot_artifacts:
            logger.info(
                f"Snapshotting disabled. Cleaning up old artifacts in {base_artifact_path}"
            )
            fs = await get_fs()
            await fs.delete_path(base_artifact_path, recursive=True)

        # Ensure MAD protocol is available before executing user-provided asset code.
        await register_mad_protocol()

        collector = DataArtifactCollector(
            cast(partial, self._fn)(),
            base_artifact_path,
            asset.options.artifact_filetype,
            read_json_options=asset.options.read_json_options,
            read_csv_options=asset.options.read_csv_options,
        )

        # Collect the artifacts yielded from the materialization fn
        collector_artifacts = await collector.collect()

        if not collector_artifacts:
            logger.info(
                f"Asset materialization for '{asset.name}' yielded no artifacts. The resulting asset will be empty."
            )

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

        await asset_run.persist(
            artifact_paths=[artifact.path for artifact in self.result_artifacts],
            asset_signature=asset.id,
            status=ManifestRunStatus.SUCCESS,
        )

        logger.info(
            f"Successfully executed asset '{asset.name}'. Duration: {duration.total_seconds():.2f}s (run_id: {asset_run.id})"
        )

        return self.result_artifacts[0]

    def _generate_asset_iteration_guid(self):
        hash_input = f"{self.asset.name}:{self.asset.path}:{self.asset.options.artifacts_dir}:{self.asset_run.runtime.isoformat() if self.asset_run.runtime else ''}:{str(self.args) if self.keywords else ''}"
        guid = hashlib.md5(hash_input.encode()).hexdigest()
        logger.debug(f"Generated asset iteration GUID: {guid}")
        return guid

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
        logger.debug(f"Fetching last materialization time for asset '{asset.name}'")

        manifest = await load_asset_manifest(asset.name, asset.id)

        if manifest and manifest.last_materialized:
            logger.debug(
                "Resolved last materialized from manifest: %s",
                manifest.last_materialized,
            )
            return manifest.last_materialized

        asset_metadata = await get_asset_metadata(asset.name, asset.id)

        if not safe_truthy(asset_metadata):
            return

        last_materialized_query = duckdb.query(
            "SELECT max(CAST(materialized AS VARCHAR)) FROM asset_metadata"
        ).fetchone()

        if last_materialized_query and last_materialized_query[0]:
            last_materialized = datetime.fromisoformat(str(last_materialized_query[0]))
            return last_materialized.replace(tzinfo=timezone.utc)

    async def _cached_result(
        self,
        result_artifact: DataArtifact,
        runtime: datetime,
        cache_expiration: timedelta,
    ):
        if cache_expiration.total_seconds() <= 0:
            return False

        # Check if data has been materialized within cache_expiration period
        if (
            self.last_materialized
            and (self.last_materialized > runtime - cache_expiration)
            and await result_artifact.exists()
        ):
            logger.info(
                f"Cache hit for asset '{self.asset.name}' (id: {self.asset.id}). Last materialized at {self.last_materialized} which is within {cache_expiration}."
            )
            return True
        logger.debug(f"Cache miss for asset '{self.asset.name}'.")
        return False

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

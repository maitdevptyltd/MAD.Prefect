import io
import os
from typing import Any
from pandas import DataFrame, read_parquet
from prefect import task
import prefect.filesystems
import prefect.utilities.asyncutils
import fsspec
import fsspec.utils
from prefect.serializers import JSONSerializer
from prefect.blocks.fields import SecretDict

FILESYSTEM_URL = os.getenv("FILESYSTEM_URL", "file://./.tmp/storage")
FILESYSTEM_BLOCK_NAME = os.getenv("FILESYSTEM_BLOCK_NAME")


class FsspecFileSystem(
    prefect.filesystems.WritableFileSystem,
    prefect.filesystems.WritableDeploymentStorage,
):
    _block_type_name = "Fsspec Advanced FileSystem"

    basepath: str
    storage_options: SecretDict = {}

    def __init__(self, basepath: str, storage_options=None, **kwargs):
        self.basepath = basepath
        self.storage_options = storage_options or kwargs

        fs, fs_url = fsspec.core.url_to_fs(basepath, **self.storage_options)

        self._fs: fsspec.AbstractFileSystem = fs
        self._fs_url: str = fs_url

    def _resolve_path(self, path: str):
        # resolve the path relative to the basepath as supplied by fsspec
        return f"{self._fs_url.rstrip('/')}/{path.lstrip('/')}"

    @prefect.utilities.asyncutils.sync_compatible
    async def read_path(self, path: str) -> bytes:
        path = self._resolve_path(path)

        # Check if the path exists
        if not self._fs.exists(path):
            raise ValueError(f"Path {path} does not exist.")

        # Validate that its a file
        if self._fs.info(path)["type"] != "file":
            raise ValueError(f"Path {path} is not a file.")

        self._fs.mkdirs(self._fs._parent(path), exist_ok=True)
        file = self._fs.read_bytes(path)

        return file

    @prefect.utilities.asyncutils.sync_compatible
    async def write_path(self, path: str, content: bytes) -> None:
        resolved_path = self._resolve_path(path)

        self._fs.mkdirs(self._fs._parent(resolved_path), exist_ok=True)
        self._fs.write_bytes(resolved_path, content)

        return path

    @prefect.utilities.asyncutils.sync_compatible
    async def move_path(self, path: str, dest: str) -> str:
        source_path = self._resolve_path(path)
        destination_path = self._resolve_path(dest)

        # Ensure source path exists
        if not self._fs.exists(source_path):
            raise ValueError(f"Source path {source_path} does not exist.")

        # Ensure destination's parent directory exists
        self._fs.mkdirs(self._fs._parent(destination_path), exist_ok=True)

        # Move the file
        self._fs.move(source_path, destination_path)

        return destination_path

    @prefect.utilities.asyncutils.sync_compatible
    async def delete_path(self, path: str) -> None:
        pass

    @prefect.utilities.asyncutils.sync_compatible
    async def get_directory(
        self, from_path: str = None, local_path: str = None
    ) -> None:
        pass

    @prefect.utilities.asyncutils.sync_compatible
    async def put_directory(
        self, local_path: str = None, to_path: str = None, ignore_file: str = None
    ) -> None:
        pass

    @prefect.utilities.asyncutils.sync_compatible
    async def write_data(
        self,
        path: str,
        data: list | dict | Any,
        indent: bool = True,
        **kwargs,
    ):
        if isinstance(data, dict):
            # if path has variables, substitute them for the values inside data
            # but only if the data is a simple dict
            path = path.format(**{**kwargs, **data})
        elif isinstance(data, list):
            path = path.format(**{**kwargs, "data": data})

        # infer the serialization type from the path
        if path.lower().endswith(".parquet"):
            buf = io.BytesIO()
            DataFrame(data).to_parquet(buf)
            buf.seek(0)
            data = buf.getvalue()

        # otherwise just write using json as default
        else:
            js = (
                JSONSerializer(dumps_kwargs={"indent": 4})
                if indent
                else JSONSerializer()
            )
            data = js.dumps(data)

        # write to the fs
        return await self.write_path(path, data)

    @prefect.utilities.asyncutils.sync_compatible
    async def read_data(self, path: str):
        data = await self.read_path(path)

        # infer the deserialization type from the path
        if path.lower().endswith(".parquet"):
            buf = io.BytesIO(data)
            data = read_parquet(buf).to_dict(orient="records")
        # otherwise assume json as default
        else:
            js = JSONSerializer()
            data = js.loads(data)

        return data

    @prefect.utilities.asyncutils.sync_compatible
    async def open(self, path: str, mode: str = "rb"):
        resolved_path = self._resolve_path(path)

        # Check if the path exists and is a file
        if not self._fs.exists(resolved_path):
            raise ValueError(f"Path {resolved_path} does not exist.")
        if self._fs.info(resolved_path)["type"] != "file":
            raise ValueError(f"Path {resolved_path} is not a file.")

        # Open the file and read the contents
        return self._fs.open(resolved_path, mode=mode)


def get_filesystem():
    result: FsspecFileSystem

    if FILESYSTEM_BLOCK_NAME:
        result = FsspecFileSystem.load(FILESYSTEM_BLOCK_NAME)
    else:
        result = FsspecFileSystem(basepath=FILESYSTEM_URL)

    return result


def create_write_to_filesystem_task(fs: FsspecFileSystem):
    @task
    def write_to_filesystem(
        path: str,
        data: list | dict | Any,
        indent: bool = True,
        **kwargs,
    ):
        if isinstance(data, dict):
            # if path has variables, substitute them for the values inside data
            # but only if the data is a simple dict
            path = path.format(**{**kwargs, **data})
        elif isinstance(data, list):
            path = path.format(**{**kwargs, "data": data})

        # infer the serialization type from the path
        if path.lower().endswith(".parquet"):
            buf = io.BytesIO()
            DataFrame(data).to_parquet(buf)
            buf.seek(0)
            data = buf.getvalue()
        # otherwise just write using json as default
        else:
            js = (
                JSONSerializer(dumps_kwargs={"indent": 4})
                if indent
                else JSONSerializer()
            )
            data = js.dumps(data)

        # write to the fs
        return fs.write_path(path, data)

    return write_to_filesystem


def create_read_from_filesystem_task(fs: FsspecFileSystem):
    @task
    def read_from_filesystem(path: str):
        data = fs.read_path(path)

        # infer the deserialization type from the path
        if path.lower().endswith(".parquet"):
            buf = io.BytesIO(data)
            data = read_parquet(buf).to_dict(orient="records")
        # otherwise assume json as default
        else:
            js = JSONSerializer()
            data = js.loads(data)

        return data

    return read_from_filesystem


write_to_filesystem = create_write_to_filesystem_task(get_filesystem())
read_from_filesystem = create_read_from_filesystem_task(get_filesystem())

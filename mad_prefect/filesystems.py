import io
import os
from typing import Any, cast
from pandas import DataFrame, read_parquet
import prefect.filesystems
import prefect.utilities.asyncutils
import fsspec
import fsspec.utils
from prefect.serializers import JSONSerializer
from prefect.blocks.fields import SecretDict
import sshfs
import tempfile

fsspec.register_implementation("ssh", sshfs.SSHFileSystem)
fsspec.register_implementation("sftp", sshfs.SSHFileSystem)

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

        return cast(bytes, file)

    @prefect.utilities.asyncutils.sync_compatible
    async def write_path(self, path: str, content: bytes):
        resolved_path = self._resolve_path(path)

        # Ensure the directory is created
        self._fs.mkdirs(self._fs._parent(resolved_path), exist_ok=True)

        # Create a temporary file
        with tempfile.NamedTemporaryFile() as temp_file:
            temp_file.write(content)
            temp_file.flush()

            # Upload the temporary file to the destination path
            self._fs.put(temp_file.name, resolved_path)

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
        raise NotImplementedError()

    @prefect.utilities.asyncutils.sync_compatible
    async def get_directory(
        self, from_path: str | None = None, local_path: str | None = None
    ) -> None:
        raise NotImplementedError()

    @prefect.utilities.asyncutils.sync_compatible
    async def put_directory(
        self,
        local_path: str | None = None,
        to_path: str | None = None,
        ignore_file: str | None = None,
    ) -> None:
        raise NotImplementedError()

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
    async def open(self, path: str, mode: str = "rb", auto_mkdir: bool = False):
        resolved_path = self._resolve_path(path)

        if "w" in mode:
            if auto_mkdir:
                self._fs.mkdirs(self._fs._parent(resolved_path), exist_ok=True)
        else:
            # Check if the path exists and is a file
            if not self._fs.exists(resolved_path):
                raise ValueError(f"Path {resolved_path} does not exist.")
            if self._fs.info(resolved_path)["type"] != "file":
                raise ValueError(f"Path {resolved_path} is not a file.")

        # Open the file and read the contents
        return self._fs.open(resolved_path, mode=mode)


@prefect.utilities.asyncutils.sync_compatible
async def get_fs():
    result: FsspecFileSystem

    if FILESYSTEM_BLOCK_NAME:
        result = await FsspecFileSystem.load(FILESYSTEM_BLOCK_NAME)
    else:
        result = FsspecFileSystem(basepath=FILESYSTEM_URL)

    return result

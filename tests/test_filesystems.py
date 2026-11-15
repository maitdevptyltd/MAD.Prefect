import json
from unittest.mock import Mock

import fsspec
import pytest

from mad_prefect.filesystems import FsspecFileSystem


def make_filesystem(monkeypatch: pytest.MonkeyPatch, root: str, backend: Mock):
    # Isolate MAD's path handling from storage drivers and network access.
    monkeypatch.setattr(fsspec.core, "url_to_fs", Mock(return_value=(backend, root)))
    return FsspecFileSystem(basepath="abfss://example-container/service")


@pytest.mark.parametrize(
    ("root", "absolute_path"),
    [
        ("example-container/service", "example-container/service/invoice/data.parquet"),
        ("example-container/service/", "example-container/service/invoice/data.parquet"),
        ("example-container/service//", "example-container/service/invoice/data.parquet"),
        ("example-container/", "example-container/invoice/data.parquet"),
        ("/data/", "/data/invoice/data.parquet"),
        ("C:/data/", "C:/data/invoice/data.parquet"),
        ("/", "/invoice/data.parquet"),
        ("", "/invoice/data.parquet"),
    ],
)
def test_glob_paths_can_be_used_for_file_access(
    monkeypatch: pytest.MonkeyPatch, root: str, absolute_path: str
):
    backend = Mock(spec=fsspec.AbstractFileSystem)
    backend.glob.return_value = [absolute_path]
    backend.exists.side_effect = lambda path: path == absolute_path
    filesystem = make_filesystem(monkeypatch, root, backend)

    paths = filesystem.glob("invoice/*.parquet")

    assert paths == ["invoice/data.parquet"]
    assert filesystem.exists(paths[0])
    backend.exists.assert_called_once_with(absolute_path)


def test_glob_preserves_matching_text_inside_relative_paths(
    monkeypatch: pytest.MonkeyPatch,
):
    backend = Mock(spec=fsspec.AbstractFileSystem)
    backend.glob.return_value = [
        "example-container/service/archive/example-container/service/data.parquet"
    ]
    filesystem = make_filesystem(monkeypatch, "example-container/service", backend)

    assert filesystem.glob("archive/**/*.parquet") == [
        "archive/example-container/service/data.parquet"
    ]


def test_glob_keeps_paths_without_the_base_prefix(monkeypatch: pytest.MonkeyPatch):
    backend = Mock(spec=fsspec.AbstractFileSystem)
    backend.glob.return_value = ["invoice/data.parquet"]
    filesystem = make_filesystem(monkeypatch, "example-container/service/", backend)

    assert filesystem.glob("invoice/*.parquet") == ["invoice/data.parquet"]


def test_glob_with_no_matches_returns_an_empty_list(monkeypatch: pytest.MonkeyPatch):
    backend = Mock(spec=fsspec.AbstractFileSystem)
    backend.glob.return_value = []
    filesystem = make_filesystem(monkeypatch, "example-container/service/", backend)

    assert filesystem.glob("invoice/*.parquet") == []


@pytest.mark.parametrize(
    ("url", "root"),
    [
        ("abfss://example-container/service/", "example-container/service/"),
        ("file:///", "/"),
        ("memory://", ""),
    ],
)
@pytest.mark.parametrize("construction", ["constructor", "model", "json"])
def test_configured_urls_are_preserved_across_validation_paths(
    monkeypatch: pytest.MonkeyPatch, url: str, root: str, construction: str
):
    backend = Mock(spec=fsspec.AbstractFileSystem)
    backend.glob.return_value = [f"{root.rstrip('/')}/invoice/data.parquet"]
    monkeypatch.setattr(fsspec.core, "url_to_fs", Mock(return_value=(backend, root)))
    configuration = {"basepath": url, "storage_options": {}}

    if construction == "model":
        filesystem = FsspecFileSystem.model_validate(configuration)
    elif construction == "json":
        filesystem = FsspecFileSystem.model_validate_json(json.dumps(configuration))
    else:
        filesystem = FsspecFileSystem(basepath=url)

    assert filesystem.basepath == url
    assert filesystem.model_dump()["basepath"] == url
    assert filesystem.glob("invoice/*.parquet") == ["invoice/data.parquet"]

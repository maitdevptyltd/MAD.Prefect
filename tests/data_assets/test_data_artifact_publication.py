from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from mad_prefect.data_assets import data_artifact
from mad_prefect.data_assets.data_artifact import DataArtifact


@pytest.fixture
def publication(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Exercise publication with local bytes and no storage or query service."""
    destination = tmp_path / "logs.parquet"
    destination.write_bytes(b"previous complete output")
    backend = Mock(protocol="abfs")
    filesystem = Mock()
    filesystem._fs = backend
    filesystem._resolve_path.return_value = str(destination)
    filesystem.exists.side_effect = lambda path: destination.exists()
    monkeypatch.setattr(data_artifact, "get_fs", AsyncMock(return_value=filesystem))
    monkeypatch.setattr(data_artifact, "register_mad_protocol", AsyncMock())

    relation_type = type("Relation", (), {})
    monkeypatch.setattr(data_artifact.duckdb, "DuckDBPyRelation", relation_type)
    artifact = DataArtifact("logs.parquet", relation_type())
    monkeypatch.setattr(artifact, "_truthy", lambda data: True)
    catalog = Mock()
    catalog.fetchone.return_value = None
    monkeypatch.setattr(data_artifact.duckdb, "sql", Mock(return_value=catalog))
    staged: list[Path] = []

    def export(query: str):
        temporary = exported_path(query)
        staged.append(temporary)
        temporary.write_bytes(b"replacement complete output")

    monkeypatch.setattr(data_artifact.duckdb, "execute", export)
    return artifact, backend, destination, staged


def exported_path(query: str) -> Path:
    filename = query.split(" TO '", 1)[1].rsplit("' (FORMAT", 1)[0]
    return Path(filename.replace("''", "'"))


async def test_failed_upload_keeps_previous_output(publication):
    artifact, backend, destination, staged = publication

    def fail_upload(source: str, target: str):
        assert Path(source).read_bytes() == b"replacement complete output"
        assert Path(target).read_bytes() == b"previous complete output"
        raise OSError("upload failed before commit")

    backend.put_file.side_effect = fail_upload

    with pytest.raises(OSError, match="upload failed"):
        await artifact.persist()

    assert destination.read_bytes() == b"previous complete output"
    assert not artifact.persisted
    assert artifact.data is not None
    assert not staged[0].exists()
    backend.rm.assert_not_called()
    backend.move.assert_not_called()


async def test_failed_export_never_starts_upload(publication, monkeypatch):
    artifact, backend, destination, staged = publication

    def fail_export(query: str):
        temporary = exported_path(query)
        staged.append(temporary)
        temporary.write_bytes(b"incomplete output")
        raise OSError("export failed")

    monkeypatch.setattr(data_artifact.duckdb, "execute", fail_export)
    with pytest.raises(OSError, match="export failed"):
        await artifact.persist()

    assert destination.read_bytes() == b"previous complete output"
    assert not artifact.persisted
    assert not staged[0].exists()
    backend.put_file.assert_not_called()


@pytest.mark.parametrize("protocol", ["abfs", "abfss", "az"])
async def test_successful_upload_replaces_output(publication, protocol):
    artifact, backend, destination, staged = publication
    backend.protocol = protocol
    backend.put_file.side_effect = lambda source, target: Path(target).write_bytes(
        Path(source).read_bytes()
    )

    assert await artifact.persist()

    assert destination.read_bytes() == b"replacement complete output"
    assert artifact.persisted
    assert artifact.data is None
    assert not staged[0].exists()
    backend.put_file.assert_called_once()
    backend.rm.assert_not_called()


@pytest.mark.parametrize("fail", [False, True])
async def test_local_replacement_preserves_old_file_until_commit(
    publication, monkeypatch, fail: bool
):
    artifact, backend, destination, staged = publication
    backend.protocol = ("file", "local")
    replace = data_artifact.os.replace

    def replace_file(source, target):
        assert Path(source).parent.parent == destination.parent
        assert destination.read_bytes() == b"previous complete output"
        if fail:
            raise OSError("replacement failed")
        replace(source, target)

    monkeypatch.setattr(data_artifact.os, "replace", replace_file)
    if fail:
        with pytest.raises(OSError, match="replacement failed"):
            await artifact.persist()
        assert destination.read_bytes() == b"previous complete output"
        assert not artifact.persisted
    else:
        assert await artifact.persist()
        assert destination.read_bytes() == b"replacement complete output"

    assert not staged[0].exists()
    backend.put_file.assert_not_called()


@pytest.mark.parametrize("filetype", ["parquet", "csv", "json"])
async def test_temporary_output_is_excluded_from_format_globs(publication, monkeypatch, filetype):
    artifact, backend, destination, staged = publication
    artifact.filetype = filetype
    backend.protocol = "file"

    def export(query):
        temporary = exported_path(query)
        staged.append(temporary)
        temporary.write_bytes(b"unfinished output")
        assert temporary.suffix == ".tmp"
        assert temporary not in destination.parent.rglob(f"*.{filetype}")
        assert f"FORMAT {filetype}" in query

    monkeypatch.setattr(data_artifact.duckdb, "execute", export)
    assert await artifact.persist()
    assert not staged[0].exists()


@pytest.mark.parametrize("protocol", ["ssh", "sftp", "s3", "memory", "adl"])
@pytest.mark.parametrize("fail", [False, True])
async def test_other_remote_storage_keeps_duckdb_export(publication, monkeypatch, protocol, fail):
    artifact, backend, destination, staged = publication
    backend.protocol = protocol
    register = Mock()
    export = Mock(side_effect=OSError("remote export failed") if fail else None)
    monkeypatch.setattr(data_artifact, "register_fsspec_filesystem", register)
    monkeypatch.setattr(data_artifact.duckdb, "execute", export)

    if fail:
        with pytest.raises(OSError, match="remote export failed"):
            await artifact.persist()
        assert artifact.data is not None
        assert not artifact.persisted
    else:
        assert await artifact.persist()

    register.assert_called_once_with(backend)
    escaped_path = f"{protocol}://{destination}".replace("'", "''")
    export.assert_called_once_with(f"COPY _d TO '{escaped_path}' (FORMAT parquet)")
    backend.put_file.assert_not_called()
    assert not staged

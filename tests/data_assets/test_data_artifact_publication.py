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
    staged: list[Path] = []

    def export(query: str, parameters: list[str]):
        temporary = Path(parameters[0])
        staged.append(temporary)
        temporary.write_bytes(b"replacement complete output")

    monkeypatch.setattr(data_artifact.duckdb, "execute", export)
    return artifact, backend, destination, staged


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

    def fail_export(query: str, parameters: list[str]):
        temporary = Path(parameters[0])
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


async def test_successful_upload_replaces_output(publication):
    artifact, backend, destination, staged = publication
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

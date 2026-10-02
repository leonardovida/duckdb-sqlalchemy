import tempfile
from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from sqlalchemy import create_engine

import duckdb_sqlalchemy.bulk
from duckdb_sqlalchemy.bulk import copy_from_rows


def _track_tempfiles(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    real_named_temporary_file = tempfile.NamedTemporaryFile
    created: list[Any] = []

    def named_temporary_file(*args: Any, **kwargs: Any) -> Any:
        kwargs["dir"] = tmp_path
        temporary_file = real_named_temporary_file(*args, **kwargs)
        created.append(temporary_file)
        return temporary_file

    monkeypatch.setattr(
        duckdb_sqlalchemy.bulk.tempfile,
        "NamedTemporaryFile",
        named_temporary_file,
    )
    return created


def test_header_writer_failure_closes_and_unlinks_owned_tempfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = _track_tempfiles(tmp_path, monkeypatch)
    primary = RuntimeError("header writer failed")

    class FailingWriter:
        def writerow(self, row: Any) -> None:
            raise primary

    monkeypatch.setattr(
        duckdb_sqlalchemy.bulk.csv, "writer", lambda *a, **k: FailingWriter()
    )

    with pytest.raises(RuntimeError) as error:
        copy_from_rows(
            object(),
            "target",
            [(1,)],
            columns=["id"],
            include_header=True,
        )

    assert error.value is primary
    assert len(created) == 1
    assert created[0].closed
    assert not Path(created[0].name).exists()


@pytest.mark.parametrize("failure", ["flush", "close"])
def test_flush_and_close_failures_unlink_tempfile_and_preserve_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    real_named_temporary_file = tempfile.NamedTemporaryFile
    created: list[Any] = []
    primary = RuntimeError(f"{failure} failed")

    class FailingTempfile:
        def __init__(self, file: Any) -> None:
            self._file = file
            self.name = file.name

        @property
        def closed(self) -> bool:
            return self._file.closed

        def write(self, data: str) -> int:
            return self._file.write(data)

        def flush(self) -> None:
            if failure == "flush":
                raise primary
            self._file.flush()

        def close(self) -> None:
            self._file.close()
            if failure == "close":
                raise primary

    def failing_named_temporary_file(*args: Any, **kwargs: Any) -> FailingTempfile:
        kwargs["dir"] = tmp_path
        temporary_file = FailingTempfile(real_named_temporary_file(*args, **kwargs))
        created.append(temporary_file)
        return temporary_file

    monkeypatch.setattr(
        duckdb_sqlalchemy.bulk.tempfile,
        "NamedTemporaryFile",
        failing_named_temporary_file,
    )
    copy_called = False

    def unexpected_copy(*args: Any, **kwargs: Any) -> None:
        nonlocal copy_called
        copy_called = True

    monkeypatch.setattr(duckdb_sqlalchemy.bulk, "copy_from_csv", unexpected_copy)

    with pytest.raises(RuntimeError) as error:
        copy_from_rows(object(), "target", [(1,)], columns=["id"])

    assert error.value is primary
    assert not copy_called
    assert len(created) == 1
    assert created[0].closed
    assert not Path(created[0].name).exists()


def test_copy_failure_unlinks_tempfile_and_preserves_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = _track_tempfiles(tmp_path, monkeypatch)
    primary = RuntimeError("COPY failed")
    copied_paths: list[Path] = []

    def failing_copy(connection: Any, table: Any, path: str, **kwargs: Any) -> None:
        copied_paths.append(Path(path))
        raise primary

    monkeypatch.setattr(duckdb_sqlalchemy.bulk, "copy_from_csv", failing_copy)

    with pytest.raises(RuntimeError) as error:
        copy_from_rows(object(), "target", [(1,)], columns=["id"])

    assert error.value is primary
    assert len(copied_paths) == 1
    assert not copied_paths[0].exists()
    assert created[0].closed


def test_iterator_failure_closes_and_unlinks_tempfile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    created = _track_tempfiles(tmp_path, monkeypatch)
    primary = RuntimeError("row iterator failed")

    def rows() -> Any:
        yield (1,)
        raise primary

    with pytest.raises(RuntimeError) as error:
        copy_from_rows(object(), "target", rows(), columns=["id"])

    assert error.value is primary
    assert len(created) == 1
    assert created[0].closed
    assert not Path(created[0].name).exists()


def test_standalone_unlink_failure_is_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_path = Path
    temporary_file = tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", delete=False, dir=tmp_path
    )
    primary = RuntimeError("unlink failed")

    class FailingPath:
        def unlink(self, *, missing_ok: bool) -> None:
            raise primary

    monkeypatch.setattr(duckdb_sqlalchemy.bulk, "Path", lambda path: FailingPath())
    try:
        with pytest.raises(RuntimeError) as error:
            duckdb_sqlalchemy.bulk._close_and_unlink_tempfile(temporary_file)
        assert error.value is primary
    finally:
        real_path(temporary_file.name).unlink(missing_ok=True)


def test_sparse_mapping_default_and_strict_transaction_rollback() -> None:
    engine = create_engine("duckdb:///:memory:")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE TABLE target (id INTEGER, label VARCHAR)"
            )
            copy_from_rows(
                connection,
                "target",
                [{"id": 1}, {"id": 2, "label": None}],
                columns=["id", "label"],
            )

        with pytest.raises(ValueError, match=r"mapping row 1.*'label'"):
            with engine.begin() as connection:
                copy_from_rows(
                    connection,
                    "target",
                    [{"id": 3, "label": "three"}, {"id": 4}],
                    columns=["id", "label"],
                    chunk_size=1,
                    strict=True,
                )

        with engine.connect() as connection:
            assert connection.exec_driver_sql(
                "SELECT id, label FROM target ORDER BY id"
            ).fetchall() == [(1, None), (2, None)]
    finally:
        engine.dispose()


def test_strict_requires_a_bool_and_is_not_a_copy_option() -> None:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE target (id INTEGER)")

    invalid_strict = cast(Any, 1)
    with pytest.raises(TypeError, match="strict"):
        copy_from_rows(connection, "target", [(1,)], strict=invalid_strict)

    copy_from_rows(connection, "target", [{"id": 1}], strict=True)
    assert connection.execute("SELECT id FROM target").fetchall() == [(1,)]

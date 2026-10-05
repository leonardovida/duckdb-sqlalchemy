from typing import Any

import pytest
from sqlalchemy.engine import Engine

from duckdb_sqlalchemy import CursorWrapper, _is_idempotent_statement


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT nextval /* comment */ ('ids')",
        "SELECT \"nextval\" -- comment\n ('ids')",
        "SELECT main.nextval/* comment */('ids')",
        "WITH n AS (SELECT nextval /* comment */ ('ids') AS id) SELECT id FROM n",
    ],
)
def test_commented_sequence_call_is_not_retried(
    engine: Engine, monkeypatch: pytest.MonkeyPatch, statement: str
) -> None:
    failure = RuntimeError("HTTP Error: 503 Service Unavailable")
    original_execute = CursorWrapper.execute
    calls = 0

    def transient_after_execute(cursor: CursorWrapper, sql: str, *args: Any) -> None:
        nonlocal calls
        original_execute(cursor, sql, *args)
        if sql == statement:
            calls += 1
            if calls == 1:
                raise failure

    with engine.connect() as connection:
        connection.exec_driver_sql("CREATE SEQUENCE ids")
        connection.commit()
        monkeypatch.setattr(CursorWrapper, "execute", transient_after_execute)
        with pytest.raises(RuntimeError) as caught:
            connection.execution_options(duckdb_retry_count=1).exec_driver_sql(
                statement
            )
        assert caught.value is failure
        connection.rollback()
        assert calls == 1
        # Sequence advancement survives rollback, so replay would consume 2.
        assert connection.exec_driver_sql("SELECT currval('ids')").scalar_one() == 1


@pytest.mark.parametrize("function", ["nextval", '"setval"', "md_run_job"])
@pytest.mark.parametrize("separator", ["/* comment */", "-- comment\n"])
def test_side_effect_function_comments_disable_retries(
    function: str, separator: str
) -> None:
    assert not _is_idempotent_statement(f"SELECT {function}{separator}('ids')")


def test_long_comment_with_repeated_side_effect_names_is_conservative() -> None:
    statement = "SELECT 42 -- " + "nextval -- x " * 10000
    assert not _is_idempotent_statement(statement)


def test_commented_read_still_retries(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    statement = "SELECT abs /* comment */ (-42)"
    assert _is_idempotent_statement(statement)
    original_execute = CursorWrapper.execute
    calls = 0

    def transient_after_execute(cursor: CursorWrapper, sql: str, *args: Any) -> None:
        nonlocal calls
        original_execute(cursor, sql, *args)
        if sql == statement:
            calls += 1
            if calls == 1:
                raise RuntimeError("HTTP Error: 503 Service Unavailable")

    monkeypatch.setattr(CursorWrapper, "execute", transient_after_execute)
    with engine.connect() as connection:
        assert (
            connection.execution_options(duckdb_retry_count=1)
            .exec_driver_sql(statement)
            .scalar_one()
            == 42
        )
    assert calls == 2

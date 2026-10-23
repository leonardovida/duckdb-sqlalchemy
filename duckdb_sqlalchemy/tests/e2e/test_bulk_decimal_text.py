"""Bulk inference must preserve individually bound Decimal text values."""

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, select

from duckdb_sqlalchemy import _bulk_insert

from .conftest import EngineFactory


@pytest.mark.parametrize("pandas_only", [False, True], ids=["arrow", "pandas"])
@pytest.mark.parametrize("insertmanyvalues", [False, True])
@pytest.mark.parametrize(
    "values, expected",
    [
        ([Decimal("1.2"), Decimal("3.456"), None], ["1.2", "3.456", None]),
        ([1, None, Decimal("2.50")], ["1", None, "2.50"]),
        ([Decimal("0.00"), Decimal("-1.234")], ["0.00", "-1.234"]),
    ],
    ids=["different-scales", "mixed-integer", "zero-scale"],
)
def test_bulk_decimal_text_matches_ordinary_inserts(
    values: list[Any],
    expected: list[Any],
    pandas_only: bool,
    insertmanyvalues: bool,
    db_path: Path,
    make_engine: EngineFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("pandas" if pandas_only else "pyarrow")
    if pandas_only:
        # Exercise the optional pandas fallback without uninstalling Arrow.
        monkeypatch.setattr(
            _bulk_insert, "build_bulk_insert_arrow_table", lambda *args: None
        )
    engine = make_engine(db_path, use_insertmanyvalues=insertmanyvalues)
    metadata = MetaData()
    tables = [
        Table(
            name,
            metadata,
            Column("id", Integer),
            Column("value", String),
        )
        for name in ("ordinary", "bulk")
    ]
    rows = [{"id": index, "value": value} for index, value in enumerate(values)]
    with engine.begin() as connection:
        metadata.create_all(connection)
        for table, threshold in zip(tables, (0, 1)):
            connection.execution_options(duckdb_copy_threshold=threshold).execute(
                table.insert(), rows
            )
    engine.dispose()

    # Reopen the real file to verify committed storage, not only result hooks.
    with engine.connect() as connection:
        for table in tables:
            assert (
                list(connection.scalars(select(table.c.value).order_by(table.c.id)))
                == expected
            )
    assert [row["value"] for row in rows] == values

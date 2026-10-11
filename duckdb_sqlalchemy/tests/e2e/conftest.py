"""Fixtures for end-to-end tests: real engines on real files, always disposed."""

from pathlib import Path
from typing import Any, Callable, Generator, List

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

EngineFactory = Callable[..., Engine]


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "app.duckdb"


@pytest.fixture
def make_engine() -> Generator[EngineFactory, None, None]:
    engines: List[Engine] = []

    def factory(url: Any, **kwargs: Any) -> Engine:
        if isinstance(url, Path):
            url = f"duckdb:///{url.as_posix()}"
        engine = create_engine(url, **kwargs)
        engines.append(engine)
        return engine

    yield factory
    for engine in engines:
        engine.dispose()

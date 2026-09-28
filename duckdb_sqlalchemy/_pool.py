from __future__ import annotations

import os
import warnings
from typing import Any, Mapping, Optional

from sqlalchemy import pool
from sqlalchemy.engine.url import URL as SAURL

from .motherduck import MOTHERDUCK_CONFIG_KEYS

_POOL_CLASS_OVERRIDES: Mapping[str, type[pool.Pool]] = {
    "queue": pool.QueuePool,
    "queuepool": pool.QueuePool,
    "singleton": pool.SingletonThreadPool,
    "singletonthreadpool": pool.SingletonThreadPool,
    "null": pool.NullPool,
    "nullpool": pool.NullPool,
}


def _looks_like_motherduck(database: Optional[str], config: Mapping[str, Any]) -> bool:
    if database is not None and database.startswith(("md:", "motherduck:")):
        return True
    return any(key in config for key in MOTHERDUCK_CONFIG_KEYS)


def _pool_override_from_url(url: SAURL) -> Optional[str]:
    value = None
    if "duckdb_sqlalchemy_pool" in url.query:
        value = url.query.get("duckdb_sqlalchemy_pool")
    elif "pool" in url.query:
        value = url.query.get("pool")
    if value is None:
        value = os.getenv("DUCKDB_SQLALCHEMY_POOL")
    if isinstance(value, (list, tuple)):
        value = value[0] if value else None
    if value is None:
        return None
    return str(value).lower()


def _pool_class_from_override(
    pool_override: Optional[str],
) -> Optional[type[pool.Pool]]:
    if pool_override is None:
        return None
    pool_class = _POOL_CLASS_OVERRIDES.get(pool_override)
    if pool_class is None:
        from . import DuckDBEngineWarning  # avoid import cycle

        warnings.warn(
            f"Unknown duckdb_sqlalchemy pool override {pool_override!r}; using the "
            "default pool class. Valid values: "
            f"{', '.join(sorted(_POOL_CLASS_OVERRIDES))}.",
            DuckDBEngineWarning,
            stacklevel=2,
        )
    return pool_class


def _default_pool_class_for_database(
    database: Optional[str], query: Mapping[str, Any]
) -> type[pool.Pool]:
    if not database or database == ":memory:":
        # An empty database is ``:memory:``; every pooled connection would
        # otherwise get its own private in-memory database.
        return pool.SingletonThreadPool
    if database.startswith(":memory:"):
        return pool.QueuePool
    if _looks_like_motherduck(database, query):
        return pool.NullPool
    return pool.QueuePool

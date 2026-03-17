from typing import Any, Mapping, Optional

from sqlalchemy.engine import URL as SAURL

from ._query import merge_query_mappings

__all__ = ["URL", "make_url"]


def URL(
    *,
    database: str = ":memory:",
    query: Optional[Mapping[str, Any]] = None,
    **kwargs: Any,
) -> SAURL:
    """
    Build a SQLAlchemy URL for duckdb-sqlalchemy.

    All keyword arguments are treated as DuckDB config options (URL query params).
    """

    query_dict = merge_query_mappings(query, kwargs)

    return SAURL.create("duckdb", database=database, query=query_dict)


def make_url(
    *,
    database: str = ":memory:",
    query: Optional[Mapping[str, Any]] = None,
    **kwargs: Any,
) -> SAURL:
    return URL(database=database, query=query, **kwargs)

from typing import Any, Mapping, Optional, Tuple, Union

from sqlalchemy.engine import URL as SAURL

from ._query import coerce_query_mapping

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

    query_dict: dict[str, Union[str, Tuple[str, ...]]] = {}
    for mapping in (query, kwargs):
        if mapping:
            query_dict.update(coerce_query_mapping(mapping))

    return SAURL.create("duckdb", database=database, query=query_dict)


def make_url(
    *,
    database: str = ":memory:",
    query: Optional[Mapping[str, Any]] = None,
    **kwargs: Any,
) -> SAURL:
    return URL(database=database, query=query, **kwargs)

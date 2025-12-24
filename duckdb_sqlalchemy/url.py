from typing import Any, Mapping, Optional, Sequence

from sqlalchemy.engine import URL as SAURL

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

    def _stringify(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)

    def _coerce(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return tuple(_stringify(v) for v in value)
        return _stringify(value)

    query_dict: dict[str, str | tuple[str, ...]] = {}
    if query:
        query_dict.update(
            {
                k: v
                for k, v in ((k, _coerce(v)) for k, v in query.items())
                if v is not None
            }
        )
    if kwargs:
        query_dict.update(
            {
                k: v
                for k, v in ((k, _coerce(v)) for k, v in kwargs.items())
                if v is not None
            }
        )

    return SAURL.create("duckdb", database=database, query=query_dict)


def make_url(
    *,
    database: str = ":memory:",
    query: Optional[Mapping[str, Any]] = None,
    **kwargs: Any,
) -> SAURL:
    return URL(database=database, query=query, **kwargs)

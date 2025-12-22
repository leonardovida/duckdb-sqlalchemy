from typing import Any, Iterable, Optional

from sqlalchemy import func

__all__ = ["table_function", "read_parquet", "read_csv", "read_csv_auto"]


def table_function(
    name: str,
    *args: Any,
    columns: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> Any:
    fn = getattr(func, name)(*args, **kwargs)
    if columns:
        if hasattr(fn, "table_valued"):
            return fn.table_valued(*columns)
        raise NotImplementedError(
            "table_valued requires SQLAlchemy >= 1.4 to name columns"
        )
    return fn


def read_parquet(
    path: str, *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return table_function("read_parquet", path, columns=columns, **kwargs)


def read_csv(
    path: str, *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return table_function("read_csv", path, columns=columns, **kwargs)


def read_csv_auto(
    path: str, *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return table_function("read_csv_auto", path, columns=columns, **kwargs)

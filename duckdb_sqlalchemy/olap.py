from typing import Any, Iterable, Optional

from sqlalchemy import bindparam, func, text

from ._validation import validate_identifier

MOTHERDUCK_USER_INFO_COLUMNS = (
    "user_id",
    "username",
    "org_id",
    "org_name",
    "org_type",
)
MOTHERDUCK_LIST_DIVES_COLUMNS = (
    "id",
    "title",
    "description",
    "owner_id",
    "current_version",
    "created_at",
    "updated_at",
    "owner_name",
    "required_resources",
)
MOTHERDUCK_ACCESS_TOKENS_COLUMNS = (
    "token_name",
    "token_type",
    "created_ts",
    "expire_at",
)

__all__ = [
    "table_function",
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "md_user_info",
    "md_list_dives",
    "md_access_tokens",
]


def _named_parameter(name: str, value: Any) -> Any:
    parameter_name = validate_identifier(name, kind="table function parameter")
    return text(f"{parameter_name} := :{parameter_name}").bindparams(
        bindparam(parameter_name, value, unique=True)
    )


def table_function(
    name: str,
    *args: Any,
    columns: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> Any:
    named_args = tuple(_named_parameter(key, value) for key, value in kwargs.items())
    fn = getattr(func, name)(*args, *named_args)
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


def md_user_info(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return table_function(
        "md_user_info",
        columns=MOTHERDUCK_USER_INFO_COLUMNS if columns is None else columns,
        **kwargs,
    )


def md_list_dives(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return table_function(
        "md_list_dives",
        columns=MOTHERDUCK_LIST_DIVES_COLUMNS if columns is None else columns,
        **kwargs,
    )


def md_access_tokens(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return table_function(
        "md_access_tokens",
        columns=MOTHERDUCK_ACCESS_TOKENS_COLUMNS if columns is None else columns,
        **kwargs,
    )

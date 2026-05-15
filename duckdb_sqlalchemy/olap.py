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
MOTHERDUCK_JOB_SUMMARY_COLUMNS = (
    "job_id",
    "job_name",
    "schedule_cron",
    "schedule_status",
    "status",
    "current_version",
    "created_at",
    "updated_at",
)
MOTHERDUCK_JOB_VERSION_COLUMNS = (
    "version_id",
    "job_id",
    "version",
    "created_at",
    "md_token_name",
    "md_secret_names",
    "config",
    "source_code",
    "requirements_txt",
)
MOTHERDUCK_JOB_RUN_COLUMNS = (
    "run_id",
    "job_id",
    "job_name",
    "job_version",
    "run_number",
    "is_scheduled",
    "status",
    "created_at",
    "started_at",
    "ended_at",
    "scheduled_at",
    "cancelled_at",
    "exit_code",
)
MOTHERDUCK_JOB_RUN_LOG_COLUMNS = ("logs",)
MOTHERDUCK_DELETE_JOB_COLUMNS = ("deleted_count",)
MOTHERDUCK_CANCEL_JOB_RUN_COLUMNS = ("canceled_count",)

__all__ = [
    "table_function",
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "md_user_info",
    "md_list_dives",
    "md_access_tokens",
    "md_create_job",
    "md_jobs",
    "md_get_job",
    "md_update_job",
    "md_delete_job",
    "md_run_job",
    "md_cancel_job_run",
    "md_job_runs",
    "md_job_run_logs",
    "md_job_versions",
    "md_get_job_version",
]


def _quote_identifier(name: str) -> str:
    return f'"{name}"'


def _named_parameter(name: str, value: Any) -> Any:
    parameter_name = validate_identifier(name, kind="table function parameter")
    return text(f"{_quote_identifier(parameter_name)} := :{parameter_name}").bindparams(
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


def _motherduck_metadata_function(
    name: str,
    default_columns: Iterable[str],
    *,
    columns: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> Any:
    return table_function(
        name,
        columns=default_columns if columns is None else columns,
        **kwargs,
    )


def md_user_info(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_user_info",
        MOTHERDUCK_USER_INFO_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_list_dives(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_list_dives",
        MOTHERDUCK_LIST_DIVES_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_access_tokens(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_access_tokens",
        MOTHERDUCK_ACCESS_TOKENS_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_create_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_create_job",
        MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_jobs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_jobs",
        MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_get_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_get_job",
        MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_update_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_update_job",
        MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_delete_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_delete_job",
        MOTHERDUCK_DELETE_JOB_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_run_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_run_job",
        MOTHERDUCK_JOB_RUN_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_cancel_job_run(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_cancel_job_run",
        MOTHERDUCK_CANCEL_JOB_RUN_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_job_runs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_job_runs",
        MOTHERDUCK_JOB_RUN_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_job_run_logs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_job_run_logs",
        MOTHERDUCK_JOB_RUN_LOG_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_job_versions(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_job_versions",
        MOTHERDUCK_JOB_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_get_job_version(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_get_job_version",
        MOTHERDUCK_JOB_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )

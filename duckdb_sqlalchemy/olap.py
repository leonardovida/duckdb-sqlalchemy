import warnings
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

from sqlalchemy import bindparam, func, select, text

from ._validation import validate_identifier

MOTHERDUCK_USER_INFO_COLUMNS = (
    "user_id",
    "username",
    "org_id",
    "org_name",
    "org_type",
    "region",
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
    "config",
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
MOTHERDUCK_FLIGHT_SUMMARY_COLUMNS = (
    "flight_id",
    "flight_name",
    "schedule_cron",
    "schedule_status",
    "status",
    "current_version",
    "created_at",
    "updated_at",
)
MOTHERDUCK_FLIGHT_VERSION_COLUMNS = (
    "version_id",
    "flight_id",
    "flight_version",
    "created_at",
    "access_token_name",
    "flight_secret_names",
    "config",
    "source_code",
    "requirements_txt",
)
MOTHERDUCK_FLIGHT_RUN_COLUMNS = (
    "run_id",
    "flight_id",
    "flight_name",
    "flight_version",
    "config",
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
MOTHERDUCK_FLIGHT_LOG_COLUMNS = ("logs",)
MOTHERDUCK_DELETE_FLIGHT_COLUMNS = ("deleted_count",)
MOTHERDUCK_CANCEL_FLIGHT_RUN_COLUMNS = ("canceled_count",)
MOTHERDUCK_DIVE_VERSION_COLUMNS = (
    "id",
    "version",
    "storage_url",
    "description",
    "created_at",
    "api_version",
    "required_resources",
)
MOTHERDUCK_CREATE_DIVE_COLUMNS = (
    *MOTHERDUCK_LIST_DIVES_COLUMNS,
    "version_id",
    "version_storage_url",
    "version_description",
    "version_created_at",
    "version_api_version",
    "version_required_resources",
)
MOTHERDUCK_GET_DIVE_COLUMNS = (*MOTHERDUCK_CREATE_DIVE_COLUMNS, "content")
MOTHERDUCK_GET_DIVE_VERSION_COLUMNS = (*MOTHERDUCK_DIVE_VERSION_COLUMNS, "content")
MOTHERDUCK_DELETE_DIVE_COLUMNS = ("success",)
PRAGMA_STORAGE_INFO_COLUMNS = (
    "row_group_id",
    "column_name",
    "column_id",
    "column_path",
    "segment_id",
    "segment_type",
    "start",
    "count",
    "compression",
    "stats",
    "has_updates",
    "persistent",
    "block_id",
    "block_offset",
    "segment_info",
    "additional_block_ids",
)

__all__ = [
    "table_function",
    "read_parquet",
    "read_csv",
    "read_csv_auto",
    "pragma_storage_info",
    "quack_query",
    "md_user_info",
    "md_list_dives",
    "md_access_tokens",
    "md_create_flight",
    "md_flights",
    "md_get_flight",
    "md_update_flight",
    "md_delete_flight",
    "md_run_flight",
    "md_cancel_flight_run",
    "md_flight_runs",
    "md_flight_logs",
    "md_flight_versions",
    "md_get_flight_version",
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
    "md_create_dive",
    "md_update_dive_metadata",
    "md_update_dive_content",
    "md_get_dive",
    "md_list_dive_versions",
    "md_get_dive_version",
    "md_delete_dive",
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


def pragma_storage_info(
    table_name: str,
    *,
    include_segment_info: Optional[bool] = None,
    columns: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> Any:
    if include_segment_info is not None:
        kwargs["include_segment_info"] = include_segment_info
    return table_function(
        "pragma_storage_info",
        table_name,
        columns=PRAGMA_STORAGE_INFO_COLUMNS if columns is None else columns,
        **kwargs,
    )


def quack_query(
    uri: str,
    query: str,
    *,
    columns: Optional[Iterable[str]] = None,
    **kwargs: Any,
) -> Any:
    return table_function("quack_query", uri, query, columns=columns, **kwargs)


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


def _validate_flight_config(kwargs: Mapping[str, Any]) -> None:
    config = kwargs.get("config")
    if not isinstance(config, Mapping):
        return
    if "" in config:
        raise ValueError("MotherDuck Flight config keys must not be empty")


def _warn_deprecated_job_helper(helper_name: str) -> None:
    replacements = {
        "md_create_job": "md_create_flight",
        "md_jobs": "md_flights",
        "md_get_job": "md_get_flight",
        "md_update_job": "md_update_flight",
        "md_delete_job": "md_delete_flight",
        "md_run_job": "md_run_flight",
        "md_cancel_job_run": "md_cancel_flight_run",
        "md_job_runs": "md_flight_runs",
        "md_job_run_logs": "md_flight_logs",
        "md_job_versions": "md_flight_versions",
        "md_get_job_version": "md_get_flight_version",
    }
    flight_name = replacements[helper_name]
    warnings.warn(
        f"`{helper_name}` is deprecated; use `{flight_name}` instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def _translate_job_parameters(kwargs: Mapping[str, Any]) -> Dict[str, Any]:
    replacements = {
        "job_id": "flight_id",
        "job_name": "name",
        "job_version": "flight_version",
        "md_token_name": "access_token_name",
        "md_secret_names": "flight_secret_names",
        "version": "version_number",
    }
    return {replacements.get(key, key): value for key, value in kwargs.items()}


def _legacy_job_columns(
    columns: Optional[Iterable[str]],
    legacy_to_flight: Mapping[str, str],
    default_columns: Iterable[str],
) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    legacy_columns = tuple(default_columns if columns is None else columns)
    flight_columns = tuple(
        legacy_to_flight.get(column, column) for column in legacy_columns
    )
    return legacy_columns, flight_columns


def _legacy_job_subquery(
    helper_name: str,
    flight_helper: Any,
    *,
    columns: Optional[Iterable[str]],
    legacy_to_flight: Mapping[str, str],
    default_columns: Iterable[str],
    **kwargs: Any,
) -> Any:
    _warn_deprecated_job_helper(helper_name)
    legacy_columns, flight_columns = _legacy_job_columns(
        columns,
        legacy_to_flight,
        default_columns,
    )
    flight_table = flight_helper(
        columns=flight_columns,
        **_translate_job_parameters(kwargs),
    )
    projections = []
    for legacy_column, flight_column in zip(legacy_columns, flight_columns):
        column = getattr(flight_table.c, flight_column)
        projections.append(
            column.label(legacy_column) if legacy_column != flight_column else column
        )
    return select(*projections).select_from(flight_table).subquery()


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


def md_create_flight(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    _validate_flight_config(kwargs)
    return _motherduck_metadata_function(
        "md_create_flight",
        MOTHERDUCK_FLIGHT_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_flights(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_flights",
        MOTHERDUCK_FLIGHT_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_get_flight(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_get_flight",
        MOTHERDUCK_FLIGHT_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_update_flight(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    _validate_flight_config(kwargs)
    return _motherduck_metadata_function(
        "md_update_flight",
        MOTHERDUCK_FLIGHT_SUMMARY_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_delete_flight(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_delete_flight",
        MOTHERDUCK_DELETE_FLIGHT_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_run_flight(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    _validate_flight_config(kwargs)
    return _motherduck_metadata_function(
        "md_run_flight",
        MOTHERDUCK_FLIGHT_RUN_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_cancel_flight_run(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_cancel_flight_run",
        MOTHERDUCK_CANCEL_FLIGHT_RUN_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_flight_runs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_flight_runs",
        MOTHERDUCK_FLIGHT_RUN_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_flight_logs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_flight_logs",
        MOTHERDUCK_FLIGHT_LOG_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_flight_versions(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_flight_versions",
        MOTHERDUCK_FLIGHT_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_get_flight_version(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_get_flight_version",
        MOTHERDUCK_FLIGHT_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_create_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_create_job",
        md_create_flight,
        columns=columns,
        legacy_to_flight={"job_id": "flight_id", "job_name": "flight_name"},
        default_columns=MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        **kwargs,
    )


def md_jobs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_jobs",
        md_flights,
        columns=columns,
        legacy_to_flight={"job_id": "flight_id", "job_name": "flight_name"},
        default_columns=MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        **kwargs,
    )


def md_get_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_get_job",
        md_get_flight,
        columns=columns,
        legacy_to_flight={"job_id": "flight_id", "job_name": "flight_name"},
        default_columns=MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        **kwargs,
    )


def md_update_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_update_job",
        md_update_flight,
        columns=columns,
        legacy_to_flight={"job_id": "flight_id", "job_name": "flight_name"},
        default_columns=MOTHERDUCK_JOB_SUMMARY_COLUMNS,
        **kwargs,
    )


def md_delete_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_delete_job",
        md_delete_flight,
        columns=columns,
        legacy_to_flight={},
        default_columns=MOTHERDUCK_DELETE_JOB_COLUMNS,
        **kwargs,
    )


def md_run_job(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_run_job",
        md_run_flight,
        columns=columns,
        legacy_to_flight={
            "job_id": "flight_id",
            "job_name": "flight_name",
            "job_version": "flight_version",
        },
        default_columns=MOTHERDUCK_JOB_RUN_COLUMNS,
        **kwargs,
    )


def md_cancel_job_run(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_cancel_job_run",
        md_cancel_flight_run,
        columns=columns,
        legacy_to_flight={},
        default_columns=MOTHERDUCK_CANCEL_JOB_RUN_COLUMNS,
        **kwargs,
    )


def md_job_runs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_job_runs",
        md_flight_runs,
        columns=columns,
        legacy_to_flight={
            "job_id": "flight_id",
            "job_name": "flight_name",
            "job_version": "flight_version",
        },
        default_columns=MOTHERDUCK_JOB_RUN_COLUMNS,
        **kwargs,
    )


def md_job_run_logs(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_job_run_logs",
        md_flight_logs,
        columns=columns,
        legacy_to_flight={},
        default_columns=MOTHERDUCK_JOB_RUN_LOG_COLUMNS,
        **kwargs,
    )


def md_job_versions(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _legacy_job_subquery(
        "md_job_versions",
        md_flight_versions,
        columns=columns,
        legacy_to_flight={
            "job_id": "flight_id",
            "version": "flight_version",
            "md_token_name": "access_token_name",
            "md_secret_names": "flight_secret_names",
        },
        default_columns=MOTHERDUCK_JOB_VERSION_COLUMNS,
        **kwargs,
    )


def md_get_job_version(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _legacy_job_subquery(
        "md_get_job_version",
        md_get_flight_version,
        columns=columns,
        legacy_to_flight={
            "job_id": "flight_id",
            "version": "flight_version",
            "md_token_name": "access_token_name",
            "md_secret_names": "flight_secret_names",
        },
        default_columns=MOTHERDUCK_JOB_VERSION_COLUMNS,
        **kwargs,
    )


def md_create_dive(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_create_dive",
        MOTHERDUCK_CREATE_DIVE_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_update_dive_metadata(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_update_dive_metadata",
        MOTHERDUCK_LIST_DIVES_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_update_dive_content(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_update_dive_content",
        MOTHERDUCK_DIVE_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_get_dive(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_get_dive",
        MOTHERDUCK_GET_DIVE_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_list_dive_versions(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_list_dive_versions",
        MOTHERDUCK_DIVE_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_get_dive_version(
    *, columns: Optional[Iterable[str]] = None, **kwargs: Any
) -> Any:
    return _motherduck_metadata_function(
        "md_get_dive_version",
        MOTHERDUCK_GET_DIVE_VERSION_COLUMNS,
        columns=columns,
        **kwargs,
    )


def md_delete_dive(*, columns: Optional[Iterable[str]] = None, **kwargs: Any) -> Any:
    return _motherduck_metadata_function(
        "md_delete_dive",
        MOTHERDUCK_DELETE_DIVE_COLUMNS,
        columns=columns,
        **kwargs,
    )

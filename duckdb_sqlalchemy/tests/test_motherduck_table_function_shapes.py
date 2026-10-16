"""Default columns of the MotherDuck table-function helpers, per DuckDB release.

Each expected shape was read with ``DESCRIBE SELECT * FROM <function>(...)``
against MotherDuck using the extension that MotherDuck serves for that DuckDB
release. Naming a column the extension does not return fails to bind, so the
helpers must not select more than these.
"""

import warnings
from typing import Any, Callable, Dict, Tuple

import pytest
from sqlalchemy import select

from duckdb_sqlalchemy import Dialect, olap

DIVE = (
    "id",
    "title",
    "description",
    "owner_id",
    "current_version",
    "created_at",
    "updated_at",
    "owner_name",
)
DIVE_STATUS = (
    "status",
    "status_changed_at",
    "status_set_by",
    "status_applies_to_version",
)
DIVE_VERSION_DETAIL = (
    "version_id",
    "version_storage_url",
    "version_description",
    "version_created_at",
    "version_api_version",
    "version_required_resources",
)
DIVE_VERSION = (
    "id",
    "version",
    "storage_url",
    "description",
    "created_at",
    "api_version",
    "required_resources",
)
FLIGHT = (
    "flight_id",
    "flight_name",
    "schedule_cron",
    "schedule_status",
    "status",
    "current_version",
    "created_at",
    "updated_at",
)
FLIGHT_VERSION = (
    "version_id",
    "flight_id",
    "flight_version",
    "created_at",
    "access_token_name",
    "flight_secret_names",
    "config",
)
SOURCE = ("source_code", "requirements_txt")
FLIGHT_RUN = (
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
TABULAR_LOGS = ("line_number", "reported_at", "line")

OBSERVED_SHAPES: Dict[str, Dict[str, Tuple[str, ...]]] = {
    "1.4.4": {
        "md_list_dives": DIVE,
        "md_get_dive": (*DIVE, *DIVE_VERSION_DETAIL, "content"),
        "md_list_dive_versions": DIVE_VERSION,
        "md_get_dive_version": (*DIVE_VERSION, "content"),
    },
    "1.5.0": {
        "md_list_dives": DIVE,
        "md_create_dive": (*DIVE, *DIVE_VERSION_DETAIL),
        "md_get_dive": (*DIVE, *DIVE_VERSION_DETAIL, "content"),
        "md_update_dive_metadata": DIVE,
        "md_update_dive_content": DIVE_VERSION,
    },
    "1.5.2": {
        "md_list_dives": (*DIVE, "required_resources"),
    },
    "1.5.3": {
        "md_list_dives": (*DIVE, "required_resources"),
        "md_create_dive": (*DIVE, *DIVE_VERSION_DETAIL),
        "md_get_dive": (*DIVE, *DIVE_VERSION_DETAIL, "content"),
        "md_update_dive_metadata": DIVE,
        "md_list_flights": FLIGHT,
        "md_create_flight": FLIGHT,
        "md_list_flight_versions": (*FLIGHT_VERSION, *SOURCE),
        "md_get_flight_version": (*FLIGHT_VERSION, *SOURCE),
        "md_list_flight_runs": FLIGHT_RUN,
        "md_run_flight": FLIGHT_RUN,
        "md_get_flight_logs": ("logs",),
    },
    "1.5.4": {
        "md_list_dives": (*DIVE, *DIVE_STATUS, "required_resources"),
        "md_create_dive": (*DIVE, *DIVE_STATUS, *DIVE_VERSION_DETAIL),
        "md_get_dive": (*DIVE, *DIVE_STATUS, *DIVE_VERSION_DETAIL, "content"),
        "md_update_dive_metadata": (*DIVE, *DIVE_STATUS),
        "md_update_dive_status": (*DIVE, *DIVE_STATUS),
        "md_list_flights": FLIGHT,
        "md_create_flight": FLIGHT,
        "md_list_flight_versions": (*FLIGHT_VERSION, *SOURCE, "max_runtime_sec"),
        "md_get_flight_version": (*FLIGHT_VERSION, *SOURCE, "max_runtime_sec"),
        "md_list_flight_runs": FLIGHT_RUN,
        "md_get_flight_logs": ("logs",),
    },
    "1.5.5": {
        "md_list_flights": (*FLIGHT, "owner_name"),
        "md_create_flight": (*FLIGHT, "owner_name"),
        "md_list_flight_versions": (*FLIGHT_VERSION, "max_runtime_sec"),
        "md_get_flight_version": (*FLIGHT_VERSION, *SOURCE, "max_runtime_sec"),
        "md_list_flight_runs": FLIGHT_RUN,
        "md_get_flight_logs": TABULAR_LOGS,
    },
    "1.5.6": {
        "md_list_dives": (*DIVE, *DIVE_STATUS, "required_resources"),
        "md_create_dive": (*DIVE, *DIVE_STATUS, *DIVE_VERSION_DETAIL),
        "md_get_dive": (*DIVE, *DIVE_STATUS, *DIVE_VERSION_DETAIL, "content"),
        "md_update_dive_metadata": (*DIVE, *DIVE_STATUS),
        "md_update_dive_status": (*DIVE, *DIVE_STATUS),
        "md_update_dive_content": DIVE_VERSION,
        "md_list_dive_versions": DIVE_VERSION,
        "md_get_dive_version": (*DIVE_VERSION, "content"),
        "md_list_flights": (*FLIGHT, "owner_name"),
        "md_get_flight": (*FLIGHT, "owner_name"),
        "md_create_flight": (*FLIGHT, "owner_name"),
        "md_list_flight_versions": (
            *FLIGHT_VERSION,
            "max_runtime_sec",
            "instance_type",
        ),
        "md_get_flight_version": (
            *FLIGHT_VERSION,
            *SOURCE,
            "max_runtime_sec",
            "instance_type",
        ),
        "md_list_flight_runs": FLIGHT_RUN,
        "md_run_flight": FLIGHT_RUN,
        "md_get_flight_logs": TABULAR_LOGS,
    },
}


@pytest.mark.parametrize(
    ("client_version", "helper_name", "expected"),
    [
        pytest.param(version, name, columns, id=f"{version}-{name}")
        for version, shapes in OBSERVED_SHAPES.items()
        for name, columns in shapes.items()
    ],
)
def test_default_columns_match_observed_extension_shape(
    monkeypatch: pytest.MonkeyPatch,
    client_version: str,
    helper_name: str,
    expected: Tuple[str, ...],
) -> None:
    monkeypatch.setattr(olap, "duckdb_version", client_version)
    helper: Callable[..., Any] = getattr(olap, helper_name)

    assert tuple(helper().c.keys()) == expected


JOB_VERSION = (
    "version_id",
    "job_id",
    "version",
    "created_at",
    "md_token_name",
    "md_secret_names",
    "config",
)


@pytest.mark.parametrize(
    ("client_version", "listed", "detailed"),
    [
        ("1.5.4", (*JOB_VERSION, *SOURCE), (*JOB_VERSION, *SOURCE)),
        ("1.5.6", JOB_VERSION, (*JOB_VERSION, *SOURCE)),
    ],
)
def test_deprecated_job_version_helpers_follow_flight_shape(
    monkeypatch: pytest.MonkeyPatch,
    client_version: str,
    listed: Tuple[str, ...],
    detailed: Tuple[str, ...],
) -> None:
    monkeypatch.setattr(olap, "duckdb_version", client_version)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        assert tuple(olap.md_job_versions().c.keys()) == listed
        assert tuple(olap.md_get_job_version().c.keys()) == detailed


@pytest.mark.parametrize(
    ("client_version", "released_column"), [("1.5.4", "logs"), ("1.5.6", "line")]
)
def test_deprecated_log_helpers_select_the_released_log_column(
    monkeypatch: pytest.MonkeyPatch, client_version: str, released_column: str
) -> None:
    monkeypatch.setattr(olap, "duckdb_version", client_version)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        helpers = (olap.md_job_run_logs, olap.md_flight_logs)
        for logs in (helper() for helper in helpers):
            assert list(logs.c.keys()) == ["logs"]
            sql = str(select(logs.c.logs).compile(dialect=Dialect()))
            assert f"anon_2.{released_column}" in sql
            assert ("anon_2.line" in sql) is (released_column == "line")


def test_explicit_columns_override_version_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(olap, "duckdb_version", "1.5.3")

    versions = olap.md_list_flight_versions(columns=["version_id", "instance_type"])

    assert list(versions.c.keys()) == ["version_id", "instance_type"]

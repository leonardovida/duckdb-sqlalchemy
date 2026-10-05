"""DuckDB capabilities exercised by SQLAlchemy's upstream dialect suite."""

from typing import Any

from sqlalchemy.testing import exclusions
from sqlalchemy.testing.requirements import SuiteRequirements


class Requirements(SuiteRequirements):
    @property
    def _supported(self) -> Any:
        return exclusions.open()

    table_ddl_if_exists = _supported
    index_ddl_if_exists = _supported
    uuid_data_type = _supported
    table_value_constructor = _supported
    boolean_col_expressions = _supported
    nullsordering = _supported
    intersect = _supported
    except_ = _supported
    window_functions = _supported
    ctes = _supported
    ctes_with_update_delete = _supported
    ctes_with_values = _supported
    tuple_in = _supported
    views = _supported
    comment_reflection = _supported
    comment_reflection_full_unicode = _supported
    temp_table_names = _supported
    has_temp_table = _supported
    temporary_views = _supported
    schema_create_delete = _supported
    unicode_ddl = _supported
    datetime_literals = _supported
    datetime_timezone = _supported
    datetime_historic = _supported
    date_historic = _supported
    timestamp_microseconds = _supported
    autocommit = _supported
    array_type = _supported
    json_type = _supported
    server_defaults = _supported
    expression_server_defaults = _supported
    precision_numerics_many_significant_digits = _supported
    precision_numerics_retains_significant_digits = _supported
    infinity_floats = _supported
    update_from = _supported
    delete_from = _supported
    supports_distinct_on = _supported
    reflect_table_options = _supported
    regexp_match = _supported

    @property
    def on_update_cascade(self) -> Any:
        return exclusions.closed("DuckDB does not support foreign-key cascades")

    @property
    def named_constraints(self) -> Any:
        return exclusions.closed("DuckDB replaces user constraint names")

    @property
    def foreign_key_constraint_name_reflection(self) -> Any:
        return exclusions.closed("DuckDB replaces user constraint names")

    @property
    def self_referential_foreign_keys(self) -> Any:
        return exclusions.closed("DuckDB rejects inserts referencing the same table")

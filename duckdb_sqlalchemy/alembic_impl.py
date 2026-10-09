"""Alembic support for DuckDB.

Importing this module registers :class:`DuckDBImpl` as Alembic's implementation
for the ``duckdb`` dialect, unless another implementation is registered
already (for example an ``AlembicDuckDBImpl`` class defined in ``env.py``).
The dialect imports it on its own once Alembic is loaded, so ``env.py`` needs
no implementation class.
"""

import re
from typing import Any, Collection, Dict, List, Optional, Tuple

from alembic.autogenerate.render import _repr_type
from alembic.ddl import impl as _alembic_impl
from alembic.ddl.base import (
    ColumnComment,
    RenameTable,
    format_column_name,
    format_table_name,
)
from alembic.ddl.impl import DefaultImpl
from sqlalchemy import inspect, schema, text
from sqlalchemy import types as sqltypes
from sqlalchemy.ext.compiler import compiles

from . import (
    _column_needs_implicit_sequence,
    _implicit_sequence_ddl_name,
    _strip_enclosing_parentheses,
)
from .datatypes import FixedArray, Map, Struct, Union

__all__ = ["DuckDBImpl"]

_previous_impl = _alembic_impl._impls.get("duckdb")


class DuckDBImpl(DefaultImpl):
    """Alembic implementation for DuckDB."""

    __dialect__ = "duckdb"
    transactional_ddl = True

    def prep_table_for_batch(self, batch_impl: Any, table: Any) -> None:
        new_table = batch_impl.new_table
        implicit = [
            column
            for column in new_table.columns
            if _column_needs_implicit_sequence(column)
        ]
        if not implicit:
            return
        # copy_from models do not carry reflected defaults. Reuse the actual
        # sequence so copying explicit ids cannot reset its position.
        if self.as_sql:
            preparer = self.dialect.identifier_preparer
            defaults = {
                column.name: "nextval("
                + self.dialect.statement_compiler(
                    self.dialect, None
                ).render_literal_value(
                    _implicit_sequence_ddl_name(preparer, column), sqltypes.String()
                )
                + ")"
                for column in table.columns
                if _column_needs_implicit_sequence(column)
            }
        else:
            inspector = inspect(self.connection)
            assert inspector is not None
            defaults = {
                column["name"]: column["default"]
                for column in inspector.get_columns(table.name, schema=table.schema)
            }
        source_names = {
            column.name: name for name, column in batch_impl.columns.items()
        }
        for column in implicit:
            default = defaults.get(source_names.get(column.name, column.name))
            if default and default.lower().startswith("nextval("):
                column.server_default = schema.DefaultClause(text(default))
                preserved = getattr(self, "_preserved_batch_sequences", set())
                preserved.add(table)
                self._preserved_batch_sequences = preserved

    def drop_table(self, table: Any, **kw: Any) -> None:
        preserved = getattr(self, "_preserved_batch_sequences", set())
        if table not in preserved:
            return super().drop_table(table, **kw)
        preserved.discard(table)
        marker = "_duckdb_preserve_implicit_sequence"
        previous = table.info.get(marker)
        table.info[marker] = True
        try:
            super().drop_table(table, **kw)
        finally:
            if previous is None:
                table.info.pop(marker, None)
            else:
                table.info[marker] = previous

    def compare_server_default(
        self,
        inspector_column: Any,
        metadata_column: Any,
        rendered_metadata_default: Any,
        rendered_inspector_default: Any,
    ) -> bool:
        # The dialect backs autoincrement primary keys with an implicit
        # sequence, which reflects as a nextval(...) default.
        if (
            metadata_column.primary_key
            and metadata_column is metadata_column.table._autoincrement_column
            and metadata_column.server_default is None
        ):
            return False
        if rendered_inspector_default == rendered_metadata_default:
            return False
        if None in (rendered_inspector_default, rendered_metadata_default):
            return True
        # DuckDB stores defaults in its own spelling: '1' for 1, (1 + 1) for
        # 1 + 1, CAST('t' AS BOOLEAN) for true. Compare the normalized text
        # instead of querying, which would abort the migration transaction
        # on an error.
        boolean = isinstance(inspector_column.type, sqltypes.Boolean)
        column_type = self.dialect.type_compiler_instance.process(inspector_column.type)
        return _normalize_default(
            rendered_inspector_default, boolean, column_type
        ) != _normalize_default(rendered_metadata_default, boolean, column_type)

    def compare_type(self, inspector_column: Any, metadata_column: Any) -> bool:
        # Alembic compares type classes, so a STRUCT that gained a field
        # looks unchanged. Compare the DDL of nested types instead.
        inspector_type = inspector_column.type
        metadata_type = metadata_column.type
        if (
            isinstance(inspector_type, FixedArray)
            or isinstance(metadata_type, FixedArray)
            or (_is_nested(inspector_type) and _is_nested(metadata_type))
        ):
            compiler = self.dialect.type_compiler_instance
            return _normalize_sql_expression(compiler.process(inspector_type)) != (
                _normalize_sql_expression(compiler.process(metadata_type))
            )
        return super().compare_type(inspector_column, metadata_column)

    def correct_for_autogen_constraints(
        self,
        conn_uniques: Any,
        conn_indexes: Any,
        metadata_unique_constraints: Any,
        metadata_indexes: Any,
    ) -> None:
        # DuckDB replaces constraint names with its own (``items_sku_key``), so
        # a named unique constraint in the model is matched by its columns.
        if not isinstance(conn_uniques, set) or not isinstance(
            metadata_unique_constraints, set
        ):
            return
        conn_names = {uc.name for uc in conn_uniques}
        conn_by_columns: Dict[Tuple[str, ...], List[Any]] = {}
        for uc in conn_uniques:
            conn_by_columns.setdefault(_column_names(uc.columns), []).append(uc)
        for uc in list(metadata_unique_constraints):
            if not isinstance(uc.name, str) or uc.name in conn_names:
                continue
            matches = conn_by_columns.get(_column_names(uc.columns))
            if matches:
                conn_uniques.discard(matches.pop())
                metadata_unique_constraints.discard(uc)

    def render_type(self, type_obj: Any, autogen_context: Any) -> Any:
        # Alembic's default rendering uses repr(), which prints member types
        # as classes (<class 'sqlalchemy...Integer'>) inside these types.
        if isinstance(type_obj, FixedArray):
            autogen_context.imports.add("import duckdb_sqlalchemy.datatypes")
            item = _repr_type(type_obj.item_type, autogen_context)
            return f"duckdb_sqlalchemy.datatypes.FixedArray({item}, {type_obj.size})"
        if isinstance(type_obj, (Struct, Union, Map)):
            autogen_context.imports.add("import duckdb_sqlalchemy.datatypes")
            name = f"duckdb_sqlalchemy.datatypes.{type(type_obj).__name__}"
            if isinstance(type_obj, Map):
                key = _repr_type(type_obj.key_type, autogen_context)
                value = _repr_type(type_obj.value_type, autogen_context)
                return f"{name}({key}, {value})"
            fields = ", ".join(
                f"{field!r}: {_repr_type(member, autogen_context)}"
                for field, member in type_obj._fields or ()
            )
            return f"{name}({{{fields}}})"
        if isinstance(type_obj, sqltypes.ARRAY) and isinstance(
            type_obj.item_type, (Struct, Union, Map, FixedArray)
        ):
            item = _repr_type(type_obj.item_type, autogen_context)
            dimensions = (
                f", dimensions={type_obj.dimensions}" if type_obj.dimensions else ""
            )
            return f"sa.ARRAY({item}{dimensions})"
        return False


_CAST_RE = re.compile(r"CAST\((.*) AS ([\w\s(),]+)\)", re.IGNORECASE | re.DOTALL)
# DuckDB stores x::VARCHAR as CAST(x AS VARCHAR)
_SHORT_CAST_RE = re.compile(r"(.*?)\s*::\s*([\w\s(),]+)", re.DOTALL)
_STRING_RE = re.compile(r"('(?:[^']|'')*')")
_BOOLEAN_DEFAULTS = {"t": "true", "true": "true", "f": "false", "false": "false"}


def _normalize_sql_expression(value: str) -> str:
    """Normalize SQL tokens while retaining quoted identifier/literal contents."""
    parts = re.split(r"""('(?:[^']|'')*'|"(?:[^"]|"")*")""", value)
    return "".join(
        part if part.startswith(("'", '"')) else re.sub(r"\s+", "", part.lower())
        for part in parts
    )


def _normalize_default(
    value: str, boolean: bool, column_type: Optional[str] = None
) -> str:
    value = _strip_enclosing_parentheses(str(value))
    match = _CAST_RE.fullmatch(value) or _SHORT_CAST_RE.fullmatch(value)
    if match and (
        column_type is None
        or _normalize_sql_expression(match.group(2)).replace("decimal", "numeric")
        == _normalize_sql_expression(column_type).replace("decimal", "numeric")
    ):
        # Casting to the column type is redundant at insertion. A cast to a
        # different type can round or truncate first and must remain visible.
        value = _strip_enclosing_parentheses(match.group(1))
    if boolean:
        if _STRING_RE.fullmatch(value):
            value = value[1:-1].replace("''", "'")
        return _BOOLEAN_DEFAULTS.get(value.lower(), value)
    # Numeric quoted literals are DuckDB's canonical spelling of numeric
    # defaults. Other strings must stay quoted: '1+1' is not the expression 1+1.
    if _STRING_RE.fullmatch(value) and re.fullmatch(
        r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", value[1:-1]
    ):
        return value[1:-1]
    return _normalize_sql_expression(value)


def _is_nested(type_obj: Any) -> bool:
    if isinstance(type_obj, sqltypes.ARRAY):
        type_obj = type_obj.item_type
    return isinstance(type_obj, (Struct, Union, Map, FixedArray))


def _column_names(columns: Collection[Any]) -> Tuple[str, ...]:
    return tuple(column.name for column in columns)


if _previous_impl is not None:
    # keep an implementation that env.py registered before this module loaded
    _alembic_impl._impls["duckdb"] = _previous_impl


@compiles(RenameTable, "duckdb")
def _visit_rename_table(element: RenameTable, compiler: Any, **kw: Any) -> str:
    # DuckDB keeps the table in its current schema. RENAME TO accepts only
    # the new identifier, unlike Alembic's generic schema-qualified target.
    preparer = compiler.preparer
    table = preparer.quote(element.table_name)
    if element.schema:
        table = f"{preparer.quote_schema(element.schema)}.{table}"
    return f"ALTER TABLE {table} RENAME TO {preparer.quote(element.new_table_name)}"


@compiles(ColumnComment, "duckdb")
def _visit_column_comment(element: ColumnComment, compiler: Any, **kw: Any) -> str:
    comment = (
        compiler.sql_compiler.render_literal_value(element.comment, sqltypes.String())
        if element.comment is not None
        else "NULL"
    )
    return "COMMENT ON COLUMN {table}.{column} IS {comment}".format(
        table=format_table_name(compiler, element.table_name, element.schema),
        column=format_column_name(compiler, element.column_name),
        comment=comment,
    )

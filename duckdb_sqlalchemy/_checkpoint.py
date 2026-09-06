from typing import Any

from sqlalchemy.exc import InvalidRequestError


def checkpoint(connection: Any, *, force: bool = False, commit: bool = True) -> None:
    """Run CHECKPOINT with explicit transaction handling.

    SQLAlchemy 2.x connections autobegin transactions on first use. Running
    ``CHECKPOINT`` inside that transaction can leave the connection in an
    aborted state after local writes. This helper mirrors the raw DuckDB
    workflow by committing before and after the checkpoint when requested.
    """

    statement = "FORCE CHECKPOINT" if force else "CHECKPOINT"

    if hasattr(connection, "exec_driver_sql"):
        if connection.in_nested_transaction():
            raise InvalidRequestError(
                "checkpoint() does not support nested transactions"
            )
        if commit and connection.in_transaction():
            connection.commit()
        connection.exec_driver_sql(statement)
        if commit and connection.in_transaction():
            connection.commit()
        return

    if hasattr(connection, "execute") and hasattr(connection, "commit"):
        if commit:
            connection.commit()
        connection.execute(statement)
        if commit:
            connection.commit()
        return

    raise TypeError(
        "checkpoint() requires a SQLAlchemy Connection or DuckDB connection"
    )

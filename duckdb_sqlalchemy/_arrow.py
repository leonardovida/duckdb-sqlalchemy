from typing import Any

from sqlalchemy.exc import InvalidRequestError


class DuckDBArrowResult:
    def __init__(self, result: Any) -> None:
        self._result = result
        self._arrow = None

    def _fetch_arrow(self) -> Any:
        if self._arrow is not None:
            return self._arrow
        cursor = getattr(self._result, "cursor", None)
        if cursor is None:
            cursor = getattr(self._result, "_cursor", None)
        if cursor is None:
            raise NotImplementedError("Arrow results are not available on this cursor")
        if getattr(cursor, "_rows_fetched", False):
            # DuckDB reads rows in chunks, so after fetchone()/fetchmany() the
            # Arrow table would silently miss part of the result.
            raise InvalidRequestError(
                "Arrow results are unavailable after rows were fetched from this "
                "result; use .arrow before fetchone(), fetchmany() or iteration"
            )
        fetch_arrow_table = getattr(cursor, "to_arrow_table", None)
        if fetch_arrow_table is None:
            fetch_arrow_table = getattr(cursor, "fetch_arrow_table", None)
        if fetch_arrow_table is None:
            raise NotImplementedError("Arrow results are not available on this cursor")
        self._arrow = fetch_arrow_table()
        return self._arrow

    @property
    def arrow(self) -> Any:
        return self._fetch_arrow()

    def all(self) -> Any:
        return self._fetch_arrow()

    def fetchall(self) -> Any:
        return self._fetch_arrow()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._result, name)

    def __iter__(self) -> Any:
        return iter(self._result)

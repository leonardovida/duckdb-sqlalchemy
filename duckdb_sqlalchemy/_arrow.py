from typing import Any, Optional

from sqlalchemy.exc import InvalidRequestError


class DuckDBArrowBatchReader:
    """A bounded Arrow stream that owns the connection result until closed."""

    def __init__(self, reader: Any, cursor: Any) -> None:
        self._reader = reader
        self._cursor = cursor
        self._result: Optional[Any] = None
        self.closed = False

    @property
    def schema(self) -> Any:
        return self._reader.schema

    def __iter__(self) -> "DuckDBArrowBatchReader":
        return self

    def __next__(self) -> Any:
        return self.read_next_batch()

    def read_next_batch(self) -> Any:
        if self.closed:
            raise StopIteration
        try:
            return self._reader.read_next_batch()
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self.close()
            raise

    def read_all(self) -> Any:
        if self.closed:
            raise InvalidRequestError("The Arrow batch reader is closed")
        try:
            return self._reader.read_all()
        finally:
            self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        try:
            self._reader.close()
        finally:
            self._cursor._native_reader_active = False
            self._cursor._native_reader = None
            self._cursor._store_buffered_result([], self._cursor.description, self._cursor.rowcount)
            self._cursor._result_consumed()
            if self._result is not None:
                self._result.close()

    def __enter__(self) -> "DuckDBArrowBatchReader":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class DuckDBArrowResult:
    def __init__(self, result: Any) -> None:
        self._result = result
        self._arrow = None

    def _available_cursor(self) -> Any:
        cursor = getattr(self._result, "cursor", None)
        if cursor is None:
            cursor = getattr(self._result, "_cursor", None)
        if cursor is None:
            raise NotImplementedError("Arrow results are not available on this cursor")
        if getattr(cursor, "_rows_fetched", False):
            raise InvalidRequestError(
                "Arrow results are unavailable after rows were fetched from this "
                "result; use .arrow or .batches() before fetching or iteration"
            )
        return cursor

    def _fetch_arrow(self) -> Any:
        if self._arrow is not None:
            return self._arrow
        cursor = self._available_cursor()
        fetch_arrow_table = getattr(cursor, "to_arrow_table", None)
        if fetch_arrow_table is None:
            fetch_arrow_table = getattr(cursor, "fetch_arrow_table", None)
        if fetch_arrow_table is None:
            raise NotImplementedError("Arrow results are not available on this cursor")
        self._arrow = fetch_arrow_table()
        self._result.close()
        return self._arrow

    def batches(self, batch_size: int = 65536) -> DuckDBArrowBatchReader:
        """Read RecordBatches without materializing a full Arrow table.

        Use as a context manager to close an early-stopped stream. Another
        statement on the same connection requires this reader to be consumed
        or closed first.
        """
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        if self._arrow is not None:
            raise InvalidRequestError("The result has already been read as an Arrow table")
        cursor = self._available_cursor()
        fetch_reader = getattr(cursor, "fetch_arrow_reader", None)
        if fetch_reader is None:
            fetch_reader = getattr(cursor, "fetch_record_batch", None)
        if fetch_reader is None:
            raise NotImplementedError("Arrow batch reads are not available on this cursor")
        reader = DuckDBArrowBatchReader(fetch_reader(batch_size), cursor)
        cursor._native_reader_active = True
        cursor._native_reader = reader
        reader._result = self._result
        return reader

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

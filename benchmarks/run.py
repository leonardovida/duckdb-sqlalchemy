"""Reproducible timings, memory and exact-value checks in fresh worker processes."""

import argparse
import importlib.metadata
import json
import platform
import statistics
import subprocess
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Any

CASES = ["native_insert", "core_insert", "bulk_insert", "arrow_insert", "pandas_insert", "tuple_read", "arrow_read", "batch_read", "reflection"]


def worker(case: str, rows: int) -> dict[str, Any]:
    import duckdb
    import pandas as pd
    import pyarrow as pa
    from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, event, text

    from duckdb_sqlalchemy import insert_from_arrow
    from duckdb_sqlalchemy._bulk_insert import build_bulk_insert_dataframe

    start_value = 2**53 + 1
    engine = create_engine("duckdb:///:memory:", connect_args={"config": {"threads": 2}})
    table = Table("benchmark_data", MetaData(), Column("id", Integer), Column("value", String))
    parameters = [(start_value + i, f"value-{i}") for i in range(rows)]
    arrow_table = pa.table({"id": [row[0] for row in parameters], "value": [row[1] for row in parameters]})
    query_count = 0

    def count_query(*args: Any) -> None:
        nonlocal query_count
        query_count += 1

    event.listen(engine, "before_cursor_execute", count_query)
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE benchmark_data(id BIGINT, value VARCHAR)")
        if case.endswith("read"):
            connection.exec_driver_sql(
                "INSERT INTO benchmark_data SELECT ? + i, 'value-' || i FROM range(?) AS t(i)",
                (start_value, rows),
            )
        if case == "reflection":
            for index in range(100):
                connection.exec_driver_sql(f"CREATE TABLE reflection_{index}(id INTEGER)")
        query_count = 0
        tracemalloc.start()
        started = time.perf_counter()
        if case == "native_insert":
            native = connection.connection.dbapi_connection
            native.executemany("INSERT INTO benchmark_data VALUES (?, ?)", parameters)
        elif case in {"core_insert", "bulk_insert"}:
            connection.execution_options(duckdb_copy_threshold=0 if case == "core_insert" else 1).execute(
                table.insert(), [{"id": row[0], "value": row[1]} for row in parameters]
            )
        elif case == "arrow_insert":
            insert_from_arrow(connection, table, arrow_table)
        elif case == "pandas_insert":
            frame = build_bulk_insert_dataframe(parameters, ["id", "value"])
            assert isinstance(frame, pd.DataFrame)
            native = connection.connection.dbapi_connection
            native.register("benchmark_frame", frame)
            try:
                connection.exec_driver_sql("INSERT INTO benchmark_data SELECT * FROM benchmark_frame")
            finally:
                native.unregister("benchmark_frame")
        elif case == "tuple_read":
            actual = connection.execute(text("SELECT * FROM benchmark_data")).all()
            assert len(actual) == rows and actual[0][0] == start_value
        elif case == "arrow_read":
            actual = connection.execution_options(duckdb_arrow=True).execute(text("SELECT * FROM benchmark_data")).arrow
            assert actual.num_rows == rows and actual.column(0)[0].as_py() == start_value
        elif case == "batch_read":
            result = connection.execution_options(duckdb_arrow=True).execute(text("SELECT * FROM benchmark_data"))
            count = 0
            with result.batches(2048) as reader:
                for batch in reader:
                    if count == 0:
                        assert batch.column(0)[0].as_py() == start_value
                    assert batch.num_rows <= 2048
                    count += batch.num_rows
            assert count == rows
        else:
            names = [f"reflection_{i}" for i in range(100)] + ["absent"]
            reflected = dict(connection.dialect.has_multi_table(connection, names))
            assert all(reflected[(None, name)] for name in names[:-1])
            assert not reflected[(None, "absent")]
            assert query_count <= 2, query_count
        seconds = time.perf_counter() - started
        _, python_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        measured_queries = query_count
        if not case.endswith("read") and case != "reflection":
            assert connection.execution_options(duckdb_arrow=False).exec_driver_sql(
                "SELECT count(*), min(id), max(id), sum(id) FROM benchmark_data"
            ).one() == (rows, start_value, start_value + rows - 1, rows * start_value + rows * (rows - 1) // 2)
    engine.dispose()
    try:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        peak_rss_bytes = rss if sys.platform == "darwin" else rss * 1024
    except ImportError:
        peak_rss_bytes = None
    return {
        "case": case, "rows": rows, "seconds": seconds,
        "rows_per_second": rows / seconds, "python_peak_bytes": python_peak,
        "process_peak_rss_bytes": peak_rss_bytes, "query_count": measured_queries,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--worker", choices=CASES)
    args = parser.parse_args()
    if args.rows <= 0 or args.repeats <= 0:
        parser.error("rows and repeats must be positive")
    if args.worker:
        print(json.dumps(worker(args.worker, args.rows)))
        return
    results = []
    for case in CASES:
        samples = []
        for _ in range(args.repeats):
            completed = subprocess.run(
                [sys.executable, str(Path(__file__).resolve()), "--worker", case, "--rows", str(args.rows)],
                check=True, capture_output=True, text=True,
            )
            samples.append(json.loads(completed.stdout))
        results.append({
            "case": case,
            "median_seconds": statistics.median(sample["seconds"] for sample in samples),
            "samples": samples,
        })
    report = {
        "metadata": {
            "python": sys.version, "platform": platform.platform(), "machine": platform.machine(),
            "versions": {name: importlib.metadata.version(name) for name in ["duckdb-sqlalchemy", "duckdb", "SQLAlchemy", "pyarrow", "pandas"]},
            "rows": args.rows, "repeats": args.repeats,
        },
        "results": results,
    }
    serialized = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.write_text(serialized, encoding="utf-8")
    print(serialized, end="")


if __name__ == "__main__":
    main()

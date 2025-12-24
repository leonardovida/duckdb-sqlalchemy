import os

from sqlalchemy import create_engine, text


def main() -> None:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise SystemExit("Set MOTHERDUCK_TOKEN before running")

    engine = create_engine("duckdb:///md:my_db?attach_mode=single")

    with engine.connect().execution_options(duckdb_arrow=True) as conn:
        result = conn.execute(text("select 1 as value"))
        arrow_table = result.arrow
        print(arrow_table)
        print(arrow_table.to_pandas())

    with engine.connect().execution_options(
        stream_results=True,
        duckdb_arraysize=1000,
    ) as conn:
        for row in conn.execute(text("select 1 as value")):
            print(row)
            break


if __name__ == "__main__":
    main()

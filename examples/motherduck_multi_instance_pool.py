import os

from sqlalchemy import text

from duckdb_sqlalchemy import create_engine_from_paths


def build_engine():
    paths = [
        "md:my_db?attach_mode=single&user=1",
        "md:my_db?attach_mode=single&user=2",
        "md:my_db?attach_mode=single&user=3",
    ]

    # Pool sizing arguments select a QueuePool; new pooled connections rotate
    # across the paths above.
    return create_engine_from_paths(paths, pool_size=5, max_overflow=10)


def main() -> None:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise SystemExit("Set MOTHERDUCK_TOKEN before running")

    engine = build_engine()

    with engine.connect() as conn:
        result = conn.execute(text("select 1 as ok")).fetchone()
        print(result)


if __name__ == "__main__":
    main()

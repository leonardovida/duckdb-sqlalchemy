import os

from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


def main() -> None:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise SystemExit("Set MOTHERDUCK_TOKEN before running")

    engine = create_engine(
        "duckdb:///md:my_db?attach_mode=single",
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=23 * 3600,
    )

    with engine.connect() as conn:
        result = conn.execute(text("select current_database()")).fetchone()
        print(result)


if __name__ == "__main__":
    main()

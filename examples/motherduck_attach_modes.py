import os

from sqlalchemy import create_engine, text


def main() -> None:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise SystemExit("Set MOTHERDUCK_TOKEN before running")

    single_engine = create_engine("duckdb:///md:my_db?attach_mode=single")
    workspace_engine = create_engine("duckdb:///md:my_db?attach_mode=workspace")

    with single_engine.connect() as conn:
        result = conn.execute(text("select current_database()"))
        print("single mode:", result.fetchone())

    with workspace_engine.connect() as conn:
        result = conn.execute(text("select current_database()"))
        print("workspace mode:", result.fetchone())


if __name__ == "__main__":
    main()

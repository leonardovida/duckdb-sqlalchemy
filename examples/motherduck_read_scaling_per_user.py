import os
from functools import lru_cache

from sqlalchemy import text

from duckdb_sqlalchemy import create_motherduck_engine, stable_session_hint


@lru_cache(maxsize=128)
def _engine_for_session(session_hint: str):
    return create_motherduck_engine(
        database="md:my_db",
        attach_mode="single",
        access_mode="read_only",
        session_hint=session_hint,
        performance=True,
        pool_size=5,
        max_overflow=10,
    )


def get_engine_for_user(user_id: str):
    salt = os.getenv("MOTHERDUCK_SESSION_SALT", "")
    session_hint = stable_session_hint(user_id, salt=salt)
    return _engine_for_session(session_hint)


def main() -> None:
    token = os.getenv("MOTHERDUCK_TOKEN")
    if not token:
        raise SystemExit("Set MOTHERDUCK_TOKEN to your read-scaling token")

    engine = get_engine_for_user("user-123")
    with engine.connect() as conn:
        result = conn.execute(text("select 1 as ok")).fetchone()
        print(result)


if __name__ == "__main__":
    main()

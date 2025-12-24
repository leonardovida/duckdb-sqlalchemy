from sqlalchemy import create_engine, select, text

from duckdb_sqlalchemy import URL, read_parquet

engine = create_engine(URL(database=":memory:"))

with engine.begin() as conn:
    conn.execute(
        text(
            """
            CREATE TABLE events (
                event_id INTEGER,
                event_ts TIMESTAMP,
                event_name VARCHAR
            )
            """
        )
    )
    conn.execute(
        text(
            """
            INSERT INTO events VALUES
                (1, '2024-01-01 00:00:00', 'signup'),
                (2, '2024-01-01 01:00:00', 'purchase')
            """
        )
    )

    rows = conn.execute(text("SELECT * FROM events ORDER BY event_id")).fetchall()
    print(rows)

# Table-valued function example. Replace with a real file path.
parquet = read_parquet("/path/to/events.parquet", columns=["event_id", "event_ts"])
stmt = select(parquet.c.event_id, parquet.c.event_ts).limit(5)
print(stmt)

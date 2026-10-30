"""Build the data for a MotherDuck Dive and deploy the Dive from Python.

A Dive is a React component stored in MotherDuck that queries live data with
the useSQLQuery hook and renders it with Recharts. Keeping the component in
your repository and deploying it with SQL makes Dives reviewable and
repeatable like any other code:

1. Load the data the Dive reads, here a small orders table.
2. Deploy the component: create the Dive on the first run, publish a new
   version only when the component changed, and do nothing otherwise.
3. Mark the reviewed version as ready, so viewers know which one to trust.

Requires MOTHERDUCK_TOKEN. The example creates a database named
duckdb_sqlalchemy_dive_<random> and a Dive titled after it, and deletes both
at the end. Pass --keep to keep them and open the Dive in the MotherDuck UI.

Run from the repository: uv run python examples/motherduck_dive.py
"""

import os
import secrets
import sys
from contextlib import contextmanager
from typing import Iterator, List, Mapping, Optional, Tuple

from sqlalchemy import Connection, select, text

from duckdb_sqlalchemy import (
    create_motherduck_engine,
    md_create_dive,
    md_delete_dive,
    md_get_dive,
    md_list_dive_versions,
    md_list_dives,
    md_update_dive_content,
    md_update_dive_status,
)

# The Dive's SQL names tables through the alias declared in required_resources.
# Use fully qualified, double-quoted names so the Dive runs in any context.
KPIS_AND_TREND = """
import { useSQLQuery } from "@motherduck/react-sql-query";
import {
  CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from "recharts";

const N = (value) => (value == null ? 0 : Number(value));

function Kpi({ label, value }) {
  return (
    <div style={{ padding: 16, border: "1px solid #ddd", borderRadius: 8 }}>
      <div style={{ fontSize: 12, color: "#666" }}>{label}</div>
      <div style={{ fontSize: 28, fontWeight: 600 }}>{value}</div>
    </div>
  );
}

export default function SalesOverview() {
  const totals = useSQLQuery(`
    SELECT count(*) AS orders, sum(amount) AS revenue
    FROM "sales"."main"."orders"
  `);
  const daily = useSQLQuery(`
    SELECT ordered_on::VARCHAR AS day, sum(amount) AS revenue
    FROM "sales"."main"."orders"
    GROUP BY ALL ORDER BY day
  `);
  const total = Array.isArray(totals.data) ? totals.data[0] : undefined;
  const trend = (Array.isArray(daily.data) ? daily.data : []).map((row) => ({
    day: row.day,
    revenue: N(row.revenue),
  }));

  return (
    <main style={{ fontFamily: "sans-serif", padding: 24 }}>
      <h1>Sales overview</h1>
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <Kpi label="Orders" value={N(total?.orders).toLocaleString()} />
        <Kpi label="Revenue" value={N(total?.revenue).toLocaleString()} />
      </div>
      {daily.isLoading ? (
        <p>Loading…</p>
      ) : (
        <ResponsiveContainer width="100%" height={320}>
          <LineChart data={trend}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="day" />
            <YAxis />
            <Tooltip />
            <Line type="monotone" dataKey="revenue" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </main>
  );
}
"""

# The second release adds a revenue-by-region chart.
WITH_REGIONS = (
    KPIS_AND_TREND.replace(
        "  CartesianGrid, Line,", "  Bar, BarChart, CartesianGrid, Line,"
    )
    .replace(
        "  return (\n    <main",
        """  const regions = useSQLQuery(`
    SELECT region, sum(amount) AS revenue
    FROM "sales"."main"."orders"
    GROUP BY ALL ORDER BY revenue DESC
  `);
  const byRegion = (Array.isArray(regions.data) ? regions.data : []).map((row) => ({
    region: row.region,
    revenue: N(row.revenue),
  }));

  return (
    <main""",
    )
    .replace(
        "    </main>",
        """      <h2>Revenue by region</h2>
      <ResponsiveContainer width="100%" height={240}>
        <BarChart data={byRegion}>
          <XAxis dataKey="region" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="revenue" />
        </BarChart>
      </ResponsiveContainer>
    </main>""",
    )
)


def find_dive(conn: Connection, title: str) -> Optional[str]:
    dives = md_list_dives()
    dive_id = conn.execute(
        select(dives.c.id).where(dives.c.title == title)
    ).scalar_one_or_none()
    return None if dive_id is None else str(dive_id)


def deploy_dive(
    conn: Connection,
    *,
    title: str,
    description: str,
    content: str,
    resources: List[Mapping[str, str]],
) -> Tuple[str, int, str]:
    """Create the Dive or publish a new version when its content changed.

    Returns the Dive id, its current version and what the deploy did.
    """
    dive_id = find_dive(conn, title)
    if dive_id is None:
        created = conn.execute(
            select(
                md_create_dive(
                    title=title,
                    description=description,
                    content=content,
                    required_resources=resources,
                )
            )
        ).one()
        return str(created.id), created.current_version, "created"

    current = conn.execute(select(md_get_dive(id=dive_id))).one()
    if current.content == content:
        return dive_id, current.current_version, "unchanged"

    # required_resources is not carried over between versions, so pass the
    # full list with every release.
    version = conn.execute(
        select(
            md_update_dive_content(
                id=dive_id,
                content=content,
                description=f"Deployed from {os.path.basename(__file__)}",
                required_resources=resources,
            ).c.version
        )
    ).scalar_one()
    return dive_id, version, "published"


@contextmanager
def scratch_database(prefix: str, keep: bool = False) -> Iterator[str]:
    """Create a uniquely named MotherDuck database and drop it afterwards."""
    name = f"duckdb_sqlalchemy_{prefix}_{secrets.token_hex(4)}"
    workspace = create_motherduck_engine(database="md:")
    try:
        with workspace.begin() as conn:
            conn.execute(text(f"CREATE DATABASE {name}"))
        yield name
    finally:
        if keep:
            print(f"kept database {name}")
        else:
            with workspace.begin() as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {name}"))
        workspace.dispose()


def main() -> None:
    if not os.getenv("MOTHERDUCK_TOKEN"):
        raise SystemExit("Set MOTHERDUCK_TOKEN before running this example")
    keep = "--keep" in sys.argv

    with scratch_database("dive", keep=keep) as database:
        engine = create_motherduck_engine(database="md:")
        title = f"Sales overview ({database})"
        resources = [{"url": f"md:{database}", "alias": "sales"}]
        dive_id = None
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        f"CREATE TABLE {database}.main.orders AS "
                        "SELECT DATE '2026-01-01' + CAST(i % 30 AS INTEGER) AS ordered_on, "
                        "['EMEA', 'AMER', 'APAC'][1 + i % 3] AS region, "
                        "round(20 + (i * 37) % 180, 2) AS amount "
                        "FROM range(3000) AS t(i)"
                    )
                )

            for content in (KPIS_AND_TREND, KPIS_AND_TREND, WITH_REGIONS):
                with engine.begin() as conn:
                    dive_id, version, action = deploy_dive(
                        conn,
                        title=title,
                        description="Daily revenue and orders by region",
                        content=content,
                        resources=resources,
                    )
                print(f"{action}: {title} is at version {version}")

            with engine.begin() as conn:
                status = conn.execute(
                    select(
                        md_update_dive_status(
                            id=dive_id, status="ready", version=version
                        )
                    )
                ).one()
                print(
                    f"marked version {status.status_applies_to_version} {status.status}"
                )
                history = md_list_dive_versions(id=dive_id)
                for row in conn.execute(
                    select(history.c.version, history.c.description).order_by(
                        history.c.version
                    )
                ):
                    print(f"version {row.version}: {row.description}")
        finally:
            if dive_id is not None:
                if keep:
                    print(
                        f"kept Dive {dive_id}, find it under Dives in the MotherDuck UI"
                    )
                else:
                    with engine.begin() as conn:
                        conn.execute(select(md_delete_dive(id=dive_id)))
            engine.dispose()


if __name__ == "__main__":
    main()

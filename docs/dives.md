---
layout: default
title: MotherDuck Dives
---

# MotherDuck Dives

A [Dive](https://motherduck.com/docs/key-tasks/dives/) is an interactive data
app stored in MotherDuck: a React component that queries live data with the
`useSQLQuery` hook and renders it with [Recharts](https://recharts.org/).
MotherDuck exposes SQL functions for creating and versioning Dives, and
`duckdb_sqlalchemy` wraps each one as a SQLAlchemy table function. That lets a
Python application or a CI job build the data a Dive reads and deploy the Dive
itself from the same code.

The complete workflow runs in
[`examples/motherduck_dive.py`](https://github.com/leonardovida/duckdb-sqlalchemy/blob/main/examples/motherduck_dive.py).

## Write the component

The component is a single default export. Declare the databases it reads in
`required_resources`, and refer to them by alias with fully qualified,
double-quoted names, so the Dive runs in any database context:

```jsx
import { useSQLQuery } from "@motherduck/react-sql-query";
import { Line, LineChart, ResponsiveContainer, XAxis, YAxis } from "recharts";

export default function SalesOverview() {
  const { data, isLoading } = useSQLQuery(`
    SELECT ordered_on::VARCHAR AS day, sum(amount) AS revenue
    FROM "sales"."main"."orders"
    GROUP BY ALL ORDER BY day
  `);
  if (isLoading) return <p>Loading…</p>;
  const rows = Array.isArray(data) ? data : [];
  return (
    <ResponsiveContainer width="100%" height={320}>
      <LineChart data={rows.map((r) => ({ day: r.day, revenue: Number(r.revenue) }))}>
        <XAxis dataKey="day" />
        <YAxis />
        <Line dataKey="revenue" dot={false} />
      </LineChart>
    </ResponsiveContainer>
  );
}
```

`data` is the row array itself and is `undefined` while the query runs, so
guard it before mapping. MotherDuck stores the content as given and does not
check that it renders, so open a new Dive in the MotherDuck UI before sharing
it.

## Deploy from Python

Keep the component in your repository and deploy it with one function that
creates the Dive on the first run and publishes a new version only when the
content changed:

```python
from sqlalchemy import select

from duckdb_sqlalchemy import (
    create_motherduck_engine,
    md_create_dive,
    md_get_dive,
    md_list_dives,
    md_update_dive_content,
)


def deploy_dive(conn, *, title, content, resources):
    dives = md_list_dives()
    dive_id = conn.execute(
        select(dives.c.id).where(dives.c.title == title)
    ).scalar_one_or_none()
    if dive_id is None:
        created = conn.execute(
            select(md_create_dive(title=title, content=content, required_resources=resources))
        ).one()
        return str(created.id), created.current_version

    current = conn.execute(select(md_get_dive(id=str(dive_id)))).one()
    if current.content == content:
        return str(dive_id), current.current_version
    version = conn.execute(
        select(
            md_update_dive_content(
                id=str(dive_id), content=content, required_resources=resources
            ).c.version
        )
    ).scalar_one()
    return str(dive_id), version


engine = create_motherduck_engine(database="md:")
with engine.begin() as conn:
    dive_id, version = deploy_dive(
        conn,
        title="Sales overview",
        content=open("dives/sales_overview.jsx").read(),
        resources=[{"url": "md:analytics", "alias": "sales"}],
    )
```

Pass the full `required_resources` list with every version: MotherDuck does
not carry it over from the previous version. A resource is a database
(`md:<database>`) or a share (`md:_share/<name>/<uuid>`).

## Review and release versions

Every content change creates a numbered version. Mark the version you have
reviewed so viewers know which one to trust. The status is one of `draft`,
`ready`, `endorsed` or `archived`:

```python
from duckdb_sqlalchemy import md_list_dive_versions, md_update_dive_status

with engine.begin() as conn:
    conn.execute(select(md_update_dive_status(id=dive_id, status="ready", version=version)))

    history = md_list_dive_versions(id=dive_id)
    for row in conn.execute(select(history.c.version, history.c.created_at)):
        print(row.version, row.created_at)
```

Store the approved version number with your release configuration when the
Dive is embedded, and pass it when creating embed sessions (below), so a later
edit in the UI does not change what customers see.

## Feed Dives with fresh data

A Dive reads whatever the database holds when it runs, so the job that loads
the data and the job that deploys the Dive can share the same engine. For
Dives that many people open at once, serve the data from
[read scaling replicas](motherduck#reader-pools-with-read-scaling): run
`CREATE SNAPSHOT OF <database>` after your load commits so the replicas pick
up the new data right away.

## Embed a Dive in your application

Embedded Dives run as a MotherDuck service account. Your backend asks the
MotherDuck REST API for a short-lived embed session, and your frontend renders
it in a sandboxed iframe. This uses the REST API rather than SQL, so it is not
part of this package. A minimal backend call with `requests`:

```python
import os

import requests

response = requests.post(
    f"https://api.motherduck.com/v1/dives/{dive_id}/embed-session",
    headers={"Authorization": f"Bearer {os.environ['MOTHERDUCK_TOKEN']}"},
    json={"username": "tenant_a_service_account", "version": 2},
    timeout=30,
)
response.raise_for_status()
session = response.json()["session"]
```

```html
<iframe
  src="https://embed-motherduck.com/sandbox/#session=<session>"
  sandbox="allow-scripts allow-same-origin"
  width="100%" height="600" style="border: none;"
></iframe>
```

The session contains a read-only token, expires after 24 hours, and must be
treated as a credential: do not log or store it. Use one service account per
tenant when tenants must not see each other's data. See
[Embedding Dives](https://motherduck.com/docs/key-tasks/dives/embedding-dives/)
for the full set of session options.

## Helper reference

Each Dive SQL function has a helper that returns a SQLAlchemy table function.
Nothing runs until the statement executes:

| Helper | Purpose |
| --- | --- |
| `md_list_dives` | List Dives with their status and current version |
| `md_get_dive` | Fetch one Dive, including its current content |
| `md_create_dive` | Create a Dive and its first version |
| `md_update_dive_content` | Publish a new version |
| `md_update_dive_metadata` | Change the title or description without a new version |
| `md_update_dive_status` | Set `draft`, `ready`, `endorsed` or `archived` for a version |
| `md_list_dive_versions` | List the version history |
| `md_get_dive_version` | Fetch one version, including its content |
| `md_delete_dive` | Delete the Dive and its version history |

```python
from duckdb_sqlalchemy import (
    md_delete_dive,
    md_get_dive_version,
    md_update_dive_metadata,
)

renamed = md_update_dive_metadata(
    id="00000000-0000-0000-0000-000000000000",
    title="Sales overview (EMEA)",
    description="Weekly revenue by region",
)
rename_stmt = select(renamed.c.title, renamed.c.description)

version = md_get_dive_version(id="00000000-0000-0000-0000-000000000000", version=2)
version_stmt = select(version.c.version, version.c.content)

deleted = md_delete_dive(id="00000000-0000-0000-0000-000000000000")
delete_stmt = select(deleted.c.success)
```

The status columns need DuckDB 1.5.4 or newer, and `md_list_dives` returns
`required_resources` from DuckDB 1.5.2. See
[columns by DuckDB version](olap#columns-by-duckdb-version) for details.

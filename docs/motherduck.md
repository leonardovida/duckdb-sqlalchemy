# MotherDuck

MotherDuck connections use the `md:` database prefix.

```python
from sqlalchemy import create_engine

engine = create_engine("duckdb:///md:my_db")
```

## Tokens

Set `MOTHERDUCK_TOKEN` (or `motherduck_token`) in the environment and it will be picked up automatically when you connect to `md:` databases.

```bash
export MOTHERDUCK_TOKEN="..."
```

You can also pass the token in the URL or via `connect_args`:

```python
engine = create_engine(
    "duckdb:///md:my_db",
    connect_args={"config": {"motherduck_token": "..."}},
)
```

## Options

Common connection options (all passed as URL query params or via `connect_args["config"]`):

- `attach_mode`: `workspace` or `single`
- `saas_mode`: `true` or `false`
- `session_hint`: read-scaling session affinity
- `access_mode`: `read_only` for read-scaling tokens
- `dbinstance_inactivity_ttl`: alias for `motherduck_dbinstance_inactivity_ttl`

Example:

```
duckdb:///md:my_db?attach_mode=single&access_mode=read_only
```

from functools import lru_cache

import duckdb

from .capabilities import get_capabilities

_capabilities = get_capabilities(duckdb.__version__)
duckdb_version = _capabilities.version
has_uhugeint_support = _capabilities.supports_uhugeint


@lru_cache()
def has_comment_support() -> bool:
    """
    See https://github.com/duckdb/duckdb/pull/10372
    """
    try:
        with duckdb.connect(":memory:") as con:
            con.execute("CREATE TABLE t (i INTEGER);")
            con.execute("COMMENT ON TABLE t IS 'test';")
    except duckdb.ParserException:
        return False
    return True

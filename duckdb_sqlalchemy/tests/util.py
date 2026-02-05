import pandas as pd
import sqlalchemy
from packaging.version import Version
from pytest import mark

pandas_sql_compatible = not (
    Version(sqlalchemy.__version__) < Version("2.0.0")
    and Version(pd.__version__) >= Version("2.2.0")
    or Version(sqlalchemy.__version__) >= Version("2.0.0")
    and Version(pd.__version__) < Version("2.0.0")
)
pandas_sql_compatible_only = mark.skipif(
    not pandas_sql_compatible,
    reason=(
        "unsupported pandas/SQLAlchemy pair: "
        "pandas>=2.2 requires SQLAlchemy>=2.0, and pandas<2.0 is incompatible with SQLAlchemy>=2.0"
    ),
)

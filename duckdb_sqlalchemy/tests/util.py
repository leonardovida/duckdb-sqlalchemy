import pandas as pd
import sqlalchemy
from packaging.version import Version
from pytest import mark

PANDAS_VERSION = Version(pd.__version__)
SQLALCHEMY_VERSION = Version(sqlalchemy.__version__)

_PANDAS_SQLALCHEMY_COMPATIBLE = (
    PANDAS_VERSION < Version("2.0") and SQLALCHEMY_VERSION.major == 1
) or (PANDAS_VERSION >= Version("2.0") and SQLALCHEMY_VERSION.major >= 2)

pandas_sqlalchemy_compatible = mark.skipif(
    not _PANDAS_SQLALCHEMY_COMPATIBLE,
    reason=("pandas>=2.0 requires SQLAlchemy>=2.0; pandas<2.0 expects SQLAlchemy 1.x"),
)

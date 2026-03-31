from __future__ import annotations

import hashlib
import warnings
from itertools import cycle
from typing import (
    Any,
    Collection,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from urllib.parse import urlencode

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL as SAURL
from sqlalchemy.engine.url import make_url as sa_make_url
from sqlalchemy.pool import Pool, QueuePool

from ._query import merge_query_mappings

DBINSTANCE_INACTIVITY_TTL_KEY = "dbinstance_inactivity_ttl"
MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL_KEY = "motherduck_dbinstance_inactivity_ttl"

MOTHERDUCK_PATH_QUERY_KEYS = {
    "user",
    "session_hint",
    "attach_mode",
    "access_mode",
    DBINSTANCE_INACTIVITY_TTL_KEY,
    MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL_KEY,
    "saas_mode",
    "cache_buster",
}

MOTHERDUCK_CONFIG_KEYS = MOTHERDUCK_PATH_QUERY_KEYS | {"motherduck_token"}

DIALECT_QUERY_KEYS = {"duckdb_sqlalchemy_pool", "pool"}
CONNECT_ARG_MAPPING_KEYS = ("config", "url_config")


def _warn_deprecated_ttl_alias() -> None:
    warnings.warn(
        "`motherduck_dbinstance_inactivity_ttl` is deprecated; use "
        "`dbinstance_inactivity_ttl` instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def _sync_ttl_alias(
    values: Dict[str, Any],
    *,
    source_key: str,
    target_key: str,
    warn_on_key: str,
    drop_source: bool = False,
) -> Dict[str, Any]:
    if warn_on_key in values:
        _warn_deprecated_ttl_alias()

    source_value = (
        values.pop(source_key, None) if drop_source else values.get(source_key)
    )
    if target_key not in values and source_value is not None:
        values[target_key] = source_value
    return values


def _normalize_path_query_aliases(path_query: Dict[str, Any]) -> Dict[str, Any]:
    return _sync_ttl_alias(
        path_query,
        source_key=MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL_KEY,
        target_key=DBINSTANCE_INACTIVITY_TTL_KEY,
        warn_on_key=MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL_KEY,
        drop_source=True,
    )


def _normalize_path_query_mapping(
    *mappings: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    return _normalize_path_query_aliases(merge_query_mappings(*mappings))


def _normalize_config_aliases(config: Dict[str, Any]) -> Dict[str, Any]:
    return _sync_ttl_alias(
        config,
        source_key=DBINSTANCE_INACTIVITY_TTL_KEY,
        target_key=MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL_KEY,
        warn_on_key=MOTHERDUCK_DBINSTANCE_INACTIVITY_TTL_KEY,
    )


def _partition_query(
    query: Mapping[str, Any], *, ignored_keys: Collection[str] = ()
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    path_query: Dict[str, Any] = {}
    remaining: Dict[str, Any] = {}
    ignored = set(ignored_keys)
    for key, value in query.items():
        if key in ignored:
            continue
        if key in MOTHERDUCK_PATH_QUERY_KEYS:
            path_query[key] = value
        else:
            remaining[key] = value
    return path_query, remaining


def _split_path_query(
    query: Mapping[str, Any], *, ignored_keys: Collection[str] = ()
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    path_query, remaining = _partition_query(query, ignored_keys=ignored_keys)
    return _normalize_path_query_mapping(path_query), remaining


def split_url_query(query: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _split_path_query(query, ignored_keys=DIALECT_QUERY_KEYS)


def extract_path_query_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    path_query, remaining = _split_path_query(config)
    config.clear()
    config.update(remaining)
    return path_query


def append_query_to_database(
    database: Optional[str], query: Dict[str, Any]
) -> Optional[str]:
    if not query:
        return database
    query_string = urlencode(query, doseq=True)
    if database is None:
        return f"?{query_string}"
    separator = "&" if "?" in database else "?"
    return f"{database}{separator}{query_string}"


def MotherDuckURL(
    *,
    database: str,
    query: Optional[Mapping[str, Any]] = None,
    path_query: Optional[Mapping[str, Any]] = None,
    **kwargs: Any,
) -> SAURL:
    """
    Build a SQLAlchemy URL for MotherDuck, ensuring routing/cache parameters
    live in the database string.
    """

    path_kwargs, config_kwargs = _partition_query(kwargs)
    path_params = _normalize_path_query_mapping(path_query, path_kwargs)
    config_params = merge_query_mappings(query, config_kwargs)

    database_with_query = append_query_to_database(database, path_params)
    return SAURL.create("duckdb", database=database_with_query, query=config_params)


def stable_session_hint(
    value: Union[str, int],
    *,
    salt: Optional[str] = None,
    length: int = 16,
) -> str:
    if length <= 0:
        raise ValueError("length must be positive")
    payload = str(value)
    if salt:
        payload = f"{salt}:{payload}"
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:length]


def create_motherduck_engine(
    *,
    database: str,
    query: Optional[Mapping[str, Any]] = None,
    connect_args: Optional[Mapping[str, Any]] = None,
    performance: bool = False,
    poolclass: Optional[Type[Pool]] = None,
    pool_pre_ping: Optional[bool] = None,
    pool_recycle: Optional[int] = None,
    **path_params: Any,
) -> sqlalchemy.engine.Engine:
    url = MotherDuckURL(database=database, query=query, **path_params)

    engine_kwargs: Dict[str, Any] = {}
    if poolclass is not None:
        engine_kwargs["poolclass"] = poolclass
    if pool_pre_ping is not None:
        engine_kwargs["pool_pre_ping"] = pool_pre_ping
    if pool_recycle is not None:
        engine_kwargs["pool_recycle"] = pool_recycle

    if performance:
        engine_kwargs.setdefault("poolclass", QueuePool)
        engine_kwargs.setdefault("pool_pre_ping", True)
        engine_kwargs.setdefault("pool_recycle", 23 * 3600)

    return create_engine(url, connect_args=dict(connect_args or {}), **engine_kwargs)


def _normalize_path_item(path: Union[str, SAURL]) -> SAURL:
    if isinstance(path, SAURL):
        return path
    if path.startswith("duckdb://"):
        return sa_make_url(path)
    return SAURL.create("duckdb", database=path)


def _merge_connect_args(
    base: Mapping[str, Any], extra: Mapping[str, Any]
) -> Dict[str, Any]:
    merged = _copy_connect_params(base)
    if not extra:
        return merged
    remaining = _copy_connect_params(extra)
    for key in CONNECT_ARG_MAPPING_KEYS:
        extra_mapping = remaining.pop(key, None)
        if extra_mapping is None:
            continue
        merged[key] = {**merged.get(key, {}), **extra_mapping}
    merged.update(remaining)
    return merged


def _copy_connect_params(params: Mapping[str, Any]) -> Dict[str, Any]:
    copied = dict(params)
    for key in CONNECT_ARG_MAPPING_KEYS:
        value = copied.get(key)
        if value is not None:
            copied[key] = dict(value)
    return copied


def create_engine_from_paths(
    paths: Sequence[Union[str, SAURL]],
    *,
    connect_args: Optional[Mapping[str, Any]] = None,
    **engine_kwargs: Any,
) -> sqlalchemy.engine.Engine:
    if not paths:
        raise ValueError("paths must not be empty")

    urls = [_normalize_path_item(path) for path in paths]
    if len({url.drivername for url in urls}) != 1:
        raise ValueError("all paths must use the same drivername")

    from . import Dialect  # avoid import cycle

    dialect = Dialect()
    connect_params = []
    for url in urls:
        _, params = dialect.create_connect_args(url)
        connect_params.append(_merge_connect_args(params, connect_args or {}))

    params_cycle = cycle(connect_params)

    def creator() -> Any:
        params = _copy_connect_params(next(params_cycle))
        return dialect.connect(**params)

    return create_engine(urls[0], creator=creator, **engine_kwargs)

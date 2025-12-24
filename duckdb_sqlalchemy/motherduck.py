from __future__ import annotations

import hashlib
from itertools import cycle
from typing import (
    Any,
    Dict,
    Mapping,
    MutableMapping,
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

MOTHERDUCK_PATH_QUERY_KEYS = {
    "user",
    "session_hint",
    "attach_mode",
    "access_mode",
    "dbinstance_inactivity_ttl",
    "motherduck_dbinstance_inactivity_ttl",
    "saas_mode",
    "cache_buster",
}

DIALECT_QUERY_KEYS = {"duckdb_sqlalchemy_pool", "pool"}


def _normalize_path_query_aliases(path_query: Dict[str, Any]) -> Dict[str, Any]:
    if "motherduck_dbinstance_inactivity_ttl" in path_query:
        if "dbinstance_inactivity_ttl" not in path_query:
            path_query["dbinstance_inactivity_ttl"] = path_query[
                "motherduck_dbinstance_inactivity_ttl"
            ]
        path_query.pop("motherduck_dbinstance_inactivity_ttl", None)
    return path_query


def split_url_query(query: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    path_query: Dict[str, Any] = {}
    url_config: Dict[str, Any] = {}
    for key, value in query.items():
        if key in DIALECT_QUERY_KEYS:
            continue
        if key in MOTHERDUCK_PATH_QUERY_KEYS:
            path_query[key] = value
        else:
            url_config[key] = value
    return _normalize_path_query_aliases(path_query), url_config


def extract_path_query_from_config(config: Dict[str, Any]) -> Dict[str, Any]:
    path_query: Dict[str, Any] = {}
    for key in list(config):
        if key in MOTHERDUCK_PATH_QUERY_KEYS:
            path_query[key] = config.pop(key)
    return _normalize_path_query_aliases(path_query)


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


def _stringify_query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _coerce_query_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_stringify_query_value(v) for v in value)
    return _stringify_query_value(value)


def _coerce_query_mapping(mapping: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in ((k, _coerce_query_value(v)) for k, v in mapping.items())
        if value is not None
    }


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

    path_params: Dict[str, Any] = dict(path_query or {})
    config_params: Dict[str, Any] = dict(query or {})

    for key, value in kwargs.items():
        if key in MOTHERDUCK_PATH_QUERY_KEYS:
            path_params[key] = value
        else:
            config_params[key] = value

    path_params = _normalize_path_query_aliases(_coerce_query_mapping(path_params))
    config_params = _coerce_query_mapping(config_params)

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
    base: MutableMapping[str, Any], extra: Mapping[str, Any]
) -> Dict[str, Any]:
    merged = dict(base)
    if not extra:
        return merged
    extra = dict(extra)
    if "config" in extra:
        merged["config"] = {**merged.get("config", {}), **extra.pop("config")}
    if "url_config" in extra:
        merged["url_config"] = {
            **merged.get("url_config", {}),
            **extra.pop("url_config"),
        }
    merged.update(extra)
    return merged


def _copy_connect_params(params: Mapping[str, Any]) -> Dict[str, Any]:
    copied = dict(params)
    if "config" in copied:
        copied["config"] = dict(copied["config"])
    if "url_config" in copied:
        copied["url_config"] = dict(copied["url_config"])
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

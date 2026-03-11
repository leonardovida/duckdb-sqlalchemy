from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence


def stringify_query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def coerce_query_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(stringify_query_value(v) for v in value)
    return stringify_query_value(value)


def coerce_query_mapping(mapping: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        key: value
        for key, value in ((k, coerce_query_value(v)) for k, v in mapping.items())
        if value is not None
    }

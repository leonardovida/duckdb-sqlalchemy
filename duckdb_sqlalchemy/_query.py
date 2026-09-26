from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Sequence


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
    coerced = {}
    for key, value in mapping.items():
        converted = coerce_query_value(value)
        if converted is not None:
            coerced[key] = converted
    return coerced


def merge_query_mappings(*mappings: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for mapping in mappings:
        if mapping:
            merged.update(coerce_query_mapping(mapping))
    return merged

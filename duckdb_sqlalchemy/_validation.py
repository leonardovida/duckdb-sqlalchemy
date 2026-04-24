import re
from typing import Iterable

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXTENSION_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _require_string(value: str, *, kind: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{kind} must be a string")
    return value


def _validate_pattern(value: str, *, kind: str, pattern: re.Pattern[str]) -> str:
    validated = _require_string(value, kind=kind)
    if not pattern.fullmatch(validated):
        raise ValueError(f"invalid {kind}: {validated!r}")
    return validated


def validate_identifier(value: str, *, kind: str = "identifier") -> str:
    return _validate_pattern(value, kind=kind, pattern=IDENTIFIER_RE)


def validate_dotted_identifier(value: str, *, kind: str = "identifier") -> str:
    validated = _require_string(value, kind=kind)
    parts = validated.split(".")
    if not parts or any(not part for part in parts):
        raise ValueError(f"invalid {kind}: {validated!r}")
    for part in parts:
        validate_identifier(part, kind=kind)
    return validated


def validate_extension_name(value: str) -> str:
    return _validate_pattern(value, kind="extension name", pattern=EXTENSION_RE)


def validate_identifier_list(
    values: Iterable[str], *, kind: str = "identifier"
) -> tuple[str, ...]:
    return tuple(validate_identifier(value, kind=kind) for value in values)

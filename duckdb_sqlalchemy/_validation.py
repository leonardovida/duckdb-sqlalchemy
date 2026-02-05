import re
from typing import Iterable

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
EXTENSION_RE = re.compile(r"^[A-Za-z0-9_]+$")


def validate_identifier(value: str, *, kind: str = "identifier") -> str:
    if not isinstance(value, str):
        raise ValueError(f"{kind} must be a string")
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"invalid {kind}: {value!r}")
    return value


def validate_dotted_identifier(value: str, *, kind: str = "identifier") -> str:
    if not isinstance(value, str):
        raise ValueError(f"{kind} must be a string")
    parts = value.split(".")
    if not parts or any(not part for part in parts):
        raise ValueError(f"invalid {kind}: {value!r}")
    for part in parts:
        validate_identifier(part, kind=kind)
    return value


def validate_extension_name(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("extension name must be a string")
    if not EXTENSION_RE.fullmatch(value):
        raise ValueError(f"invalid extension name: {value!r}")
    return value


def validate_identifier_list(
    values: Iterable[str], *, kind: str = "identifier"
) -> tuple[str, ...]:
    return tuple(validate_identifier(value, kind=kind) for value in values)

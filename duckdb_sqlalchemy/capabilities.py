from dataclasses import dataclass
from typing import Union

from packaging.version import Version


@dataclass(frozen=True)
class DuckDBCapabilities:
    version: Version
    supports_attach: bool
    supports_user_agent: bool
    supports_uhugeint: bool
    supports_varint: bool


def get_capabilities(version: Union[str, Version]) -> DuckDBCapabilities:
    resolved = version if isinstance(version, Version) else Version(version)
    return DuckDBCapabilities(
        version=resolved,
        supports_attach=resolved >= Version("0.7.0"),
        supports_user_agent=resolved >= Version("0.9.2"),
        supports_uhugeint=resolved >= Version("0.10.0"),
        supports_varint=resolved > Version("1.0.0"),
    )

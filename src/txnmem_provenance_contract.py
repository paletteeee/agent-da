"""Frozen public contracts shared by provenance evidence producers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


FORMAL_RUNNER_UID = 65532
FORMAL_RUNNER_GID = 65532


# Formal evidence intentionally accepts only versions reviewed with the
# implementation.  A generic dotted-number grammar is unsafe because shortened
# IPv4 addresses such as ``203.0.113`` are syntactically indistinguishable from
# semantic versions.
FORMAL_SERVICE_VERSION_ALLOWLIST: Mapping[str, frozenset[str]] = {
    "client": frozenset({"3.9.6", "3.11.9", "3.14.6"}),
    "qdrant": frozenset({"1.11.5", "1.15.4"}),
    "neo4j": frozenset({"5.22.0", "5.26.0"}),
    "toxiproxy": frozenset({"2.5.0", "2.9.0"}),
}

FORMAL_CONTAINER_IMAGE_MANIFEST_DIGESTS: Mapping[str, str] = {
    "qdrant": "7a4788934788a7ed9cbf6b8cc3ca1ee880dcd969cf8c6639dc7d0e446cbd4b47",
    "neo4j": "9317a2941a9641169aa2ea8470cdda184ff7a9ee1914b5429126d0db4828edd2",
    "toxiproxy": "927c797a2115a193ae3a527e5a36782b938419904ac6706ca0efa029ebea58cb",
}


def is_registered_service_version(role: str, value: Any) -> bool:
    """Return whether ``value`` is an exact source-reviewed version for role."""

    return isinstance(value, str) and value in FORMAL_SERVICE_VERSION_ALLOWLIST.get(
        role, frozenset()
    )

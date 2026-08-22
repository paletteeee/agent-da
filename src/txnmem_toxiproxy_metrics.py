"""Strict parsing and validation for Toxiproxy 2.5 proxy byte counters."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal, InvalidOperation
import hashlib
import re
from typing import Any

from txnmem_formal_io import canonical_json_bytes


PROXY_COUNTER_SCHEMA = "txnmem-provenance-proxy-counters-v1"
PROXY_COUNTER_DELTA_SCHEMA = "txnmem-provenance-proxy-counter-deltas-v1"
_PHASES = frozenset({"baseline_a", "baseline_b", "final"})
_LABEL_KEYS = frozenset({"direction", "proxy", "listener", "upstream"})
_METRIC_NAMES = frozenset(
    {
        "toxiproxy_proxy_received_bytes_total",
        "toxiproxy_proxy_sent_bytes_total",
    }
)
_MAX_EXACT_COUNTER = 2**53 - 1
_ROLES = ("qdrant", "neo4j")
_ROUTE_KEYS = frozenset(
    {"role", "proxy_name", "listen", "upstream", "enabled", "toxics_count"}
)
_COUNTER_ROUTE_KEYS = frozenset(
    {
        "role",
        "proxy_name",
        "listener",
        "upstream",
        "received_upstream_bytes",
        "sent_upstream_bytes",
        "received_downstream_bytes",
        "sent_downstream_bytes",
        "total_bytes",
    }
)
_COUNTER_COMPONENTS = (
    "received_upstream_bytes",
    "sent_upstream_bytes",
    "received_downstream_bytes",
    "sent_downstream_bytes",
)
_SAMPLE_RE = re.compile(r"^(?P<name>[A-Za-z_:][A-Za-z0-9_:]*)(?P<labels>\{.*\})\s+(?P<value>\S+)\s*$")
_NUMBER_RE = re.compile(r"^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$")
_LABEL_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_WILDCARD_LISTENER_RE = re.compile(r"(?:0\.0\.0\.0|\[::\]):([1-9][0-9]{0,4})$")


class ToxiproxyMetricsError(ValueError):
    """Raised when Toxiproxy metrics do not meet the formal closure."""


def _fail(message: str) -> None:
    raise ToxiproxyMetricsError(message)


def _is_counter(value: Any) -> bool:
    return type(value) is int and 0 <= value <= _MAX_EXACT_COUNTER


def _is_derived_total(value: Any) -> bool:
    return type(value) is int and value >= 0


def _parse_wildcard_listener(value: Any) -> tuple[str, int]:
    if not isinstance(value, str):
        _fail("Toxiproxy listener is invalid")
    match = _WILDCARD_LISTENER_RE.fullmatch(value)
    if match is None:
        _fail("Toxiproxy listener is not a wildcard address")
    port = int(match.group(1))
    if port > 65535:
        _fail("Toxiproxy listener port is invalid")
    return value, port


def _normalize_proxy_routes(proxy_routes: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(proxy_routes, (str, bytes)) or not isinstance(proxy_routes, Sequence):
        _fail("Toxiproxy proxy routes are invalid")
    if len(proxy_routes) != len(_ROLES):
        _fail("Toxiproxy proxy routes are incomplete")
    normalized: list[dict[str, Any]] = []
    for expected_role, route in zip(_ROLES, proxy_routes):
        if not isinstance(route, Mapping) or set(route) != _ROUTE_KEYS:
            _fail("Toxiproxy proxy route fields are invalid")
        role = route["role"]
        proxy_name = route["proxy_name"]
        upstream = route["upstream"]
        if role != expected_role:
            _fail("Toxiproxy proxy route order is invalid")
        if not isinstance(proxy_name, str) or not proxy_name:
            _fail("Toxiproxy proxy name is invalid")
        if not isinstance(upstream, str) or not upstream:
            _fail("Toxiproxy upstream is invalid")
        listener, _port = _parse_wildcard_listener(route["listen"])
        if route["enabled"] is not True or type(route["toxics_count"]) is not int or route["toxics_count"] != 0:
            _fail("Toxiproxy proxy route state is invalid")
        normalized.append(
            {
                "role": role,
                "proxy_name": proxy_name,
                "listener": listener,
                "upstream": upstream,
            }
        )
    if len({route["proxy_name"] for route in normalized}) != len(normalized):
        _fail("Toxiproxy proxy routes are duplicated")
    return normalized


def _parse_labels(text: str) -> dict[str, str]:
    if len(text) < 2 or text[0] != "{" or text[-1] != "}":
        _fail("Toxiproxy metric labels are invalid")
    cursor = 1
    end = len(text) - 1
    labels: dict[str, str] = {}
    while cursor < end:
        while cursor < end and text[cursor].isspace():
            cursor += 1
        match = _LABEL_NAME_RE.match(text, cursor)
        if match is None:
            _fail("Toxiproxy metric label name is invalid")
        key = match.group(0)
        cursor = match.end()
        if cursor >= end or text[cursor] != "=":
            _fail("Toxiproxy metric label assignment is invalid")
        cursor += 1
        if cursor >= end or text[cursor] != '"':
            _fail("Toxiproxy metric label value is invalid")
        cursor += 1
        value: list[str] = []
        while cursor < end:
            character = text[cursor]
            if character == '"':
                cursor += 1
                break
            if character == "\\":
                cursor += 1
                if cursor >= end:
                    _fail("Toxiproxy metric escape is invalid")
                escaped = text[cursor]
                if escaped == "\\":
                    value.append("\\")
                elif escaped == '"':
                    value.append('"')
                elif escaped == "n":
                    value.append("\n")
                else:
                    _fail("Toxiproxy metric escape is invalid")
            else:
                value.append(character)
            cursor += 1
        else:
            _fail("Toxiproxy metric label value is unterminated")
        if key in labels:
            _fail("Toxiproxy metric label is duplicated")
        labels[key] = "".join(value)
        while cursor < end and text[cursor].isspace():
            cursor += 1
        if cursor == end:
            break
        if text[cursor] != ",":
            _fail("Toxiproxy metric labels are invalid")
        cursor += 1
    if cursor != end or set(labels) != _LABEL_KEYS:
        _fail("Toxiproxy metric label closure is invalid")
    return labels


def _parse_counter(value: str) -> int:
    if _NUMBER_RE.fullmatch(value) is None:
        _fail("Toxiproxy byte counter is invalid")
    try:
        decimal = Decimal(value)
    except InvalidOperation as exc:
        raise ToxiproxyMetricsError("Toxiproxy byte counter is invalid") from exc
    if not decimal.is_finite() or decimal < 0 or decimal != decimal.to_integral_value():
        _fail("Toxiproxy byte counter is not an exact integer")
    counter = int(decimal)
    if counter > _MAX_EXACT_COUNTER:
        _fail("Toxiproxy byte counter exceeds exact range")
    return counter


def _component_name(metric_name: str, direction: str) -> str:
    if direction not in {"upstream", "downstream"}:
        _fail("Toxiproxy metric direction is invalid")
    if metric_name == "toxiproxy_proxy_received_bytes_total":
        prefix = "received"
    elif metric_name == "toxiproxy_proxy_sent_bytes_total":
        prefix = "sent"
    else:
        _fail("Toxiproxy proxy metric family is invalid")
    return f"{prefix}_{direction}_bytes"


def parse_toxiproxy_byte_counters(
    metrics: str, *, phase: str, proxy_routes: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    """Parse the exact eight official Toxiproxy 2.5 proxy byte series."""

    if not isinstance(metrics, str):
        _fail("Toxiproxy metrics must be text")
    if phase not in _PHASES:
        _fail("Toxiproxy counter phase is invalid")
    routes = _normalize_proxy_routes(proxy_routes)
    by_proxy = {route["proxy_name"]: route for route in routes}
    counters: dict[tuple[str, str], int] = {}
    for raw_line in metrics.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        name = line.split("{", 1)[0].split(None, 1)[0]
        if name.startswith("toxiproxy_proxy_") and name not in _METRIC_NAMES:
            _fail("Toxiproxy proxy metric family is invalid")
        if name not in _METRIC_NAMES:
            continue
        match = _SAMPLE_RE.fullmatch(line)
        if match is None or match.group("name") != name:
            _fail("Toxiproxy proxy metric sample is invalid")
        labels = _parse_labels(match.group("labels"))
        route = by_proxy.get(labels["proxy"])
        if route is None:
            _fail("Toxiproxy metric proxy is not registered")
        _metric_listener, metric_port = _parse_wildcard_listener(labels["listener"])
        _route_listener, route_port = _parse_wildcard_listener(route["listener"])
        if metric_port != route_port or labels["upstream"] != route["upstream"]:
            _fail("Toxiproxy metric route does not match registration")
        component = _component_name(name, labels["direction"])
        key = (route["role"], component)
        if key in counters:
            _fail("Toxiproxy proxy metric series is duplicated")
        counters[key] = _parse_counter(match.group("value"))
    normalized_rows = []
    for route in routes:
        values = {}
        for component in _COUNTER_COMPONENTS:
            key = (route["role"], component)
            if key not in counters:
                _fail("Toxiproxy proxy metric series is missing")
            values[component] = counters[key]
        total = sum(values.values())
        normalized_rows.append({**route, **values, "total_bytes": total})
    document = {
        "schema": PROXY_COUNTER_SCHEMA,
        "phase": phase,
        "routes": normalized_rows,
        "toxiproxy_total_bytes": sum(row["total_bytes"] for row in normalized_rows),
    }
    document["snapshot_sha256"] = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
    return document


def validate_proxy_counter_snapshot(
    value: Any, *, expected_phase: str | None = None
) -> dict[str, Any]:
    """Validate and return a detached, exact proxy-counter snapshot."""

    expected_keys = {
        "schema",
        "phase",
        "routes",
        "toxiproxy_total_bytes",
        "snapshot_sha256",
    }
    if not isinstance(value, Mapping) or set(value) != expected_keys:
        _fail("Toxiproxy counter snapshot fields are invalid")
    if value["schema"] != PROXY_COUNTER_SCHEMA or value["phase"] not in _PHASES:
        _fail("Toxiproxy counter snapshot header is invalid")
    if expected_phase is not None and value["phase"] != expected_phase:
        _fail("Toxiproxy counter snapshot phase is invalid")
    routes = value["routes"]
    if not isinstance(routes, list) or len(routes) != len(_ROLES):
        _fail("Toxiproxy counter snapshot routes are invalid")
    normalized_routes = []
    for expected_role, row in zip(_ROLES, routes):
        if not isinstance(row, Mapping) or set(row) != _COUNTER_ROUTE_KEYS:
            _fail("Toxiproxy counter route fields are invalid")
        if row["role"] != expected_role:
            _fail("Toxiproxy counter route order is invalid")
        if not isinstance(row["proxy_name"], str) or not row["proxy_name"]:
            _fail("Toxiproxy counter proxy name is invalid")
        if not isinstance(row["upstream"], str) or not row["upstream"]:
            _fail("Toxiproxy counter upstream is invalid")
        listener, _port = _parse_wildcard_listener(row["listener"])
        normalized = {
            "role": row["role"],
            "proxy_name": row["proxy_name"],
            "listener": listener,
            "upstream": row["upstream"],
        }
        if any(not _is_counter(row[component]) for component in _COUNTER_COMPONENTS):
            _fail("Toxiproxy counter component is invalid")
        normalized.update({component: row[component] for component in _COUNTER_COMPONENTS})
        expected_total = sum(normalized[component] for component in _COUNTER_COMPONENTS)
        if row["total_bytes"] != expected_total or not _is_derived_total(
            row["total_bytes"]
        ):
            _fail("Toxiproxy counter route total is invalid")
        normalized["total_bytes"] = row["total_bytes"]
        normalized_routes.append(normalized)
    if len({route["proxy_name"] for route in normalized_routes}) != len(normalized_routes):
        _fail("Toxiproxy counter proxy names are duplicated")
    expected_global_total = sum(row["total_bytes"] for row in normalized_routes)
    if value["toxiproxy_total_bytes"] != expected_global_total or not _is_derived_total(
        value["toxiproxy_total_bytes"]
    ):
        _fail("Toxiproxy counter total is invalid")
    document = {
        "schema": value["schema"],
        "phase": value["phase"],
        "routes": normalized_routes,
        "toxiproxy_total_bytes": value["toxiproxy_total_bytes"],
    }
    expected_hash = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
    if value["snapshot_sha256"] != expected_hash:
        _fail("Toxiproxy counter snapshot hash is invalid")
    document["snapshot_sha256"] = expected_hash
    return document


def proxy_counter_values(value: Mapping[str, Any]) -> tuple[int, int, int, int, int, int, int, int]:
    """Return eight absolute counter values in the formal role/component order."""

    snapshot = validate_proxy_counter_snapshot(value)
    return tuple(
        row[component]
        for row in snapshot["routes"]
        for component in _COUNTER_COMPONENTS
    )  # type: ignore[return-value]


def proxy_counter_payload_sha256(value: Mapping[str, Any]) -> str:
    """Hash the phase-independent normalized proxy counter payload."""

    snapshot = validate_proxy_counter_snapshot(value)
    payload = {
        "routes": snapshot["routes"],
        "toxiproxy_total_bytes": snapshot["toxiproxy_total_bytes"],
    }
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def derive_proxy_counter_deltas(
    baseline: Mapping[str, Any], final: Mapping[str, Any]
) -> dict[str, Any]:
    """Derive monotonic component deltas from validated snapshots."""

    normalized_baseline = validate_proxy_counter_snapshot(baseline)
    normalized_final = validate_proxy_counter_snapshot(final, expected_phase="final")
    if normalized_baseline["phase"] not in {"baseline_a", "baseline_b"}:
        _fail("Toxiproxy delta baseline phase is invalid")
    delta_rows = []
    for baseline_row, final_row in zip(
        normalized_baseline["routes"], normalized_final["routes"]
    ):
        identity_keys = ("role", "proxy_name", "listener", "upstream")
        if any(baseline_row[key] != final_row[key] for key in identity_keys):
            _fail("Toxiproxy delta routes do not match")
        values = {}
        for component in _COUNTER_COMPONENTS:
            delta = final_row[component] - baseline_row[component]
            if delta < 0:
                _fail("Toxiproxy counter decreased")
            values[component] = delta
        delta_rows.append(
            {
                **{key: final_row[key] for key in identity_keys},
                **values,
                "total_bytes": sum(values.values()),
            }
        )
    return {
        "schema": PROXY_COUNTER_DELTA_SCHEMA,
        "routes": delta_rows,
        "toxiproxy_total_bytes": sum(row["total_bytes"] for row in delta_rows),
    }

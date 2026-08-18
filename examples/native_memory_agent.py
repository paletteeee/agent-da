"""Minimal native connector example that records real memory calls.

This is a deterministic fixture for validating the connector boundary.  It is
not an LLM agent, a production storage connector, or a source of ground truth
for an external benchmark.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

# Allow ``python3 examples/native_memory_agent.py`` from the repository root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from txnmem_backend import InstrumentedMemoryBackend  # noqa: E402


def run_native_example() -> list[dict[str, Any]]:
    """Run actual backend calls and return the validated native event log."""

    backend = InstrumentedMemoryBackend()
    backend.write(
        "native_source",
        value="generic source",
        agent_id="agent_native",
        projection="native_connector_example",
    )
    backend.read(
        "native_source",
        agent_id="agent_native",
        projection="native_connector_example",
    )
    backend.derive(
        "native_derived",
        source_ids=["native_source"],
        value="generic derived",
        agent_id="agent_native",
        projection="native_connector_example",
    )
    backend.write(
        "native_written",
        value="generic output",
        agent_id="agent_native",
        projection="native_connector_example",
    )
    return backend.validated_events()


def _safe_event_metadata(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return metadata suitable for a terminal example without payload values."""

    fields = {"event_id", "kind", "agent_id", "step", "memory_id", "source_ids", "projection"}
    return [{key: value for key, value in event.items() if key in fields} for event in events]


if __name__ == "__main__":
    print(json.dumps(_safe_event_metadata(run_native_example()), ensure_ascii=False, indent=2))

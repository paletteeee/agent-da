"""Availability marker for the executable LoCoMo contextual-agent runtime.

The actual model loop is shared with :mod:`txnmem_real_agent`.  Keeping this
small importable module makes the external runtime boundary explicit: a
LoCoMo native run is available only in an environment that deliberately
installs this adapter and exposes the TxnMem source tree.
"""

from __future__ import annotations

RUNTIME_NAME = "txnmem-locomo-contextual-agent"
RUNTIME_VERSION = "1"

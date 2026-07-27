"""Public facade for relational boundary audit routines.

The concrete ratchet implementation still lives in the repositories package
while the boundary gate is being strangled. Application-layer gates import this
facade instead of reaching into the outbound package directly.
"""

from __future__ import annotations

import importlib
from typing import Any

_RELATIONAL_BOUNDARY_MODULE = "okto_pulse.core.repositories.relational_boundary_gate"


def _module() -> Any:
    return importlib.import_module(_RELATIONAL_BOUNDARY_MODULE)


def run_relational_ratchet_gate(*args: Any, **kwargs: Any) -> Any:
    return _module().run_relational_ratchet_gate(*args, **kwargs)


__all__ = ["run_relational_ratchet_gate"]

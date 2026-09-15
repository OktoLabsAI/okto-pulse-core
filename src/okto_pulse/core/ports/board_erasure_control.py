"""Public cross-edition error contract for board-erasure control.

The erasure workflow and its lock implementation remain Core internals. An
edition that exposes the workflow over HTTP only needs to distinguish a safe,
retryable lock contention from an erasure failure, so it imports this stable
contract rather than the private governance module.
"""

from okto_pulse.core.kg.governance import BoardErasureLockContention

__all__ = ["BoardErasureLockContention"]

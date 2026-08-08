"""Edition-owned read and isolated-copy execution ports for terminal debt.

Each operational debt domain has a separate reader contract.  The neutral
manifest type is shared evidence, not a command bus: readers cannot rearm or
mutate another domain and this module intentionally exposes no runtime-global
registry.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, runtime_checkable

from okto_pulse.core.domain.terminal_debt import (
    TerminalDebtDomain,
    TerminalDebtExecutionResult,
    TerminalDebtManifest,
    TerminalDebtRecoveryPlan,
)


@runtime_checkable
class ConsolidationTerminalDebtReader(Protocol):
    async def list_consolidation_terminal_debt(
        self,
        *,
        scope_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> TerminalDebtManifest: ...


@runtime_checkable
class GlobalOutboxTerminalDebtReader(Protocol):
    async def list_global_outbox_terminal_debt(
        self,
        *,
        scope_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> TerminalDebtManifest: ...


@runtime_checkable
class CanonicalDebtTerminalReader(Protocol):
    async def list_canonical_terminal_debt(
        self,
        *,
        scope_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> TerminalDebtManifest: ...


@runtime_checkable
class PolicyProjectionTerminalDebtReader(Protocol):
    async def list_policy_projection_terminal_debt(
        self,
        *,
        scope_id: str,
        limit: int = 100,
        offset: int = 0,
    ) -> TerminalDebtManifest: ...


@runtime_checkable
class TerminalDebtCopyExecutor(Protocol):
    """A caller-supplied command capability scoped to one isolated copy."""

    @property
    def domain(self) -> TerminalDebtDomain: ...

    @property
    def target_fingerprint(self) -> str:
        """Mechanism-derived identity of the copy this executor can mutate."""

        ...

    async def execute(
        self,
        plan: TerminalDebtRecoveryPlan,
    ) -> Sequence[TerminalDebtExecutionResult]: ...


__all__ = [
    "CanonicalDebtTerminalReader",
    "ConsolidationTerminalDebtReader",
    "GlobalOutboxTerminalDebtReader",
    "PolicyProjectionTerminalDebtReader",
    "TerminalDebtCopyExecutor",
]

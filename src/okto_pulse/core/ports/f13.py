"""Public contracts used to prove edition-adapter provenance."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Protocol, runtime_checkable

from okto_pulse.core.domain.realm import RealmScope


@dataclass(frozen=True, slots=True)
class ApplicationScope:
    """Minimum tenant/realm identity shared by every edition operation."""

    realm: RealmScope

    @classmethod
    def local(cls) -> "ApplicationScope":
        return cls(RealmScope.local())


@dataclass(frozen=True, slots=True)
class EditionOutcome:
    outcome: str
    data: object | None = None


@runtime_checkable
class EditionPort(Protocol):
    """Edition-neutral operation boundary used by composition adapters."""

    async def execute(
        self,
        scope: ApplicationScope,
        operation: str,
        payload: Mapping[str, object],
    ) -> EditionOutcome: ...


@dataclass(frozen=True, slots=True)
class AdapterProvenanceRegistration:
    """Executable claim that a local implementation satisfies a public port."""

    adapter_key: str
    owner: str
    implementation_module: str
    implementation_symbol: str
    port_module: str
    port_symbol: str
    dependencies: tuple[str, ...]
    contract_test: str

    @property
    def implementation_target(self) -> str:
        return f"{self.implementation_module}.{self.implementation_symbol}"

    @property
    def port_target(self) -> str:
        return f"{self.port_module}.{self.port_symbol}"


@dataclass(frozen=True, slots=True)
class AdapterBridgeLedgerEntry:
    """Temporary bridge metadata. The terminal F13 ledger is empty."""

    file_path: str
    scope: str
    bridge_kind: str
    target: str
    owner: str
    target_port: str
    removal_path: str
    withdrawal_criterion: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (self.file_path, self.scope, self.bridge_kind, self.target)


__all__ = [
    "ApplicationScope",
    "AdapterBridgeLedgerEntry",
    "AdapterProvenanceRegistration",
    "EditionOutcome",
    "EditionPort",
]

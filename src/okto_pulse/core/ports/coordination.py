"""Coordination ports for runtime leases, write locks and queue claims.

These contracts keep replica coordination and local-first locking outside the
core application logic. Core code may depend on these Protocols and on the
registry below, but concrete implementations belong to an edition adapter.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, reset_runtime_values, resolve_runtime_value

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


class CoordinationProviderMissing(RuntimeError):
    """Raised when a required coordination provider was not registered."""

    code = "coordination_provider_missing"

    def __init__(self, provider_key: str) -> None:
        self.provider_key = provider_key
        super().__init__(
            f"coordination_provider_missing: required provider not supplied: "
            f"{provider_key}"
        )


@dataclass(frozen=True, slots=True)
class LeaseHandle:
    """Opaque lease ownership token returned by a LeaseProvider."""

    resource: str
    owner_token: str
    fencing_token: str
    expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class WriteLockHandle:
    """Opaque write-lock ownership token returned by a WriteLockPort."""

    board_id: str
    artifact_id: str
    owner_token: str
    fencing_token: str


@runtime_checkable
class LeaseProvider(Protocol):
    """Replica lease / leader-election contract."""

    async def try_acquire(
        self,
        resource: str,
        *,
        ttl_seconds: int | None = None,
        owner_token: str | None = None,
    ) -> LeaseHandle | None:
        ...

    async def release(self, handle: LeaseHandle) -> None:
        ...

    def is_held(self, resource: str) -> bool:
        ...


@runtime_checkable
class WriteLockPort(Protocol):
    """Board/artifact write-lock contract."""

    async def acquire(
        self,
        board_id: str,
        artifact_id: str,
        *,
        owner_token: str | None = None,
    ) -> WriteLockHandle:
        ...

    async def release(self, handle: WriteLockHandle) -> None:
        ...

    def acquire_sync(
        self,
        board_id: str,
        artifact_id: str,
        *,
        owner_token: str | None = None,
    ) -> WriteLockHandle:
        ...

    def release_sync(self, handle: WriteLockHandle) -> None:
        ...

    def is_locked(self, board_id: str, artifact_id: str) -> bool:
        ...

    def reset_for_tests(self) -> None:
        ...


@runtime_checkable
class ClaimRepository(Protocol):
    """Queue/outbox claim contract."""

    async def claim_global_outbox(
        self,
        session: Any,
        *,
        limit: int,
    ) -> Sequence[Any]:
        ...

    async def claim_domain_event_executions(
        self,
        session: Any,
        *,
        limit: int,
        now: datetime,
    ) -> Sequence[tuple[str, str]]:
        ...

    async def claim_consolidation_queue(
        self,
        session: Any,
        *,
        board_id: str | None,
        limit: int,
    ) -> Sequence[Any]:
        ...


@runtime_checkable
class RuntimeSettingsProvider(Protocol):
    """Read-only runtime-settings boundary for workers that only need values."""

    async def read_runtime_settings(self, scope: str = "global") -> Mapping[str, Any]:
        ...


@runtime_checkable
class ConfigValidationPort(Protocol):
    """Edition-owned runtime config validation boundary."""

    def validate_runtime_settings(self, values: Mapping[str, Any]) -> None:
        ...


_LEASE_KEY = "ports.coordination.lease"
_WRITE_LOCK_KEY = "ports.coordination.write_lock"
_CLAIM_KEY = "ports.coordination.claim_repository"
_SETTINGS_KEY = "ports.coordination.runtime_settings"
_VALIDATION_KEY = "ports.coordination.config_validation"


def register_coordination_providers(
    *,
    lease_provider: LeaseProvider | None = None,
    write_lock_port: WriteLockPort | None = None,
    claim_repository: ClaimRepository | None = None,
    runtime_settings_provider: RuntimeSettingsProvider | None = None,
    config_validation_port: ConfigValidationPort | None = None,
) -> None:
    """Register edition-owned coordination providers."""

    if lease_provider is not None:
        register_runtime_value(_LEASE_KEY, lease_provider)
    if write_lock_port is not None:
        register_runtime_value(_WRITE_LOCK_KEY, write_lock_port)
    if claim_repository is not None:
        register_runtime_value(_CLAIM_KEY, claim_repository)
    if runtime_settings_provider is not None:
        register_runtime_value(_SETTINGS_KEY, runtime_settings_provider)
    if config_validation_port is not None:
        register_runtime_value(_VALIDATION_KEY, config_validation_port)


def get_lease_provider() -> LeaseProvider:
    provider = resolve_runtime_value(_LEASE_KEY)
    if provider is None:
        raise CoordinationProviderMissing("lease_provider")
    return provider


def get_write_lock_port() -> WriteLockPort:
    port = resolve_runtime_value(_WRITE_LOCK_KEY)
    if port is None:
        raise CoordinationProviderMissing("write_lock_port")
    return port


def get_claim_repository() -> ClaimRepository:
    repository = resolve_runtime_value(_CLAIM_KEY)
    if repository is None:
        raise CoordinationProviderMissing("claim_repository")
    return repository


def get_runtime_settings_provider() -> RuntimeSettingsProvider:
    provider = resolve_runtime_value(_SETTINGS_KEY)
    if provider is None:
        raise CoordinationProviderMissing("runtime_settings_provider")
    return provider


def get_config_validation_port() -> ConfigValidationPort:
    port = resolve_runtime_value(_VALIDATION_KEY)
    if port is None:
        raise CoordinationProviderMissing("config_validation_port")
    return port


def reset_coordination_providers_for_tests() -> None:
    reset_runtime_values(
        _LEASE_KEY, _WRITE_LOCK_KEY, _CLAIM_KEY, _SETTINGS_KEY, _VALIDATION_KEY
    )


__all__ = [
    "ClaimRepository",
    "ConfigValidationPort",
    "CoordinationProviderMissing",
    "LeaseHandle",
    "LeaseProvider",
    "RuntimeSettingsProvider",
    "WriteLockHandle",
    "WriteLockPort",
    "get_claim_repository",
    "get_config_validation_port",
    "get_lease_provider",
    "get_runtime_settings_provider",
    "get_write_lock_port",
    "register_coordination_providers",
    "reset_coordination_providers_for_tests",
]

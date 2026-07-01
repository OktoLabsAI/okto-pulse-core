"""RuntimeComposition + PulseRuntime — explicit composition root (spec #03).

Card 2fb92914 / tr_1436632a / tr_b6ac52cf / tr_f9d2f84f. ``RuntimeComposition``
is the immutable wiring of the providers the composition root owns; ``create_app``
consumes it and, under ``strict_runtime=True``, fails fast with
``runtime_provider_missing`` when a required owned provider is absent — never a
silent fallback to a concrete default.

Ownership (Remediacao #03 v2): the providers OWNED/wired by #03 in this phase are
settings, auth, storage, session_factory, event_bus, scheduler_control,
telemetry, lifecycle_hooks and mcp_session_factory. R01B REPLAN-IMP1 adds the
optional ``uow_factory`` (the edition-owned relational UnitOfWorkFactory). The KG registry
(``kg_registry``) is transitional debt deferred to #05 and is NOT a required
provider here. ``event_bus`` is a #03-owned provider key supplied by the edition
composition root; core code must not instantiate a concrete relational event-bus
adapter.

Providers are duck-typed (held as opaque objects) so this module imports no
concrete adapter; it is composition-layer wiring, not application logic.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Protocol, Sequence, runtime_checkable

#: Providers owned by #03 that MUST be present under ``strict_runtime=True``.
REQUIRED_OWNED_PROVIDERS: tuple[str, ...] = (
    "settings_provider",
    "auth_provider",
    "storage_provider",
    "session_factory",
    "event_bus",
)

#: Optional owned providers (may be ``None`` even in strict mode).
OPTIONAL_OWNED_PROVIDERS: tuple[str, ...] = (
    "scheduler_control",
    "telemetry",
    "mcp_session_factory",
    "uow_factory",
    "lifecycle_hooks",
)

#: Providers that must be present when an environment explicitly opts into
#: strict runtime-shell mode. ``lifecycle_hooks`` stays optional for the
#: historical default path, but empty hooks are not a valid strict runtime.
STRICT_RUNTIME_REQUIRED_PROVIDERS: tuple[str, ...] = (
    *REQUIRED_OWNED_PROVIDERS,
    "lifecycle_hooks",
)

#: Boundary deferred to spec #05 — never a ``runtime_provider_missing`` here.
DEFERRED_TO_05_PROVIDERS: tuple[str, ...] = ("kg_registry",)

ALL_PROVIDER_KEYS: tuple[str, ...] = (
    *REQUIRED_OWNED_PROVIDERS,
    *OPTIONAL_OWNED_PROVIDERS,
    *DEFERRED_TO_05_PROVIDERS,
)


class RuntimeProviderMissing(Exception):
    """Raised when a required owned provider is absent (runtime_provider_missing)."""

    code = "runtime_provider_missing"

    def __init__(self, provider_key: str, *, missing: Sequence[str] | None = None):
        self.provider_key = provider_key
        self.missing = list(missing) if missing is not None else [provider_key]
        super().__init__(
            f"runtime_provider_missing: required provider(s) not supplied: "
            f"{', '.join(self.missing)}"
        )


class InvalidRuntimeComposition(Exception):
    """Raised when the composition object is structurally incomplete."""

    code = "invalid_runtime_composition"


@runtime_checkable
class LifecycleHook(Protocol):
    """A startup/shutdown hook driven by the runtime, not by the app layer."""

    async def on_startup(self) -> None:
        ...

    async def on_shutdown(self) -> None:
        ...


@dataclass(frozen=True)
class RuntimeComposition:
    """Immutable wiring of the providers the #03 composition root owns."""

    settings_provider: Any
    auth_provider: Any
    storage_provider: Any
    session_factory: Any
    event_bus: Any
    kg_registry: Any = None  # deferred_to_05 boundary
    scheduler_control: Any = None
    telemetry: Any = None
    mcp_session_factory: Any = None
    # R01B REPLAN-IMP1: the relational UnitOfWorkFactory the edition composition
    # root owns. Optional/duck-typed (no concrete adapter import). Supplied by the
    # Community composition (build_community_unit_of_work_factory); re-pointing the
    # REST/MCP consumers to it is IMP2 (FR3).
    uow_factory: Any = None
    lifecycle_hooks: Sequence[LifecycleHook] = field(default_factory=tuple)

    def missing_required(self, *, require_lifecycle_hooks: bool = False) -> list[str]:
        """Required owned providers that are absent (``None``)."""
        missing = [
            key for key in REQUIRED_OWNED_PROVIDERS if getattr(self, key, None) is None
        ]
        if require_lifecycle_hooks and not self.lifecycle_hooks:
            missing.append("lifecycle_hooks")
        return missing

    def provider_keys(self) -> list[str]:
        """All provider keys whose value is currently supplied (non-None)."""
        return [key for key in ALL_PROVIDER_KEYS if getattr(self, key, None) is not None]

    def require_provider(self, key: str) -> Any:
        """Return a supplied provider or raise ``runtime_provider_missing``."""
        if key not in ALL_PROVIDER_KEYS:
            raise InvalidRuntimeComposition(f"unknown provider key: {key}")
        value = getattr(self, key, None)
        if value is None:
            raise RuntimeProviderMissing(key)
        return value


def validate_required_providers(
    composition: RuntimeComposition, *, require_lifecycle_hooks: bool = False
) -> None:
    """Raise ``RuntimeProviderMissing`` if any required owned provider is absent."""
    missing = composition.missing_required(
        require_lifecycle_hooks=require_lifecycle_hooks
    )
    if missing:
        raise RuntimeProviderMissing(missing[0], missing=missing)


@runtime_checkable
class PulseRuntime(Protocol):
    """Composition-driven runtime lifecycle (tr_b6ac52cf)."""

    async def startup(self) -> None:
        ...

    async def shutdown(self) -> None:
        ...

    def app_lifespan(self) -> Any:  # AsyncContextManager[None]
        ...

    def require_provider(self, key: str) -> Any:
        ...


class CompositionRuntime:
    """Default :class:`PulseRuntime` driven entirely by a RuntimeComposition.

    ``startup``/``shutdown`` run the composition's lifecycle hooks (shutdown in
    reverse); ``require_provider`` delegates to the composition. No concrete
    provider is instantiated here — that is the edition adapter's job.
    """

    def __init__(self, composition: RuntimeComposition, *, strict_runtime: bool = True):
        if not isinstance(composition, RuntimeComposition):
            raise InvalidRuntimeComposition("composition must be a RuntimeComposition")
        if strict_runtime:
            validate_required_providers(
                composition, require_lifecycle_hooks=True
            )
        self._composition = composition
        self._started = False

    @property
    def composition(self) -> RuntimeComposition:
        return self._composition

    async def startup(self) -> None:
        for hook in self._composition.lifecycle_hooks:
            await hook.on_startup()
        self._started = True

    async def shutdown(self) -> None:
        for hook in reversed(list(self._composition.lifecycle_hooks)):
            await hook.on_shutdown()
        self._started = False

    @asynccontextmanager
    async def app_lifespan(self) -> AsyncIterator[None]:
        await self.startup()
        try:
            yield None
        finally:
            await self.shutdown()

    def require_provider(self, key: str) -> Any:
        return self._composition.require_provider(key)

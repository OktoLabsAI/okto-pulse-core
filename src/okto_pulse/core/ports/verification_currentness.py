"""Edition seams for reserved issuance and transactional verification authority."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from okto_pulse.core.domain.verification_currentness import (
    AuthenticatedVerificationResult,
    VerificationReservation,
    VerificationScope,
)
from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)


@dataclass(frozen=True, slots=True)
class VerificationReservationRequest:
    scope: VerificationScope
    scenario_sha256: str
    verification_method: str
    actor_id: str
    actor_kind: str
    expected_spec_version: int


class VerificationReservationPort(Protocol):
    async def reserve(self, request: VerificationReservationRequest) -> VerificationReservation:
        """Recheck authorized scope and persist a unique context in a short UoW.

        Return only after releasing the reservation write transaction. Do not
        execute a runner, issue a receipt, publish a head or grant Delivery proof.
        The adapter must compare actual generation/semantic/version facts and
        reject a deleted/recreated or changed scenario rather than repairing it.
        """
        ...


@dataclass(frozen=True, slots=True)
class AcceptedVerificationResult:
    accepted_result_id: str
    result: AuthenticatedVerificationResult
    current: bool
    evidence: Mapping[str, object] | None = None
    evidence_status: str | None = None
    origin: str = "native"


@dataclass(frozen=True, slots=True)
class VerificationPublicationResult:
    acceptance: AcceptedVerificationResult
    replayed: bool
    replace_operational_summary: bool


@dataclass(frozen=True, slots=True)
class VerificationCurrentnessRead:
    results: tuple[AcceptedVerificationResult, ...]
    complete: bool
    source_revision: int
    reason_codes: tuple[str, ...] = ()


class VerificationCurrentnessStore(Protocol):
    async def replay_legacy(
        self, *, scope: VerificationScope, evidence: Mapping[str, object], evidence_status: str,
        actor_id: str, expected_spec_version: int,
    ) -> VerificationPublicationResult:
        """Return only an exact, authenticated, still-current migrated seed.

        Never append authority or revive a stale seed. The caller releases the
        transaction fence without changing summary or audit. Historical actor
        kind remains unknown; the actual caller must match the signed actor ID.
        """
        ...

    async def publish(
        self, result: AuthenticatedVerificationResult, *, evidence: Mapping[str, object],
        evidence_status: str, expected_spec_version: int,
    ) -> VerificationPublicationResult:
        """Stage acceptance and its scoped basis head in the owner's current UoW.

        Revalidate reservation, generation, authentication and exact replay under
        the write fence. Enforce immutable/unique receipt and reservation IDs;
        never choose an older passing result. Do not commit: the native owner
        commits these rows with scenario summary and actual actor audit, or rolls
        all of them back. An accepted failure replaces only its exact full basis.
        """
        ...

    async def read_current(
        self, *, board_id: str, spec_id: str, spec_edition: int,
        scenario_ids: tuple[str, ...], accepted_result_ids: tuple[str, ...] = (),
    ) -> VerificationCurrentnessRead:
        """Read authenticated scoped facts with indexed joins, never all history.

        Reauthorize before child lookup; retain 5000-scenario/20000-expanded-fact
        bounds. Limit, missing authority or authentication failures are explicit
        incomplete responses, never empty-success or repaired persistent state.
        Requested stale acceptance identities remain distinguishable from absent
        identities. Currentness alone does not prove a Delivery association.
        """
        ...


_RESERVATION_KEY = "ports.verification_currentness.reservation"
_STORE_FACTORY_KEY = "ports.verification_currentness.store_factory"


def register_verification_currentness_store_factory(
    factory: Callable[[Any], VerificationCurrentnessStore],
) -> None:
    register_runtime_value(_STORE_FACTORY_KEY, factory)


def require_verification_currentness_store(session: Any) -> VerificationCurrentnessStore:
    from okto_pulse.core.domain.verification_currentness import VerificationCurrentnessError

    factory = resolve_runtime_value(_STORE_FACTORY_KEY)
    if factory is None:
        raise VerificationCurrentnessError("verification_currentness_unavailable")
    return factory(session)


def register_verification_reservation_port(port: VerificationReservationPort) -> None:
    register_runtime_value(_RESERVATION_KEY, port)


def resolve_verification_reservation_port() -> VerificationReservationPort | None:
    return resolve_runtime_value(_RESERVATION_KEY)  # type: ignore[return-value]


def reset_verification_reservation_port_for_tests() -> None:
    reset_runtime_values(_RESERVATION_KEY)


def reset_verification_currentness_store_factory_for_tests() -> None:
    reset_runtime_values(_STORE_FACTORY_KEY)

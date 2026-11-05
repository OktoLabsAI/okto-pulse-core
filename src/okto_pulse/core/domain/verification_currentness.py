"""Exact-basis verification identity and publication policy.

These values describe native reservations and authenticated observations. Parsing
them does not authenticate evidence or authorize a write. The scoped application
owner and edition adapter must supply authenticated facts and commit the planned
head, operational summary, immutable acceptance and audit in one transaction.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

from okto_pulse.core.domain.test_scenarios import VerificationMethod

MAX_VERIFICATION_COUNTER = (1 << 63) - 1
MAX_VERIFICATION_SOURCES = 20
MAX_RUNNER_SOURCES = 2
MAX_VERIFICATION_SCENARIOS = 5000
MAX_VERIFICATION_FACTS = 20000

Reference = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=2048, pattern=r"\S")
]
Digest = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]
Counter = Annotated[int, Field(strict=True, ge=1, le=MAX_VERIFICATION_COUNTER)]
VerificationOutcome = Literal["passed", "failed", "inconclusive", "aborted", "unavailable"]


class VerificationCurrentnessError(ValueError):
    """Bounded domain error; no evidence, credential or caller payload in text."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _ImmutableValue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class VerificationScope(_ImmutableValue):
    board_id: Reference
    spec_id: Reference
    spec_edition: Counter
    scenario_id: Reference


class VerificationContext(VerificationScope):
    """Server reservation identity included in the authenticated evidence hash."""

    contract_version: Literal["verification-context/v1"] = "verification-context/v1"
    reservation_id: Reference
    scenario_sha256: Digest
    verification_method: VerificationMethod
    generation: Counter
    ordinal: Counter

    @property
    def scope(self) -> VerificationScope:
        return VerificationScope(
            board_id=self.board_id,
            spec_id=self.spec_id,
            spec_edition=self.spec_edition,
            scenario_id=self.scenario_id,
        )


class VerificationSourceVersion(_ImmutableValue):
    source_ref: Reference
    revision: Reference


class VerificationBasis(_ImmutableValue):
    """The complete source set, including companions, never a required subset."""

    sources: tuple[VerificationSourceVersion, ...] = Field(
        min_length=1, max_length=MAX_VERIFICATION_SOURCES
    )

    @model_validator(mode="after")
    def unique_sorted_sources(self) -> VerificationBasis:
        refs = [source.source_ref for source in self.sources]
        # Even identical duplicates are invalid: silently deduplicating would
        # erase malformed producer data and allow different envelopes to merge.
        if len(refs) != len(set(refs)):
            raise VerificationCurrentnessError("verification_basis_invalid")
        ordered = tuple(sorted(self.sources, key=lambda source: (source.source_ref, source.revision)))
        object.__setattr__(self, "sources", ordered)
        return self

    @property
    def digest(self) -> str:
        payload = [[source.source_ref, source.revision] for source in self.sources]
        canonical = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def require_runner_shape(self) -> None:
        """The existing runner permits one primary and at most one companion."""
        if len(self.sources) > MAX_RUNNER_SOURCES:
            raise VerificationCurrentnessError("verification_basis_invalid")


class VerificationReservation(_ImmutableValue):
    context: VerificationContext
    actor_id: Reference
    actor_kind: Reference


class AuthenticatedVerificationResult(_ImmutableValue):
    """Only construct from the native verifier's authenticated receipt payload.

    This type intentionally has no ``verified=True`` input or receipt-file IO.
    Its digest covers the unsigned evidence, including the exact reserved
    context. Acceptance IDs are assigned by persistence, not by the caller.
    """

    context: VerificationContext
    actor_id: Reference
    actor_kind: Reference
    receipt_id: Reference
    evidence_sha256: Digest
    basis: VerificationBasis
    outcome: VerificationOutcome


@dataclass(frozen=True, slots=True)
class VerificationPublicationPlan:
    append_acceptance: bool
    replace_basis_head: bool
    replace_operational_summary: bool
    replayed: bool
    current: bool


def canonical_scenario_sha256(value: str) -> str:
    """Translate the existing semantic digest to the context's raw SHA-256.

    Evidence V2 retains its existing ``sha256:`` spelling. The versioned context
    contract uses 64 hexadecimal characters; accept only these two exact forms
    at the native boundary, never strip arbitrary prefixes or normalize case.
    """
    if not isinstance(value, str):
        raise VerificationCurrentnessError("verification_context_invalid")
    digest = value.removeprefix("sha256:")
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise VerificationCurrentnessError("verification_context_invalid")
    return digest


def next_verification_counter(value: int) -> int:
    """Allocate without allowing bool/string coercion, overflow or wraparound."""
    if type(value) is not int or value < 0 or value > MAX_VERIFICATION_COUNTER:
        raise VerificationCurrentnessError("verification_context_invalid")
    if value == MAX_VERIFICATION_COUNTER:
        raise VerificationCurrentnessError("verification_counter_exhausted")
    return value + 1


def require_current_verification_context(
    context: VerificationContext,
    *,
    scope: VerificationScope,
    generation: int,
    scenario_sha256: str,
    verification_method: str,
) -> None:
    """Check the current persisted generation, even on an exact receipt replay."""
    if (
        context.scope != scope
        or type(generation) is not int
        or context.generation != generation
        or context.scenario_sha256 != scenario_sha256
        or context.verification_method != verification_method
    ):
        raise VerificationCurrentnessError("verification_context_stale")


def plan_verification_publication(
    result: AuthenticatedVerificationResult,
    *,
    reservation: VerificationReservation,
    scope: VerificationScope,
    generation: int,
    scenario_sha256: str,
    verification_method: str,
    basis_head: AuthenticatedVerificationResult | None,
    prior_acceptance: AuthenticatedVerificationResult | None,
    operational_summary_ordinal: int,
) -> VerificationPublicationPlan:
    """Plan against facts read under the native publication transaction.

    ``prior_acceptance`` is the immutable acceptance for this reservation, not
    the most recent pass. Persistence must enforce one acceptance per reservation
    and receipt, and one current head per scope/generation/full basis. No read
    repair or automatic execution belongs here. A same-generation exact replay
    can return its original history after supersession, but gains no current
    credit; a reset must reject even that replay.
    """
    require_current_verification_context(
        result.context, scope=scope, generation=generation,
        scenario_sha256=scenario_sha256, verification_method=verification_method,
    )
    if (
        result.context != reservation.context
        or result.actor_id != reservation.actor_id
        or result.actor_kind != reservation.actor_kind
    ):
        raise VerificationCurrentnessError("verification_reservation_invalid")
    if (
        type(operational_summary_ordinal) is not int
        or not 0 <= operational_summary_ordinal <= MAX_VERIFICATION_COUNTER
    ):
        raise VerificationCurrentnessError("verification_currentness_unavailable")
    if basis_head is not None and (
        basis_head.context.scope != scope
        or basis_head.context.generation != generation
        or basis_head.context.scenario_sha256 != scenario_sha256
        or basis_head.context.verification_method != verification_method
        or basis_head.basis != result.basis
    ):
        raise VerificationCurrentnessError("verification_currentness_unavailable")
    if prior_acceptance is not None:
        if prior_acceptance != result:
            raise VerificationCurrentnessError("verification_currentness_conflict")
        return VerificationPublicationPlan(False, False, False, True, basis_head == result)
    if basis_head is not None:
        if result.context.ordinal < basis_head.context.ordinal:
            raise VerificationCurrentnessError("verification_result_superseded")
        if result.context.ordinal == basis_head.context.ordinal:
            raise VerificationCurrentnessError("verification_currentness_conflict")
    return VerificationPublicationPlan(
        True, True, result.context.ordinal > operational_summary_ordinal, False, True
    )

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
MAX_VERIFICATION_HISTORY_PAGE = 100

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


class VerificationHistoryCursor(_ImmutableValue):
    """Internal keyset, never authorization or an unsigned transport token.

    A public adapter must reauthorize the explicit requested scope on every
    page and authenticate an opaque token bound to actor, realm and this value.
    The store also fences actual edition/revision and verifies the exact anchor.
    """

    scope: VerificationScope
    spec_edition_at_read: Counter
    source_revision: Counter
    page_size: Annotated[int, Field(strict=True, ge=1, le=MAX_VERIFICATION_HISTORY_PAGE)]
    before_generation: Counter
    before_ordinal: Counter
    before_acceptance_id: Reference

    @model_validator(mode="after")
    def observed_edition_contains_scope(self):
        if self.spec_edition_at_read < self.scope.spec_edition:
            raise VerificationCurrentnessError("verification_history_cursor_invalid")
        return self


class VerificationHistoryQuery(_ImmutableValue):
    scope: VerificationScope
    limit: Annotated[int, Field(strict=True, ge=1, le=MAX_VERIFICATION_HISTORY_PAGE)] = 50
    cursor: VerificationHistoryCursor | None = None

    @model_validator(mode="after")
    def cursor_belongs_to_query(self):
        if self.cursor is not None and (self.cursor.scope != self.scope or self.cursor.page_size != self.limit):
            raise VerificationCurrentnessError("verification_history_cursor_invalid")
        return self


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


def result_from_authenticated_evidence(evidence, *, actor_id: str, actor_kind: str):
    """Normalize an already authenticated envelope; this does not verify a receipt.

    Native publication must authenticate the original envelope first. Preserve
    every source and hash the exact unsigned payload, rather than a model dump
    which could introduce unsigned defaults.
    """
    try:
        context = VerificationContext.model_validate(evidence["verification_context"])
    except (TypeError, ValueError, KeyError) as exc:
        raise VerificationCurrentnessError("verification_result_invalid") from exc
    return _normalize_authenticated_evidence(evidence, context=context, actor_id=actor_id, actor_kind=actor_kind)


def result_from_authenticated_legacy_evidence(evidence, *, context: VerificationContext, actor_id: str):
    """Normalize an authenticated current-only migration seed, not fresh issuance.

    The converter supplies a snapshot-bound context. It never inserts that
    unsigned context into the original receipt payload. Historical receipts do
    not authenticate actor kind, so preserve that absence explicitly as unknown.
    Parsing here grants neither migration provenance nor replay authority.
    """
    if not isinstance(evidence, dict) or evidence.get("verification_context") is not None:
        raise VerificationCurrentnessError("verification_legacy_seed_invalid")
    result = _normalize_authenticated_evidence(evidence, context=context, actor_id=actor_id, actor_kind="unknown")
    if result.outcome not in {"passed", "failed"}:
        raise VerificationCurrentnessError("verification_legacy_seed_invalid")
    return result


def _normalize_authenticated_evidence(evidence, *, context, actor_id, actor_kind):
    try:
        if evidence.get("evidence_class") == "verification_report":
            from okto_pulse.core.domain.verification_report import parse_verification_report

            report = parse_verification_report(evidence["verification_report"])
            if report.method != context.verification_method:
                raise ValueError("method mismatch")
            sources = tuple(VerificationSourceVersion(source_ref=s.reference, revision=s.revision)
                            for s in report.sources)
            outcome = report.result
        else:
            if context.verification_method != "automated_test":
                raise ValueError("method mismatch")
            attestation = evidence["execution_attestation"]
            execution_basis = attestation["execution_basis"]
            companions = execution_basis.get("companion_sources", [])
            if not isinstance(companions, list) or len(companions) > 1:
                raise ValueError("companion sources invalid")
            sources = tuple(VerificationSourceVersion(source_ref=s["source_ref"], revision=s["revision"])
                            for s in [execution_basis, *companions])
            outcome = attestation["outcome"]
        basis = VerificationBasis(sources=sources)
        if context.verification_method == "automated_test":
            basis.require_runner_shape()
        unsigned = {key: value for key, value in evidence.items() if key != "execution_receipt"}
        digest = hashlib.sha256(json.dumps(unsigned, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode("utf-8")).hexdigest()
        return AuthenticatedVerificationResult(context=context, actor_id=actor_id, actor_kind=actor_kind,
            receipt_id=evidence["execution_receipt"], evidence_sha256=digest, basis=basis, outcome=outcome)
    except (TypeError, ValueError, KeyError, AttributeError) as exc:
        raise VerificationCurrentnessError("verification_result_invalid") from exc


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

"""Immutable evidence contracts for terminal operational debt.

The four debt domains deliberately share only this evidence vocabulary.  No
value in this module can requeue, retry, delete, rearm, or otherwise mutate an
operational store.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

from okto_pulse.core.domain.quality_canonicalization import (
    canonical_json_bytes,
    canonical_sha256,
)

TERMINAL_DEBT_MANIFEST_SCHEMA = "terminal-debt-manifest/v1"
TERMINAL_DEBT_PLAN_SCHEMA = "terminal-debt-recovery-plan/v1"
TERMINAL_DEBT_PROOF_SCHEMA = "terminal-debt-proof/v1"
TERMINAL_DEBT_MAX_SELECTION = 100
TERMINAL_DEBT_MAX_FAILURE_DETAIL = 500
TERMINAL_DEBT_MAX_ATTRIBUTES = 32

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class TerminalDebtContractError(ValueError):
    """One immutable terminal-debt value violates its closed contract."""


def required_text(value: object, code: str, *, max_length: int = 500) -> str:
    if not isinstance(value, str) or not value.strip():
        raise TerminalDebtContractError(code)
    normalized = value.strip()
    if len(normalized) > max_length:
        raise TerminalDebtContractError(code)
    return normalized


def normalize_sha256(value: object, code: str) -> str:
    normalized = required_text(value, code, max_length=64).lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise TerminalDebtContractError(code)
    return normalized


class TerminalDebtDomain(str, Enum):
    CONSOLIDATION_DLQ = "consolidation_dlq"
    GLOBAL_OUTBOX_DEAD_LETTER = "global_outbox_dead_letter"
    CANONICAL_DEBT = "canonical_debt"
    POLICY_CONSTRAINT_PROJECTION_DLQ = "policy_constraint_projection_dlq"


class TerminalDebtActionOwner(str, Enum):
    AUTOMATION = "automation"
    HUMAN = "human"
    TICK = "tick"


class TerminalDebtCopyAction(str, Enum):
    """Closed copy-only commands that existing domain ownership permits."""

    REQUEUE_CONSOLIDATION_COPY = "requeue_consolidation_copy"
    REPROCESS_GLOBAL_OUTBOX_COPY = "reprocess_global_outbox_copy"


class TerminalDebtRefusalCode(str, Enum):
    SELECTION_INVALID = "selection_invalid"
    SELECTION_REQUIRED = "selection_required"
    SELECTION_TOO_LARGE = "selection_too_large"
    DUPLICATE_SELECTION = "duplicate_selection"
    MIXED_DOMAIN_SELECTION = "mixed_domain_selection"
    ITEM_NOT_FOUND = "item_not_found"
    REPLAY_UNSAFE = "replay_unsafe"
    ACTION_NOT_COPY_SAFE = "action_not_copy_safe"
    FINGERPRINT_INVALID = "fingerprint_invalid"
    MANIFEST_ORIGIN_MISMATCH = "manifest_origin_mismatch"
    ORIGIN_COPY_ALIAS = "origin_copy_alias"
    COPY_EXECUTOR_NOT_CONFIGURED = "copy_executor_not_configured"
    SOURCE_IDENTITY_UNPROVEN = "source_identity_unproven"
    COPY_TARGET_MISMATCH = "copy_target_mismatch"
    ORIGIN_CHANGED_BEFORE_EXECUTION = "origin_changed_before_execution"
    COPY_CHANGED_BEFORE_EXECUTION = "copy_changed_before_execution"


class TerminalDebtExecutionOutcome(str, Enum):
    RESOLVED = "resolved"
    FAILED = "failed"


class TerminalDebtProofInvariantName(str, Enum):
    PLAN_MATCHES_INPUTS = "plan_matches_inputs"
    ORIGIN_UNCHANGED = "origin_unchanged"
    COPY_BASELINE_MATCHES_ORIGIN = "copy_baseline_matches_origin"
    RESULT_SET_EXACT = "result_set_exact"
    SELECTED_OUTCOMES = "selected_outcomes"
    UNSELECTED_UNCHANGED = "unselected_unchanged"


@dataclass(frozen=True, slots=True, order=True)
class TerminalDebtIdentity:
    domain: TerminalDebtDomain
    value: str

    def __post_init__(self) -> None:
        if not isinstance(self.domain, TerminalDebtDomain):
            raise TerminalDebtContractError("terminal_debt_identity_domain_invalid")
        object.__setattr__(
            self,
            "value",
            required_text(
                self.value,
                "terminal_debt_identity_value_required",
                max_length=500,
            ),
        )

    def as_dict(self) -> dict[str, str]:
        return {"domain": self.domain.value, "value": self.value}


@dataclass(frozen=True, slots=True)
class TerminalDebtItem:
    """Neutral immutable evidence for one item in exactly one debt domain."""

    identity: TerminalDebtIdentity
    recovery_class: str
    replay_safe: bool
    action_owner: TerminalDebtActionOwner
    source_version: int
    content_hash: str
    copy_action: TerminalDebtCopyAction | None = None
    failure_detail: str | None = None
    attributes: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.identity, TerminalDebtIdentity):
            raise TerminalDebtContractError("terminal_debt_item_identity_invalid")
        object.__setattr__(
            self,
            "recovery_class",
            required_text(
                self.recovery_class,
                "terminal_debt_recovery_class_required",
                max_length=100,
            ),
        )
        if not isinstance(self.replay_safe, bool):
            raise TerminalDebtContractError("terminal_debt_replay_safe_invalid")
        if not isinstance(self.action_owner, TerminalDebtActionOwner):
            raise TerminalDebtContractError("terminal_debt_action_owner_invalid")
        if (
            not isinstance(self.source_version, int)
            or isinstance(self.source_version, bool)
            or self.source_version < 1
        ):
            raise TerminalDebtContractError("terminal_debt_source_version_invalid")
        object.__setattr__(
            self,
            "content_hash",
            normalize_sha256(
                self.content_hash,
                "terminal_debt_content_hash_invalid",
            ),
        )
        if self.copy_action is not None:
            if not isinstance(self.copy_action, TerminalDebtCopyAction):
                raise TerminalDebtContractError("terminal_debt_copy_action_invalid")
            expected_action = {
                TerminalDebtDomain.CONSOLIDATION_DLQ: (
                    TerminalDebtCopyAction.REQUEUE_CONSOLIDATION_COPY
                ),
                TerminalDebtDomain.GLOBAL_OUTBOX_DEAD_LETTER: (
                    TerminalDebtCopyAction.REPROCESS_GLOBAL_OUTBOX_COPY
                ),
            }.get(self.domain)
            if self.copy_action is not expected_action:
                raise TerminalDebtContractError(
                    "terminal_debt_copy_action_domain_mismatch"
                )
        if self.failure_detail is not None:
            object.__setattr__(
                self,
                "failure_detail",
                required_text(
                    self.failure_detail,
                    "terminal_debt_failure_detail_invalid",
                    max_length=TERMINAL_DEBT_MAX_FAILURE_DETAIL,
                ),
            )
        if not isinstance(self.attributes, tuple | list):
            raise TerminalDebtContractError("terminal_debt_attributes_invalid")
        normalized_attributes: list[tuple[str, str]] = []
        for item in self.attributes:
            if not isinstance(item, tuple | list) or len(item) != 2:
                raise TerminalDebtContractError("terminal_debt_attributes_invalid")
            key = required_text(
                item[0],
                "terminal_debt_attribute_key_invalid",
                max_length=100,
            )
            value = required_text(
                item[1],
                "terminal_debt_attribute_value_invalid",
                max_length=500,
            )
            normalized_attributes.append((key, value))
        if len(normalized_attributes) > TERMINAL_DEBT_MAX_ATTRIBUTES:
            raise TerminalDebtContractError("terminal_debt_attributes_too_large")
        normalized_attributes.sort()
        if len({key for key, _ in normalized_attributes}) != len(normalized_attributes):
            raise TerminalDebtContractError("terminal_debt_attribute_duplicate")
        object.__setattr__(self, "attributes", tuple(normalized_attributes))

    @property
    def domain(self) -> TerminalDebtDomain:
        return self.identity.domain

    def digest_material(self) -> dict[str, object]:
        return {
            "identity": self.identity.as_dict(),
            "recovery_class": self.recovery_class,
            "replay_safe": self.replay_safe,
            "action_owner": self.action_owner.value,
            "source_version": self.source_version,
            "content_hash": self.content_hash,
            "copy_action": self.copy_action.value if self.copy_action else None,
            "failure_detail": self.failure_detail,
            "attributes": [
                {"key": key, "value": value} for key, value in self.attributes
            ],
        }

    @property
    def item_digest(self) -> str:
        return canonical_sha256(self.digest_material())


@dataclass(frozen=True, slots=True)
class TerminalDebtManifest:
    """One sealed inventory for one domain and one read scope."""

    domain: TerminalDebtDomain
    scope_id: str
    source_fingerprint: str
    items: tuple[TerminalDebtItem, ...]
    captured_at: str | None = None
    schema_version: str = TERMINAL_DEBT_MANIFEST_SCHEMA

    def __post_init__(self) -> None:
        if not isinstance(self.domain, TerminalDebtDomain):
            raise TerminalDebtContractError("terminal_debt_manifest_domain_invalid")
        if self.schema_version != TERMINAL_DEBT_MANIFEST_SCHEMA:
            raise TerminalDebtContractError("terminal_debt_manifest_schema_invalid")
        object.__setattr__(
            self,
            "scope_id",
            required_text(
                self.scope_id,
                "terminal_debt_manifest_scope_required",
                max_length=500,
            ),
        )
        object.__setattr__(
            self,
            "source_fingerprint",
            normalize_sha256(
                self.source_fingerprint,
                "terminal_debt_manifest_fingerprint_invalid",
            ),
        )
        if not isinstance(self.items, tuple | list) or any(
            not isinstance(item, TerminalDebtItem) for item in self.items
        ):
            raise TerminalDebtContractError("terminal_debt_manifest_items_invalid")
        items = tuple(
            sorted(
                self.items,
                key=lambda item: (
                    item.identity.domain.value,
                    item.identity.value,
                ),
            )
        )
        if any(item.domain is not self.domain for item in items):
            raise TerminalDebtContractError("terminal_debt_manifest_domain_mismatch")
        identities = tuple(item.identity for item in items)
        if len(set(identities)) != len(identities):
            raise TerminalDebtContractError("terminal_debt_manifest_identity_duplicate")
        object.__setattr__(self, "items", items)
        if self.captured_at is not None:
            object.__setattr__(
                self,
                "captured_at",
                required_text(
                    self.captured_at,
                    "terminal_debt_manifest_captured_at_invalid",
                    max_length=100,
                ),
            )

    def digest_material(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "domain": self.domain.value,
            "scope_id": self.scope_id,
            "source_fingerprint": self.source_fingerprint,
            "items": [item.digest_material() for item in self.items],
        }

    def semantic_material(self) -> dict[str, object]:
        """Return inventory evidence without the store-instance fingerprint."""

        return {
            "schema_version": self.schema_version,
            "domain": self.domain.value,
            "scope_id": self.scope_id,
            "items": [item.digest_material() for item in self.items],
        }

    @property
    def manifest_digest(self) -> str:
        return canonical_sha256(self.digest_material())

    @property
    def semantic_digest(self) -> str:
        return canonical_sha256(self.semantic_material())

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.digest_material())

    def item_map(self) -> dict[TerminalDebtIdentity, TerminalDebtItem]:
        return {item.identity: item for item in self.items}


@dataclass(frozen=True, slots=True)
class TerminalDebtRecoveryPlan:
    domain: TerminalDebtDomain
    scope_id: str
    manifest_digest: str
    origin_fingerprint: str
    copy_fingerprint: str
    selected_identities: tuple[TerminalDebtIdentity, ...]
    plan_digest: str
    schema_version: str = TERMINAL_DEBT_PLAN_SCHEMA

    def __post_init__(self) -> None:
        if not isinstance(self.domain, TerminalDebtDomain):
            raise TerminalDebtContractError("terminal_debt_plan_domain_invalid")
        if self.schema_version != TERMINAL_DEBT_PLAN_SCHEMA:
            raise TerminalDebtContractError("terminal_debt_plan_schema_invalid")
        object.__setattr__(
            self,
            "scope_id",
            required_text(
                self.scope_id,
                "terminal_debt_plan_scope_required",
                max_length=500,
            ),
        )
        for field_name in (
            "manifest_digest",
            "origin_fingerprint",
            "copy_fingerprint",
            "plan_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_sha256(
                    getattr(self, field_name),
                    f"terminal_debt_plan_{field_name}_invalid",
                ),
            )
        if self.origin_fingerprint == self.copy_fingerprint:
            raise TerminalDebtContractError("terminal_debt_plan_origin_copy_alias")
        if not isinstance(self.selected_identities, tuple | list):
            raise TerminalDebtContractError("terminal_debt_plan_selection_invalid")
        selected = tuple(sorted(self.selected_identities))
        if not selected or len(selected) > TERMINAL_DEBT_MAX_SELECTION:
            raise TerminalDebtContractError("terminal_debt_plan_selection_invalid")
        if any(
            not isinstance(identity, TerminalDebtIdentity)
            or identity.domain is not self.domain
            for identity in selected
        ):
            raise TerminalDebtContractError("terminal_debt_plan_selection_invalid")
        if len(set(selected)) != len(selected):
            raise TerminalDebtContractError("terminal_debt_plan_selection_duplicate")
        object.__setattr__(self, "selected_identities", selected)
        if canonical_sha256(self.digest_material()) != self.plan_digest:
            raise TerminalDebtContractError("terminal_debt_plan_digest_mismatch")

    def digest_material(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "domain": self.domain.value,
            "scope_id": self.scope_id,
            "manifest_digest": self.manifest_digest,
            "origin_fingerprint": self.origin_fingerprint,
            "copy_fingerprint": self.copy_fingerprint,
            "selected_identities": [
                identity.as_dict() for identity in self.selected_identities
            ],
        }


@dataclass(frozen=True, slots=True)
class TerminalDebtPlanRefusal:
    code: TerminalDebtRefusalCode
    identities: tuple[TerminalDebtIdentity, ...] = ()
    detail: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.code, TerminalDebtRefusalCode):
            raise TerminalDebtContractError("terminal_debt_refusal_code_invalid")
        if not isinstance(self.identities, tuple | list) or any(
            not isinstance(identity, TerminalDebtIdentity)
            for identity in self.identities
        ):
            raise TerminalDebtContractError("terminal_debt_refusal_identities_invalid")
        object.__setattr__(self, "identities", tuple(sorted(self.identities)))
        if self.detail is not None:
            object.__setattr__(
                self,
                "detail",
                required_text(
                    self.detail,
                    "terminal_debt_refusal_detail_invalid",
                    max_length=500,
                ),
            )


@dataclass(frozen=True, slots=True)
class TerminalDebtPlanDecision:
    plan: TerminalDebtRecoveryPlan | None = None
    refusal: TerminalDebtPlanRefusal | None = None

    def __post_init__(self) -> None:
        if (self.plan is None) == (self.refusal is None):
            raise TerminalDebtContractError("terminal_debt_plan_decision_invalid")
        if self.plan is not None and not isinstance(
            self.plan, TerminalDebtRecoveryPlan
        ):
            raise TerminalDebtContractError("terminal_debt_plan_decision_invalid")
        if self.refusal is not None and not isinstance(
            self.refusal, TerminalDebtPlanRefusal
        ):
            raise TerminalDebtContractError("terminal_debt_plan_decision_invalid")

    @property
    def allowed(self) -> bool:
        return self.plan is not None


@dataclass(frozen=True, slots=True)
class TerminalDebtExecutionResult:
    identity: TerminalDebtIdentity
    outcome: TerminalDebtExecutionOutcome
    before_item_digest: str
    evidence_hash: str
    after_item_digest: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.identity, TerminalDebtIdentity):
            raise TerminalDebtContractError("terminal_debt_result_identity_invalid")
        if not isinstance(self.outcome, TerminalDebtExecutionOutcome):
            raise TerminalDebtContractError("terminal_debt_result_outcome_invalid")
        object.__setattr__(
            self,
            "before_item_digest",
            normalize_sha256(
                self.before_item_digest,
                "terminal_debt_result_before_digest_invalid",
            ),
        )
        object.__setattr__(
            self,
            "evidence_hash",
            normalize_sha256(
                self.evidence_hash,
                "terminal_debt_result_evidence_hash_invalid",
            ),
        )
        if self.after_item_digest is not None:
            object.__setattr__(
                self,
                "after_item_digest",
                normalize_sha256(
                    self.after_item_digest,
                    "terminal_debt_result_after_digest_invalid",
                ),
            )

    def as_dict(self) -> dict[str, object]:
        return {
            "identity": self.identity.as_dict(),
            "outcome": self.outcome.value,
            "before_item_digest": self.before_item_digest,
            "after_item_digest": self.after_item_digest,
            "evidence_hash": self.evidence_hash,
        }


@dataclass(frozen=True, slots=True)
class TerminalDebtProofInvariant:
    name: TerminalDebtProofInvariantName
    passed: bool
    detail: str

    def __post_init__(self) -> None:
        if not isinstance(self.name, TerminalDebtProofInvariantName):
            raise TerminalDebtContractError("terminal_debt_invariant_name_invalid")
        if not isinstance(self.passed, bool):
            raise TerminalDebtContractError("terminal_debt_invariant_passed_invalid")
        object.__setattr__(
            self,
            "detail",
            required_text(
                self.detail,
                "terminal_debt_invariant_detail_required",
                max_length=500,
            ),
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "name": self.name.value,
            "passed": self.passed,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class TerminalDebtProof:
    plan_digest: str
    verified: bool
    invariants: tuple[TerminalDebtProofInvariant, ...]
    item_results: tuple[TerminalDebtExecutionResult, ...]
    origin_before_digest: str
    origin_after_digest: str
    copy_before_digest: str
    copy_after_digest: str
    proof_digest: str
    schema_version: str = TERMINAL_DEBT_PROOF_SCHEMA

    def __post_init__(self) -> None:
        if self.schema_version != TERMINAL_DEBT_PROOF_SCHEMA:
            raise TerminalDebtContractError("terminal_debt_proof_schema_invalid")
        if not isinstance(self.verified, bool):
            raise TerminalDebtContractError("terminal_debt_proof_verified_invalid")
        for field_name in (
            "plan_digest",
            "origin_before_digest",
            "origin_after_digest",
            "copy_before_digest",
            "copy_after_digest",
            "proof_digest",
        ):
            object.__setattr__(
                self,
                field_name,
                normalize_sha256(
                    getattr(self, field_name),
                    f"terminal_debt_proof_{field_name}_invalid",
                ),
            )
        if not isinstance(self.invariants, tuple | list) or any(
            not isinstance(item, TerminalDebtProofInvariant) for item in self.invariants
        ):
            raise TerminalDebtContractError("terminal_debt_proof_invariants_invalid")
        invariants = tuple(self.invariants)
        if len({item.name for item in invariants}) != len(invariants):
            raise TerminalDebtContractError("terminal_debt_proof_invariant_duplicate")
        if {item.name for item in invariants} != set(TerminalDebtProofInvariantName):
            raise TerminalDebtContractError("terminal_debt_proof_invariants_incomplete")
        object.__setattr__(self, "invariants", invariants)
        if not isinstance(self.item_results, tuple | list) or any(
            not isinstance(item, TerminalDebtExecutionResult)
            for item in self.item_results
        ):
            raise TerminalDebtContractError("terminal_debt_proof_results_invalid")
        object.__setattr__(
            self,
            "item_results",
            tuple(
                sorted(
                    self.item_results,
                    key=lambda result: (
                        result.identity.domain.value,
                        result.identity.value,
                        result.outcome.value,
                    ),
                )
            ),
        )
        if self.verified != all(item.passed for item in invariants):
            raise TerminalDebtContractError("terminal_debt_proof_verified_mismatch")
        if canonical_sha256(self.digest_material()) != self.proof_digest:
            raise TerminalDebtContractError("terminal_debt_proof_digest_mismatch")

    def digest_material(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "plan_digest": self.plan_digest,
            "verified": self.verified,
            "invariants": [item.as_dict() for item in self.invariants],
            "item_results": [item.as_dict() for item in self.item_results],
            "origin_before_digest": self.origin_before_digest,
            "origin_after_digest": self.origin_after_digest,
            "copy_before_digest": self.copy_before_digest,
            "copy_after_digest": self.copy_after_digest,
        }


__all__ = [
    "TERMINAL_DEBT_MANIFEST_SCHEMA",
    "TERMINAL_DEBT_MAX_FAILURE_DETAIL",
    "TERMINAL_DEBT_MAX_SELECTION",
    "TERMINAL_DEBT_PLAN_SCHEMA",
    "TERMINAL_DEBT_PROOF_SCHEMA",
    "TerminalDebtActionOwner",
    "TerminalDebtContractError",
    "TerminalDebtCopyAction",
    "TerminalDebtDomain",
    "TerminalDebtExecutionOutcome",
    "TerminalDebtExecutionResult",
    "TerminalDebtIdentity",
    "TerminalDebtItem",
    "TerminalDebtManifest",
    "TerminalDebtPlanDecision",
    "TerminalDebtPlanRefusal",
    "TerminalDebtProof",
    "TerminalDebtProofInvariant",
    "TerminalDebtProofInvariantName",
    "TerminalDebtRecoveryPlan",
    "TerminalDebtRefusalCode",
    "normalize_sha256",
]

"""Fail-closed planning and deterministic proof for terminal-debt recovery.

This use case is deliberately store-agnostic.  It can authorize work only
against a caller-declared isolated copy and can prove the outcome only from
sealed before/after manifests.  It has no persistence or retry capability.
"""

from __future__ import annotations

from collections.abc import Sequence

from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.domain.terminal_debt import (
    TERMINAL_DEBT_MAX_SELECTION,
    TERMINAL_DEBT_PLAN_SCHEMA,
    TERMINAL_DEBT_PROOF_SCHEMA,
    TerminalDebtActionOwner,
    TerminalDebtContractError,
    TerminalDebtExecutionOutcome,
    TerminalDebtExecutionResult,
    TerminalDebtIdentity,
    TerminalDebtManifest,
    TerminalDebtPlanDecision,
    TerminalDebtPlanRefusal,
    TerminalDebtProof,
    TerminalDebtProofInvariant,
    TerminalDebtProofInvariantName,
    TerminalDebtRecoveryPlan,
    TerminalDebtRefusalCode,
    normalize_sha256,
)


def _refuse(
    code: TerminalDebtRefusalCode,
    *,
    identities: Sequence[TerminalDebtIdentity] = (),
    detail: str | None = None,
) -> TerminalDebtPlanDecision:
    return TerminalDebtPlanDecision(
        refusal=TerminalDebtPlanRefusal(
            code=code,
            identities=tuple(identities),
            detail=detail,
        )
    )


def build_recovery_plan(
    *,
    manifest: TerminalDebtManifest,
    selection: Sequence[TerminalDebtIdentity],
    origin_fingerprint: str,
    copy_fingerprint: str,
) -> TerminalDebtPlanDecision:
    """Authorize a homogeneous, replay-safe selection for an isolated copy.

    User-controlled selection and fingerprints produce a typed refusal rather
    than an exception.  A malformed manifest remains a programmer contract
    error because :class:`TerminalDebtManifest` validates on construction.
    """

    if not isinstance(manifest, TerminalDebtManifest):
        return _refuse(TerminalDebtRefusalCode.SELECTION_INVALID)
    if not isinstance(selection, Sequence) or isinstance(
        selection, str | bytes | bytearray
    ):
        return _refuse(TerminalDebtRefusalCode.SELECTION_INVALID)

    selected = tuple(selection)
    if not selected:
        return _refuse(TerminalDebtRefusalCode.SELECTION_REQUIRED)
    if len(selected) > TERMINAL_DEBT_MAX_SELECTION:
        return _refuse(
            TerminalDebtRefusalCode.SELECTION_TOO_LARGE,
            detail=f"max_selection={TERMINAL_DEBT_MAX_SELECTION}",
        )
    if any(not isinstance(identity, TerminalDebtIdentity) for identity in selected):
        return _refuse(TerminalDebtRefusalCode.SELECTION_INVALID)
    if len(set(selected)) != len(selected):
        return _refuse(
            TerminalDebtRefusalCode.DUPLICATE_SELECTION,
            identities=selected,
        )
    domains = {identity.domain for identity in selected}
    if len(domains) != 1 or manifest.domain not in domains:
        return _refuse(
            TerminalDebtRefusalCode.MIXED_DOMAIN_SELECTION,
            identities=selected,
        )

    try:
        normalized_origin = normalize_sha256(
            origin_fingerprint,
            "terminal_debt_origin_fingerprint_invalid",
        )
        normalized_copy = normalize_sha256(
            copy_fingerprint,
            "terminal_debt_copy_fingerprint_invalid",
        )
    except TerminalDebtContractError:
        return _refuse(TerminalDebtRefusalCode.FINGERPRINT_INVALID)
    if manifest.source_fingerprint != normalized_origin:
        return _refuse(TerminalDebtRefusalCode.MANIFEST_ORIGIN_MISMATCH)
    if normalized_origin == normalized_copy:
        return _refuse(TerminalDebtRefusalCode.ORIGIN_COPY_ALIAS)

    item_map = manifest.item_map()
    missing = tuple(identity for identity in selected if identity not in item_map)
    if missing:
        return _refuse(
            TerminalDebtRefusalCode.ITEM_NOT_FOUND,
            identities=missing,
        )
    replay_unsafe = tuple(
        identity for identity in selected if not item_map[identity].replay_safe
    )
    if replay_unsafe:
        return _refuse(
            TerminalDebtRefusalCode.REPLAY_UNSAFE,
            identities=replay_unsafe,
        )
    action_unsafe = tuple(
        identity
        for identity in selected
        if item_map[identity].action_owner is not TerminalDebtActionOwner.AUTOMATION
        or item_map[identity].copy_action is None
    )
    if action_unsafe:
        return _refuse(
            TerminalDebtRefusalCode.ACTION_NOT_COPY_SAFE,
            identities=action_unsafe,
        )

    ordered = tuple(sorted(selected))
    digest_material = {
        "schema_version": TERMINAL_DEBT_PLAN_SCHEMA,
        "domain": manifest.domain.value,
        "scope_id": manifest.scope_id,
        "manifest_digest": manifest.manifest_digest,
        "origin_fingerprint": normalized_origin,
        "copy_fingerprint": normalized_copy,
        "selected_identities": [identity.as_dict() for identity in ordered],
    }
    plan = TerminalDebtRecoveryPlan(
        domain=manifest.domain,
        scope_id=manifest.scope_id,
        manifest_digest=manifest.manifest_digest,
        origin_fingerprint=normalized_origin,
        copy_fingerprint=normalized_copy,
        selected_identities=ordered,
        plan_digest=canonical_sha256(digest_material),
    )
    return TerminalDebtPlanDecision(plan=plan)


def _invariant(
    name: TerminalDebtProofInvariantName,
    passed: bool,
    *,
    passed_detail: str,
    failed_detail: str,
) -> TerminalDebtProofInvariant:
    return TerminalDebtProofInvariant(
        name=name,
        passed=passed,
        detail=passed_detail if passed else failed_detail,
    )


def verify_recovery_proof(
    *,
    plan: TerminalDebtRecoveryPlan,
    origin_before: TerminalDebtManifest,
    origin_after: TerminalDebtManifest,
    copy_before: TerminalDebtManifest,
    copy_after: TerminalDebtManifest,
    results: Sequence[TerminalDebtExecutionResult],
) -> TerminalDebtProof:
    """Build a tamper-evident proof from immutable before/after evidence."""

    if not isinstance(plan, TerminalDebtRecoveryPlan):
        raise TerminalDebtContractError("terminal_debt_proof_plan_invalid")
    manifests = (origin_before, origin_after, copy_before, copy_after)
    if any(not isinstance(item, TerminalDebtManifest) for item in manifests):
        raise TerminalDebtContractError("terminal_debt_proof_manifest_invalid")
    if not isinstance(results, Sequence) or isinstance(
        results, str | bytes | bytearray
    ):
        raise TerminalDebtContractError("terminal_debt_proof_results_invalid")
    item_results = tuple(results)
    if any(not isinstance(item, TerminalDebtExecutionResult) for item in item_results):
        raise TerminalDebtContractError("terminal_debt_proof_results_invalid")

    selected = set(plan.selected_identities)
    result_identities = tuple(result.identity for result in item_results)
    result_identity_set = set(result_identities)
    results_unique = len(result_identity_set) == len(result_identities)

    plan_matches_inputs = (
        origin_before.domain is plan.domain
        and origin_after.domain is plan.domain
        and copy_before.domain is plan.domain
        and copy_after.domain is plan.domain
        and all(manifest.scope_id == plan.scope_id for manifest in manifests)
        and origin_before.manifest_digest == plan.manifest_digest
        and origin_before.source_fingerprint == plan.origin_fingerprint
        and origin_after.source_fingerprint == plan.origin_fingerprint
        and copy_before.source_fingerprint == plan.copy_fingerprint
        and copy_after.source_fingerprint == plan.copy_fingerprint
    )
    origin_unchanged = origin_before.manifest_digest == origin_after.manifest_digest
    copy_baseline_matches_origin = (
        origin_before.semantic_digest == copy_before.semantic_digest
    )
    result_set_exact = results_unique and result_identity_set == selected

    copy_before_map = copy_before.item_map()
    copy_after_map = copy_after.item_map()
    result_map = {result.identity: result for result in item_results}
    selected_outcomes = result_set_exact
    if selected_outcomes:
        for identity in plan.selected_identities:
            before_item = copy_before_map.get(identity)
            result = result_map[identity]
            if (
                before_item is None
                or result.outcome is not TerminalDebtExecutionOutcome.RESOLVED
                or result.before_item_digest != before_item.item_digest
                or result.after_item_digest is not None
                or identity in copy_after_map
            ):
                selected_outcomes = False
                break

    before_unselected = {
        identity: item.item_digest
        for identity, item in copy_before_map.items()
        if identity not in selected
    }
    after_unselected = {
        identity: item.item_digest
        for identity, item in copy_after_map.items()
        if identity not in selected
    }
    unselected_unchanged = before_unselected == after_unselected

    invariants = (
        _invariant(
            TerminalDebtProofInvariantName.PLAN_MATCHES_INPUTS,
            plan_matches_inputs,
            passed_detail="plan is bound to all four manifests",
            failed_detail="plan or source fingerprint does not match manifest inputs",
        ),
        _invariant(
            TerminalDebtProofInvariantName.ORIGIN_UNCHANGED,
            origin_unchanged,
            passed_detail="origin manifest remained byte-equivalent",
            failed_detail="origin manifest changed during copy execution",
        ),
        _invariant(
            TerminalDebtProofInvariantName.COPY_BASELINE_MATCHES_ORIGIN,
            copy_baseline_matches_origin,
            passed_detail="copy baseline matches origin semantic inventory",
            failed_detail="copy baseline diverges from origin semantic inventory",
        ),
        _invariant(
            TerminalDebtProofInvariantName.RESULT_SET_EXACT,
            result_set_exact,
            passed_detail="result identities exactly match the selected identities",
            failed_detail="result identities are missing, duplicated, or unexpected",
        ),
        _invariant(
            TerminalDebtProofInvariantName.SELECTED_OUTCOMES,
            selected_outcomes,
            passed_detail="every selected item was resolved with matching before evidence",
            failed_detail="one or more selected outcomes are incomplete or inconsistent",
        ),
        _invariant(
            TerminalDebtProofInvariantName.UNSELECTED_UNCHANGED,
            unselected_unchanged,
            passed_detail="all unselected copy items remained unchanged",
            failed_detail="an unselected copy item changed, appeared, or disappeared",
        ),
    )
    verified = all(item.passed for item in invariants)
    proof_material = {
        "schema_version": TERMINAL_DEBT_PROOF_SCHEMA,
        "plan_digest": plan.plan_digest,
        "verified": verified,
        "invariants": [item.as_dict() for item in invariants],
        "item_results": [
            item.as_dict()
            for item in sorted(
                item_results,
                key=lambda result: (
                    result.identity.domain.value,
                    result.identity.value,
                    result.outcome.value,
                ),
            )
        ],
        "origin_before_digest": origin_before.manifest_digest,
        "origin_after_digest": origin_after.manifest_digest,
        "copy_before_digest": copy_before.manifest_digest,
        "copy_after_digest": copy_after.manifest_digest,
    }
    return TerminalDebtProof(
        plan_digest=plan.plan_digest,
        verified=verified,
        invariants=invariants,
        item_results=item_results,
        origin_before_digest=origin_before.manifest_digest,
        origin_after_digest=origin_after.manifest_digest,
        copy_before_digest=copy_before.manifest_digest,
        copy_after_digest=copy_after.manifest_digest,
        proof_digest=canonical_sha256(proof_material),
    )


__all__ = ["build_recovery_plan", "verify_recovery_proof"]

from __future__ import annotations

from dataclasses import replace

import pytest

from okto_pulse.core.application.terminal_debt_recovery import (
    build_recovery_plan,
    verify_recovery_proof,
)
from okto_pulse.core.domain.quality_canonicalization import canonical_sha256
from okto_pulse.core.domain.terminal_debt import (
    TerminalDebtActionOwner,
    TerminalDebtContractError,
    TerminalDebtCopyAction,
    TerminalDebtDomain,
    TerminalDebtExecutionOutcome,
    TerminalDebtExecutionResult,
    TerminalDebtIdentity,
    TerminalDebtItem,
    TerminalDebtManifest,
    TerminalDebtProofInvariantName,
    TerminalDebtRefusalCode,
)
from okto_pulse.core.ports.terminal_debt import (
    CanonicalDebtTerminalReader,
    ConsolidationTerminalDebtReader,
    GlobalOutboxTerminalDebtReader,
    PolicyProjectionTerminalDebtReader,
)


ORIGIN_FINGERPRINT = "1" * 64
COPY_FINGERPRINT = "2" * 64


def _identity(
    value: str,
    domain: TerminalDebtDomain = TerminalDebtDomain.CONSOLIDATION_DLQ,
) -> TerminalDebtIdentity:
    return TerminalDebtIdentity(domain=domain, value=value)


def _item(
    value: str,
    *,
    domain: TerminalDebtDomain = TerminalDebtDomain.CONSOLIDATION_DLQ,
    replay_safe: bool = True,
    owner: TerminalDebtActionOwner = TerminalDebtActionOwner.AUTOMATION,
    copy_action: TerminalDebtCopyAction | None | object = ...,
    marker: str | None = None,
) -> TerminalDebtItem:
    if copy_action is ...:
        copy_action = {
            TerminalDebtDomain.CONSOLIDATION_DLQ: (
                TerminalDebtCopyAction.REQUEUE_CONSOLIDATION_COPY
            ),
            TerminalDebtDomain.GLOBAL_OUTBOX_DEAD_LETTER: (
                TerminalDebtCopyAction.REPROCESS_GLOBAL_OUTBOX_COPY
            ),
        }.get(domain)
    return TerminalDebtItem(
        identity=_identity(value, domain),
        recovery_class=f"{domain.value}_terminal",
        replay_safe=replay_safe,
        action_owner=owner,
        source_version=1,
        content_hash=canonical_sha256({"value": value, "marker": marker}),
        copy_action=copy_action,
        failure_detail="bounded failure",
        attributes=(("source", domain.value),),
    )


def _manifest(
    *items: TerminalDebtItem,
    fingerprint: str = ORIGIN_FINGERPRINT,
    domain: TerminalDebtDomain | None = None,
    captured_at: str | None = None,
) -> TerminalDebtManifest:
    selected_domain = domain or items[0].domain
    return TerminalDebtManifest(
        domain=selected_domain,
        scope_id="board-1",
        source_fingerprint=fingerprint,
        items=items,
        captured_at=captured_at,
    )


def _valid_decision(
    manifest: TerminalDebtManifest,
    selection: tuple[TerminalDebtIdentity, ...],
):
    decision = build_recovery_plan(
        manifest=manifest,
        selection=selection,
        origin_fingerprint=ORIGIN_FINGERPRINT,
        copy_fingerprint=COPY_FINGERPRINT,
    )
    assert decision.allowed
    assert decision.plan is not None
    return decision


def test_four_domains_share_evidence_without_collapsing_reader_contracts() -> None:
    manifests = tuple(
        _manifest(_item("item-1", domain=domain), domain=domain)
        for domain in TerminalDebtDomain
    )

    assert {manifest.domain for manifest in manifests} == set(TerminalDebtDomain)
    assert len({manifest.manifest_digest for manifest in manifests}) == 4

    class ConsolidationOnly:
        async def list_consolidation_terminal_debt(self, **_kwargs):
            return manifests[0]

    reader = ConsolidationOnly()
    assert isinstance(reader, ConsolidationTerminalDebtReader)
    assert not isinstance(reader, GlobalOutboxTerminalDebtReader)
    assert not isinstance(reader, CanonicalDebtTerminalReader)
    assert not isinstance(reader, PolicyProjectionTerminalDebtReader)


@pytest.mark.parametrize("domain", tuple(TerminalDebtDomain))
def test_manifest_digest_is_canonical_for_item_permutations_and_capture_time(
    domain: TerminalDebtDomain,
) -> None:
    alpha = _item("alpha", domain=domain)
    omega = _item("omega", domain=domain)
    first = _manifest(
        omega,
        alpha,
        domain=domain,
        captured_at="2026-08-05T10:00:00Z",
    )
    second = _manifest(
        alpha,
        omega,
        domain=domain,
        captured_at="2026-08-05T11:00:00Z",
    )
    copied = _manifest(
        omega,
        alpha,
        domain=domain,
        fingerprint=COPY_FINGERPRINT,
    )

    assert first.items == (alpha, omega)
    assert second.items == (alpha, omega)
    assert first.manifest_digest == second.manifest_digest
    assert first.canonical_bytes == second.canonical_bytes
    assert first.manifest_digest != copied.manifest_digest
    assert first.semantic_digest == copied.semantic_digest


@pytest.mark.parametrize(
    ("selection_factory", "origin", "copy", "expected"),
    (
        (
            lambda _manifest: (),
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.SELECTION_REQUIRED,
        ),
        (
            lambda manifest: (manifest.items[0].identity,) * 2,
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.DUPLICATE_SELECTION,
        ),
        (
            lambda _manifest: tuple(
                _identity(f"item-{index:03d}") for index in range(101)
            ),
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.SELECTION_TOO_LARGE,
        ),
        (
            lambda manifest: (
                manifest.items[0].identity,
                _identity("foreign", TerminalDebtDomain.CANONICAL_DEBT),
            ),
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.MIXED_DOMAIN_SELECTION,
        ),
        (
            lambda _manifest: (_identity("missing"),),
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.ITEM_NOT_FOUND,
        ),
        (
            lambda manifest: (
                next(
                    item.identity
                    for item in manifest.items
                    if item.identity.value == "unsafe"
                ),
            ),
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.REPLAY_UNSAFE,
        ),
        (
            lambda manifest: (
                next(
                    item.identity
                    for item in manifest.items
                    if item.identity.value == "human"
                ),
            ),
            ORIGIN_FINGERPRINT,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.ACTION_NOT_COPY_SAFE,
        ),
        (
            lambda manifest: (manifest.items[0].identity,),
            "invalid",
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.FINGERPRINT_INVALID,
        ),
        (
            lambda manifest: (manifest.items[0].identity,),
            "3" * 64,
            COPY_FINGERPRINT,
            TerminalDebtRefusalCode.MANIFEST_ORIGIN_MISMATCH,
        ),
        (
            lambda manifest: (manifest.items[0].identity,),
            ORIGIN_FINGERPRINT,
            ORIGIN_FINGERPRINT,
            TerminalDebtRefusalCode.ORIGIN_COPY_ALIAS,
        ),
    ),
    ids=(
        "empty",
        "duplicate",
        "too-large",
        "mixed-domain",
        "missing",
        "replay-unsafe",
        "human-owned",
        "fingerprint-invalid",
        "manifest-mismatch",
        "origin-copy-alias",
    ),
)
def test_plan_refuses_invalid_selection_and_ownership_before_execution(
    selection_factory,
    origin: str,
    copy: str,
    expected: TerminalDebtRefusalCode,
) -> None:
    manifest = _manifest(
        _item("safe"),
        _item("unsafe", replay_safe=False),
        _item("human", owner=TerminalDebtActionOwner.HUMAN, copy_action=None),
    )

    decision = build_recovery_plan(
        manifest=manifest,
        selection=selection_factory(manifest),
        origin_fingerprint=origin,
        copy_fingerprint=copy,
    )

    assert not decision.allowed
    assert decision.refusal is not None
    assert decision.refusal.code is expected


@pytest.mark.parametrize(
    ("owner", "copy_action"),
    (
        (TerminalDebtActionOwner.HUMAN, None),
        (TerminalDebtActionOwner.TICK, None),
        (TerminalDebtActionOwner.AUTOMATION, None),
    ),
    ids=("human-owned", "tick-owned", "automation-missing-action"),
)
def test_plan_refuses_every_non_copy_safe_ownership_combination(
    owner: TerminalDebtActionOwner,
    copy_action: TerminalDebtCopyAction | None,
) -> None:
    item = _item("unsafe", owner=owner, copy_action=copy_action)
    manifest = _manifest(item)

    decision = build_recovery_plan(
        manifest=manifest,
        selection=(item.identity,),
        origin_fingerprint=ORIGIN_FINGERPRINT,
        copy_fingerprint=COPY_FINGERPRINT,
    )

    assert decision.refusal is not None
    assert decision.refusal.code is TerminalDebtRefusalCode.ACTION_NOT_COPY_SAFE


def test_item_rejects_untyped_or_cross_domain_copy_actions() -> None:
    with pytest.raises(
        TerminalDebtContractError,
        match="terminal_debt_copy_action_invalid",
    ):
        _item("untyped", copy_action="requeue_consolidation_copy")  # type: ignore[arg-type]
    with pytest.raises(
        TerminalDebtContractError,
        match="terminal_debt_copy_action_domain_mismatch",
    ):
        _item(
            "wrong-domain",
            copy_action=TerminalDebtCopyAction.REPROCESS_GLOBAL_OUTBOX_COPY,
        )


def test_plan_digest_is_deterministic_for_selection_order_and_detects_tamper() -> None:
    manifest = _manifest(_item("a"), _item("b"))
    a, b = (item.identity for item in manifest.items)

    first = _valid_decision(manifest, (a, b)).plan
    second = _valid_decision(manifest, (b, a)).plan

    assert first is not None and second is not None
    assert first.plan_digest == second.plan_digest
    assert first.selected_identities == (a, b)
    with pytest.raises(
        TerminalDebtContractError,
        match="terminal_debt_plan_digest_mismatch",
    ):
        replace(first, scope_id="different-board")


def _proof_fixture():
    selected = _item("selected")
    untouched = _item("untouched")
    origin_before = _manifest(selected, untouched)
    copy_before = _manifest(
        selected,
        untouched,
        fingerprint=COPY_FINGERPRINT,
    )
    plan = _valid_decision(origin_before, (selected.identity,)).plan
    assert plan is not None
    result = TerminalDebtExecutionResult(
        identity=selected.identity,
        outcome=TerminalDebtExecutionOutcome.RESOLVED,
        before_item_digest=selected.item_digest,
        after_item_digest=None,
        evidence_hash=canonical_sha256({"copy": "resolved", "id": "selected"}),
    )
    copy_after = _manifest(untouched, fingerprint=COPY_FINGERPRINT)
    return plan, origin_before, copy_before, copy_after, result


def test_post_correction_proof_binds_origin_copy_results_and_unselected_items() -> None:
    plan, origin_before, copy_before, copy_after, result = _proof_fixture()

    first = verify_recovery_proof(
        plan=plan,
        origin_before=origin_before,
        origin_after=origin_before,
        copy_before=copy_before,
        copy_after=copy_after,
        results=(result,),
    )
    second = verify_recovery_proof(
        plan=plan,
        origin_before=origin_before,
        origin_after=origin_before,
        copy_before=copy_before,
        copy_after=copy_after,
        results=(result,),
    )

    assert first.verified
    assert first.proof_digest == second.proof_digest
    assert {item.name for item in first.invariants} == set(
        TerminalDebtProofInvariantName
    )
    with pytest.raises(
        TerminalDebtContractError,
        match="terminal_debt_proof_proof_digest_invalid|terminal_debt_proof_digest_mismatch",
    ):
        replace(first, proof_digest="tampered")


def test_proof_detects_origin_and_unselected_tampering() -> None:
    plan, origin_before, copy_before, copy_after, result = _proof_fixture()
    selected, untouched = origin_before.items
    origin_after = _manifest(selected, replace(untouched, failure_detail="changed"))
    copy_after_tampered = _manifest(
        replace(untouched, failure_detail="changed"),
        fingerprint=COPY_FINGERPRINT,
    )

    proof = verify_recovery_proof(
        plan=plan,
        origin_before=origin_before,
        origin_after=origin_after,
        copy_before=copy_before,
        copy_after=copy_after_tampered,
        results=(result,),
    )
    by_name = {item.name: item.passed for item in proof.invariants}

    assert not proof.verified
    assert not by_name[TerminalDebtProofInvariantName.ORIGIN_UNCHANGED]
    assert not by_name[TerminalDebtProofInvariantName.UNSELECTED_UNCHANGED]


@pytest.mark.parametrize(
    "tamper",
    (
        "missing",
        "duplicate",
        "extra",
        "cross_domain",
        "before_digest",
        "failed",
    ),
)
def test_proof_detects_result_tampering(tamper: str) -> None:
    plan, origin_before, copy_before, copy_after, result = _proof_fixture()
    if tamper == "missing":
        results = ()
    elif tamper == "duplicate":
        results = (result, result)
    elif tamper in {"extra", "cross_domain"}:
        domain = (
            TerminalDebtDomain.CANONICAL_DEBT
            if tamper == "cross_domain"
            else TerminalDebtDomain.CONSOLIDATION_DLQ
        )
        unexpected = TerminalDebtExecutionResult(
            identity=_identity("unexpected", domain),
            outcome=TerminalDebtExecutionOutcome.RESOLVED,
            before_item_digest="7" * 64,
            after_item_digest=None,
            evidence_hash="8" * 64,
        )
        results = (result, unexpected)
    elif tamper == "before_digest":
        results = (replace(result, before_item_digest="9" * 64),)
    else:
        results = (
            replace(
                result,
                outcome=TerminalDebtExecutionOutcome.FAILED,
                after_item_digest=result.before_item_digest,
            ),
        )

    proof = verify_recovery_proof(
        plan=plan,
        origin_before=origin_before,
        origin_after=origin_before,
        copy_before=copy_before,
        copy_after=copy_after,
        results=results,
    )
    by_name = {item.name: item.passed for item in proof.invariants}

    assert not proof.verified
    if tamper in {"missing", "duplicate", "extra", "cross_domain"}:
        assert not by_name[TerminalDebtProofInvariantName.RESULT_SET_EXACT]
    assert not by_name[TerminalDebtProofInvariantName.SELECTED_OUTCOMES]

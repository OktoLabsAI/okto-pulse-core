from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from types import MappingProxyType, SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.domain.checklist import (
    CHECKLIST_STALE_REASON_ORDER,
    SPECIFY_CHECKLIST_ITEM_IDS,
    SPECIFY_CHECKLIST_ITEM_ID_SET,
    SPECIFY_CHECKLIST_ITEMS_V1,
    SPECIFY_CHECKLIST_MANIFEST_V1,
    SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
    SPECIFY_CHECKLIST_TEMPLATE_ID,
    SPECIFY_CHECKLIST_TEMPLATE_V1,
    SPECIFY_CHECKLIST_TEMPLATE_VERSION,
    ChecklistBinding,
    ChecklistCommitResult,
    ChecklistContractError,
    ChecklistItemOutcome,
    ChecklistItemResult,
    ChecklistMode,
    ChecklistPage,
    ChecklistPhase,
    ChecklistPreflight,
    ChecklistReceipt,
    ChecklistReceiptSource,
    ChecklistSpecSnapshot,
    ChecklistStaleReason,
    ChecklistSubmission,
    ChecklistTargetType,
    ChecklistTemplate,
    evaluate_checklist_currentness,
    evaluate_checklist_gate,
    require_specify_checklist_item,
)
from okto_pulse.core.domain.quality_canonicalization import (
    normative_manifest_digest_v1,
)
from okto_pulse.core.domain.enums import SpecStatus
from okto_pulse.core.application.use_cases.allowed_transitions import (
    ListAllowedTransitionsUseCase,
)
from okto_pulse.core.ports.checklist import (
    ChecklistBindingConflict,
    ChecklistContentDigestConflict,
    ChecklistHeadRevisionConflict,
    ChecklistIdempotencyConflict,
    ChecklistInputDigestConflict,
    ChecklistListQuery,
    ChecklistPersistencePort,
    ChecklistSpecLifecycleConflict,
    ChecklistSpecVersionConflict,
    ChecklistTemplateConflict,
)
from okto_pulse.core.services.checklist import (
    ChecklistConflictError,
    ChecklistPortContractError,
    ChecklistService,
    ChecklistValidationError,
)

EXPECTED_ITEM_IDS = (
    "chk_scope_boundaries",
    "chk_fr_value",
    "chk_fr_ac_testable",
    "chk_ac_measurable",
    "chk_edge_failures",
    "chk_dependencies_assumptions",
    "chk_no_placeholders",
    "chk_fr_tr_separation",
    "chk_ids_traceability",
    "chk_decisions_rationale",
)
ALLOW_NA_IDS = frozenset(
    {
        "chk_edge_failures",
        "chk_dependencies_assumptions",
        "chk_fr_tr_separation",
        "chk_decisions_rationale",
    }
)
NOW = datetime(2026, 7, 27, 12, 0, tzinfo=timezone.utc)
CONTENT_DIGEST = "1" * 64
INPUT_DIGEST = "2" * 64


def make_binding(
    *,
    mode: ChecklistMode = ChecklistMode.BLOCKING,
    version: int = 1,
) -> ChecklistBinding:
    return ChecklistBinding(board_id="board-1", mode=mode, version=version)


def make_subject(
    *,
    spec_version: int = 3,
    content_digest: str = CONTENT_DIGEST,
    input_digest: str = INPUT_DIGEST,
) -> ChecklistSpecSnapshot:
    return ChecklistSpecSnapshot(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=spec_version,
        content_digest=content_digest,
        input_digest=input_digest,
        status="validation",
    )


def make_preflight(
    *,
    binding: ChecklistBinding | None = None,
    subject: ChecklistSpecSnapshot | None = None,
    head_revision: int = 0,
    head_receipt_id: str | None = None,
) -> ChecklistPreflight:
    return ChecklistPreflight(
        subject=subject or make_subject(),
        binding=binding or make_binding(),
        current_head_revision=head_revision,
        current_head_receipt_id=head_receipt_id,
    )


def make_items(
    *,
    outcomes: dict[str, ChecklistItemOutcome] | None = None,
    reverse: bool = False,
) -> tuple[ChecklistItemResult, ...]:
    outcomes = outcomes or {}
    items = tuple(
        ChecklistItemResult(
            item_id=item_id,
            outcome=outcomes.get(item_id, ChecklistItemOutcome.PASS),
            anchor=f"field:{item_id}",
            rationale=(
                "Not relevant to this Spec."
                if outcomes.get(item_id) is ChecklistItemOutcome.NOT_APPLICABLE
                else None
            ),
        )
        for item_id in EXPECTED_ITEM_IDS
    )
    return tuple(reversed(items)) if reverse else items


def make_submission(
    *,
    binding: ChecklistBinding | None = None,
    subject: ChecklistSpecSnapshot | None = None,
    items: tuple[ChecklistItemResult, ...] | None = None,
    head_revision: int = 0,
    manual_checklist_ref: str | None = None,
) -> ChecklistSubmission:
    binding = binding or make_binding()
    subject = subject or make_subject()
    is_manual = manual_checklist_ref is not None
    return ChecklistSubmission(
        board_id=subject.board_id,
        spec_id=subject.spec_id,
        spec_version=subject.spec_version,
        content_digest=subject.content_digest,
        input_digest=subject.input_digest,
        template_version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
        template_digest=SPECIFY_CHECKLIST_TEMPLATE_DIGEST,
        binding_version=binding.version,
        binding_digest=binding.digest or "",
        expected_head_revision=head_revision,
        items=() if is_manual else (items if items is not None else make_items()),
        idempotency_key=None if is_manual else "idem-1",
        manual_checklist_ref=manual_checklist_ref,
    )


def make_service() -> ChecklistService:
    return ChecklistService(
        id_factory=lambda prefix: f"{prefix}-1",
        clock=lambda: NOW,
    )


def prepare_receipt(
    *,
    binding: ChecklistBinding | None = None,
    subject: ChecklistSpecSnapshot | None = None,
    items: tuple[ChecklistItemResult, ...] | None = None,
    manual_checklist_ref: str | None = None,
) -> ChecklistReceipt:
    binding = binding or make_binding()
    subject = subject or make_subject()
    bundle = make_service().prepare_execution(
        make_submission(
            binding=binding,
            subject=subject,
            items=items,
            manual_checklist_ref=manual_checklist_ref,
        ),
        actor_id="agent-1",
        preflight=make_preflight(binding=binding, subject=subject),
    )
    return bundle.receipt


class FakeChecklistPersistence:
    def __init__(
        self,
        *,
        apply_error: Exception | None = None,
        result: ChecklistCommitResult | None = None,
        current: Any = None,
        receipt: Any = None,
        page: Any = None,
        binding: Any = None,
        subject: Any = None,
    ) -> None:
        self.apply_error = apply_error
        self.result = result
        self.current = current
        self.receipt = receipt
        self.page = page
        self.binding = binding
        self.subject = subject
        self.applied_bundle = None
        self.binding_call = None

    async def apply_execution_cas(self, bundle):
        self.applied_bundle = bundle
        if self.apply_error is not None:
            raise self.apply_error
        if self.result is not None:
            return self.result
        return ChecklistCommitResult(
            board_id=bundle.receipt.board_id,
            spec_id=bundle.receipt.spec_id,
            spec_version=bundle.receipt.spec_version,
            receipt_id=bundle.receipt.id,
            request_digest=bundle.request_digest,
            head_revision=bundle.next_head.revision,
        )

    async def apply_binding_cas(
        self,
        binding,
        *,
        expected_version,
        expected_digest,
    ):
        self.binding_call = (
            binding,
            expected_version,
            expected_digest,
        )
        if self.apply_error is not None:
            raise self.apply_error
        return binding

    async def get_binding(self, **_kwargs):
        return self.binding

    async def get_current(self, **_kwargs):
        return self.current

    async def get_receipt(self, **_kwargs):
        return self.receipt

    async def get_spec_snapshot(self, **_kwargs):
        return self.subject

    async def list_executions(self, _query):
        return self.page


def test_specify_v1_template_is_exact_ordered_and_digest_pinned() -> None:
    assert SPECIFY_CHECKLIST_TEMPLATE_ID == "/specify"
    assert SPECIFY_CHECKLIST_TEMPLATE_VERSION == "/specify/v1"
    assert SPECIFY_CHECKLIST_ITEM_IDS == EXPECTED_ITEM_IDS
    assert SPECIFY_CHECKLIST_ITEM_ID_SET == frozenset(EXPECTED_ITEM_IDS)
    assert len(SPECIFY_CHECKLIST_ITEMS_V1) == 10
    assert isinstance(SPECIFY_CHECKLIST_ITEMS_V1, tuple)
    assert SPECIFY_CHECKLIST_TEMPLATE_V1.items is SPECIFY_CHECKLIST_ITEMS_V1
    assert SPECIFY_CHECKLIST_TEMPLATE_DIGEST == normative_manifest_digest_v1(
        namespace="checklist_template",
        version=SPECIFY_CHECKLIST_TEMPLATE_VERSION,
        manifest=SPECIFY_CHECKLIST_MANIFEST_V1,
    )
    assert (
        SPECIFY_CHECKLIST_TEMPLATE_DIGEST
        == "e487e91865b933535a5392dfaf3779210a41921a2b343ddb82dc0206be4c6155"
    )


def test_template_manifest_and_values_are_recursively_immutable() -> None:
    assert isinstance(SPECIFY_CHECKLIST_MANIFEST_V1, MappingProxyType)
    manifest_items = SPECIFY_CHECKLIST_MANIFEST_V1["items"]
    assert isinstance(manifest_items, tuple)
    assert all(isinstance(item, MappingProxyType) for item in manifest_items)
    with pytest.raises(TypeError):
        SPECIFY_CHECKLIST_MANIFEST_V1["template_id"] = "changed"  # type: ignore[index]
    with pytest.raises(TypeError):
        manifest_items[0]["title_en"] = "changed"  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        SPECIFY_CHECKLIST_ITEMS_V1[0].allow_na = True  # type: ignore[misc]


def test_template_na_policy_is_explicit_and_lookup_fails_closed() -> None:
    assert (
        frozenset(item.item_id for item in SPECIFY_CHECKLIST_ITEMS_V1 if item.allow_na)
        == ALLOW_NA_IDS
    )
    for item_id in EXPECTED_ITEM_IDS:
        assert require_specify_checklist_item(item_id).item_id == item_id
    for invalid in ("unknown", " chk_scope_boundaries", "", None):
        with pytest.raises(ChecklistContractError):
            require_specify_checklist_item(invalid)  # type: ignore[arg-type]


def test_no_template_authoring_surface_exists_on_persistence_port() -> None:
    assert "create_template" not in ChecklistPersistencePort.__dict__
    assert "update_template" not in ChecklistPersistencePort.__dict__
    assert "delete_template" not in ChecklistPersistencePort.__dict__
    assert "commit" not in ChecklistPersistencePort.__dict__


def test_binding_is_exact_versioned_immutable_and_digest_bound() -> None:
    binding = make_binding()
    assert binding.target_type is ChecklistTargetType.SPEC
    assert binding.phase is ChecklistPhase.SPEC_VALIDATION
    assert binding.template_version == SPECIFY_CHECKLIST_TEMPLATE_VERSION
    assert binding.revision == binding.version == 1
    assert binding.digest is not None and len(binding.digest) == 64
    promoted = replace(
        binding,
        mode=ChecklistMode.ADVISORY,
        version=2,
        revision=2,
        digest=None,
    )
    assert promoted.digest == binding.digest
    with pytest.raises(FrozenInstanceError):
        binding.mode = ChecklistMode.OFF  # type: ignore[misc]
    with pytest.raises(ChecklistContractError) as exc:
        replace(binding, digest="f" * 64)
    assert exc.value.code == "checklist_binding_digest_mismatch"
    with pytest.raises(ChecklistContractError) as exc:
        replace(binding, template_version="/specify/v2", digest=None)
    assert exc.value.code == "checklist_template_version_unsupported"


def test_synthetic_off_binding_exposes_initial_cas_revision() -> None:
    synthetic = ChecklistBinding.synthetic_off(board_id="board-1")
    persisted = ChecklistBinding(
        board_id="board-1",
        mode=ChecklistMode.OFF,
        version=1,
    )

    assert synthetic.mode is ChecklistMode.OFF
    assert synthetic.version == persisted.version == 1
    assert synthetic.revision == 0
    assert synthetic.is_synthetic is True
    assert persisted.revision == 1
    assert persisted.is_synthetic is False
    assert synthetic.digest == persisted.digest

    with pytest.raises(ChecklistContractError) as exc:
        replace(synthetic, mode=ChecklistMode.ADVISORY)
    assert exc.value.code == "checklist_binding_synthetic_invalid"
    with pytest.raises(ChecklistContractError) as exc:
        replace(persisted, revision=2)
    assert exc.value.code == "checklist_binding_revision_invalid"


def test_binding_service_creates_new_immutable_versions() -> None:
    first = ChecklistService.prepare_binding(
        board_id="board-1",
        mode=ChecklistMode.ADVISORY,
        current_binding=None,
    )
    second = ChecklistService.prepare_binding(
        board_id="board-1",
        mode=ChecklistMode.BLOCKING,
        current_binding=first,
    )
    assert first.version == 1
    assert second.version == 2
    assert second.digest == first.digest
    with pytest.raises(ChecklistValidationError) as exc:
        ChecklistService.prepare_binding(
            board_id="board-1",
            mode=ChecklistMode.ADVISORY,
            current_binding=first,
        )
    assert exc.value.code == "checklist_binding_unchanged"


def test_item_result_closes_outcomes_and_requires_anchor_and_na_rationale() -> None:
    with pytest.raises(ChecklistContractError) as exc:
        ChecklistItemResult(
            item_id="chk_scope_boundaries",
            outcome="negative",  # type: ignore[arg-type]
            anchor="field:scope",
        )
    assert exc.value.code == "checklist_item_outcome_invalid"
    with pytest.raises(ChecklistContractError) as exc:
        ChecklistItemResult(
            item_id="chk_scope_boundaries",
            outcome=ChecklistItemOutcome.PASS,
            anchor=" ",
        )
    assert exc.value.code == "checklist_item_anchor_required"
    with pytest.raises(ChecklistContractError) as exc:
        ChecklistItemResult(
            item_id="chk_edge_failures",
            outcome=ChecklistItemOutcome.NOT_APPLICABLE,
            anchor="field:edge_cases",
        )
    assert exc.value.code == "checklist_item_na_rationale_required"


def test_prepare_requires_all_ten_once_and_canonicalizes_order() -> None:
    service = make_service()
    submission = make_submission(items=make_items(reverse=True))
    bundle = service.prepare_execution(
        submission,
        actor_id=" agent-1 ",
        preflight=make_preflight(),
    )
    receipt = bundle.receipt
    assert tuple(item.item_id for item in receipt.items) == EXPECTED_ITEM_IDS
    assert receipt.source is ChecklistReceiptSource.NATIVE
    assert receipt.verified is True
    assert receipt.replayable is True
    assert receipt.blocking_satisfied is True
    assert receipt.spec_version == 3
    assert receipt.content_digest == CONTENT_DIGEST
    assert receipt.input_digest == INPUT_DIGEST
    assert receipt.template_digest == SPECIFY_CHECKLIST_TEMPLATE_DIGEST
    assert receipt.binding_digest == make_binding().digest
    assert bundle.next_head.receipt_id == receipt.id
    assert bundle.next_head.revision == 1
    assert service.submission_fingerprint(
        submission,
        actor_id="agent-1",
    ) == service.submission_fingerprint(
        make_submission(items=make_items()),
        actor_id="agent-1",
    )
    with pytest.raises(FrozenInstanceError):
        receipt.spec_version = 4  # type: ignore[misc]


@pytest.mark.parametrize("variant", ["missing", "duplicate", "unknown"])
def test_prepare_rejects_incomplete_duplicate_and_unknown_items(
    variant: str,
) -> None:
    items = make_items()
    if variant == "missing":
        invalid_items = items[:-1]
    elif variant == "duplicate":
        invalid_items = (*items[:-1], items[0])
    else:
        invalid_items = (
            *items[:-1],
            replace(items[-1], item_id="chk_not_curated"),
        )
    with pytest.raises(ChecklistValidationError) as exc:
        make_service().prepare_execution(
            make_submission(items=invalid_items),
            actor_id="agent-1",
            preflight=make_preflight(),
        )
    assert exc.value.code == "checklist_items_incomplete"
    assert exc.value.details


def test_na_is_accepted_only_when_template_allows_it() -> None:
    allowed_items = make_items(
        outcomes={
            "chk_edge_failures": ChecklistItemOutcome.NOT_APPLICABLE,
        }
    )
    receipt = prepare_receipt(items=allowed_items)
    assert receipt.blocking_satisfied is True
    forbidden_items = make_items(
        outcomes={
            "chk_scope_boundaries": ChecklistItemOutcome.NOT_APPLICABLE,
        }
    )
    with pytest.raises(ChecklistValidationError) as exc:
        make_service().prepare_execution(
            make_submission(items=forbidden_items),
            actor_id="agent-1",
            preflight=make_preflight(),
        )
    assert exc.value.code == "checklist_item_na_not_allowed"
    assert exc.value.details["item_id"] == "chk_scope_boundaries"


def test_prepare_fails_closed_on_every_version_and_digest_fence() -> None:
    service = make_service()
    preflight = make_preflight()
    base = make_submission()
    variants = (
        (replace(base, spec_version=4), "checklist_spec_version_conflict"),
        (
            replace(base, content_digest="3" * 64),
            "checklist_content_digest_conflict",
        ),
        (
            replace(base, input_digest="4" * 64),
            "checklist_input_digest_conflict",
        ),
        (
            replace(base, template_digest="5" * 64),
            "checklist_template_mismatch",
        ),
        (
            replace(base, binding_digest="6" * 64),
            "checklist_binding_conflict",
        ),
        (
            replace(base, expected_head_revision=1),
            "checklist_head_revision_conflict",
        ),
    )
    for submission, expected_code in variants:
        with pytest.raises((ChecklistConflictError, ChecklistValidationError)) as exc:
            service.prepare_execution(
                submission,
                actor_id="agent-1",
                preflight=preflight,
            )
        assert exc.value.code == expected_code


def test_currentness_compares_subject_and_template_execution_fences() -> None:
    receipt = prepare_receipt()
    changed_subject = make_subject(
        spec_version=4,
        content_digest="3" * 64,
        input_digest="4" * 64,
    )
    changed_binding = make_binding(
        mode=ChecklistMode.ADVISORY,
        version=2,
    )
    changed_template = ChecklistTemplate(
        template_id="/specify",
        version="/specify/v2",
        digest="5" * 64,
        items=SPECIFY_CHECKLIST_ITEMS_V1,
    )
    currentness = evaluate_checklist_currentness(
        receipt,
        current_subject=changed_subject,
        current_binding=changed_binding,
        current_template=changed_template,
    )
    assert currentness.current is False
    assert currentness.stale_reasons == (
        ChecklistStaleReason.SPEC_VERSION_CHANGED,
        ChecklistStaleReason.CONTENT_DIGEST_CHANGED,
        ChecklistStaleReason.INPUT_DIGEST_CHANGED,
        ChecklistStaleReason.TEMPLATE_VERSION_CHANGED,
        ChecklistStaleReason.TEMPLATE_DIGEST_CHANGED,
    )


def test_mode_only_promotion_preserves_receipt_currentness_and_passes_gate() -> None:
    subject = make_subject()
    advisory = make_binding(mode=ChecklistMode.ADVISORY, version=1)
    receipt = prepare_receipt(binding=advisory, subject=subject)
    blocking = make_binding(mode=ChecklistMode.BLOCKING, version=2)

    assert advisory.digest == blocking.digest
    assert receipt.binding_mode is ChecklistMode.ADVISORY
    currentness = evaluate_checklist_currentness(
        receipt,
        current_subject=subject,
        current_binding=blocking,
    )
    assert currentness.current is True
    assert currentness.stale_reasons == ()

    gate = evaluate_checklist_gate(
        binding=blocking,
        current_subject=subject,
        receipt=receipt,
    )
    assert gate.allowed is True
    assert gate.reason == "checklist_satisfied"
    assert gate.currentness == currentness


def test_mode_only_revision_preserves_start_and_submission_idempotency() -> None:
    service = make_service()
    subject = make_subject()
    advisory = make_binding(mode=ChecklistMode.ADVISORY, version=1)
    blocking = make_binding(mode=ChecklistMode.BLOCKING, version=2)

    advisory_execution = service.prepare_execution_start(
        preflight=make_preflight(binding=advisory, subject=subject),
        actor_id="agent-1",
        idempotency_key="start-idem",
    )
    blocking_execution = service.prepare_execution_start(
        preflight=make_preflight(binding=blocking, subject=subject),
        actor_id="agent-1",
        idempotency_key="start-idem",
    )
    assert advisory_execution.binding_version == 1
    assert advisory_execution.binding_mode is ChecklistMode.ADVISORY
    assert blocking_execution.binding_version == 2
    assert blocking_execution.binding_mode is ChecklistMode.BLOCKING
    assert advisory_execution.request_digest == blocking_execution.request_digest

    advisory_submission = make_submission(binding=advisory, subject=subject)
    blocking_submission = make_submission(binding=blocking, subject=subject)
    assert (
        service.submission_fingerprint(
            advisory_submission,
            actor_id="agent-1",
        )
        == service.submission_fingerprint(
            blocking_submission,
            actor_id="agent-1",
        )
    )


def test_open_advisory_execution_can_be_receipted_after_blocking_promotion() -> None:
    service = make_service()
    subject = make_subject()
    advisory = make_binding(mode=ChecklistMode.ADVISORY, version=1)
    blocking = make_binding(mode=ChecklistMode.BLOCKING, version=2)
    submission = make_submission(binding=advisory, subject=subject)

    bundle = service.prepare_execution(
        submission,
        actor_id="agent-1",
        preflight=make_preflight(binding=blocking, subject=subject),
    )

    assert bundle.receipt.binding_digest == advisory.digest == blocking.digest
    assert bundle.receipt.binding_version == 2
    assert bundle.receipt.binding_mode is ChecklistMode.BLOCKING
    assert bundle.expected_binding_version == 2
    assert bundle.expected_binding_mode is ChecklistMode.BLOCKING


def test_template_repin_still_makes_existing_receipt_stale() -> None:
    subject = make_subject()
    advisory = make_binding(mode=ChecklistMode.ADVISORY)
    receipt = prepare_receipt(binding=advisory, subject=subject)
    repinned_template = ChecklistTemplate(
        template_id="/specify",
        version="/specify/v2",
        digest="5" * 64,
        items=SPECIFY_CHECKLIST_ITEMS_V1,
    )

    currentness = evaluate_checklist_currentness(
        receipt,
        current_subject=subject,
        current_binding=advisory,
        current_template=repinned_template,
    )
    assert currentness.current is False
    assert currentness.stale_reasons == (
        ChecklistStaleReason.TEMPLATE_VERSION_CHANGED,
        ChecklistStaleReason.TEMPLATE_DIGEST_CHANGED,
    )


def test_gate_modes_are_explicit_and_legacy_never_satisfies_blocking() -> None:
    subject = make_subject()
    blocking = make_binding(mode=ChecklistMode.BLOCKING)
    passing = prepare_receipt(binding=blocking, subject=subject)
    assert evaluate_checklist_gate(
        binding=blocking,
        current_subject=subject,
        receipt=passing,
    ).allowed

    failed = prepare_receipt(
        binding=blocking,
        subject=subject,
        items=make_items(outcomes={"chk_fr_value": ChecklistItemOutcome.FAIL}),
    )
    failed_gate = evaluate_checklist_gate(
        binding=blocking,
        current_subject=subject,
        receipt=failed,
    )
    assert failed_gate.allowed is False
    assert failed_gate.reason == "checklist_item_failed"

    legacy = prepare_receipt(
        binding=blocking,
        subject=subject,
        manual_checklist_ref="legacy/checklist.md",
    )
    assert legacy.source is ChecklistReceiptSource.LEGACY_UNVERIFIED
    assert legacy.verified is False
    assert legacy.replayable is False
    assert legacy.blocking_satisfied is False
    legacy_gate = evaluate_checklist_gate(
        binding=blocking,
        current_subject=subject,
        receipt=legacy,
    )
    assert legacy_gate.allowed is False
    assert legacy_gate.reason == "manual_checklist_legacy_unverified"

    advisory = make_binding(mode=ChecklistMode.ADVISORY)
    off = make_binding(mode=ChecklistMode.OFF)
    assert evaluate_checklist_gate(
        binding=advisory,
        current_subject=subject,
        receipt=None,
    ).allowed
    assert evaluate_checklist_gate(
        binding=off,
        current_subject=subject,
        receipt=None,
    ).allowed


@pytest.mark.asyncio
async def test_allowed_transition_preview_uses_the_same_blocking_predicate() -> None:
    board = SimpleNamespace(
        id="board-1",
        settings={"require_spec_validation": False},
    )
    cards = SimpleNamespace(
        **{
            name: AsyncMock()
            for name in (
                "check_test_coverage",
                "check_rules_coverage",
                "check_trs_coverage",
                "check_contract_coverage",
                "check_ir_coverage",
                "check_or_coverage",
                "check_task_requirement_links_for_spec",
                "check_decision_presence",
                "check_decisions_coverage",
            )
        }
    )
    persistence = FakeChecklistPersistence(
        binding=make_binding(mode=ChecklistMode.BLOCKING),
        subject=make_subject(),
        current=None,
    )
    services = SimpleNamespace(
        boards=SimpleNamespace(get_board=AsyncMock(return_value=board)),
        cards=cards,
        checklists=persistence,
        resource_gate=SimpleNamespace(
            validate_or_raise_spec_architecture_validation_resource=AsyncMock(),
        ),
    )
    reason = await ListAllowedTransitionsUseCase()._spec_blocked_reason(
        services,
        SimpleNamespace(
            id="spec-1",
            board_id="board-1",
            status=SpecStatus.APPROVED,
        ),
        "validated",
    )
    assert reason == (
        "spec_checklist_gate_required: checklist_receipt_required"
    )


def test_manual_reference_is_structurally_non_replayable() -> None:
    binding = make_binding()
    subject = make_subject()
    submission = make_submission(
        binding=binding,
        subject=subject,
        manual_checklist_ref="legacy/checklist.md",
    )
    with pytest.raises(ChecklistValidationError) as exc:
        make_service().resolve_replay(
            submission,
            actor_id="agent-1",
            result=ChecklistCommitResult(
                board_id="board-1",
                spec_id="spec-1",
                spec_version=3,
                receipt_id="old",
                request_digest="f" * 64,
                head_revision=1,
                replayed=True,
            ),
        )
    assert exc.value.code == "manual_checklist_non_replayable"
    with pytest.raises(ChecklistContractError) as exc:
        replace(submission, idempotency_key="not-allowed")
    assert exc.value.code == "manual_checklist_non_replayable"


def test_native_replay_validates_same_request_digest() -> None:
    service = make_service()
    submission = make_submission()
    fingerprint = service.submission_fingerprint(
        submission,
        actor_id="agent-1",
    )
    replay = ChecklistCommitResult(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=3,
        receipt_id="clr-original",
        request_digest=fingerprint,
        head_revision=1,
        replayed=True,
    )
    assert (
        service.resolve_replay(
            submission,
            actor_id="agent-1",
            result=replay,
        )
        is replay
    )
    with pytest.raises(ChecklistConflictError) as exc:
        service.resolve_replay(
            submission,
            actor_id="different-agent",
            result=replay,
        )
    assert exc.value.code == "checklist_idempotency_conflict"
    assert exc.value.retryable is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("adapter_error", "expected_code", "retryable"),
    [
        (
            ChecklistHeadRevisionConflict(),
            "checklist_head_revision_conflict",
            True,
        ),
        (
            ChecklistSpecVersionConflict(),
            "checklist_spec_version_conflict",
            True,
        ),
        (
            ChecklistContentDigestConflict(),
            "checklist_content_digest_conflict",
            True,
        ),
        (
            ChecklistInputDigestConflict(),
            "checklist_input_digest_conflict",
            True,
        ),
        (
            ChecklistTemplateConflict(),
            "checklist_template_conflict",
            True,
        ),
        (
            ChecklistBindingConflict(),
            "checklist_binding_conflict",
            True,
        ),
        (
            ChecklistIdempotencyConflict(),
            "checklist_idempotency_conflict",
            False,
        ),
        (
            ChecklistSpecLifecycleConflict(),
            "checklist_spec_lifecycle_conflict",
            False,
        ),
    ],
)
async def test_apply_translates_every_cas_conflict(
    adapter_error: Exception,
    expected_code: str,
    retryable: bool,
) -> None:
    bundle = make_service().prepare_execution(
        make_submission(),
        actor_id="agent-1",
        preflight=make_preflight(),
    )
    persistence = FakeChecklistPersistence(apply_error=adapter_error)
    with pytest.raises(ChecklistConflictError) as exc:
        await make_service().apply_prepared(
            bundle,
            persistence=persistence,  # type: ignore[arg-type]
        )
    assert exc.value.code == expected_code
    assert exc.value.retryable is retryable


@pytest.mark.asyncio
async def test_apply_accepts_valid_result_and_rejects_legacy_replay() -> None:
    service = make_service()
    bundle = service.prepare_execution(
        make_submission(),
        actor_id="agent-1",
        preflight=make_preflight(),
    )
    persistence = FakeChecklistPersistence()
    result = await service.apply_prepared(
        bundle,
        persistence=persistence,  # type: ignore[arg-type]
    )
    assert result.receipt_id == bundle.receipt.id
    assert persistence.applied_bundle is bundle

    manual_bundle = service.prepare_execution(
        make_submission(manual_checklist_ref="legacy/checklist.md"),
        actor_id="agent-1",
        preflight=make_preflight(),
    )
    replay = ChecklistCommitResult(
        board_id="board-1",
        spec_id="spec-1",
        spec_version=3,
        receipt_id="old",
        request_digest=manual_bundle.request_digest,
        head_revision=1,
        replayed=True,
    )
    with pytest.raises(ChecklistPortContractError) as exc:
        await service.apply_prepared(
            manual_bundle,
            persistence=FakeChecklistPersistence(  # type: ignore[arg-type]
                result=replay
            ),
        )
    assert exc.value.code == "manual_checklist_replay_forbidden"


@pytest.mark.asyncio
async def test_binding_apply_uses_previous_version_and_digest_as_cas() -> None:
    service = make_service()
    previous = make_binding(mode=ChecklistMode.ADVISORY)
    next_binding = service.prepare_binding(
        board_id="board-1",
        mode=ChecklistMode.BLOCKING,
        current_binding=previous,
    )
    persistence = FakeChecklistPersistence()
    assert (
        await service.apply_binding(
            next_binding,
            previous_binding=previous,
            persistence=persistence,  # type: ignore[arg-type]
        )
        is next_binding
    )
    assert persistence.binding_call == (
        next_binding,
        previous.version,
        previous.digest,
    )


@pytest.mark.asyncio
async def test_synthetic_off_binding_starts_real_cas_at_zero() -> None:
    service = make_service()
    synthetic = ChecklistBinding.synthetic_off(board_id="board-1")
    first_persisted = service.prepare_binding(
        board_id="board-1",
        mode=ChecklistMode.ADVISORY,
        current_binding=synthetic,
    )
    persistence = FakeChecklistPersistence()

    assert first_persisted.version == first_persisted.revision == 1
    await service.apply_binding(
        first_persisted,
        previous_binding=synthetic,
        persistence=persistence,  # type: ignore[arg-type]
    )
    assert persistence.binding_call == (first_persisted, 0, None)


@pytest.mark.asyncio
async def test_current_and_list_reads_validate_scope_currentness_and_order() -> None:
    subject = make_subject()
    binding = make_binding()
    service = make_service()
    bundle = service.prepare_execution(
        make_submission(binding=binding, subject=subject),
        actor_id="agent-1",
        preflight=make_preflight(binding=binding, subject=subject),
    )
    receipt = bundle.receipt
    persistence = FakeChecklistPersistence(
        current=(receipt, bundle.next_head),
        receipt=receipt,
        page=ChecklistPage(
            items=(receipt,),
            total=1,
            offset=0,
            limit=2,
        ),
    )
    current = await service.get_current(
        board_id="board-1",
        spec_id="spec-1",
        current_subject=subject,
        current_binding=binding,
        persistence=persistence,  # type: ignore[arg-type]
    )
    assert current.currentness.current is True
    assert current.gate.allowed is True
    assert (
        await service.get_receipt(
            board_id="board-1",
            receipt_id=receipt.id,
            persistence=persistence,  # type: ignore[arg-type]
        )
        is receipt
    )
    page = await service.list_executions(
        ChecklistListQuery(
            board_id="board-1",
            spec_id="spec-1",
            offset=0,
            limit=2,
        ),
        current_subject=subject,
        current_binding=binding,
        head_receipt_id=receipt.id,
        persistence=persistence,  # type: ignore[arg-type]
    )
    assert page.total == 1
    assert page.items[0].is_head is True
    assert page.items[0].currentness.current is True


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 201])
async def test_list_pagination_limit_is_closed_to_one_through_two_hundred(
    limit: int,
) -> None:
    with pytest.raises(ChecklistValidationError) as exc:
        await make_service().list_executions(
            ChecklistListQuery(
                board_id="board-1",
                spec_id="spec-1",
                offset=0,
                limit=limit,
            ),
            current_subject=make_subject(),
            current_binding=make_binding(),
            head_receipt_id=None,
            persistence=FakeChecklistPersistence(),  # type: ignore[arg-type]
        )
    assert exc.value.code == "invalid_pagination"
    assert exc.value.category.value == "invalid_argument"


def test_page_contract_accepts_limits_one_and_two_hundred() -> None:
    one = ChecklistPage(items=(), total=0, offset=0, limit=1)
    two_hundred = ChecklistPage(
        items=(),
        total=0,
        offset=0,
        limit=200,
    )
    assert one.limit == 1
    assert two_hundred.limit == 200
    with pytest.raises(ChecklistContractError):
        ChecklistPage(items=(), total=0, offset=0, limit=201)


def test_enum_values_are_exactly_the_persisted_contract() -> None:
    assert tuple(item.value for item in ChecklistItemOutcome) == (
        "pass",
        "fail",
        "not_applicable",
    )
    assert tuple(item.value for item in ChecklistMode) == (
        "off",
        "advisory",
        "blocking",
    )
    assert tuple(item.value for item in ChecklistReceiptSource) == (
        "native",
        "legacy_unverified",
    )
    assert tuple(item.value for item in ChecklistStaleReason) == tuple(
        item.value for item in CHECKLIST_STALE_REASON_ORDER
    )

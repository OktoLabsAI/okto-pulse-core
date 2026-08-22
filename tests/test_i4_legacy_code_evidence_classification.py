from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.code_traceability import (
    ClassifyLegacyCodeEvidenceUseCase,
    PreviewSpecCodeEvidenceRebaseUseCase,
)
from okto_pulse.core.domain.code_traceability import (
    CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS,
    CodeEvidenceBaselinePresence,
    CodeEvidenceBaselineProvenance,
    CodeEvidenceContextOrigin,
    CodeEvidenceDisposition,
    CodeEvidenceDispositionKind,
    CodeEvidenceLegacyClassificationHumanRequired,
    CodeEvidenceLegacyClassificationLegacyRequired,
    CodeEvidenceLegacyClassificationPayloadConflict,
    CodeEvidenceLegacyClassificationPersistenceConflict,
    CodeEvidenceLegacyClassificationRevisionConflict,
    CodeEvidenceSourceRole,
    CodeInvestigationSubmissionLimitExceeded,
    CodeEvidenceSpecLink,
    CodeEvidenceSpecRelationType,
    CodeTraceabilityContext,
    CodeTraceabilityContextScope,
    CodeTraceabilityOmittedContent,
    CodeTraceabilityProjectionProfile,
    CodeTraceabilitySubjectType,
    DeliveryContext,
    ContextualInvestigationOutcomeV2,
    DirectSpecDeliveryContextProvenance,
    RefinementDeliveryContextProvenance,
    SpecEntityType,
    SpecDeliveryContextProvenance,
    build_source_context_summary_v2,
    canonical_code_traceability_sha256,
    source_context_classification_input_v2,
    source_context_classification_fence_v2,
    source_context_evidence_payload_v2,
    source_context_evidence_item_v2,
)
from okto_pulse.core.events.code_traceability import (
    code_traceability_event_digest,
)
from okto_pulse.core.models.code_traceability import (
    LegacyEvidenceClassificationBatchInput,
    LegacyEvidenceClassificationItemInput,
    SpecCodeEvidenceRebasePreviewInput,
)
from okto_pulse.core.models.schemas import CodeTraceabilitySettings
from okto_pulse.core.ports.code_traceability import (
    LegacyEvidenceClassificationRevisionConflict as StoreRevisionConflict,
)
from okto_pulse.core.services.code_traceability_gate import (
    CodeTraceabilityGateEvaluator,
)
from okto_pulse.core.services.legacy_code_evidence_classification import (
    LegacyCodeEvidenceClassificationService,
)
from okto_pulse.core.services.code_evidence_rebase import (
    SpecCodeEvidenceRebaseService,
)
from test_code_traceability_application import (
    FakeInvestigationStore,
    FakeTraceabilityStore,
    FakeUnitOfWork,
    H1,
    H2,
    NOW,
    _context_sha256,
    _legacy_evidence_record,
    _source_context_manifest,
)
from test_code_traceability_gate import _accepted, _evidence


class _Ids:
    def __init__(self) -> None:
        self._next = 0

    def __call__(self, prefix: str) -> str:
        self._next += 1
        return f"{prefix}-{self._next}"


class _ClassificationStore:
    def __init__(self, evidence: tuple[object, ...]) -> None:
        self.evidence = {item.id: item for item in evidence}
        self.heads: dict[str, object] = {}
        self.history: dict[tuple[str, int], object] = {}
        self.replays: dict[tuple[str, str, str], object] = {}
        self.append_calls = 0
        self.fail_cas = False

    async def get_evidence(self, *, board_id: str, evidence_id: str):
        item = self.evidence.get(evidence_id)
        return item if item is not None and item.board_id == board_id else None

    async def list_latest_evidence_classifications(
        self,
        *,
        board_id: str,
        evidence_ids: tuple[str, ...],
    ):
        return tuple(
            self.heads[evidence_id]
            for evidence_id in sorted(evidence_ids)
            if evidence_id in self.heads
            and self.heads[evidence_id].board_id == board_id
        )

    async def resolve_legacy_classification_batch_replay(
        self,
        *,
        board_id: str,
        classified_by: str,
        idempotency_key: str,
    ):
        return self.replays.get((board_id, classified_by, idempotency_key))

    async def get_evidence_classification(
        self,
        *,
        board_id: str,
        evidence_id: str,
        revision: int,
    ):
        item = self.history.get((evidence_id, revision))
        return item if item is not None and item.board_id == board_id else None

    async def append_legacy_evidence_classification_batch(
        self,
        *,
        receipt,
        expected_revisions: dict[str, int],
    ):
        self.append_calls += 1
        if self.fail_cas or any(
            (
                0
                if self.heads.get(evidence_id) is None
                else self.heads[evidence_id].revision
            )
            != revision
            for evidence_id, revision in expected_revisions.items()
        ):
            raise StoreRevisionConflict()
        for item in receipt.classifications:
            self.heads[item.evidence_id] = item
            self.history[(item.evidence_id, item.revision)] = item
        self.replays[
            (receipt.board_id, receipt.classified_by, receipt.idempotency_key)
        ] = receipt
        return receipt


async def _legacy_evidence(evidence_id: str = "legacy-1"):
    accepted, _service, _store, _clock = await _accepted(
        subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    authored = _evidence(
        accepted.receipt,
        evidence_id=evidence_id,
        parent_version=3,
    )
    return replace(
        authored,
        source_role=CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY,
        relevance_summary=None,
        scope_relation=None,
        source_origin=None,
        interpretation_limit=None,
        baseline_provenance=None,
        context_contract_version=None,
    )


def _baseline(evidence) -> CodeEvidenceBaselineProvenance:
    dirty = evidence.workspace_state.declared_dirty
    return CodeEvidenceBaselineProvenance(
        presence=(
            CodeEvidenceBaselinePresence.PREEXISTING_WORKTREE
            if dirty
            else CodeEvidenceBaselinePresence.COMMITTED_SNAPSHOT
        ),
        workspace_state_id=evidence.workspace_state.workspace_state_id,
        provenance_note=("Present before classification began." if dirty else None),
    )


def _contextual_evidence(
    evidence,
    *,
    evidence_id: str,
    source_role: CodeEvidenceSourceRole,
):
    return replace(
        evidence,
        id=evidence_id,
        source_role=source_role,
        relevance_summary="Human-reviewed effective source context.",
        scope_relation="same bounded delivery scope",
        source_origin="accepted repository baseline",
        interpretation_limit=(
            "Context only; it does not prove delivered behavior."
            if source_role
            in {
                CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
                CodeEvidenceSourceRole.REFERENCE_PATTERN,
            }
            else None
        ),
        baseline_provenance=_baseline(evidence),
        context_contract_version=2,
    )


def _command(
    *evidence,
    revision: int = 0,
    idempotency_key: str = "classify-legacy-1",
) -> LegacyEvidenceClassificationBatchInput:
    return LegacyEvidenceClassificationBatchInput(
        board_id="board-1",
        items=tuple(
            LegacyEvidenceClassificationItemInput(
                evidence_id=item.id,
                expected_evidence_payload_sha256=item.payload_sha256,
                expected_classification_revision=revision,
                source_role=CodeEvidenceSourceRole.EXISTING_CONSTRAINT,
                relevance_summary="This item constrains the planned change.",
                scope_relation="same bounded delivery scope",
                source_origin="repository baseline observed by the prior run",
                baseline_provenance=_baseline(item),
            )
            for item in reversed(evidence)
        ),
        justification="Human review classified ambiguous legacy evidence.",
        idempotency_key=idempotency_key,
    )


@pytest.mark.asyncio
async def test_human_batch_is_append_only_atomic_canonical_and_replayable() -> None:
    legacy_b = await _legacy_evidence("legacy-b")
    legacy_a = replace(legacy_b, id="legacy-a")
    store = _ClassificationStore((legacy_a, legacy_b))
    command = _command(legacy_a, legacy_b)
    service = LegacyCodeEvidenceClassificationService(
        clock=lambda: NOW,
        id_factory=_Ids(),
    )

    receipt = await service.classify(
        command,
        actor_id="user-1",
        store=store,  # type: ignore[arg-type]
    )

    assert tuple(item.evidence_id for item in receipt.classifications) == (
        "legacy-a",
        "legacy-b",
    )
    assert tuple(item.batch_item_index for item in receipt.classifications) == (
        1,
        2,
    )
    assert all(item.batch_item_count == 2 for item in receipt.classifications)
    assert all(item.revision == 1 for item in receipt.classifications)
    assert all(item.context_contract_version == 2 for item in receipt.classifications)
    assert all(
        item.justification == command.justification for item in receipt.classifications
    )
    assert all(item.classification_sha256 for item in receipt.classifications)
    assert store.evidence[legacy_a.id] is legacy_a
    assert store.evidence[legacy_b.id] is legacy_b
    assert legacy_a.source_role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY

    replay = await service.classify(
        command,
        actor_id="user-1",
        store=store,  # type: ignore[arg-type]
    )
    assert replay.replayed is True
    assert replay.batch_id == receipt.batch_id
    assert replay.classifications == receipt.classifications
    assert store.append_calls == 1

    revised = LegacyEvidenceClassificationBatchInput(
        board_id=command.board_id,
        items=tuple(
            item.model_copy(
                update={
                    "expected_classification_revision": 1,
                    "source_origin": "human-reviewed repository baseline",
                }
            )
            for item in command.items
        ),
        justification="A second human review corrected the source origin.",
        idempotency_key="classify-legacy-2",
    )
    second = await service.classify(
        revised,
        actor_id="user-1",
        store=store,  # type: ignore[arg-type]
    )
    assert all(item.revision == 2 for item in second.classifications)
    assert tuple(
        item.predecessor_classification_id for item in second.classifications
    ) == tuple(item.id for item in receipt.classifications)
    assert (
        await store.get_evidence_classification(
            board_id="board-1",
            evidence_id="legacy-a",
            revision=1,
        )
        is receipt.classifications[0]
    )
    assert store.heads["legacy-a"] is second.classifications[0]
    assert legacy_a.source_role is CodeEvidenceSourceRole.UNCATEGORIZED_LEGACY

    corrupt_first = replace(
        receipt.classifications[0],
        source_origin="corrupted persisted origin",
        classification_sha256=None,
    )
    store.replays[("board-1", "user-1", command.idempotency_key)] = replace(
        receipt,
        classifications=(corrupt_first, *receipt.classifications[1:]),
    )
    with pytest.raises(CodeEvidenceLegacyClassificationPersistenceConflict):
        await service.classify(
            command,
            actor_id="user-1",
            store=store,  # type: ignore[arg-type]
        )
    assert store.append_calls == 2


@pytest.mark.asyncio
async def test_classification_actor_id_is_bounded_before_persistence() -> None:
    legacy = await _legacy_evidence()
    store = _ClassificationStore((legacy,))
    service = LegacyCodeEvidenceClassificationService(
        clock=lambda: NOW,
        id_factory=_Ids(),
    )

    with pytest.raises(CodeInvestigationSubmissionLimitExceeded) as exc_info:
        await service.classify(
            _command(legacy),
            actor_id="a" * 256,
            store=store,  # type: ignore[arg-type]
        )

    assert exc_info.value.details == {
        "field": "code_evidence_legacy_classification_classified_by_invalid",
        "max_bytes": 255,
    }
    assert store.append_calls == 0


@pytest.mark.asyncio
async def test_invalid_batch_member_and_cas_race_never_partially_advance_heads() -> (
    None
):
    legacy = await _legacy_evidence()
    authored = replace(
        legacy,
        id="authored-v2",
        source_role=CodeEvidenceSourceRole.EXISTING_CONSTRAINT,
        relevance_summary="Existing constraint.",
        scope_relation="same bounded delivery scope",
        source_origin="repository baseline",
        baseline_provenance=_baseline(legacy),
        context_contract_version=2,
    )
    store = _ClassificationStore((legacy, authored))
    service = LegacyCodeEvidenceClassificationService(
        clock=lambda: NOW,
        id_factory=_Ids(),
    )

    with pytest.raises(CodeEvidenceLegacyClassificationLegacyRequired):
        await service.classify(
            _command(legacy, authored),
            actor_id="user-1",
            store=store,  # type: ignore[arg-type]
        )
    assert store.append_calls == 0
    assert store.heads == {}

    store.fail_cas = True
    with pytest.raises(CodeEvidenceLegacyClassificationRevisionConflict):
        await service.classify(
            _command(legacy),
            actor_id="user-1",
            store=store,  # type: ignore[arg-type]
        )
    assert store.heads == {}


@pytest.mark.asyncio
async def test_stale_evidence_payload_is_typed_and_stops_before_any_append() -> None:
    legacy = await _legacy_evidence()
    store = _ClassificationStore((legacy,))
    command = _command(legacy)
    stale_item = command.items[0].model_copy(
        update={"expected_evidence_payload_sha256": "f" * 64}
    )
    stale = command.model_copy(update={"items": (stale_item,)})

    with pytest.raises(CodeEvidenceLegacyClassificationPayloadConflict) as caught:
        await LegacyCodeEvidenceClassificationService(
            clock=lambda: NOW,
            id_factory=_Ids(),
        ).classify(
            stale,
            actor_id="user-1",
            store=store,  # type: ignore[arg-type]
        )

    assert caught.value.code == "code_evidence_legacy_classification_payload_conflict"
    assert caught.value.details == {"evidence_id": legacy.id}
    assert store.append_calls == 0
    assert store.heads == {}
    assert store.history == {}
    assert store.replays == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source", "actor_kind"),
    (("worker", "agent"), ("rest", "system"), ("mcp", "user")),
)
async def test_unsupported_identity_or_transport_is_rejected_before_any_read(
    source: str,
    actor_kind: str,
) -> None:
    legacy = await _legacy_evidence()
    actor = ActorContext(
        "actor-1",
        source,
        actor_kind=actor_kind,
        board_id="board-1",
        permissions=("code_traceability.evidence.classify_legacy",),
    )

    with pytest.raises(CodeEvidenceLegacyClassificationHumanRequired):
        await ClassifyLegacyCodeEvidenceUseCase().execute(
            _command(legacy),
            actor=actor,
            uow=object(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
async def test_rest_human_without_permission_is_denied_before_board_read() -> None:
    legacy = await _legacy_evidence()
    actor = ActorContext(
        "user-1",
        "rest",
        actor_kind="user",
        board_id="board-1",
        permissions=(),
    )

    with pytest.raises(PermissionDeniedError):
        await ClassifyLegacyCodeEvidenceUseCase().execute(
            _command(legacy),
            actor=actor,
            uow=object(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("actor_id", "source", "actor_kind"),
    (("user-1", "rest", "user"), ("agent-1", "mcp", "agent")),
)
async def test_authorized_actor_stages_metadata_only_events_and_no_replay_duplicates(
    actor_id: str,
    source: str,
    actor_kind: str,
) -> None:
    legacy = await _legacy_evidence()
    store = _ClassificationStore((legacy,))
    uow = FakeUnitOfWork(SimpleNamespace(), store)
    actor = ActorContext(
        actor_id,
        source,
        actor_kind=actor_kind,
        board_id="board-1",
        permissions=("code_traceability.evidence.classify_legacy",),
    )
    use_case = ClassifyLegacyCodeEvidenceUseCase(
        LegacyCodeEvidenceClassificationService(
            clock=lambda: NOW,
            id_factory=_Ids(),
        )
    )
    command = _command(legacy)

    result = await use_case.execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )

    assert result.replayed is False
    assert result.classifications[0].justification == command.justification
    assert uow.commit_count == 1
    assert len(uow.published_events) == 1
    event = uow.published_events[0]
    payload = event.payload_for_storage()
    assert event.event_type == "code_evidence.legacy_classified"
    assert payload["justification_sha256"] == code_traceability_event_digest(
        command.justification
    )
    assert "justification" not in payload
    assert "relative_path" not in payload
    assert "excerpt" not in payload

    replay = await use_case.execute(
        command,
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )
    assert replay.replayed is True
    assert uow.commit_count == 1
    assert len(uow.published_events) == 1


@pytest.mark.asyncio
async def test_effective_context_projection_is_source_blind_and_profile_redacted() -> (
    None
):
    legacy = await _legacy_evidence()
    store = _ClassificationStore((legacy,))
    receipt = await LegacyCodeEvidenceClassificationService(
        clock=lambda: NOW,
        id_factory=_Ids(),
    ).classify(
        _command(legacy),
        actor_id="user-1",
        store=store,  # type: ignore[arg-type]
    )
    classification = receipt.classifications[0]
    provenance = RefinementDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.BROWNFIELD,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(),
        evidence=(legacy,),
        classifications=(classification,),
    )
    link = CodeEvidenceSpecLink(
        id="link-1",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=legacy.id,
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="This evidence constrains the obligation.",
        evidence_content_sha256=legacy.content_sha256,
        source_refinement_version=3,
        spec_version=4,
        created_by="user-1",
        created_at=NOW,
    )
    evaluator = CodeTraceabilityGateEvaluator()
    settings = CodeTraceabilitySettings(mode="advisory")

    projected = {}
    for profile, context_scope in (
        (
            CodeTraceabilityProjectionProfile.SUMMARY,
            CodeTraceabilityContextScope.DEFAULT,
        ),
        (
            CodeTraceabilityProjectionProfile.DETAIL,
            CodeTraceabilityContextScope.DEFAULT,
        ),
        (CodeTraceabilityProjectionProfile.FULL, CodeTraceabilityContextScope.DEFAULT),
        (CodeTraceabilityProjectionProfile.FULL, CodeTraceabilityContextScope.GATE),
    ):
        reveal_actor = (
            profile
            in {
                CodeTraceabilityProjectionProfile.DETAIL,
                CodeTraceabilityProjectionProfile.FULL,
            }
            and context_scope is not CodeTraceabilityContextScope.GATE
        )
        context = CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
            profile=profile,
            context_scope=context_scope,
            evidence=(legacy,),
            evidence_links=(link,),
            source_context=summary,
            source_context_items=(
                source_context_evidence_item_v2(
                    legacy,
                    classification,
                    include_classification_actor=reveal_actor,
                ),
            ),
            source_context_classification_inputs=(
                (source_context_classification_input_v2(legacy, classification),)
                if reveal_actor
                else ()
            ),
        )
        projected[(profile, context_scope)] = evaluator.project(
            context,
            settings,
        ).as_dict()

    summary_payload = projected[
        (
            CodeTraceabilityProjectionProfile.SUMMARY,
            CodeTraceabilityContextScope.DEFAULT,
        )
    ]
    gate_payload = projected[
        (
            CodeTraceabilityProjectionProfile.FULL,
            CodeTraceabilityContextScope.GATE,
        )
    ]
    assert list(summary_payload).index("source_context_items") < list(
        summary_payload
    ).index("referenced_evidence_ids")
    summary_item = summary_payload["source_context_items"][0]
    assert summary_item == {
        "evidence_id": legacy.id,
        "context_contract_version": 2,
        "context_origin": "human_legacy_classification",
        "source_role": "existing_constraint",
        "relevance_summary": "This item constrains the planned change.",
        "scope_relation": "same bounded delivery scope",
        "source_origin": "repository baseline observed by the prior run",
        "interpretation_limit": None,
        "evidence_applicable": False,
    }
    assert "classified_by" not in summary_item
    assert "classified_by" not in gate_payload["source_context_items"][0]
    assert summary_payload["source_context_classification_inputs"] == []
    assert gate_payload["source_context_classification_inputs"] == []
    assert gate_payload["source_context_items"][0]["classification_revision"] == 1
    for profile in (
        CodeTraceabilityProjectionProfile.DETAIL,
        CodeTraceabilityProjectionProfile.FULL,
    ):
        item = projected[(profile, CodeTraceabilityContextScope.DEFAULT)][
            "source_context_items"
        ][0]
        assert item["classified_by"] == "user-1"
        assert item["classified_at"] == NOW.isoformat()
        assert projected[(profile, CodeTraceabilityContextScope.DEFAULT)][
            "source_context_classification_inputs"
        ] == [
            {
                "evidence_id": legacy.id,
                "expected_evidence_payload_sha256": legacy.payload_sha256,
                "expected_classification_revision": 1,
                "baseline_provenance": {
                    "presence": "committed_snapshot",
                    "workspace_state_id": legacy.workspace_state.workspace_state_id,
                    "provenance_note": None,
                    "provenance_note_required": False,
                },
            }
        ]

    mapping = summary_payload["obligation_evidence_mappings"][0]
    assert mapping["obligation_ref"] == "technical_requirement:tr-1"
    assert mapping["evidence_id"] == legacy.id
    assert mapping["evidence_applicable"] is False
    assert mapping["context_origin"] == (
        CodeEvidenceContextOrigin.HUMAN_LEGACY_CLASSIFICATION.value
    )


@pytest.mark.asyncio
async def test_contextual_coverage_excludes_context_only_evidence() -> None:
    legacy = await _legacy_evidence()
    current = _contextual_evidence(
        legacy,
        evidence_id="current-implementation",
        source_role=CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION,
    )
    context_only = _contextual_evidence(
        legacy,
        evidence_id="existing-scaffold",
        source_role=CodeEvidenceSourceRole.EXISTING_SCAFFOLD,
    )
    provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.HYBRID,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.HYBRID,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(
            ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        ),
        evidence=(current, context_only),
    )
    link = CodeEvidenceSpecLink(
        id="link-current",
        board_id="board-1",
        spec_id="spec-1",
        evidence_id=current.id,
        entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
        entity_id="tr-1",
        relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
        rationale="The implemented baseline supports this requirement.",
        evidence_content_sha256=current.content_sha256,
        source_refinement_version=3,
        spec_version=4,
        created_by="user-1",
        created_at=NOW,
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-1",
        subject_version=4,
        profile=CodeTraceabilityProjectionProfile.DETAIL,
        evidence=(current, context_only),
        evidence_links=(link,),
        source_refinement_id="refinement-1",
        source_refinement_snapshot_id="snapshot-3",
        source_refinement_version=3,
        source_context=summary,
        source_context_items=(
            source_context_evidence_item_v2(current),
            source_context_evidence_item_v2(context_only),
        ),
    )
    payload = (
        CodeTraceabilityGateEvaluator()
        .project(
            context,
            CodeTraceabilitySettings(mode="advisory"),
        )
        .as_dict()
    )

    assert payload["contextual_evidence_coverage"] == {
        "total": 1,
        "linked": 1,
        "dispositioned": 0,
        "pending": 0,
        "pending_ids": [],
        "unresolved_applicability_count": 0,
        "coverage_pct": 100.0,
        "projection_complete": True,
    }
    assert payload["coverage"]["total"] == 2
    assert payload["coverage"]["coverage_pct"] == 50.0


def test_contextual_coverage_greenfield_absence_is_not_applicable() -> None:
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        source_spec_id="spec-greenfield",
        source_spec_version=1,
    )
    summary = build_source_context_summary_v2(
        delivery_context=DeliveryContext.GREENFIELD,
        delivery_context_provenance=provenance,
        current_investigation_outcomes=(
            ContextualInvestigationOutcomeV2.NO_RELEVANT_EXISTING_IMPLEMENTATION,
        ),
        evidence=(),
    )
    context = CodeTraceabilityContext(
        board_id="board-1",
        subject_type=CodeTraceabilitySubjectType.SPEC,
        subject_id="spec-greenfield",
        subject_version=1,
        profile=CodeTraceabilityProjectionProfile.SUMMARY,
        source_context=summary,
    )
    payload = (
        CodeTraceabilityGateEvaluator()
        .project(
            context,
            CodeTraceabilitySettings(mode="advisory"),
        )
        .as_dict()
    )

    coverage = payload["contextual_evidence_coverage"]
    assert coverage["total"] == 0
    assert coverage["coverage_pct"] is None
    assert coverage["projection_complete"] is True


@pytest.mark.asyncio
async def test_contextual_coverage_unknown_partial_and_incomplete_are_null() -> None:
    legacy = await _legacy_evidence()
    current = _contextual_evidence(
        legacy,
        evidence_id="current-implementation",
        source_role=CodeEvidenceSourceRole.CURRENT_IMPLEMENTATION,
    )
    provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.HYBRID,
        inherited_value=DeliveryContext.HYBRID,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )

    def payload_for(
        *,
        outcomes: tuple[ContextualInvestigationOutcomeV2 | None, ...],
        include_unknown: bool = False,
        incomplete: bool = False,
    ) -> dict[str, object]:
        evidence = (current, legacy) if include_unknown else (current,)
        summary = build_source_context_summary_v2(
            delivery_context=DeliveryContext.HYBRID,
            delivery_context_provenance=provenance,
            current_investigation_outcomes=outcomes,
            evidence=evidence,
        )
        omitted = (
            (
                CodeTraceabilityOmittedContent(
                    collection="evidence",
                    hard_limit=CODE_TRACEABILITY_CONTEXT_COLLECTION_LIMITS["evidence"],
                    included_count=len(evidence),
                ),
            )
            if incomplete
            else ()
        )
        context = CodeTraceabilityContext(
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.SPEC,
            subject_id="spec-1",
            subject_version=4,
            profile=CodeTraceabilityProjectionProfile.DETAIL,
            evidence=evidence,
            omitted_content_manifest=omitted,
            source_refinement_id="refinement-1",
            source_refinement_snapshot_id="snapshot-3",
            source_refinement_version=3,
            source_context=summary,
            source_context_items=tuple(
                source_context_evidence_item_v2(item) for item in evidence
            ),
        )
        return (
            CodeTraceabilityGateEvaluator()
            .project(
                context,
                CodeTraceabilitySettings(mode="advisory"),
            )
            .as_dict()["contextual_evidence_coverage"]
        )

    unknown = payload_for(
        outcomes=(ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,),
        include_unknown=True,
    )
    partial = payload_for(
        outcomes=(ContextualInvestigationOutcomeV2.PARTIAL,),
    )
    legacy_outcome = payload_for(outcomes=(None,))
    incomplete = payload_for(
        outcomes=(ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,),
        incomplete=True,
    )

    assert unknown["unresolved_applicability_count"] == 1
    assert unknown["coverage_pct"] is None
    assert partial["coverage_pct"] is None
    assert legacy_outcome["coverage_pct"] is None
    assert incomplete["coverage_pct"] is None
    assert incomplete["projection_complete"] is False


@pytest.mark.asyncio
async def test_classification_only_rebase_invalidates_only_affected_evidence() -> None:
    evidence_a = _legacy_evidence_record(
        "legacy-a",
        parent_version=3,
        payload_sha256=H1,
    )
    evidence_b = _legacy_evidence_record(
        "legacy-b",
        parent_version=3,
        payload_sha256=H2,
    )
    classification_store = _ClassificationStore((evidence_a, evidence_b))
    classification = (
        await LegacyCodeEvidenceClassificationService(
            clock=lambda: NOW,
            id_factory=_Ids(),
        ).classify(
            _command(evidence_a),
            actor_id="user-1",
            store=classification_store,  # type: ignore[arg-type]
        )
    ).classifications[0]
    fence = source_context_classification_fence_v2((classification,))

    current_context, current_context_sha256 = _source_context_manifest(
        refinement_version=3,
        delivery_context=DeliveryContext.BROWNFIELD,
        receipt_id="receipt-context",
        generation=3,
        head_revision=3,
        receipt_sha256=H1,
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
    )
    target_context, target_context_sha256 = _source_context_manifest(
        refinement_version=4,
        delivery_context=DeliveryContext.BROWNFIELD,
        receipt_id="receipt-context",
        generation=3,
        head_revision=3,
        receipt_sha256=H1,
        outcome=ContextualInvestigationOutcomeV2.EVIDENCE_APPLICABLE,
        role_counts={
            "current_implementation_count": 0,
            "existing_scaffold_count": 0,
            "existing_constraint_count": 1,
            "reference_pattern_count": 0,
            "uncategorized_legacy_count": 1,
        },
        classification_revision=fence.revision,
        classification_sha256=fence.payload_sha256,
    )
    current_evidence_manifest = [
        {
            "evidence_id": item.id,
            "content_sha256": item.content_sha256,
            "lifecycle_status": "active",
            "context_sha256": _context_sha256(),
            "classification_revision": None,
            "classification_sha256": None,
        }
        for item in (evidence_a, evidence_b)
    ]
    target_evidence_manifest = [dict(item) for item in current_evidence_manifest]
    target_a = target_evidence_manifest[0]
    target_a.update(
        {
            "context_contract_version": 2,
            "context_origin": "human_legacy_classification",
            "context_sha256": canonical_code_traceability_sha256(
                source_context_evidence_payload_v2(
                    source_context_evidence_item_v2(
                        evidence_a,
                        classification,
                    )
                )
            ),
            "classification_revision": classification.revision,
            "classification_sha256": classification.classification_sha256,
        }
    )

    investigations = FakeInvestigationStore()
    traceability = FakeTraceabilityStore(investigations)
    traceability.evidence = {
        evidence_a.id: evidence_a,
        evidence_b.id: evidence_b,
    }
    traceability.evidence_classifications[(evidence_a.id, 1)] = classification
    links = tuple(
        CodeEvidenceSpecLink(
            id=f"link-{item.id}",
            board_id="board-1",
            spec_id="spec-1",
            evidence_id=item.id,
            entity_type=SpecEntityType.TECHNICAL_REQUIREMENT,
            entity_id=f"tr-{item.id}",
            relation_type=CodeEvidenceSpecRelationType.SUPPORTS,
            rationale="Pinned evidence supports this obligation.",
            evidence_content_sha256=item.content_sha256,
            source_refinement_version=3,
            spec_version=7,
            created_by="user-1",
            created_at=NOW,
        )
        for item in (evidence_a, evidence_b)
    )
    dispositions = tuple(
        CodeEvidenceDisposition(
            id=f"disposition-{item.id}",
            board_id="board-1",
            spec_id="spec-1",
            evidence_id=item.id,
            disposition=CodeEvidenceDispositionKind.NOT_RELEVANT,
            justification="Reviewed against the pinned Spec.",
            spec_version=7,
            active=True,
            created_by="user-1",
            created_at=NOW,
            cleared_by=None,
            cleared_at=None,
        )
        for item in (evidence_a, evidence_b)
    )
    traceability.links = {item.id: item for item in links}
    traceability.dispositions = {
        (item.spec_id, item.evidence_id): item for item in dispositions
    }
    uow = FakeUnitOfWork(investigations, traceability)
    uow.services.refinements.snapshots = {
        3: SimpleNamespace(
            id="snapshot-3",
            refinement_id="refinement-1",
            version=3,
            delivery_context=DeliveryContext.BROWNFIELD,
            code_evidence_manifest=current_evidence_manifest,
            source_context_manifest=current_context,
            source_context_sha256=current_context_sha256,
        ),
        4: SimpleNamespace(
            id="snapshot-4",
            refinement_id="refinement-1",
            version=4,
            delivery_context=DeliveryContext.BROWNFIELD,
            code_evidence_manifest=target_evidence_manifest,
            source_context_manifest=target_context,
            source_context_sha256=target_context_sha256,
        ),
    }
    current_provenance = SpecDeliveryContextProvenance(
        value=DeliveryContext.BROWNFIELD,
        inherited_value=DeliveryContext.BROWNFIELD,
        source_refinement_id="refinement-1",
        source_refinement_version=3,
    )

    async def get_spec(spec_id: str):
        if spec_id != "spec-1":
            return None
        return SimpleNamespace(
            id="spec-1",
            board_id="board-1",
            version=7,
            status="draft",
            refinement_id="refinement-1",
            source_refinement_snapshot_id="snapshot-3",
            source_refinement_version=3,
            delivery_context=DeliveryContext.BROWNFIELD,
            delivery_context_provenance=current_provenance,
            source_context_manifest=current_context,
            source_context_sha256=current_context_sha256,
        )

    uow.services.specs.get_spec = get_spec
    actor = ActorContext(
        "user-1",
        "rest",
        actor_kind="user",
        board_id="board-1",
        permissions=("code_traceability.spec_link.rebase",),
    )

    preview = await PreviewSpecCodeEvidenceRebaseUseCase(
        SpecCodeEvidenceRebaseService(clock=lambda: NOW)
    ).execute(
        SpecCodeEvidenceRebasePreviewInput(
            board_id="board-1",
            spec_id="spec-1",
            target_refinement_version=4,
            expected_spec_version=7,
        ),
        actor=actor,
        uow=uow,  # type: ignore[arg-type]
    )

    assert preview.delivery_context_delta.effective_value_changed is False
    assert preview.source_context_delta.investigation.changed is False
    assert preview.source_context_delta.classification.overlay_changed_evidence_ids == (
        evidence_a.id,
    )
    assert preview.stale_link_ids == (f"link-{evidence_a.id}",)
    assert preview.invalid_disposition_ids == (f"disposition-{evidence_a.id}",)
    assert f"link-{evidence_b.id}" not in preview.stale_link_ids
    assert f"disposition-{evidence_b.id}" not in preview.invalid_disposition_ids

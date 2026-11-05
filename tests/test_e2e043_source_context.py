"""R2 source provenance must survive only proven status-only transitions."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import uuid

import pytest
from sqlalchemy import select

from okto_pulse.core.domain.refinement_source_compatibility import (
    FIELD,
    MAX_HISTORY_ROWS,
    MAX_NEUTRAL_AUDITS,
    compatibility_change,
    refinement_source_digest,
    resolve_source_compatibility,
)
from okto_pulse.core.domain.enums import IdeationStatus, RefinementStatus
from okto_pulse.core.models.schemas import RefinementMove
from okto_pulse.core.services.main import RefinementService


def _subject():
    return SimpleNamespace(
        id="ref",
        board_id="board",
        ideation_id="idea",
        edition=1,
        title="Preserve context",
        description="A bounded source review",
        in_scope=["source"],
        status="draft",
        version=1,
        delivery_context="brownfield",
    )


def _move(subject, status):
    before_version, before_status, digest = (
        subject.version,
        subject.status,
        refinement_source_digest(subject),
    )
    subject.version += 1
    subject.status = status
    proof = compatibility_change(
        subject=subject,
        before_version=before_version,
        before_status=before_status,
        before_digest=digest,
    )
    return {
        "id": f"history-{subject.version}",
        "refinement_id": subject.id,
        "action": "status_changed",
        "version": subject.version,
        "changes": [{"field": "status", "old": before_status, "new": status}, proof],
    }


def test_source_compatibility_is_explicit_and_contiguous():
    subject = _subject()
    history = [
        _move(subject, "review"),
        _move(subject, "approved"),
        _move(subject, "done"),
    ]
    history.append(
        {
            "id": "assessment-projection",
            "refinement_id": subject.id,
            "action": "quality_assessment_lifecycle",
            "version": 2,
            "changes": [
                {
                    "field": "quality_assessment_projection",
                    "old": "review",
                    "new": "approved",
                }
            ],
        }
    )
    proof = resolve_source_compatibility(subject, history)
    assert proof.compatible_versions == (4, 3, 2, 1)
    assert proof.history_ids == ("history-4", "history-3", "history-2")
    assert subject.version == 4  # operational versions and history remain intact


@pytest.mark.parametrize(
    "mutation",
    [
        "content",
        "child_version",
        "edition",
        "foreign_board",
        "foreign_subject",
        "tampered_digest",
        "duplicate",
        "legacy",
        "extra_change",
        "gap",
        "wrong_status",
    ],
)
def test_source_compatibility_fails_closed_at_an_unproven_edge(mutation):
    subject = _subject()
    history = [_move(subject, "review"), _move(subject, "approved")]
    proof = history[-1]["changes"][1]["new"]
    if mutation == "content":
        subject.description = "Changed content"
    elif mutation == "child_version":
        subject.version += 1
    elif mutation == "edition":
        subject.edition += 1
    elif mutation == "foreign_board":
        proof["board_id"] = "elsewhere"
    elif mutation == "foreign_subject":
        history[-1]["refinement_id"] = "elsewhere"
    elif mutation == "tampered_digest":
        proof["after_sha256"] = "0" * 64
    elif mutation == "duplicate":
        history.append(deepcopy(history[-1]))
    elif mutation == "legacy":
        history[-1]["changes"].pop()
    elif mutation == "extra_change":
        history[-1]["changes"].append({"field": "analysis"})
    elif mutation == "gap":
        proof["before_version"] = 1
    elif mutation == "wrong_status":
        subject.status = "cancelled"
    assert resolve_source_compatibility(subject, history) is None


def _quality_audits(subject, count):
    rows, receipts = [], []
    for index in range(count):
        identity, previous = (
            f"quality-{index}",
            f"quality-{index - 1}" if index else None,
        )
        row = {
            "id": f"audit-{index}",
            "refinement_id": subject.id,
            "version": subject.version,
            "action": "quality_assessment_recorded",
            "changes": [
                {"field": "quality_assessment_head", "old": previous, "new": identity}
            ],
        }
        receipts.append(
            {
                "id": identity,
                "board_id": subject.board_id,
                "subject_type": "refinement",
                "subject_id": subject.id,
                "subject_version": subject.version,
                "subject_edition": subject.edition,
                "assessment_kind": "ambiguity",
                "source": "native",
                "history_id": row["id"],
                "predecessor_receipt_id": previous,
            }
        )
        rows.append(row)
    return rows, receipts


@pytest.mark.parametrize(
    "audit_count", [1, 2, 61, MAX_NEUTRAL_AUDITS, MAX_NEUTRAL_AUDITS + 1]
)
def test_native_quality_audits_preserve_source_chain_with_finite_budget(audit_count):
    subject = _subject()
    history = [_move(subject, "review"), _move(subject, "approved")]
    audits, receipts = _quality_audits(subject, audit_count)
    original = deepcopy((history, audits, receipts))
    proof = resolve_source_compatibility(
        subject, history + audits, quality_receipts=receipts
    )
    if audit_count <= MAX_NEUTRAL_AUDITS:
        assert proof.compatible_versions == (3, 2, 1)
        assert proof.history_ids == ("history-3", "history-2")
    else:
        assert proof is None
    assert (history, audits, receipts) == original


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_receipt",
        "extra_change",
        "extra_field",
        "foreign_row",
        "foreign_board",
        "foreign_subject",
        "wrong_version",
        "wrong_edition",
        "wrong_kind",
        "wrong_source",
        "wrong_history",
        "wrong_predecessor",
        "duplicate_audit",
        "duplicate_receipt",
        "invalid_previous",
        "forged_lifecycle",
        "foreign_lifecycle",
    ],
)
def test_quality_audit_neutrality_requires_exact_native_identity(mutation):
    subject = _subject()
    history = [_move(subject, "review"), _move(subject, "approved")]
    audits, receipts = _quality_audits(subject, 2)
    row, receipt = audits[-1], receipts[-1]
    fields = {
        "foreign_board": ("board_id", "elsewhere"),
        "foreign_subject": ("subject_id", "elsewhere"),
        "wrong_version": ("subject_version", 2),
        "wrong_edition": ("subject_edition", 2),
        "wrong_kind": ("assessment_kind", "requirement_lint"),
        "wrong_source": ("source", "forged"),
        "wrong_history": ("history_id", "elsewhere"),
        "wrong_predecessor": ("predecessor_receipt_id", None),
    }
    if mutation in fields:
        field, value = fields[mutation]
        receipt[field] = value
    elif mutation == "missing_receipt":
        receipts.pop()
    elif mutation == "extra_change":
        row["changes"].append({"field": "analysis"})
    elif mutation == "extra_field":
        row["changes"][0]["content"] = "changed"
    elif mutation == "foreign_row":
        row["refinement_id"] = "elsewhere"
    elif mutation == "duplicate_audit":
        audits.append(deepcopy(row))
    elif mutation == "duplicate_receipt":
        receipts.append(deepcopy(receipt))
    elif mutation == "invalid_previous":
        row["changes"][0]["old"] = []
    elif mutation in ("forged_lifecycle", "foreign_lifecycle"):
        row["action"] = "quality_assessment_lifecycle"
        row["changes"] = [
            {
                "field": "quality_assessment_projection",
                "old": "review",
                "new": "approved",
            }
        ]
        if mutation == "forged_lifecycle":
            row["changes"][0]["extra"] = True
        else:
            row["refinement_id"] = "elsewhere"
    assert (
        resolve_source_compatibility(
            subject, history + audits, quality_receipts=receipts
        )
        is None
    )


@pytest.mark.parametrize("row_count", [MAX_HISTORY_ROWS, MAX_HISTORY_ROWS + 1])
def test_history_scan_exhaustion_cannot_grant_a_compatible_prefix(row_count):
    subject = _subject()
    history = [_move(subject, "review"), _move(subject, "approved")]
    # Old non-compatible history remains outside the proven 3 -> 2 -> 1 edges.
    history.extend(
        {
            "id": f"old-{i}",
            "refinement_id": subject.id,
            "version": 1,
            "action": "created",
            "changes": [],
        }
        for i in range(row_count - len(history))
    )
    proof = resolve_source_compatibility(subject, history)
    assert (proof is not None) is (row_count == MAX_HISTORY_ROWS)


def test_previous_edition_audits_cannot_invalidate_a_new_source_baseline():
    subject = _subject()
    history = [_move(subject, "review"), _move(subject, "approved")]
    audits, receipts = _quality_audits(subject, 1)
    history.extend(audits)
    subject.edition, subject.version, subject.status = 2, 4, "draft"
    history.append(
        {
            "id": "reopened",
            "refinement_id": subject.id,
            "version": 4,
            "action": "status_changed",
            "changes": [{"field": "status", "old": "approved", "new": "draft"}],
        }
    )
    history.extend([_move(subject, "review"), _move(subject, "approved")])
    proof = resolve_source_compatibility(subject, history, quality_receipts=receipts)
    assert proof.compatible_versions == (6, 5, 4)
    assert 3 not in proof.compatible_versions  # old edition remains historical


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["current", "no_proof", "expired", "revoked", "head_changed"]
)
async def test_compatible_receipt_still_requires_current_native_authority(case):
    from test_code_traceability_gate import _accepted, _evidence, _head
    from okto_pulse.core.domain import code_traceability as domain
    from okto_pulse.core.services.code_traceability_gate import (
        CodeTraceabilityGateEvaluator,
        CodeTraceabilityGatePhase,
        CodeTraceabilitySettings,
    )

    accepted, _, store, clock = await _accepted(
        subject_type=domain.CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=3,
    )
    receipt = accepted.receipt
    evidence = _evidence(receipt, evidence_id="evidence-compatible", parent_version=3)
    subject = _subject()
    subject.id, subject.board_id = "refinement-1", "board-1"
    subject.version, subject.status = 3, "review"
    compatibility = resolve_source_compatibility(subject, [_move(subject, "approved")])
    head = _head(store, receipt)
    revocations = ()
    if case == "no_proof":
        compatibility = None
    elif case == "expired":
        clock.value = receipt.expires_at + timedelta(seconds=1)
    elif case == "head_changed":
        head = replace(head, current_receipt_id="another-receipt")
    elif case == "revoked":
        revocations = (
            domain.CodeInvestigationReceiptRevocation(
                id="revocation",
                receipt_id=receipt.id,
                board_id="board-1",
                reason_code="test_revocation",
                justification="Withdrawn source observation",
                revoked_by="owner",
                revoked_at=clock(),
            ),
        )
    context = domain.CodeTraceabilityContext(
        board_id="board-1",
        subject_type=domain.CodeTraceabilitySubjectType.REFINEMENT,
        subject_id="refinement-1",
        subject_version=4,
        profile=domain.CodeTraceabilityProjectionProfile.FULL,
        context_scope=domain.CodeTraceabilityContextScope.GATE,
        heads=(head,),
        receipts=(receipt,),
        receipt_revocations=revocations,
        evidence=(evidence,),
        refinement_source_compatibility=compatibility,
    )
    evaluation = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
        context,
        CodeTraceabilitySettings(
            mode="blocking",
            receipt_content="metadata_only",
            evidence_attestation="required",
        ),
        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
        referenced_evidence_ids=(evidence.id,),
    )
    assert evaluation.allowed is (case == "current")
    assert receipt.subject_version == evidence.parent_version == 3


@pytest.fixture
async def native_source_factory(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from okto_pulse.community.adapters.sqlalchemy_models import Base
    from okto_pulse.community.adapters.sqlalchemy_policy_subject_versioning import (
        CommunitySemanticSession,
    )
    from okto_pulse.community.adapters.sqlalchemy_application_persistence import (
        CommunitySqlAlchemyApplicationPersistence,
    )
    from okto_pulse.community.adapters.relational_application import (
        CommunityRelationalApplicationAdapter,
    )
    from okto_pulse.core.ports.application_persistence import (
        register_application_persistence_port,
    )
    from okto_pulse.core.ports.relational_application import (
        register_relational_application_adapter,
    )
    from okto_pulse.core.domain.realm import RealmScope

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'native-source-context.db'}"
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    db_factory = async_sessionmaker(
        engine,
        sync_session_class=CommunitySemanticSession,
        expire_on_commit=False,
        info={"realm_scope": RealmScope.local()},
    )
    register_application_persistence_port(CommunitySqlAlchemyApplicationPersistence())
    register_relational_application_adapter(CommunityRelationalApplicationAdapter())
    try:
        yield db_factory
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_count", [0, 2, 61])
async def test_native_lifecycle_records_actual_version_and_freezes_compatible_context(
    native_source_factory,
    audit_count,
):
    from okto_pulse.community.adapters.sqlalchemy_models import (
        Board,
        Ideation,
        Refinement,
        RefinementHistory,
        RefinementSnapshot,
        ResourceNotApplicable,
        QualityAssessmentReceiptRow,
    )
    from okto_pulse.community.adapters.sqlalchemy_policy_subject_versioning import (
        bind_semantic_subject_actor,
    )
    from okto_pulse.core.application.use_cases.base import ActorContext
    from okto_pulse.core.ports.application_persistence import (
        get_application_persistence_port,
    )

    db_factory = native_source_factory
    board_id = "board-1"
    idea_id, refinement_id = (str(uuid.uuid4()) for _ in range(2))
    actor = "source-context-owner"
    evidence_id = "code_evidence_r2h_fixture"
    async with db_factory() as db:
        bind_semantic_subject_actor(
            db, ActorContext(actor, "test", actor_kind="user", board_id=board_id)
        )
        db.add(
            Board(
                id=board_id,
                realm_id="local",
                name="Source context fixture",
                owner_id=actor,
                settings={},
            )
        )
        db.add(
            Ideation(
                id=idea_id,
                board_id=board_id,
                title="Parent",
                status=IdeationStatus.DONE,
                created_by=actor,
            )
        )
        db.add(
            Refinement(
                id=refinement_id,
                board_id=board_id,
                ideation_id=idea_id,
                title="Source review",
                in_scope=["Receipt provenance"],
                analysis=f"Inspected baseline evidence:{evidence_id}",
                status=RefinementStatus.DRAFT,
                edition=1,
                version=1,
                delivery_context="brownfield",
                created_by=actor,
                skip_ambiguity_gate=not audit_count,
                skip_ambiguity_gate_edition=1 if not audit_count else None,
            )
        )
        # Fixture-only resource declarations keep this test focused on the
        # native lifecycle/source owners without inventing an architecture UI.
        for resource_type in ("architecture", "mockup"):
            db.add(
                ResourceNotApplicable(
                    board_id=board_id,
                    entity_type="refinement",
                    entity_id=refinement_id,
                    resource_type=resource_type,
                    justification="Isolated source-context contract fixture",
                    created_by=actor,
                )
            )
        await get_application_persistence_port().commit(db)
    from test_code_traceability_application import (
        accepted_receipt,
        evidence_submission,
        MutableClock,
        StableIds,
        challenge_policy,
    )
    from okto_pulse.core.services.code_investigation import (
        CodeInvestigationService,
        required_capabilities_for_subject,
        selector_scope_digest_for_subject,
    )
    from okto_pulse.core.domain import code_traceability as domain
    from okto_pulse.community.adapters.relational_application import (
        CommunityRelationalApplicationAdapter,
    )

    clock = MutableClock(datetime.now(timezone.utc))
    investigation_service = CodeInvestigationService(
        challenge_policy=challenge_policy(), clock=clock, id_factory=StableIds()
    )
    async with db_factory() as db:
        accepted = await accepted_receipt(
            service=investigation_service,
            store=CommunityRelationalApplicationAdapter().code_investigations(db),
            clock=clock,
            actor_id="fixture-agent",
            subject_type=domain.CodeTraceabilitySubjectType.REFINEMENT,
            subject_id=refinement_id,
            subject_version=1,
            source_ref=None,
            selector_scope_digest=selector_scope_digest_for_subject(
                board_id=board_id,
                subject_type=domain.CodeTraceabilitySubjectType.REFINEMENT,
                subject_id=refinement_id,
                subject_version=1,
            ),
            capabilities=required_capabilities_for_subject(
                domain.CodeTraceabilitySubjectType.REFINEMENT
            ),
            request_key="fixture-request",
            receipt_key="fixture-receipt",
        )
        from okto_pulse.core.services.code_evidence import CodeEvidenceService
        from okto_pulse.core.models.code_traceability import CodeEvidenceSelectorInput

        submission = evidence_submission(
            receipt_id=accepted.receipt.id, idempotency_key="native-source-evidence"
        ).model_copy(
            update={
                "parent_id": refinement_id,
                "selector": CodeEvidenceSelectorInput(
                    kind=domain.CodeEvidenceSelectorKind.FILE,
                    relative_path="src/service.py",
                    language="python",
                ),
                "excerpt": None,
                "excerpt_sha256": None,
            }
        )
        evidence = await CodeEvidenceService(
            clock=clock, id_factory=lambda _: evidence_id
        ).submit(
            submission,
            actor_id="fixture-agent",
            actor_kind="agent",
            current_parent_version=1,
            minimum_trust=domain.CodeInvestigationTrustLevel.SINGLE_ATTESTATION,
            require_committed_state=True,
            investigation_service=investigation_service,
            investigation_store=CommunityRelationalApplicationAdapter().code_investigations(
                db
            ),
            store=CommunityRelationalApplicationAdapter().code_traceability(db),
            receipt_content="metadata_only",
        )
        assert evidence.evidence.parent_version == 1
        await db.commit()
    from okto_pulse.core.application.use_cases.quality_assessment import (
        RecordAmbiguityAssessmentCommand,
        RecordAmbiguityAssessmentUseCase,
    )
    from okto_pulse.core.domain.quality_assessment import AssessmentSubjectType
    from okto_pulse.core.domain.realm import RealmScope
    from okto_pulse.community.adapters.sqlalchemy_quality_assessment import (
        CommunitySqlAlchemyQualityAssessmentPreflightReader,
    )
    from okto_pulse.community.adapters.sqlalchemy_unit_of_work import (
        CommunityUnitOfWorkFactory,
    )

    assessment = RecordAmbiguityAssessmentUseCase(
        preflight_reader=CommunitySqlAlchemyQualityAssessmentPreflightReader(
            db_factory
        ),
        uow_factory=CommunityUnitOfWorkFactory(db_factory),
    )
    quality_actor = ActorContext(
        actor,
        "rest",
        actor_kind="user",
        board_id=board_id,
        realm_scope=RealmScope.local(),
    )
    head_revision = 0

    async def record_quality(version):
        nonlocal head_revision
        result = await assessment.execute(
            RecordAmbiguityAssessmentCommand(
                board_id=board_id,
                subject_type=AssessmentSubjectType.REFINEMENT,
                subject_id=refinement_id,
                idempotency_key=f"source-quality-{head_revision}",
                expected_subject_version=version,
                expected_subject_edition=1,
                expected_head_revision=head_revision,
                score=0,
                summary="Exact source identity and bounded behavior are defined.",
            ),
            actor=quality_actor,
        )
        head_revision += 1
        return result

    for expected, status in enumerate(
        (RefinementStatus.REVIEW, RefinementStatus.APPROVED, RefinementStatus.DONE), 2
    ):
        if audit_count and status == RefinementStatus.DONE:
            for index in range(audit_count):
                await record_quality(3)
                if index not in (0, 1, audit_count - 1):
                    continue
                async with db_factory() as db:
                    from okto_pulse.core.ports.code_traceability import (
                        CodeTraceabilityProjectionQuery,
                    )
                    from okto_pulse.core.services.code_traceability_gate import (
                        CodeTraceabilityGateEvaluator,
                        CodeTraceabilityGatePhase,
                        CodeTraceabilitySettings,
                    )

                    port = (
                        CommunityRelationalApplicationAdapter().code_traceability_read(
                            db
                        )
                    )
                    source = await port.refinement_source_compatibility(
                        board_id=board_id, refinement_id=refinement_id
                    )
                    context = await port.refinement_context(
                        CodeTraceabilityProjectionQuery(
                            board_id=board_id,
                            subject_type=domain.CodeTraceabilitySubjectType.REFINEMENT,
                            subject_id=refinement_id,
                            subject_version=3,
                            profile=domain.CodeTraceabilityProjectionProfile.FULL,
                        )
                    )
                    assert source == context.refinement_source_compatibility
                    assert source.compatible_versions == (3, 2, 1)
                    assert [item.id for item in context.evidence] == [evidence_id]
                    assert [item.id for item in context.receipts] == [
                        accepted.receipt.id
                    ]
                    gate = CodeTraceabilityGateEvaluator(clock=clock).evaluate(
                        context,
                        CodeTraceabilitySettings(
                            mode="blocking",
                            receipt_content="metadata_only",
                            evidence_attestation="required",
                        ),
                        phases=(CodeTraceabilityGatePhase.REFINEMENT_EVIDENCE,),
                        referenced_evidence_ids=(evidence_id,),
                    )
                    assert gate.allowed, gate.blockers
        async with db_factory() as db:
            bind_semantic_subject_actor(
                db, ActorContext(actor, "test", actor_kind="user", board_id=board_id)
            )
            service = RefinementService(db)
            before = await service.get_refinement(refinement_id)
            before_digest = refinement_source_digest(before)
            moved = await service.move_refinement(
                refinement_id, actor, RefinementMove(status=status), actor_name="Owner"
            )
            assert moved.version == expected
            assert refinement_source_digest(moved) == before_digest, (
                before.values,
                moved.values,
            )
            await get_application_persistence_port().commit(db)
    async with db_factory() as db:
        refinement = await RefinementService(db).get_refinement(refinement_id)
        history = (
            (
                await db.execute(
                    select(RefinementHistory).where(
                        RefinementHistory.refinement_id == refinement_id
                    )
                )
            )
            .scalars()
            .all()
        )
        quality_receipts = (
            (await db.execute(select(QualityAssessmentReceiptRow))).scalars().all()
        )
        assert len(quality_receipts) == audit_count
        proof = resolve_source_compatibility(
            refinement, history, quality_receipts=quality_receipts
        )
        assert proof.compatible_versions == (4, 3, 2, 1), [
            (
                row.action,
                row.version,
                [change.get("field") for change in row.changes or []],
            )
            for row in history
        ]
        native_refinement = await db.get(Refinement, refinement_id)
        assert (
            resolve_source_compatibility(
                native_refinement, history, quality_receipts=quality_receipts
            )
            == proof
        )
        assert all(
            any(change.get("field") == FIELD for change in row.changes)
            for row in history
            if row.action == "status_changed"
        )
        snapshot = (
            await db.execute(
                select(RefinementSnapshot).where(
                    RefinementSnapshot.refinement_id == refinement_id
                )
            )
        ).scalar_one()
        assert snapshot.version == refinement.version == 4
        assert snapshot.source_context_manifest["subject_version"] == 4
        assert [item["evidence_id"] for item in snapshot.code_evidence_manifest] == [
            evidence_id
        ]
        assert [
            item["receipt_id"]
            for item in snapshot.source_context_manifest["current_receipts"]
        ] == [accepted.receipt.id]
        # The original signed observation stays at v1; the snapshot references
        # that immutable receipt rather than replacing or rebinding it to v4.
        stored = (
            await CommunityRelationalApplicationAdapter()
            .code_investigations(db)
            .get_receipt(board_id=board_id, receipt_id=accepted.receipt.id)
        )
        assert stored.subject_version == 1
        frozen_digest = snapshot.source_context_sha256
        frozen_snapshot_id = snapshot.id
    async with db_factory() as db:
        bind_semantic_subject_actor(
            db, ActorContext(actor, "test", actor_kind="user", board_id=board_id)
        )
        derived = await RefinementService(db).derive_spec(refinement_id, actor)
        assert derived.source_refinement_snapshot_id == frozen_snapshot_id
        assert derived.source_context_sha256 == frozen_digest
        assert derived.source_refinement_version == 4
        await get_application_persistence_port().commit(db)
    async with db_factory() as db:
        bind_semantic_subject_actor(
            db, ActorContext(actor, "test", actor_kind="user", board_id=board_id)
        )
        reopened = await RefinementService(db).move_refinement(
            refinement_id,
            actor,
            RefinementMove(status=RefinementStatus.DRAFT),
            actor_name="Owner",
        )
        assert reopened.edition == 2
        await get_application_persistence_port().commit(db)
        reopened_version = reopened.version
    async with db_factory() as db:
        from okto_pulse.core.ports.code_traceability import (
            CodeTraceabilityProjectionQuery,
        )

        context = (
            await CommunityRelationalApplicationAdapter()
            .code_traceability_read(db)
            .refinement_context(
                CodeTraceabilityProjectionQuery(
                    board_id=board_id,
                    subject_type=domain.CodeTraceabilitySubjectType.REFINEMENT,
                    subject_id=refinement_id,
                    subject_version=reopened_version,
                    profile=domain.CodeTraceabilityProjectionProfile.FULL,
                )
            )
        )
        assert context.refinement_source_compatibility is None
        assert context.source_context_snapshot is None
        assert context.source_context.investigation_outcome is None
        old_snapshot = (
            await db.execute(
                select(RefinementSnapshot).where(
                    RefinementSnapshot.refinement_id == refinement_id
                )
            )
        ).scalar_one()
        assert old_snapshot.source_context_sha256 == frozen_digest

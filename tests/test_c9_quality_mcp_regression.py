"""C9 regression contracts for the public SK-A Quality surface.

These tests intentionally exercise only Core-owned public contracts.  Edition
adapters have their own REST/SQL suites, but both transports consume the
projectors, pagination rules, errors, and resource manifest pinned here.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases import quality_assessment as quality_uc
from okto_pulse.core.application.use_cases.base import ActorContext
from okto_pulse.core.application.use_cases.quality_assessment import (
    GetQualityAssessmentReceiptResult,
    ListQualityReceiptFindingsCommand,
    QualityAssessmentReadUseCases,
)
from okto_pulse.core.domain.quality_assessment import (
    AssessmentCurrentness,
    AssessmentDigestSet,
    AssessmentKind,
    AssessmentOrigin,
    AssessmentOutcome,
    AssessmentReceipt,
    AssessmentReceiptState,
    AssessmentReceiptView,
    AssessmentScale,
    AssessmentScaleKind,
    AssessmentSource,
    AssessmentStaleReason,
    AssessmentSubjectHead,
    AssessmentSubjectRef,
    AssessmentSubjectType,
    AssessmentVersionSet,
    QualityPage,
    QualityPageCursor,
    QualityAssessmentContractError,
    ScoreDirection,
)
from okto_pulse.core.domain.realm import RealmScope
from okto_pulse.core.inbound.quality_assessment_error import (
    project_quality_assessment_error,
)
from okto_pulse.core.inbound.ska_contract_error import (
    project_ska_contract_error,
)
from okto_pulse.core.mcp import server
from okto_pulse.core.mcp.ska_resource_manifest import (
    build_ska_resource_manifest,
    checked_in_manifest_path,
    verify_checked_in_manifest,
)
from okto_pulse.core.models.quality_assessment import (
    decode_quality_cursor,
    project_current_quality_assessment,
    project_quality_keyset_page,
    project_quality_receipt_currentness,
    project_quality_receipt_view,
)
from okto_pulse.core.ports import quality_assessment as quality_ports
from okto_pulse.core.ports.quality_assessment import (
    AssessmentHeadRevisionConflict,
    QualityAssessmentReadContext,
)
from okto_pulse.core.services.quality_assessment import (
    CurrentAssessmentView,
    QualityAssessmentConflictError,
    QualityAssessmentForbiddenError,
    QualityAssessmentNotFoundError,
    QualityAssessmentValidationError,
    QualityGatePreview,
)


NOW = datetime(2026, 7, 28, 9, 0, tzinfo=timezone.utc)

QUALITY_TOOLS = frozenset(
    {
        "okto_pulse_record_ambiguity_assessment",
        "okto_pulse_record_requirement_lint",
        "okto_pulse_get_requirement_lint_preflight",
        "okto_pulse_get_current_quality_assessment",
        "okto_pulse_get_quality_assessment_receipt",
        "okto_pulse_list_quality_assessments",
        "okto_pulse_list_quality_findings",
    }
)


def _digests() -> AssessmentDigestSet:
    return AssessmentDigestSet(
        content_digest="1" * 64,
        clarification_digest="2" * 64,
        ruleset_digest="3" * 64,
        taxonomy_digest="4" * 64,
        policy_digest="5" * 64,
    )


def _receipt(
    *,
    receipt_id: str = "qar-1",
    created_at: datetime = NOW,
) -> AssessmentReceipt:
    return AssessmentReceipt(
        id=receipt_id,
        subject=AssessmentSubjectRef(
            board_id="board-1",
            subject_type=AssessmentSubjectType.REFINEMENT,
            subject_id="refinement-1",
            subject_version=3,
        ),
        assessment_kind=AssessmentKind.AMBIGUITY,
        origin=AssessmentOrigin.HUMAN_OR_AGENT,
        source=AssessmentSource.NATIVE,
        channel="mcp",
        outcome=AssessmentOutcome.RECORDED,
        scale=AssessmentScale(
            kind=AssessmentScaleKind.AMBIGUITY_SCORE,
            minimum=1,
            maximum=5,
            direction=ScoreDirection.LOWER_BETTER,
        ),
        score=2,
        justification="Pinpointed ambiguity assessment.",
        digests=_digests(),
        versions=AssessmentVersionSet(
            ruleset_version="rules/v1",
            taxonomy_version="taxonomy/v1",
            analyzer_version="analyzer/v1",
            policy_version="policy/v1",
        ),
        run_identity_digest="6" * 64,
        authority_digest="7" * 64,
        idempotency_key=f"idem-{receipt_id}",
        request_digest="8" * 64,
        created_by="agent-1",
        created_at=created_at,
    )


def _current_view() -> CurrentAssessmentView:
    receipt = _receipt()
    return CurrentAssessmentView(
        receipt=receipt,
        head=AssessmentSubjectHead(
            board_id=receipt.subject.board_id,
            subject_type=receipt.subject.subject_type,
            subject_id=receipt.subject.subject_id,
            assessment_kind=receipt.assessment_kind,
            receipt_id=receipt.id,
            revision=4,
            updated_at=NOW,
        ),
        currentness=AssessmentCurrentness(current=True),
        gate_preview=QualityGatePreview(
            applicable=True,
            enabled=True,
            allowed=True,
            reason_code="ambiguity_gate_ready",
            threshold=3,
            score=receipt.score,
            skipped=False,
        ),
    )


@pytest.mark.asyncio
async def test_quality_mcp_inventory_remains_seven_tools_at_334() -> None:
    tools = await server.mcp.get_tools()

    # Semantic guideline v2 replaces the evaluation command with evidence
    # recording and retains four bounded read surfaces (list, get, current and
    # findings).  Those reads are part of the public projection/pagination
    # contract; Code Traceability adds 19 reviewed, typed commands.
    assert len(tools) == 334
    assert {
        "okto_pulse_list_semantic_guideline_assessments",
        "okto_pulse_get_semantic_guideline_assessment",
        "okto_pulse_get_current_semantic_guideline_assessment",
        "okto_pulse_list_semantic_guideline_findings",
    } <= tools.keys()
    assert QUALITY_TOOLS <= tools.keys()
    assert len(QUALITY_TOOLS) == 7

    record = tools["okto_pulse_record_ambiguity_assessment"].parameters
    assert record["properties"]["subject_type"]["enum"] == [
        "ideation",
        "refinement",
    ]
    assert set(record["required"]) == {
        "board_id",
        "subject_type",
        "subject_id",
        "idempotency_key",
        "expected_subject_version",
        "expected_subject_edition",
        "expected_head_revision",
        "score",
        "summary",
    }
    finding = record["properties"]["findings"]["$defs"]["QualityFindingInput"]
    assert finding["additionalProperties"] is False
    assert not {
        "id",
        "receipt_id",
        "blocking_eligible",
        "created_at",
    } & finding["properties"].keys()

    current = tools[
        "okto_pulse_get_current_quality_assessment"
    ].parameters["properties"]
    assert current["subject_type"]["enum"] == [
        "ideation",
        "refinement",
        "spec",
    ]
    assert current["assessment_kind"]["enum"] == [
        "ambiguity",
        "spec_validation",
        "requirement_lint",
    ]

    for name in (
        "okto_pulse_list_quality_assessments",
        "okto_pulse_list_quality_findings",
    ):
        properties = tools[name].parameters["properties"]
        assert properties["limit"] == {"default": 50, "type": "integer"}
        assert properties["offset"] == {"default": 0, "type": "integer"}
        assert properties["cursor"] == {"default": "", "type": "string"}


def test_shared_current_and_receipt_projectors_are_flat_closed_envelopes() -> None:
    view = _current_view()

    current = project_current_quality_assessment(view)
    assert set(current) == {
        "receipt",
        "edition",
        "lifecycle_state",
        "head_revision",
        "currentness",
        "stale_reasons",
        "gate_preview",
    }
    assert current["currentness"] == "current"
    assert current["lifecycle_state"] == "current"
    assert current["edition"] == view.receipt.subject.subject_edition
    assert current["stale_reasons"] == []
    assert not isinstance(current["currentness"], dict)
    assert current["head_revision"] == 4
    assert current["gate_preview"] == {
        "applicable": True,
        "enabled": True,
        "allowed": True,
        "reason_code": "ambiguity_gate_ready",
        "threshold": 3,
        "score": 2,
        "skipped": False,
    }

    stale = AssessmentCurrentness(
        current=False,
        stale_reasons=(AssessmentStaleReason.CONTENT_CHANGED,),
    )
    receipt = project_quality_receipt_currentness(view.receipt, stale)
    assert set(receipt) == {"receipt", "currentness", "stale_reasons"}
    assert receipt["receipt"]["id"] == "qar-1"
    assert receipt["currentness"] == "previous"
    assert receipt["stale_reasons"] == ["content_changed"]


@pytest.mark.asyncio
async def test_live_mcp_current_and_receipt_use_the_shared_flat_projectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _agent_ctx(_board_id: str):
        return server.AgentContext(
            "agent-1",
            "Quality agent",
            "board-1",
            ["*"],
            realm_id="local",
        )

    async def _get_current(self, command, *, actor):
        del self, command, actor
        return _current_view()

    async def _get_receipt(self, command, *, actor):
        del self, command, actor
        return GetQualityAssessmentReceiptResult(
            receipt=_receipt(),
            currentness=AssessmentCurrentness(current=True),
        )

    monkeypatch.setattr(server, "_get_agent_ctx", _agent_ctx)
    monkeypatch.setattr(server, "check_permission", lambda *_args: None)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: object(),
    )
    monkeypatch.setattr(
        quality_ports,
        "get_quality_assessment_preflight_reader",
        lambda: object(),
    )
    monkeypatch.setattr(QualityAssessmentReadUseCases, "get_current", _get_current)
    monkeypatch.setattr(QualityAssessmentReadUseCases, "get_receipt", _get_receipt)

    current = json.loads(
        await server.okto_pulse_get_current_quality_assessment.fn(
            board_id="board-1",
            subject_type="refinement",
            subject_id="refinement-1",
            assessment_kind="ambiguity",
        )
    )
    assert current == {
        "outcome": "success",
        "data": project_current_quality_assessment(_current_view()),
    }
    assert isinstance(current["data"]["currentness"], str)
    assert "gate_preview" in current["data"]

    receipt = json.loads(
        await server.okto_pulse_get_quality_assessment_receipt.fn(
            board_id="board-1",
            receipt_id="qar-1",
        )
    )
    assert receipt == {
        "outcome": "success",
        "data": project_quality_receipt_currentness(
            _receipt(),
            AssessmentCurrentness(current=True),
        ),
    }
    assert set(receipt["data"]) == {
        "receipt",
        "currentness",
        "stale_reasons",
    }


def test_keyset_projection_preserves_page_envelope_and_real_boundary() -> None:
    receipts = tuple(
        _receipt(
            receipt_id=f"qar-{index}",
            created_at=NOW - timedelta(minutes=index),
        )
        for index in range(1, 4)
    )
    views = tuple(
        AssessmentReceiptView(
            receipt=receipt,
            is_head=index == 0,
            freshness=AssessmentCurrentness(current=True),
            state=(
                AssessmentReceiptState.CURRENT
                if index == 0
                else AssessmentReceiptState.SUPERSEDED
            ),
        )
        for index, receipt in enumerate(receipts)
    )

    first = project_quality_keyset_page(
        QualityPage(
            items=views[:2],
            total_filtered=3,
            total_overall=7,
            offset=0,
            limit=2,
        ),
        projector=project_quality_receipt_view,
        created_at=lambda item: item.receipt.created_at,
        item_id=lambda item: item.receipt.id,
    )
    assert set(first) == {
        "items",
        "limit",
        "offset",
        "total_filtered",
        "total_overall",
        "next_cursor",
        "has_more",
        "ordering",
    }
    assert (first["limit"], first["offset"]) == (2, 0)
    assert (first["total_filtered"], first["total_overall"]) == (3, 7)
    assert first["has_more"] is True
    assert first["ordering"] == "created_at_desc_id_desc"
    boundary = decode_quality_cursor(first["next_cursor"])
    assert boundary == QualityPageCursor(
        created_at=receipts[1].created_at,
        item_id=receipts[1].id,
        offset=2,
    )

    last = project_quality_keyset_page(
        QualityPage(
            items=views[2:],
            total_filtered=3,
            total_overall=7,
            offset=2,
            limit=2,
        ),
        projector=project_quality_receipt_view,
        created_at=lambda item: item.receipt.created_at,
        item_id=lambda item: item.receipt.id,
    )
    assert last["next_cursor"] is None
    assert last["has_more"] is False
    assert last["offset"] == 2

    assert quality_uc._quality_page_offset(0, boundary) == 2
    with pytest.raises(ValueError, match="quality_cursor_offset_conflict"):
        quality_uc._quality_page_offset(2, boundary)


@pytest.mark.asyncio
async def test_receipt_findings_command_derives_scope_with_one_preflight() -> None:
    calls: list[tuple[str, object]] = []
    context = QualityAssessmentReadContext(
        subject=_receipt().subject,
        currentness_inputs=(),
    )

    class _Reader:
        async def resolve_receipt_read_context(self, **kwargs):
            calls.append(("receipt_preflight", kwargs))
            return context

        async def resolve_assessment_read_context(self, **kwargs):
            calls.append(("subject_preflight", kwargs))
            raise AssertionError("receipt listing must not repeat subject preflight")

    class _Persistence:
        async def list_findings(self, query):
            calls.append(("list_findings", query))
            return QualityPage(
                items=(),
                total_filtered=0,
                total_overall=0,
                offset=query.offset,
                limit=query.limit,
            )

    @asynccontextmanager
    async def _uow_factory(**kwargs):
        calls.append(("uow", kwargs))
        yield SimpleNamespace(
            services=SimpleNamespace(
                quality_assessments=_Persistence(),
            )
        )

    actor = ActorContext(
        "agent-1",
        "rest",
        realm_scope=RealmScope.local(),
    )
    page = await QualityAssessmentReadUseCases(
        preflight_reader=_Reader(),
        uow_factory=_uow_factory,
    ).list_receipt_findings(
        ListQualityReceiptFindingsCommand(
            receipt_id="qar-1",
            limit=25,
        ),
        actor=actor,
    )

    assert page.items == ()
    assert [kind for kind, _ in calls].count("receipt_preflight") == 1
    assert "subject_preflight" not in [kind for kind, _ in calls]
    preflight = next(value for kind, value in calls if kind == "receipt_preflight")
    assert preflight["receipt_id"] == "qar-1"
    assert preflight["board_id"] is None
    query = next(value for kind, value in calls if kind == "list_findings")
    assert query.board_id == "board-1"
    assert query.subject_type is AssessmentSubjectType.REFINEMENT
    assert query.subject_id == "refinement-1"
    assert query.receipt_id == "qar-1"


def test_checked_in_ska_resource_manifest_is_current_and_drift_fails(
    tmp_path: Path,
) -> None:
    checked = verify_checked_in_manifest()
    assert checked == checked_in_manifest_path()

    expected = build_ska_resource_manifest()
    actual = json.loads(checked.read_text(encoding="utf-8"))
    assert actual == expected
    assert actual["manifest_version"] == "agent-resources/v2"
    assert actual["resource_count"] == len(actual["resources"])
    assert {
        "okto-pulse://reference/policy-compliance",
        "okto-pulse://reference/quality-assessments",
        "okto-pulse://reference/tool-docs/quality",
        "okto-pulse://reference/tools_catalog",
        "okto-pulse://workflows/preflight",
    } <= {item["uri"] for item in actual["resources"]}
    assert all(
        len(item["content_sha256"]) == 64
        and item["required_headings"]
        and item["required_cross_links"]
        for item in actual["resources"]
    )

    drifted = tmp_path / "ska_resource_manifest.json"
    drifted.write_text(
        checked.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="agent resource manifest drift"):
        verify_checked_in_manifest(drifted)


@pytest.mark.parametrize(
    ("error", "public_code", "category", "retryable"),
    (
        (
            QualityAssessmentValidationError("assessment_score_invalid"),
            "validation_failed",
            "unprocessable",
            False,
        ),
        (
            QualityAssessmentForbiddenError("assessment_permission_denied"),
            "forbidden",
            "forbidden",
            False,
        ),
        (
            QualityAssessmentNotFoundError("assessment_receipt_not_found"),
            "not_found",
            "not_found",
            False,
        ),
        (
            QualityAssessmentConflictError("assessment_subject_version_conflict"),
            "assessment_subject_version_conflict",
            "conflict",
            True,
        ),
        (
            AssessmentHeadRevisionConflict(),
            "assessment_head_revision_conflict",
            "conflict",
            True,
        ),
        (
            QualityAssessmentConflictError("assessment_subject_status_conflict"),
            "assessment_subject_status_conflict",
            "conflict",
            True,
        ),
        (
            QualityAssessmentConflictError("requirement_lint_required"),
            "requirement_lint_required",
            "conflict",
            True,
        ),
    ),
)
def test_quality_public_error_codes_are_exact_and_identical_in_mcp(
    error: Exception,
    public_code: str,
    category: str,
    retryable: bool,
) -> None:
    projected = project_quality_assessment_error(error)

    assert projected["outcome"] == "error"
    assert projected["error"] == public_code
    assert projected["code"] == public_code
    assert projected["error_code"] == public_code
    assert projected["category"] == category
    assert projected["retryable"] is retryable
    assert projected["details"]["reason_code"] == str(getattr(error, "code"))
    assert json.loads(server._quality_mcp_error(error)) == projected


@pytest.mark.parametrize(
    "error",
    (
        QualityAssessmentValidationError("invalid_pagination"),
        QualityAssessmentContractError("question_budget_exceeded"),
    ),
)
def test_quality_declared_specific_error_codes_stay_top_level(
    error: Exception,
) -> None:
    projected = project_quality_assessment_error(error)
    reason_code = str(getattr(error, "code"))

    assert projected["error_code"] == reason_code
    assert projected["details"]["reason_code"] == reason_code
    assert json.loads(server._quality_mcp_error(error)) == projected


@pytest.mark.parametrize(
    ("raw_code", "public_code", "next_action"),
    (
        (
            "checklist_execution_revision_conflict",
            "checklist_execution_conflict",
            "refresh_checklist_execution",
        ),
        (
            "checklist_spec_lifecycle_conflict",
            "checklist_spec_status_conflict",
            "refresh_spec_validation_cycle",
        ),
        (
            "checklist_spec_version_conflict",
            "checklist_spec_status_conflict",
            "refresh_spec_validation_cycle",
        ),
        (
            "checklist_spec_edition_conflict",
            "checklist_spec_edition_conflict",
            "refresh_spec_validation_cycle",
        ),
        (
            "checklist_binding_conflict",
            "checklist_binding_conflict",
            "refresh_checklist_binding",
        ),
    ),
)
def test_ska_checklist_conflicts_use_the_frozen_public_vocabulary(
    raw_code: str,
    public_code: str,
    next_action: str,
) -> None:
    projected = project_ska_contract_error(
        QualityAssessmentConflictError(raw_code),
        family="checklist",
    )

    assert projected["error_code"] == public_code
    assert projected["details"]["reason_code"] == raw_code
    assert projected["next_action"] == next_action
    assert projected["retryable"] is True

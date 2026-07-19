"""Adversarial coverage for the canonical bug closeout context."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from okto_pulse.core.kg.bug_cognitive_closure import (
    classify_bug_evidence,
    evaluate_bug_cognitive_closure,
)
from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessVerdict
from okto_pulse.core.ports.bug_cognitive_context import BugCognitiveContext
from okto_pulse.core.ports.bug_cognitive_context import (
    register_bug_cognitive_context_assembler,
)
from okto_pulse.core.ports.test_evidence import (
    TestEvidenceWriteVerification as EvidenceWriteVerification,
    register_test_evidence_write_verifier,
    reset_test_evidence_write_verifier_for_tests,
)


BUG_ID = "11111111-1111-1111-1111-111111111111"
BOARD_ID = "board-1"
SPEC_ID = "spec-1"
ACCEPTANCE_CRITERIA = (
    {"id": "ac-about", "text": "The About page renders v0.3.0."},
)


class _TrustedEvidenceVerifier:
    def verify(self, **_request):  # noqa: ANN003, ANN201
        return EvidenceWriteVerification(True)


@pytest.fixture(autouse=True)
def _trusted_evidence_verifier():
    register_test_evidence_write_verifier(_TrustedEvidenceVerifier())
    yield
    reset_test_evidence_write_verifier_for_tests()


def _verified_scenario(*, runtime_exercised: bool = True) -> dict:
    from okto_pulse.core.services.test_scenario_lifecycle import (
        compute_execution_attestation_sha256,
        compute_test_scenario_semantic_sha256,
    )

    scenario = {
        "id": "scenario-1",
        "title": "Regression reproduces and verifies the corrected behaviour",
        "scenario_type": "e2e",
        "given": "the compiled Community frontend is installed",
        "when": "the About page is opened",
        "then": "the page renders v0.3.0",
        "linked_criteria": ["ac-about"],
        "status": "passed",
    }
    scenario_sha256 = compute_test_scenario_semantic_sha256(
        board_id=BOARD_ID,
        spec_id=SPEC_ID,
        scenario=scenario,
        acceptance_criteria=list(ACCEPTANCE_CRITERIA),
    )
    manifest_ref = "mcp://replay/about-version"
    attestation = {
        "schema_version": 2,
        "run_id": "run-about-version",
        "executed_at": "2026-07-14T12:00:00+00:00",
        "scenario_id": "scenario-1",
        "scenario_sha256": scenario_sha256,
        "outcome": "passed",
        "product_runtime_exercised": runtime_exercised,
        "manifest_sha256": "sha256:" + "a" * 64,
        "assertions": [
            {
                "name": "observed output equals expected output",
                "expected": "v0.3.0",
                "observed": "v0.3.0",
                "status": "passed",
                "message": None,
            }
        ],
        "provenance": {
            "producer": "okto-pulse-community",
            "producer_version": "0.3.0",
            "adapter": "mcp-replay-runner",
            "environment": "test",
        },
    }
    attestation["attestation_sha256"] = compute_execution_attestation_sha256(
        attestation,
        manifest_ref=manifest_ref,
    )
    return {
        **scenario,
        "evidence": {
            "manifest_ref": manifest_ref,
            "execution_attestation": attestation,
            "execution_receipt": "opaque-test-receipt",
        },
    }


def _complete_context(**changes) -> BugCognitiveContext:
    base = BugCognitiveContext(
        board_id=BOARD_ID,
        bug_id=BUG_ID,
        card_exists=True,
        card_type="bug",
        status="done",
        title="Incorrect version is rendered",
        expected_behavior="The About page renders v0.3.0.",
        observed_behavior="The About page renders v0.2.5.",
        steps_to_reproduce="Open About and read the Community version.",
        action_plan=(
            "Root cause: the compiled frontend consumed stale package metadata; "
            "rebuild from the release version source."
        ),
        spec_id=SPEC_ID,
        origin_task_id="task-1",
        conclusions=(
            {"text": "Rebuilt the frontend and verified the About version source."},
        ),
        validations=({"outcome": "success", "confidence": 100},),
        comments=({"content": "Bundle hash and package version now agree."},),
        acceptance_criteria=ACCEPTANCE_CRITERIA,
        test_scenarios=(_verified_scenario(),),
        canonical_bug_present=True,
        provenance_refs=("sql:cards/bug-1", "kg:canonical/bug/bug-1"),
    )
    return replace(base, **changes)


def test_false_valued_dicts_are_not_cognitive_evidence() -> None:
    result = classify_bug_evidence(
        {
            "root_cause": {"confirmed": False, "description": "looks plausible"},
            "fix_narrative": {"implemented": False, "text": "not deployed"},
            "validation": {"outcome": "failed"},
        }
    )

    assert result["categories_present"]["root_cause"] is False
    assert result["categories_present"]["fix_narrative"] is False
    assert result["categories_present"]["validation"] is False
    assert result["has_reusable_learning"] is False
    assert result["evidence_ready"] is False


def test_complete_canonical_context_classifies_content_and_state() -> None:
    result = classify_bug_evidence(None, context=_complete_context())

    assert result["context_verified"] is True
    assert result["missing_categories"] == ()


def test_bug_closeout_consumer_fails_closed_without_receipt_authenticator() -> None:
    reset_test_evidence_write_verifier_for_tests()
    result = classify_bug_evidence(None, context=_complete_context())
    assert "regression_proof" in result["missing_categories"]
    assert result["evidence_ready"] is False
    assert result["categories_present"]["regression_proof"] is False
    assert set(result["category_sources"]["root_cause"]) == {
        "canonical_context"
    }


def test_bug_closeout_rejects_receipt_after_semantic_scenario_edit() -> None:
    stale_receipt_scenario = {
        **_verified_scenario(),
        "given": "a different product state is active",
    }
    result = classify_bug_evidence(
        None,
        context=_complete_context(test_scenarios=(stale_receipt_scenario,)),
    )

    assert result["categories_present"]["regression_proof"] is False
    assert "regression_proof" in result["missing_categories"]


def test_caller_evidence_is_additive_and_cannot_delete_canonical_facts() -> None:
    result = classify_bug_evidence(
        {
            "root_cause": {"confirmed": False},
            "technical_comments": "Additional operator observation.",
        },
        context=_complete_context(comments=()),
    )

    assert result["categories_present"]["root_cause"] is True
    assert result["category_sources"]["root_cause"] == ("canonical_context",)
    assert result["categories_present"]["technical_comments"] is True
    assert result["category_sources"]["technical_comments"] == (
        "caller_evidence:technical_comments",
    )


def test_passed_label_without_runtime_attestation_is_not_regression_proof() -> None:
    context = _complete_context(
        test_scenarios=(_verified_scenario(runtime_exercised=False),)
    )

    result = classify_bug_evidence(None, context=context)

    assert result["categories_present"]["test_scenarios"] is True
    assert result["categories_present"]["regression_proof"] is False
    assert "regression_proof" in result["missing_categories"]
    assert result["evidence_ready"] is False


class _ReadinessService:
    def __init__(self, *, has_item: bool) -> None:
        self.has_item = has_item

    async def evaluate_artifact(self, *_args, **_kwargs) -> CognitiveReadinessVerdict:
        return CognitiveReadinessVerdict(
            artifact_id=f"card:{BUG_ID}",
            ready=True,
            blocking=False,
            tier="ready",
        )

    def cognitive_items_for(self, *_args, **_kwargs):
        return [object()] if self.has_item else []


@pytest.mark.asyncio
async def test_done_bug_without_work_item_fails_closed_with_bounded_status() -> None:
    result = await evaluate_bug_cognitive_closure(
        _ReadinessService(has_item=False),
        object(),
        board_id="board-1",
        bug_id=BUG_ID,
        evidence={},
        bug_context=_complete_context(),
    )

    assert result["status"] == "missing_cognitive_work_item"
    assert result["blocking"] is True
    assert result["readiness_effect"] == "blocking_cognitive"
    assert result["technical_remediation"] == "requeue_cognitive_closeout"
    assert result["pipeline_readiness"]["blocking"] is False
    assert result["cognitive_work_item"] == {"present": False, "required": True}


@pytest.mark.asyncio
async def test_create_learning_requires_real_canonical_bug_node() -> None:
    result = await evaluate_bug_cognitive_closure(
        _ReadinessService(has_item=True),
        object(),
        board_id="board-1",
        bug_id=BUG_ID,
        evidence={},
        requested_action="create_learning",
        bug_context=_complete_context(canonical_bug_present=False),
    )

    assert result["status"] == "canonical_bug_node_absent"
    assert result["blocking"] is True
    assert result["readiness_effect"] == "blocking_technical"
    assert result["graph_commit_required"] is False
    assert result["technical_remediation"] == "reconcile_canonical_bug_node"


@pytest.mark.asyncio
async def test_complete_context_and_pipeline_are_ready_to_commit() -> None:
    result = await evaluate_bug_cognitive_closure(
        _ReadinessService(has_item=True),
        object(),
        board_id="board-1",
        bug_id=BUG_ID,
        evidence={"impact": {"confirmed": False}},
        requested_action="create_learning",
        bug_context=_complete_context(),
    )

    assert result["status"] == "ready_to_commit"
    assert result["blocking"] is False
    assert result["evidence_readiness"]["ready"] is True
    assert result["graph_commit_required"] is True


class _Assembler:
    def __init__(self, context: BugCognitiveContext) -> None:
        self.context = context
        self.calls: list[tuple[object, str, str]] = []

    async def assemble(
        self,
        relational_context: object,
        *,
        board_id: str,
        bug_id: str,
    ) -> BugCognitiveContext:
        self.calls.append((relational_context, board_id, bug_id))
        return self.context


@pytest.mark.asyncio
async def test_transport_operation_resolves_the_registered_shared_assembler() -> None:
    from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations

    relational_context = object()
    assembler = _Assembler(_complete_context())
    register_bug_cognitive_context_assembler(assembler)

    result = await CoreKnowledgeGraphOperations(
        relational_context
    ).evaluate_bug_cognitive_closure(
        _ReadinessService(has_item=True),
        board_id="board-1",
        bug_id=BUG_ID,
        evidence={},
        requested_action="create_learning",
    )

    assert assembler.calls == [(relational_context, "board-1", BUG_ID)]
    assert result["status"] == "ready_to_commit"


@pytest.mark.asyncio
async def test_worker_loader_uses_the_same_registered_context() -> None:
    from okto_pulse.core.kg.workers.cognitive_closeout import (
        build_closeout_input_loader,
    )
    from okto_pulse.core.ports.domain_event_delivery import (
        register_domain_event_fact_reader,
    )

    context = _complete_context()
    assembler = _Assembler(context)
    register_bug_cognitive_context_assembler(assembler)

    class _FactReader:
        async def load_board_settings(self, _db, *, board_id):  # noqa: ANN001
            assert board_id == "board-1"
            return {}

    register_domain_event_fact_reader(_FactReader())

    relational_context = object()

    @asynccontextmanager
    async def _scope():
        yield relational_context

    loader = build_closeout_input_loader(_scope)
    inputs = await loader(
        "board-1",
        SimpleNamespace(source_ref=f"bug:{BUG_ID}", artifact_type="bug"),
    )

    assert assembler.calls == [(relational_context, "board-1", BUG_ID)]
    assert inputs["bug_context"] is context
    assert inputs["bug_probe"](BUG_ID) is True
    assert inputs["bug_probe"]("different-bug") is False


def test_core_bug_context_boundary_has_no_edition_or_orm_imports() -> None:
    import okto_pulse.core.ports.bug_cognitive_context as contract

    source = Path(contract.__file__).read_text(encoding="utf-8")
    assert "okto_pulse.community" not in source
    assert "sqlalchemy" not in source.lower()

"""C8 deterministic-worker contracts for lean SK-A relational projections."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import re

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
)
from okto_pulse.core.kg.connectivity_guard import (
    KGConnectivityOutcome,
    KGNodeConnectivityGuard,
)
from okto_pulse.core.kg.primitives import (
    _validated_deterministic_rdl_alternative_grants,
)


def _quality_summary(subject_type: str, subject_id: str) -> dict:
    return {
        "board_id": "board-c8",
        "subject_type": subject_type,
        "subject_id": subject_id,
        "subject_version": 4,
        "assessment_kind": "ambiguity",
        "receipt_id": "receipt-current",
        "head_revision": 3,
        "outcome": "recorded",
        "score": 2.0,
        "justification": "Two bounded ambiguities remain.",
        "scale_kind": "ambiguity_score",
        "scale_minimum": 0.0,
        "scale_maximum": 10.0,
        "scale_direction": "lower_better",
        "input_digest": "a" * 64,
        "ruleset_version": "ambiguity/v1",
        "taxonomy_version": "quality/v1",
        "analyzer_version": "analyzer/v1",
        "policy_version": "policy/v1",
        "projection_fingerprint": "b" * 64,
    }


@pytest.mark.parametrize(
    ("artifact_type", "artifact"),
    [
        (
            "ideation",
            {
                "id": "ideation-c8",
                "board_id": "board-c8",
                "title": "Lean ideation",
                "status": "draft",
            },
        ),
        (
            "refinement",
            {
                "id": "refinement-c8",
                "board_id": "board-c8",
                "title": "Lean refinement",
                "status": "draft",
            },
        ),
        (
            "spec",
            {
                "id": "spec-c8",
                "board_id": "board-c8",
                "title": "Lean spec",
                "status": "draft",
            },
        ),
    ],
)
def test_quality_current_head_enriches_only_the_root(
    artifact_type: str,
    artifact: dict,
) -> None:
    artifact = dict(artifact)
    artifact["quality_assessments"] = [_quality_summary(artifact_type, artifact["id"])]

    result = DeterministicWorker().process_artifact(artifact_type, artifact)

    assert [node.node_type for node in result.nodes] == ["Entity", "Entity"]
    root = next(
        node
        for node in result.nodes
        if node.source_artifact_ref == f"{artifact_type}:{artifact['id']}"
    )
    assert "Quality assessments (current heads):" in root.context
    assert "Two bounded ambiguities remain." in root.context
    assert "quality_assessments" in result.raw_content
    assert not {
        "QualityFinding",
        "Checklist",
        "ChecklistItem",
    }.intersection(node.node_type for node in result.nodes)


def _resolved_decision() -> dict:
    return {
        "board_id": "board-c8",
        "refinement_id": "refinement-c8",
        "refinement_version": 5,
        "ledger_id": "ledger-retry",
        "entry_id": "entry-resolved",
        "head_revision": 2,
        "predecessor_entry_id": "entry-open",
        "unknown": "Which retry policy should be used?",
        "status": "resolved",
        "anchor_type": "functional_requirement",
        "anchor_ref": "fr_retry",
        "evidence_refs": ["kb:retry", "spec:load"],
        "alternatives": ["Fixed delay", "  Bounded   backoff  "],
        "decision": "Use bounded backoff.",
        "rationale": "It bounds pressure.",
        "confidence": 0.9,
        "evidence_absence_justification": None,
        "projection_fingerprint": "c" * 64,
    }


def _open_decision() -> dict:
    return {
        "board_id": "board-c8",
        "refinement_id": "refinement-c8",
        "refinement_version": 4,
        "ledger_id": "ledger-observability",
        "entry_id": "entry-open",
        "head_revision": 1,
        "predecessor_entry_id": None,
        "unknown": "Which signal should page the team?",
        "status": "investigating",
        "anchor_type": "qa",
        "anchor_ref": "qa_alert",
        "evidence_refs": [],
        "alternatives": ["queue depth"],
        "decision": None,
        "rationale": None,
        "confidence": None,
        "evidence_absence_justification": None,
        "projection_fingerprint": "d" * 64,
    }


def _refinement(records: list[dict]) -> dict:
    return {
        "id": "refinement-c8",
        "board_id": "board-c8",
        "title": "Retry refinement",
        "description": "Resolve retry behavior.",
        "status": "draft",
        "quality_assessments": [],
        "research_decisions": records,
    }


def test_resolved_rdl_projection_is_stable_and_explicitly_owned() -> None:
    resolved = _resolved_decision()
    first = DeterministicWorker().process_refinement(
        _refinement([resolved, _open_decision()])
    )
    reordered = dict(resolved)
    reordered["alternatives"] = list(reversed(resolved["alternatives"]))
    reordered["evidence_refs"] = list(reversed(resolved["evidence_refs"]))
    second = DeterministicWorker().process_refinement(
        _refinement([_open_decision(), reordered])
    )

    projected = [
        node for node in first.nodes if node.node_type in {"Decision", "Alternative"}
    ]
    assert [node.node_type for node in projected].count("Decision") == 1
    assert [node.node_type for node in projected].count("Alternative") == 2
    assert {node.source_artifact_ref for node in projected} == {
        node.source_artifact_ref
        for node in second.nodes
        if node.node_type in {"Decision", "Alternative"}
    }
    assert first.relational_projection_candidate_ids == (
        second.relational_projection_candidate_ids
    )
    assert first.content_hash == second.content_hash
    assert all(
        re.fullmatch(
            (
                r"refinement:refinement-c8:rdl:ledger-retry:"
                r"(?:decision|alternative:[0-9a-f]{64})"
            ),
            node.source_artifact_ref,
        )
        for node in projected
    )
    assert first.relational_projection_active_set_intent is not None
    assert first.relational_projection_active_set_intent.owner_type == "refinement"
    assert first.relational_projection_active_set_intent.owner_id == "refinement-c8"
    assert first.relational_projection_active_set_intent.namespace == "rdl"
    assert len(first.relational_projection_active_set_intent.active_refs) == 3
    projection_edges = [
        edge for edge in first.edges if edge.candidate_id.startswith("relproj_edge_")
    ]
    assert {edge.edge_type for edge in projection_edges} == {
        "belongs_to",
        "mentions",
        "relates_to",
    }
    decision = next(node for node in projected if node.node_type == "Decision")
    refinement = next(
        node
        for node in first.nodes
        if node.source_artifact_ref == "refinement:refinement-c8"
    )
    decision_owner_edges = [
        edge
        for edge in projection_edges
        if edge.from_candidate_id == decision.candidate_id
        and edge.to_candidate_id == refinement.candidate_id
    ]
    assert {(edge.edge_type, edge.rule_id) for edge in decision_owner_edges} == {
        (
            "belongs_to",
            "belongs_to/relational_rdl_decision@v2.0",
        ),
        (
            "mentions",
            "mentions/relational_rdl_owner@v2.0",
        ),
    }


def test_resolved_rdl_without_alternatives_emits_both_connectivity_edges() -> None:
    resolved = _resolved_decision()
    resolved["alternatives"] = []

    result = DeterministicWorker().process_refinement(_refinement([resolved]))

    decision = next(node for node in result.nodes if node.node_type == "Decision")
    assert not [node for node in result.nodes if node.node_type == "Alternative"]
    refinement = next(
        node
        for node in result.nodes
        if node.source_artifact_ref == "refinement:refinement-c8"
    )
    decision_owner_edges = [
        edge
        for edge in result.edges
        if edge.from_candidate_id == decision.candidate_id
        and edge.to_candidate_id == refinement.candidate_id
    ]
    assert {(edge.edge_type, edge.rule_id) for edge in decision_owner_edges} == {
        (
            "belongs_to",
            "belongs_to/relational_rdl_decision@v2.0",
        ),
        (
            "mentions",
            "mentions/relational_rdl_owner@v2.0",
        ),
    }


def test_rdl_demotion_emits_an_explicit_empty_active_set() -> None:
    result = DeterministicWorker().process_refinement(_refinement([_open_decision()]))

    assert not [
        node for node in result.nodes if node.node_type in {"Decision", "Alternative"}
    ]
    assert result.relational_projection_candidate_ids == set()
    assert result.relational_projection_active_set_intent is not None
    assert result.relational_projection_active_set_intent.active_refs == ()


def _refinement_with_alternative_count(
    refinement_id: str,
    alternative_count: int,
) -> dict:
    decision = _resolved_decision()
    decision["board_id"] = "board-c8-rebuild"
    decision["refinement_id"] = refinement_id
    decision["ledger_id"] = f"ledger-{refinement_id}"
    decision["entry_id"] = f"entry-{refinement_id}"
    decision["alternatives"] = [
        f"Alternative {index} for {refinement_id}" for index in range(alternative_count)
    ]
    return {
        "id": refinement_id,
        "board_id": "board-c8-rebuild",
        "title": f"Refinement {refinement_id}",
        "description": "Rebuild the exact relational RDL projection.",
        "status": "draft",
        "quality_assessments": [],
        "research_decisions": [decision],
    }


def _rdl_grants(result, refinement_id: str) -> frozenset[str]:
    return _validated_deterministic_rdl_alternative_grants(
        agent_id="system:historical_consolidation",
        session_artifact_type="refinement",
        session_artifact_id=refinement_id,
        node_candidates={node.candidate_id: node for node in result.nodes},
        edge_candidates={edge.candidate_id: edge for edge in result.edges},
        relational_projection_candidate_ids=frozenset(
            result.relational_projection_candidate_ids
        ),
        relational_projection_active_set_intent=(
            result.relational_projection_active_set_intent
        ),
    )


def test_exact_rdl_grant_covers_real_rebuild_shape_18_refinements_137_alternatives() -> (
    None
):
    # Distribution observed in the pristine recovery snapshot. Keep the test
    # self-contained: no live/copy path is needed to prove all affected shapes.
    alternative_counts = (9, 3, 12, 9, 3, 9, 18, 3, 9, 9, 9, 3, 10, 3, 9, 4, 12, 3)
    granted_total = 0

    for index, count in enumerate(alternative_counts):
        refinement_id = f"refinement-rebuild-{index:02d}"
        result = DeterministicWorker().process_refinement(
            _refinement_with_alternative_count(refinement_id, count)
        )
        alternative_ids = frozenset(
            node.candidate_id
            for node in result.nodes
            if node.node_type == "Alternative"
        )
        grants = _rdl_grants(result, refinement_id)

        assert grants == alternative_ids
        validation = KGNodeConnectivityGuard().validate(
            board_id="board-c8-rebuild",
            writer_path="deterministic_worker",
            kg_health_state="healthy",
            nodes=result.nodes,
            edges=result.edges,
            deterministic_rdl_alternative_candidate_ids=grants,
        )
        assert validation.outcome is KGConnectivityOutcome.PASSED
        assert validation.violations == ()
        granted_total += len(grants)

    assert len(alternative_counts) == 18
    assert granted_total == 137


def test_exact_rdl_grant_rejects_identity_scope_and_provenance_forgeries() -> None:
    refinement_id = "refinement-rdl-forgery"
    baseline = DeterministicWorker().process_refinement(
        _refinement_with_alternative_count(refinement_id, 2)
    )
    alternative = next(
        node for node in baseline.nodes if node.node_type == "Alternative"
    )

    wrong_agent = deepcopy(baseline)
    wrong_agent_grants = _validated_deterministic_rdl_alternative_grants(
        agent_id="system:other_worker",
        session_artifact_type="refinement",
        session_artifact_id=refinement_id,
        node_candidates={node.candidate_id: node for node in wrong_agent.nodes},
        edge_candidates={edge.candidate_id: edge for edge in wrong_agent.edges},
        relational_projection_candidate_ids=frozenset(
            wrong_agent.relational_projection_candidate_ids
        ),
        relational_projection_active_set_intent=(
            wrong_agent.relational_projection_active_set_intent
        ),
    )

    wrong_owner = deepcopy(baseline)
    assert wrong_owner.relational_projection_active_set_intent is not None
    wrong_owner.relational_projection_active_set_intent = replace(
        wrong_owner.relational_projection_active_set_intent,
        owner_id="different-refinement",
    )

    wrong_belongs = deepcopy(baseline)
    next(
        edge
        for edge in wrong_belongs.edges
        if edge.edge_type == "belongs_to"
        and edge.from_candidate_id == alternative.candidate_id
    ).created_by = "forged-worker"

    wrong_relation = deepcopy(baseline)
    next(
        edge
        for edge in wrong_relation.edges
        if edge.edge_type == "relates_to"
        and edge.to_candidate_id == alternative.candidate_id
    ).rule_id = "relates_to/relational_rdl_alternative@v99.0"

    wrong_decision_provenance = deepcopy(baseline)
    decision = next(
        node for node in wrong_decision_provenance.nodes if node.node_type == "Decision"
    )
    next(
        edge
        for edge in wrong_decision_provenance.edges
        if edge.edge_type == "belongs_to"
        and edge.from_candidate_id == decision.candidate_id
    ).rule_id = "belongs_to/relational_rdl_decision@v99.0"

    assert wrong_agent_grants == frozenset()
    for forged in (
        wrong_owner,
        wrong_belongs,
        wrong_relation,
        wrong_decision_provenance,
    ):
        assert _rdl_grants(forged, refinement_id) == frozenset()

    ungranted = KGNodeConnectivityGuard().validate(
        board_id="board-c8-rebuild",
        writer_path="deterministic_worker",
        kg_health_state="healthy",
        nodes=baseline.nodes,
        edges=baseline.edges,
    )
    assert ungranted.outcome is KGConnectivityOutcome.REJECTED
    assert {
        (violation.candidate_id, violation.reason)
        for violation in ungranted.violations
        if violation.node_type == "Alternative"
    } == {
        (node.candidate_id, "writer_not_connectivity_owner")
        for node in baseline.nodes
        if node.node_type == "Alternative"
    }

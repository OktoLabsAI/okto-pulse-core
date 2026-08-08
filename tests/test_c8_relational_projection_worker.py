"""C8 deterministic-worker contracts for lean SK-A relational projections."""

from __future__ import annotations

import re

import pytest

from okto_pulse.core.application.processors.deterministic_kg import (
    DeterministicWorker,
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
    artifact["quality_assessments"] = [
        _quality_summary(artifact_type, artifact["id"])
    ]

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
        node
        for node in first.nodes
        if node.node_type in {"Decision", "Alternative"}
    ]
    assert [node.node_type for node in projected].count("Decision") == 1
    assert [node.node_type for node in projected].count("Alternative") == 2
    assert {
        node.source_artifact_ref for node in projected
    } == {
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
        edge
        for edge in first.edges
        if edge.candidate_id.startswith("relproj_edge_")
    ]
    assert {edge.edge_type for edge in projection_edges} == {
        "belongs_to",
        "mentions",
        "relates_to",
    }
    decision = next(
        node for node in projected if node.node_type == "Decision"
    )
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
    assert {
        (edge.edge_type, edge.rule_id) for edge in decision_owner_edges
    } == {
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

    result = DeterministicWorker().process_refinement(
        _refinement([resolved])
    )

    decision = next(
        node for node in result.nodes if node.node_type == "Decision"
    )
    assert not [
        node for node in result.nodes if node.node_type == "Alternative"
    ]
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
    assert {
        (edge.edge_type, edge.rule_id) for edge in decision_owner_edges
    } == {
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
    result = DeterministicWorker().process_refinement(
        _refinement([_open_decision()])
    )

    assert not [
        node
        for node in result.nodes
        if node.node_type in {"Decision", "Alternative"}
    ]
    assert result.relational_projection_candidate_ids == set()
    assert result.relational_projection_active_set_intent is not None
    assert result.relational_projection_active_set_intent.active_refs == ()

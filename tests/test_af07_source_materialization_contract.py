"""AF-07 source materialization contracts stay core-owned and deterministic."""

from __future__ import annotations

from okto_pulse.core.kg.board_source_store import (
    AMENDMENT_CONTENT_COLUMNS,
    CARD_CONTENT_COLUMNS,
    IDEATION_CONTENT_COLUMNS,
    REFINEMENT_CONTENT_COLUMNS,
    SPEC_CONTENT_COLUMNS_V1,
    SPEC_CONTENT_COLUMNS_V2,
    SPRINT_CONTENT_COLUMNS,
    STORY_CONTENT_COLUMNS,
    _canonical_content_hash,
)
from okto_pulse.core.kg.source_maturity import (
    CANONICAL_ARTIFACT_TYPES,
    REBUILD_ARTIFACT_TYPES,
)


def test_source_content_column_contracts_are_verbatim() -> None:
    assert STORY_CONTENT_COLUMNS == (
        "title",
        "description",
        "actor",
        "goal",
        "benefit",
        "topic_id",
        "status",
        "labels",
    )
    assert IDEATION_CONTENT_COLUMNS == (
        "title",
        "description",
        "problem_statement",
        "proposed_approach",
        "scope_assessment",
        "complexity",
        "status",
        "version",
        "labels",
    )
    assert REFINEMENT_CONTENT_COLUMNS == (
        "title",
        "description",
        "in_scope",
        "out_of_scope",
        "analysis",
        "decisions",
        "status",
        "version",
        "labels",
    )
    assert SPRINT_CONTENT_COLUMNS == (
        "title",
        "description",
        "spec_id",
        "spec_version",
        "status",
        "lane_type",
        "objective",
        "expected_outcome",
        "test_scenario_ids",
        "business_rule_ids",
        "evaluations",
        "version",
        "labels",
    )
    assert CARD_CONTENT_COLUMNS == (
        "title",
        "description",
        "details",
        "status",
        "priority",
        "card_type",
        "spec_id",
        "sprint_id",
        "test_scenario_ids",
        "conclusions",
        "screen_mockups",
        "knowledge_bases",
        "validations",
        "origin_task_id",
        "severity",
        "expected_behavior",
        "observed_behavior",
        "steps_to_reproduce",
        "action_plan",
        "linked_test_task_ids",
    )
    assert "labels" not in CARD_CONTENT_COLUMNS
    assert "version" not in STORY_CONTENT_COLUMNS


def test_content_hashes_match_pinned_core_contracts() -> None:
    cases = (
        (
            STORY_CONTENT_COLUMNS,
            _story_row(),
            "6572cc2f0be2cbebb11f2b4cf7aa1f74a5137831ff332f23c011bd4a636774ee",
        ),
        (
            IDEATION_CONTENT_COLUMNS,
            _ideation_row(),
            "20a9ebfb04ce236af8efaa4af2607be503958c023bce4f605f0a451f3f36140a",
        ),
        (
            REFINEMENT_CONTENT_COLUMNS,
            _refinement_row(),
            "b408edb6bae8d164ce9a627e1915a97e62839c3e06492b80aabaa988a5491876",
        ),
        (
            SPEC_CONTENT_COLUMNS_V1,
            _spec_row(),
            "782f82fda3d62288c89347a71815c6a378b53ebc3ae230bbf47aa59cb0f84556",
        ),
        (
            SPEC_CONTENT_COLUMNS_V2,
            _spec_row(),
            "97f6bbcac06974bcc93807e1b83dc01a11cc81f03e00a2ff029cc629f4d839bd",
        ),
        (
            SPRINT_CONTENT_COLUMNS,
            _sprint_row(),
            "da417a8366787a6f89a00a0f78a1cc6fe84dbc1d4dc68b4d7ff22fd9d6f42c25",
        ),
        (
            CARD_CONTENT_COLUMNS,
            _card_row(),
            "8ecdc7ad757f2f7cddc421737afe5233aec2e158a01a383ac2c09ded6e407730",
        ),
        (
            AMENDMENT_CONTENT_COLUMNS,
            _amendment_row(),
            "d45224c82145e2eda1ecb858d44d1a59aefcd4e4eedb219e192343cf25f1c6ae",
        ),
    )

    for columns, row, expected in cases:
        assert _canonical_content_hash(row, columns) == expected


def test_derived_source_fields_stay_outside_hash_contracts() -> None:
    derived_fields = {
        "has_minimal_evidence",
        "lineage_complete",
        "source_artifact_status",
        "working_ttl_days",
    }
    contracts = (
        STORY_CONTENT_COLUMNS,
        IDEATION_CONTENT_COLUMNS,
        REFINEMENT_CONTENT_COLUMNS,
        SPEC_CONTENT_COLUMNS_V2,
        SPRINT_CONTENT_COLUMNS,
        CARD_CONTENT_COLUMNS,
        AMENDMENT_CONTENT_COLUMNS,
    )
    assert all(derived_fields.isdisjoint(columns) for columns in contracts)

    base_hash = _canonical_content_hash(_story_row(), STORY_CONTENT_COLUMNS)
    mutated = {
        **_story_row(),
        "has_minimal_evidence": False,
        "lineage_complete": False,
        "source_artifact_status": "discarded",
        "working_ttl_days": 365,
    }
    assert _canonical_content_hash(mutated, STORY_CONTENT_COLUMNS) == base_hash


def test_source_maturity_tracks_rebuild_and_canonical_artifact_sets() -> None:
    assert REBUILD_ARTIFACT_TYPES == (
        "story",
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "task",
        "test",
        "bug",
        "amendment_hotfix_revision",
    )
    assert CANONICAL_ARTIFACT_TYPES == (
        "refinement",
        "spec",
        "task",
        "test",
        "bug",
        "amendment_hotfix_revision",
    )
    assert "card" not in REBUILD_ARTIFACT_TYPES
    assert "card" not in CANONICAL_ARTIFACT_TYPES
    assert set(CANONICAL_ARTIFACT_TYPES).issubset(REBUILD_ARTIFACT_TYPES)


def _story_row() -> dict[str, object]:
    return {
        "title": "Story title",
        "description": "Story description",
        "actor": "Developer",
        "goal": "Keep hashes stable",
        "benefit": "Deterministic rebuilds",
        "topic_id": "topic-1",
        "status": "review",
        "labels": '["kg", "source"]',
        "version": 99,
    }


def _ideation_row() -> dict[str, object]:
    return {
        "title": "Ideation title",
        "description": "Ideation description",
        "problem_statement": "Problem",
        "proposed_approach": "Approach",
        "scope_assessment": "medium",
        "complexity": "m",
        "status": "review",
        "version": 3,
        "labels": '["idea"]',
    }


def _refinement_row() -> dict[str, object]:
    return {
        "title": "Refinement title",
        "description": "Refinement description",
        "in_scope": '["core"]',
        "out_of_scope": '["adapter"]',
        "analysis": "Analysis",
        "decisions": '[{"id":"d1","title":"Keep core pure"}]',
        "status": "approved",
        "version": 4,
        "labels": '["refinement"]',
    }


def _spec_row() -> dict[str, object]:
    return {
        "title": "Spec title",
        "description": "Spec description",
        "context": "Context",
        "version": 5,
        "functional_requirements": '["FR"]',
        "technical_requirements": '["TR"]',
        "acceptance_criteria": '["AC"]',
        "test_scenarios": '["TS"]',
        "business_rules": '["BR"]',
        "api_contracts": '[{"path":"\\/v1"}]',
        "decisions": '[{"id":"d1"}]',
        "integration_requirements": '["IR"]',
        "observability_requirements": '["OR"]',
    }


def _sprint_row() -> dict[str, object]:
    return {
        "title": "Sprint title",
        "description": "Sprint description",
        "spec_id": "spec-1",
        "spec_version": 5,
        "status": "planned",
        "lane_type": "delivery",
        "objective": "Objective",
        "expected_outcome": "Outcome",
        "test_scenario_ids": '["ts1"]',
        "business_rule_ids": '["br1"]',
        "evaluations": '[{"id":"eval1"}]',
        "version": 2,
        "labels": '["sprint"]',
    }


def _card_row() -> dict[str, object]:
    return {
        "title": "Card title",
        "description": "Card description",
        "details": "Details",
        "status": "in_progress",
        "priority": "high",
        "card_type": "bug",
        "spec_id": "spec-1",
        "sprint_id": "sprint-1",
        "test_scenario_ids": '["ts1"]',
        "conclusions": "[]",
        "screen_mockups": "[]",
        "knowledge_bases": "[]",
        "validations": "[]",
        "origin_task_id": "task-1",
        "severity": "major",
        "expected_behavior": "Expected",
        "observed_behavior": "Observed",
        "steps_to_reproduce": "Step",
        "action_plan": "Plan",
        "linked_test_task_ids": '["test-1"]',
        "labels": '["ignored"]',
    }


def _amendment_row() -> dict[str, object]:
    return {
        "original_spec_id": "spec-1",
        "origin_bug_id": "bug-1",
        "origin_task_ids": '["task-1"]',
        "affected_task_ids": '["task-2"]',
        "revision_spec_id": "spec-2",
        "regression_scenario_ids": '["ts-r"]',
        "regression_test_task_ids": '["test-r"]',
        "automated_regression_refs": '["tests/test_regression.py::test_x"]',
        "status": "done",
        "lineage_state": "complete",
        "validation_metadata": '{"ignored":true}',
    }

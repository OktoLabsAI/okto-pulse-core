"""Pure aggregate entities used by relational repository ports."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from okto_pulse.core.domain.enums import (
    IdeationComplexity,
    IdeationStatus,
    SpecStatus,
)


@dataclass(kw_only=True)
class Board:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    owner_id: str
    description: str | None = None
    realm_id: str | None = None
    settings: dict[str, Any] | None = None
    default_config_snapshot: dict[str, Any] | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    cards: list[Any] = field(default_factory=list, repr=False)
    ideations: list[Any] = field(default_factory=list, repr=False)
    topics: list[Any] = field(default_factory=list, repr=False)
    stories: list[Any] = field(default_factory=list, repr=False)
    specs: list[Any] = field(default_factory=list, repr=False)
    sprints: list[Any] = field(default_factory=list, repr=False)
    agent_grants: list[Any] = field(default_factory=list, repr=False)
    shares: list[Any] = field(default_factory=list, repr=False)


@dataclass(kw_only=True)
class Ideation:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    board_id: str
    title: str
    created_by: str
    description: str | None = None
    problem_statement: str | None = None
    proposed_approach: str | None = None
    scope_assessment: dict[str, Any] | None = None
    complexity: IdeationComplexity | None = None
    status: IdeationStatus = IdeationStatus.DRAFT
    edition: int = 1
    version: int = 1
    assignee_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    labels: list[str] | None = None
    screen_mockups: list[Any] | None = None
    archived: bool = False
    pre_archive_status: str | None = None
    skip_ambiguity_gate: bool = False
    skip_ambiguity_gate_edition: int | None = None
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None
    refinements: list[Any] = field(default_factory=list, repr=False)
    specs: list[Any] = field(default_factory=list, repr=False)
    qa_items: list[Any] = field(default_factory=list, repr=False)
    knowledge_bases: list[Any] = field(default_factory=list, repr=False)
    history: list[Any] = field(default_factory=list, repr=False)
    snapshots: list[Any] = field(default_factory=list, repr=False)
    architecture_designs: list[Any] = field(default_factory=list, repr=False)
    story_links: list[Any] = field(default_factory=list, repr=False)

    @property
    def stories(self) -> list[Any]:
        return [link.story for link in self.story_links if getattr(link, "story", None)]


@dataclass(kw_only=True)
class Spec:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    board_id: str
    title: str
    created_by: str
    ideation_id: str | None = None
    refinement_id: str | None = None
    source_refinement_snapshot_id: str | None = None
    source_refinement_version: int | None = None
    description: str | None = None
    context: str | None = None
    functional_requirements: list[Any] | None = None
    technical_requirements: list[Any] | None = None
    acceptance_criteria: list[Any] | None = None
    test_scenarios: list[Any] | None = None
    screen_mockups: list[Any] | None = None
    business_rules: list[Any] | None = None
    api_contracts: list[Any] | None = None
    integration_requirements: list[Any] | None = None
    observability_requirements: list[Any] | None = None
    decisions: list[Any] | None = None
    skip_test_coverage: bool = False
    skip_rules_coverage: bool = False
    skip_trs_coverage: bool = False
    skip_decisions_coverage: bool = False
    skip_contract_coverage: bool = False
    skip_ir_coverage: bool = False
    skip_or_coverage: bool = False
    skip_qualitative_validation: bool = False
    validation_threshold: int | None = None
    require_task_validation: bool | None = None
    validation_min_confidence: int | None = None
    validation_min_completeness: int | None = None
    validation_max_drift: int | None = None
    evaluations: list[Any] | None = None
    validations: list[Any] | None = None
    current_validation_id: str | None = None
    archived: bool = False
    pre_archive_status: str | None = None
    cancellation_reason: str | None = None
    cancelled_at: datetime | None = None
    cancelled_by: str | None = None
    status: SpecStatus = SpecStatus.DRAFT
    # Human-facing lifecycle counter. Unlike ``version`` (the technical
    # revision/CAS token), this only advances when a Spec re-enters draft.
    edition: int = 1
    version: int = 1
    assignee_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    labels: list[str] | None = None
    cards: list[Any] = field(default_factory=list, repr=False)
    sprints: list[Any] = field(default_factory=list, repr=False)
    knowledge_bases: list[Any] = field(default_factory=list, repr=False)
    qa_items: list[Any] = field(default_factory=list, repr=False)
    history: list[Any] = field(default_factory=list, repr=False)
    architecture_designs: list[Any] = field(default_factory=list, repr=False)


__all__ = ["Board", "Ideation", "Spec"]

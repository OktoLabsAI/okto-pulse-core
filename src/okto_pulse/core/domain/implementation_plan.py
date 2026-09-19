"""Authored contribution scope on the requirement, under its content authority.

This annotates the canonical linked_task_ids relationship; it never creates a
second editable relationship, an approval or an execution-completion claim.
"""

import json
from collections import Counter
from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

_Reference = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=255)
]
_Summary = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=2000)]


class ImplementationContribution(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    card_id: _Reference
    scope: Literal["whole_requirement", "selected_criteria"]
    criterion_ids: list[_Reference] = Field(default_factory=list, max_length=100)
    summary: _Summary | None = None

    @model_validator(mode="after")
    def valid_scope(self):
        if not self.card_id.strip() or any(
            not value.strip() for value in self.criterion_ids
        ):
            raise ValueError("implementation_contribution_identity_invalid")
        if len(set(self.criterion_ids)) != len(self.criterion_ids):
            raise ValueError("implementation_contribution_criterion_duplicate")
        if self.summary is not None and not self.summary.strip():
            raise ValueError("implementation_contribution_summary_required")
        if self.scope == "whole_requirement" and self.criterion_ids:
            raise ValueError("implementation_contribution_scope_conflict")
        if self.scope == "selected_criteria" and (
            not self.criterion_ids or not self.summary
        ):
            raise ValueError("implementation_contribution_scope_required")
        return self


class RequirementImplementationPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    contract_version: Literal["implementation-plan/v1"] = "implementation-plan/v1"
    contributions: list[ImplementationContribution] = Field(min_length=1, max_length=50)

    @model_validator(mode="after")
    def bounded_unique(self):
        ids = [entry.card_id for entry in self.contributions]
        if len(set(ids)) != len(ids):
            raise ValueError("implementation_contribution_card_duplicate")
        if (
            len(json.dumps(self.model_dump(mode="json"), ensure_ascii=False).encode())
            > 32 * 1024
        ):
            raise ValueError("implementation_plan_payload_too_large")
        return self


def implementation_plan_fields(value: Mapping) -> dict:
    if "implementation_plan" not in value:
        return {}
    raw = value["implementation_plan"]
    return {
        "implementation_plan": None
        if raw is None
        else RequirementImplementationPlan.model_validate(raw).model_dump(mode="json")
    }


def authored_contribution_card_ids(
    *, collections, criteria, previous_collections=None
) -> set[str]:
    """Validate changed declarations and return exact Cards requiring scope checks.

    An unchanged plan survives cancellation/removal of a dependency as a pending
    historical declaration. A new/changed declaration must use current canonical
    links and same-Spec criterion IDs. Runtime responsibility resolution checks
    applicability and completeness; Draft may remain incomplete.
    """
    criterion_counts = Counter(
        item.get("id")
        for item in criteria
        if isinstance(item, Mapping) and isinstance(item.get("id"), str)
    )
    needed = set()
    for field, items in collections.items():
        previous = {
            item.get("id"): item
            for item in (previous_collections or {}).get(field, ())
            if isinstance(item, Mapping)
        }
        for item in items:
            if not isinstance(item, Mapping) or item.get("implementation_plan") is None:
                continue
            plan = RequirementImplementationPlan.model_validate(
                item["implementation_plan"]
            )
            old = previous.get(item.get("id"), {}).get("implementation_plan")
            try:
                old = (
                    RequirementImplementationPlan.model_validate(old)
                    if old is not None
                    else None
                )
            except ValueError:
                old = None
            if old == plan:
                continue
            links = item.get("linked_task_ids") or []
            for contribution in plan.contributions:
                if contribution.card_id not in links:
                    raise ValueError(
                        "implementation_contribution_requires_canonical_task_link"
                    )
                if any(
                    criterion_counts[identity] != 1
                    for identity in contribution.criterion_ids
                ):
                    raise ValueError("implementation_contribution_criterion_unresolved")
                needed.add(contribution.card_id)
    return needed

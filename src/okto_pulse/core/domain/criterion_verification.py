"""Authored verification metadata on the existing acceptance criteria.

The criterion owns the link. Reverse views are derived; neither a link nor a
profile proves semantic adequacy or grants delivery credit. Missing metadata is
a planning gap, not a waiver. No read migration or approximate ID matching.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

VerificationProfile = Literal["functional", "integration", "technical", "operational"]
VerificationRequirementType = Literal[
    "functional_requirement",
    "technical_requirement",
    "integration_requirement",
    "observability_requirement",
    "business_rule",
]
VERIFICATION_REQUIREMENT_FIELDS = {
    "functional_requirement": "functional_requirements",
    "technical_requirement": "technical_requirements",
    "integration_requirement": "integration_requirements",
    "observability_requirement": "observability_requirements",
    "business_rule": "business_rules",
}
CRITERION_VERIFICATION_FIELDS = frozenset({"verification_profile", "requirement_links"})
_Reference = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=255)
]
_Aspect = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=2000)]


class CriterionRequirementLink(BaseModel):
    """Same-Spec obligation identity and optional authored aspect of its condition."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    requirement_type: VerificationRequirementType
    requirement_id: _Reference
    aspect: _Aspect | None = None

    @model_validator(mode="after")
    def nonblank(self):
        if not self.requirement_id.strip() or (
            self.aspect is not None and not self.aspect.strip()
        ):
            raise ValueError("criterion_requirement_link_blank")
        return self


class CriterionVerification(BaseModel):
    """Drafts may be incomplete. These fields are never evidence or approval."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    verification_profile: VerificationProfile | None = None
    requirement_links: list[CriterionRequirementLink] | None = Field(
        None, max_length=100
    )

    @model_validator(mode="after")
    def unique_targets(self):
        targets = [
            (link.requirement_type, link.requirement_id)
            for link in self.requirement_links or ()
        ]
        if len(set(targets)) != len(targets):
            raise ValueError("criterion_requirement_link_duplicate")
        return self


def criterion_verification_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    """Validate only the new closed fields, preserving legacy criterion shape."""
    return CriterionVerification.model_validate(
        {key: value[key] for key in CRITERION_VERIFICATION_FIELDS if key in value}
    ).model_dump(mode="json", exclude_unset=True)


def validate_criterion_requirement_links(
    criteria: Sequence[Any],
    collections: Mapping[str, Sequence[Any]],
) -> None:
    """Final-state referential integrity shared by bulk and structured writers.

    Inactive targets remain addressable for historical integrity. Their activity,
    profile compatibility and adequacy belong to planning/semantic evaluation;
    this function never calls them verified. Text/index aliases are not IDs.
    """
    identities = {
        kind: Counter(
            item.get("id")
            for item in collections.get(field, ())
            if isinstance(item, Mapping) and isinstance(item.get("id"), str)
        )
        for kind, field in VERIFICATION_REQUIREMENT_FIELDS.items()
    }
    for criterion in criteria:
        if not isinstance(criterion, Mapping):
            continue
        metadata = CriterionVerification.model_validate(
            criterion_verification_fields(criterion)
        )
        for link in metadata.requirement_links or ():
            if identities[link.requirement_type][link.requirement_id] != 1:
                # Deliberately no echoed title, source body or submitted value.
                raise ValueError(
                    "criterion_requirement_link_unresolved: use an exact, unambiguous same-Spec requirement ID and type"
                )

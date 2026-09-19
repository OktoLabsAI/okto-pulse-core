"""Authored requirement qualification and deterministic reference semantics.

The fixed evidence policy selects the new verification contract; it does not
relax existing evidence authorities. Defaults are proposals, never read-time
backfills. Their explicit values are stored only through an authorized writer.
"""

import hashlib
from okto_pulse.core.domain.implementation_plan import (
    RequirementImplementationPlan,
    implementation_plan_fields,
)
import json
from collections.abc import Mapping, Sequence
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    model_serializer,
    model_validator,
)

from okto_pulse.core.domain.criterion_verification import (
    VERIFICATION_REQUIREMENT_FIELDS,
    VerificationProfile,
    VerificationRequirementType,
)

Reference = Annotated[
    str, StringConstraints(strict=True, min_length=1, max_length=255, pattern=r"\S")
]
Digest = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9a-f]{64}$")]
DEFAULTS_VERSION = "requirement-verification-defaults/v1"
EVIDENCE_POLICY_REF = "pulse-verification/v1"
DEFAULT_PROFILES = {
    "functional_requirement": "functional",
    "technical_requirement": "technical",
    "integration_requirement": "integration",
    "observability_requirement": "operational",
}


class VerificationRequirementRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    requirement_type: VerificationRequirementType
    requirement_id: Reference

    @property
    def key(self) -> tuple[str, str]:
        return self.requirement_type, self.requirement_id


class VerificationInheritanceSelection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    source: VerificationRequirementRef
    source_digest: Digest
    criterion_ids: tuple[Reference, ...] = Field(min_length=1, max_length=100)
    covered_aspect: Annotated[
        str,
        StringConstraints(strict=True, min_length=1, max_length=2000, pattern=r"\S"),
    ]

    @model_validator(mode="after")
    def unique_criteria(self):
        if len(set(self.criterion_ids)) != len(self.criterion_ids):
            raise ValueError("verification_inheritance_duplicate_criterion")
        return self


class RequirementVerification(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    mode: Literal["explicit", "inherited"]
    required_profiles: tuple[VerificationProfile, ...] = Field(
        min_length=1, max_length=4
    )
    inheritance: tuple[VerificationInheritanceSelection, ...] = Field(
        default=(), max_length=20
    )
    evidence_policy_ref: Literal["pulse-verification/v1"] = EVIDENCE_POLICY_REF

    @model_validator(mode="after")
    def coherent(self):
        if len(set(self.required_profiles)) != len(self.required_profiles):
            raise ValueError("verification_duplicate_profile")
        if (self.mode == "inherited") != bool(self.inheritance):
            raise ValueError("verification_inheritance_mode_conflict")
        sources = [selection.source.key for selection in self.inheritance]
        if len(set(sources)) != len(sources):
            raise ValueError("verification_inheritance_duplicate_source")
        if (
            len(
                json.dumps(
                    self.model_dump(mode="json"),
                    ensure_ascii=False,
                    separators=(",", ":"),
                ).encode()
            )
            > 32 * 1024
        ):
            raise ValueError("verification_configuration_too_large")
        return self


class VerificationQualifiedModel(BaseModel):
    """Additive typed field without manufacturing nulls in older JSON objects."""

    verification: RequirementVerification | None = None
    implementation_plan: RequirementImplementationPlan | None = None

    @model_serializer(mode="wrap")
    def preserve_unset_verification(self, handler):
        result = handler(self)
        if "verification" not in self.model_fields_set:
            result.pop("verification", None)
        if "implementation_plan" not in self.model_fields_set:
            result.pop("implementation_plan", None)
        return result


def requirement_verification_fields(value: Mapping[str, Any]) -> dict[str, Any]:
    if "verification" not in value:
        return implementation_plan_fields(value)
    raw = value["verification"]
    return {
        **implementation_plan_fields(value),
        "verification": None
        if raw is None
        else RequirementVerification.model_validate(raw).model_dump(mode="json"),
    }


def verification_default_proposal(kind: str) -> dict[str, Any] | None:
    profile = DEFAULT_PROFILES.get(kind)
    if profile is None:
        return None
    return {
        "version": DEFAULTS_VERSION,
        "origin": "product",
        "requires_author_acceptance": True,
        "verification": {
            "mode": "explicit",
            "required_profiles": [profile],
            "inheritance": [],
            "evidence_policy_ref": EVIDENCE_POLICY_REF,
        },
    }


def requirement_verification_digest(
    spec_id: str, kind: str, item: Mapping[str, Any]
) -> str:
    """Definition binding: editorial metadata and task assignments are excluded.

    Remaining fields include qualification and selected inheritance, but never
    evidence results. Reordering profile/criterion sets is not a definition edit.
    Unknown authored fields stay conservative rather than disappearing from the
    binding. Activity is evaluated separately and never reinterpreted as proof.
    """
    semantic = {
        key: value
        for key, value in item.items()
        if key
        not in {
            "id",
            "notes",
            "locale",
            "status",
            "linked_task_ids",
            "implementation_plan",
            "created_at",
            "updated_at",
        }
    }
    if semantic.get("verification") is not None:
        qualification = RequirementVerification.model_validate(
            semantic["verification"]
        ).model_dump(mode="json")
        qualification["required_profiles"].sort()
        for selection in qualification["inheritance"]:
            selection["criterion_ids"].sort()
        qualification["inheritance"].sort(
            key=lambda selection: (
                selection["source"]["requirement_type"],
                selection["source"]["requirement_id"],
            )
        )
        semantic["verification"] = qualification
    return hashlib.sha256(
        json.dumps(
            {
                "spec_id": spec_id,
                "requirement_type": kind,
                "id": item.get("id"),
                "definition": semantic,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def validate_requirement_verification_references(
    *,
    spec_id: str,
    collections: Mapping[str, Sequence[Any]],
    criteria: Sequence[Any],
    previous_collections: Mapping[str, Sequence[Any]] | None = None,
) -> None:
    """Validate authored references; stale unchanged bindings remain reviewable.

    A material edit to a source does not rewrite other authors' selections.
    A newly authored/changed selection must bind the source currently in scope.
    Cycle/termination/profile adequacy are resolved by the read model; incomplete
    drafts are not treated as admission or successful delivery.
    """
    indexed: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    before: dict[tuple[str, str], Mapping[str, Any]] = {}
    for kind, field in VERIFICATION_REQUIREMENT_FIELDS.items():
        for item in collections.get(field, ()):
            if isinstance(item, Mapping) and isinstance(item.get("id"), str):
                indexed.setdefault((kind, item["id"]), []).append(item)
        for item in (previous_collections or {}).get(field, ()):
            if isinstance(item, Mapping) and isinstance(item.get("id"), str):
                before[(kind, item["id"])] = item
    criterion_counts: dict[str, int] = {}
    for item in criteria:
        if isinstance(item, Mapping) and isinstance(item.get("id"), str):
            criterion_counts[item["id"]] = criterion_counts.get(item["id"], 0) + 1
    for owner, items in indexed.items():
        for item in items:
            qualification = item.get("verification")
            if qualification is None:
                continue
            config = RequirementVerification.model_validate(qualification)
            previous = before.get(owner, {}).get("verification")
            # Compare the typed values: filling omitted schema defaults is not
            # an author changing an existing (possibly stale) selection.
            try:
                previous = (
                    RequirementVerification.model_validate(previous)
                    if previous is not None
                    else None
                )
            except ValueError:
                previous = None
            changed = previous != config
            for selection in config.inheritance:
                targets = indexed.get(selection.source.key, ())
                if selection.source.key == owner or len(targets) != 1:
                    raise ValueError("verification_inheritance_source_invalid")
                if any(
                    criterion_counts.get(identity) != 1
                    for identity in selection.criterion_ids
                ):
                    raise ValueError("verification_inheritance_criterion_invalid")
                if (
                    changed
                    and selection.source_digest
                    != requirement_verification_digest(
                        spec_id, selection.source.requirement_type, targets[0]
                    )
                ):
                    raise ValueError(
                        "spec_scope_revision_conflict: refresh the inherited source before changing this qualification"
                    )

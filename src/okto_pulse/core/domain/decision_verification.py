"""Decision adherence planning. References select proof, never manufacture it."""

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

Reference = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=255, pattern=r"^\S+$")]
ObligationReference = Annotated[str, StringConstraints(strict=True, max_length=255, pattern=r"^(fr|tr|br|ir|or|ac|api):[^:\s]+$")]
Text = Annotated[str, StringConstraints(strict=True, min_length=1, max_length=2000, pattern=r"\S")]
OBLIGATION_FIELDS = {
    "fr": "functional_requirements", "tr": "technical_requirements",
    "br": "business_rules", "ir": "integration_requirements",
    "or": "observability_requirements", "ac": "acceptance_criteria",
    "api": "api_contracts",
}
INACTIVE = frozenset({"superseded", "revoked", "deprecated", "cancelled"})


class DecisionInspectionScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    kind: Literal["spec", "obligation"]
    id: Reference


class DecisionInspection(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    condition: Text
    scope_refs: list[DecisionInspectionScope] = Field(min_length=1, max_length=20)

    @model_validator(mode="after")
    def unique_scope(self):
        if len({(ref.kind, ref.id) for ref in self.scope_refs}) != len(self.scope_refs):
            raise ValueError("decision_inspection_scope_duplicate")
        return self


class DecisionVerification(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    obligation_refs: list[ObligationReference] = Field(default_factory=list, max_length=100)
    inspection: DecisionInspection | None = None

    @model_validator(mode="after")
    def explicit_path(self):
        if not self.obligation_refs and self.inspection is None:
            raise ValueError("decision_verification_path_required")
        if len(set(self.obligation_refs)) != len(self.obligation_refs):
            raise ValueError("decision_verification_reference_duplicate")
        return self

    @property
    def method(self):
        return "both" if self.obligation_refs and self.inspection else "obligations" if self.obligation_refs else "inspection"


@dataclass(frozen=True, slots=True)
class DecisionVerificationPlan:
    decision_id: str
    verification: DecisionVerification | None
    blockers: tuple[str, ...]

    @property
    def complete(self):
        return self.verification is not None and not self.blockers


def resolve_decision_verification(*, spec_id, decisions, collections):
    """Resolve the entire active population; missing/ambiguous data fails closed."""
    if not isinstance(decisions, (list, tuple)) or len(decisions) > 5000:
        raise ValueError("decision_verification_population_unavailable")
    counts = Counter()
    active = set()
    for prefix, field in OBLIGATION_FIELDS.items():
        values = collections.get(field)
        if not isinstance(values, (list, tuple)) or len(values) > 5000:
            raise ValueError("decision_verification_population_unavailable")
        for value in values:
            if not isinstance(value, Mapping) or not isinstance(value.get("id"), str):
                raise ValueError("decision_verification_population_invalid")
            ref = f"{prefix}:{value['id']}"
            counts[ref] += 1
            if value.get("status", "active") not in INACTIVE:
                active.add(ref)
    identities = Counter(d.get("id") for d in decisions if isinstance(d, Mapping))
    plans = []
    for decision in decisions:
        if not isinstance(decision, Mapping) or not isinstance(decision.get("id"), str) or not decision["id"].strip():
            raise ValueError("decision_verification_population_invalid")
        if decision.get("status", "active") in INACTIVE:
            continue
        blockers = set()
        if identities[decision["id"]] != 1:
            blockers.add("decision_verification_identity_ambiguous")
        raw = decision.get("verification")
        verification = None
        if raw is None:
            blockers.add("decision_verification_required")
        else:
            try:
                verification = DecisionVerification.model_validate(raw)
            except (ValueError, TypeError):
                blockers.add("decision_verification_invalid")
        if verification:
            references = list(verification.obligation_refs)
            for ref in verification.inspection.scope_refs if verification.inspection else ():
                if ref.kind == "spec":
                    if ref.id != spec_id:
                        blockers.add("decision_inspection_scope_unresolved")
                else:
                    references.append(ref.id)
            if any(counts[ref] != 1 or ref not in active for ref in references):
                blockers.add("decision_verification_reference_unresolved")
        plans.append(DecisionVerificationPlan(decision["id"], verification, tuple(sorted(blockers))))
    return tuple(plans)


def decision_verification_plans(spec):
    return resolve_decision_verification(spec_id=spec.id, decisions=getattr(spec, "decisions", None),
        collections={field: getattr(spec, field, None) for field in OBLIGATION_FIELDS.values()})


def validate_decision_verification_references(*, spec_id, decisions, collections):
    """Writers permit an unfinished Draft, but never malformed explicit paths."""
    for plan in resolve_decision_verification(spec_id=spec_id, decisions=decisions, collections=collections):
        invalid = set(plan.blockers) - {"decision_verification_required"}
        if invalid:
            raise ValueError("; ".join(sorted(invalid)))

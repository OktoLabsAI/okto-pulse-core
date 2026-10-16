"""KG §5.5: current explicit references, independent of projection machinery.

Absence of an optional association is valid. Required associations retain their
existing domain gates; this policy never waives those gates or creates new ones.
"""
from collections import Counter
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from typing import Literal


MissingLinkMode = Literal["advisory", "blocking"]


def missing_link_mode(settings: Mapping | None) -> MissingLinkMode:
    mode = (settings or {}).get("missing_link_gate", "advisory")
    if mode not in ("advisory", "blocking"):
        raise ValueError("missing_link_gate_policy_invalid")
    return mode


class MissingLinkSourceUnavailable(ValueError):
    code = "missing_link_source_unavailable"


@dataclass(frozen=True, slots=True)
class SemanticLinkFinding:
    source_ref: str
    field: str
    target_ref: str
    reason: Literal["target_absent", "target_ambiguous", "target_out_of_scope"]
    correction_operation: str

    def to_payload(self) -> dict:
        return asdict(self)


# Closed normative link grammar used by the native structured Spec contract.
# No title matching, co-occurrence, KG IDs or generic edge authoring.
SPEC_LINK_FIELDS = (
    ("test_scenarios", "linked_criteria", ("acceptance_criteria",)),
    ("decisions", "linked_requirements", ("functional_requirements", "technical_requirements")),
    ("decisions", "supersedes_decision_id", ("decisions",)),
    ("business_rules", "linked_requirements", ("functional_requirements",)),
    ("integration_requirements", "linked_requirements", ("functional_requirements", "technical_requirements")),
    ("integration_requirements", "linked_api_contracts", ("api_contracts",)),
    ("observability_requirements", "linked_requirements", ("functional_requirements", "technical_requirements")),
    ("observability_requirements", "linked_integration_requirements", ("integration_requirements",)),
    ("api_contracts", "linked_rules", ("business_rules",)),
    ("api_contracts", "linked_requirements", ("functional_requirements", "technical_requirements")),
)


def spec_semantic_link_findings(*, spec_id: str, collections: Mapping) -> tuple[SemanticLinkFinding, ...]:
    """Evaluate a complete current source, with stable child IDs in diagnostics."""
    needed = {name for origin, _, targets in SPEC_LINK_FIELDS for name in (origin, *targets)}
    indices = {}
    for name in needed:
        if name not in collections or not isinstance(collections[name], (list, tuple)):
            raise MissingLinkSourceUnavailable(f"missing_link_source_unavailable:{name}")
        rows = collections[name]
        if any(not isinstance(row, Mapping) or type(row.get("id")) is not str or not row["id"] for row in rows):
            raise MissingLinkSourceUnavailable(f"missing_link_source_unavailable:{name}")
        indices[name] = Counter(row["id"] for row in rows)
    findings = []
    for source, field, targets in SPEC_LINK_FIELDS:
        counts = sum((indices[target] for target in targets), Counter())
        for row in collections[source]:
            if row.get("status") in {"revoked", "superseded", "retired", "cancelled"}:
                continue
            raw = row.get(field)
            if field == "supersedes_decision_id":
                refs = [] if raw is None else [raw]
            else:
                refs = [] if raw is None else raw
            if not isinstance(refs, (list, tuple)) or any(type(ref) is not str or not ref for ref in refs):
                raise MissingLinkSourceUnavailable(f"missing_link_source_unavailable:{source}:{row['id']}:{field}")
            for ref in sorted(set(refs)):
                if counts[ref] == 1:
                    continue
                findings.append(SemanticLinkFinding(
                    source_ref=f"spec:{spec_id}:{source}:{row['id']}", field=field,
                    target_ref=ref, reason="target_absent" if counts[ref] == 0 else "target_ambiguous",
                    correction_operation="update_spec_entity",
                ))
    return tuple(findings)

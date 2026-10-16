"""Public ownership of the current Code Evidence → Spec projection."""
from dataclasses import dataclass

CODE_EVIDENCE_LINK_NAMESPACE = "code_evidence_spec_links"
CODE_EVIDENCE_LINK_RULE = "supports/code_traceability_spec_link@v2.0"
CODE_TRACEABILITY_SPEC_ENDPOINTS = {
    "spec": ("Entity", ""),
    "functional_requirement": ("Requirement", "fr"),
    "technical_requirement": ("Constraint", "tr"),
    "acceptance_criterion": ("Criterion", "ac"),
    "business_rule": ("Constraint", "business_rule"),
    "api_contract": ("APIContract", "api_contract"),
    "integration_requirement": ("Requirement", "integration_requirement"),
    "observability_requirement": ("Constraint", "observability_requirement"),
    "decision": ("Decision", "decision"),
    "test_scenario": ("TestScenario", "test_scenario"),
}


@dataclass(frozen=True, slots=True)
class CodeEvidenceLinkFamily:
    owner_type: str = "code_evidence"
    edge_type: str = "supports"
    source_type: str = "Entity"
    rules: frozenset[str] = frozenset({CODE_EVIDENCE_LINK_RULE})
    target_sections: tuple[tuple[str, str], ...] = tuple(CODE_TRACEABILITY_SPEC_ENDPOINTS.values())

    def owns_writer(self, *, rule_id, layer, created_by):
        return (rule_id in self.rules and layer == "deterministic"
            and created_by == "worker_layer1")

    def matches_rule_family(self, rule_id):
        return isinstance(rule_id, str) and any(rule_id.startswith(
            rule.split("@", 1)[0] + "@") for rule in self.rules)

    def owns_endpoints(self, *, owner_id, source_type, target_type, source_ref, target_ref):
        if (source_type != "Entity" or source_ref != f"code_evidence:{owner_id}"
                or type(target_ref) is not str):
            return False
        parts = target_ref.split(":")
        if len(parts) not in (2, 4) or parts[0] != "spec" or any(
                not part or part.strip() != part for part in parts):
            return False
        return (target_type, "" if len(parts) == 2 else parts[2]) in self.target_sections


CODE_EVIDENCE_LINK_FAMILY = CodeEvidenceLinkFamily()


TARGET_EVIDENCE_NAMESPACE = "implementation_target_evidence"
TARGET_EVIDENCE_RULE = "derives_from/code_traceability_evidence@v2.0"


@dataclass(frozen=True, slots=True)
class TargetEvidenceFamily(CodeEvidenceLinkFamily):
    owner_type: str = "implementation_target"
    edge_type: str = "derives_from"
    rules: frozenset[str] = frozenset({TARGET_EVIDENCE_RULE})
    target_sections: tuple[tuple[str, str], ...] = (("Entity", ""),)

    def owns_endpoints(self, *, owner_id, source_type, target_type, source_ref, target_ref):
        if (source_type != "Entity" or target_type != "Entity"
                or source_ref != f"implementation_target:{owner_id}"
                or type(target_ref) is not str):
            return False
        parts = target_ref.split(":")
        return (len(parts) == 2 and parts[0] == "code_evidence"
            and bool(parts[1]) and parts[1].strip() == parts[1])


TARGET_OVERLAP_NAMESPACE = "implementation_target_overlaps"
TARGET_OVERLAP_RULE = "overlaps/code_traceability_current@v2.0"


@dataclass(frozen=True, slots=True)
class TargetOverlapFamily(TargetEvidenceFamily):
    """The lexically first Target owns the canonical directed pair."""

    edge_type: str = "overlaps"
    rules: frozenset[str] = frozenset({TARGET_OVERLAP_RULE})

    def owns_endpoints(self, *, owner_id, source_type, target_type, source_ref, target_ref):
        if (source_type != "Entity" or target_type != "Entity"
                or source_ref != f"implementation_target:{owner_id}"
                or type(target_ref) is not str):
            return False
        parts = target_ref.split(":")
        return (len(parts) == 2 and parts[0] == "implementation_target"
            and bool(parts[1]) and parts[1].strip() == parts[1] and owner_id < parts[1])


_TRACEABILITY_FAMILIES = {
    CODE_EVIDENCE_LINK_NAMESPACE: CODE_EVIDENCE_LINK_FAMILY,
    TARGET_EVIDENCE_NAMESPACE: TargetEvidenceFamily(),
    TARGET_OVERLAP_NAMESPACE: TargetOverlapFamily(),
}
TRACEABILITY_RELATIONSHIP_NAMESPACES = frozenset(_TRACEABILITY_FAMILIES)


def traceability_relationship_family(namespace):
    try:
        return _TRACEABILITY_FAMILIES[namespace]
    except (KeyError, TypeError):
        raise ValueError("traceability_relationship_namespace_invalid") from None

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
    edge_type: str = "supports"
    source_type: str = "Entity"
    rules: frozenset[str] = frozenset({CODE_EVIDENCE_LINK_RULE})
    target_sections: tuple[tuple[str, str], ...] = tuple(CODE_TRACEABILITY_SPEC_ENDPOINTS.values())

    def owns_writer(self, *, rule_id, layer, created_by):
        return (rule_id in self.rules and layer == "deterministic"
            and created_by == "worker_layer1")

    def matches_rule_family(self, rule_id):
        return isinstance(rule_id, str) and rule_id.startswith(
            "supports/code_traceability_spec_link@")

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

"""Source identities cannot be synthesized from positions or old ID aliases."""
import pytest

from okto_pulse.core.application.processors.deterministic_kg import DeterministicWorker


FAMILIES = [
    ("functional_requirements", "fr"),
    ("technical_requirements", "tr"),
    ("business_rules", "business_rule"),
    ("test_scenarios", "test_scenario"),
    ("api_contracts", "api_contract"),
    ("integration_requirements", "integration_requirement"),
    ("observability_requirements", "observability_requirement"),
    ("decisions", "decision"),
]


@pytest.mark.parametrize("family,section", FAMILIES)
@pytest.mark.parametrize("identity", [
    {}, {"id": None}, {"id": ""}, {"id": "  "}, {"id": 7}, {"id": True},
    {"decision_id": "old"}, {"scenario_id": "old"},
    {"contract_id": "old"}, {"rule_id": "old"},
])
def test_projection_rejects_missing_native_child_identity(family, section, identity):
    item = {"title": "Native child", "text": "Native child", **identity}
    with pytest.raises(ValueError, match="spec_child_identity_required"):
        DeterministicWorker().process_spec({"id": "spec-native", family: [item]})


@pytest.mark.parametrize("family,section", FAMILIES)
def test_native_child_identity_survives_reorder(family, section):
    children = [
        {"id": "first", "title": "First", "text": "First", "method": "GET", "path": "/first"},
        {"id": "second", "title": "Second", "text": "Second", "method": "GET", "path": "/second"},
    ]
    worker = DeterministicWorker()
    def identities(items):
        result = worker.process_spec({"id": "spec-native", family: items})
        return {n.title: n.source_artifact_ref for n in result.nodes
                if n.source_artifact_ref.startswith(f"spec:spec-native:{section}:")}
    assert identities(children) == identities(list(reversed(children)))
    assert set(identities(children).values()) == {
        f"spec:spec-native:{section}:first", f"spec:spec-native:{section}:second",
    }

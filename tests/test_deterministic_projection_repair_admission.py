"""F4: no manual source replay admission remains distributed."""
from importlib.util import find_spec
from okto_pulse.core.ports.application_services import KnowledgeGraphOperations


def test_manual_projection_repair_contracts_are_absent():
    assert find_spec("okto_pulse.core.application.use_cases.deterministic_projection_repair") is None
    assert find_spec("okto_pulse.core.kg.deterministic_projection_repair") is None
    assert not hasattr(KnowledgeGraphOperations, "stage_spec_projection_repair")

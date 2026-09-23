"""F4 retires the detailed audit; write-path schema validation remains."""
import importlib.util
import pytest
from okto_pulse.core.application.use_cases import mcp_kg_crud
from okto_pulse.core.kg.primitives import KGPrimitiveError, _validate_local_edge_pair
from okto_pulse.core.mcp import server
from okto_pulse.core.services import application_kg


def test_retired_audit_has_no_hidden_reader_or_use_case():
    assert importlib.util.find_spec("okto_pulse.core.kg.originates_from_audit") is None
    assert not hasattr(application_kg, "audit_originates_from_contract")
    assert not hasattr(server, "okto_pulse_kg_originates_from_contract_audit")
    for name in ("AuditOriginatesFromContractCommand", "AuditOriginatesFromContractResult", "AuditOriginatesFromContractUseCase"):
        assert not hasattr(mcp_kg_crud, name)


def test_kgh1_existing_write_path_still_rejects_known_bug_to_bug():
    with pytest.raises(KGPrimitiveError) as exc_info:
        _validate_local_edge_pair(
            "originates_from",
            "Bug",
            "Bug",
            session_id="session-kgh1",
        )

    assert exc_info.value.code == "invalid_edge_endpoint_types"


def test_existing_write_path_keeps_the_valid_bug_to_entity_contract():
    _validate_local_edge_pair("originates_from", "Bug", "Entity", session_id="session-valid")

"""One current API interaction contract across authorship and reads."""
from copy import deepcopy
from typing import get_args

import pytest
from pydantic import ValidationError

from okto_pulse.core.mcp.server import _project_api_contracts
from okto_pulse.core.models.schemas import ApiContract, DecisionStatus


@pytest.mark.parametrize("kind", ["in_process", "grpc", "event"])
def test_explicit_non_http_needs_no_http_method_or_path(kind):
    contract = ApiContract(id="c", contract_type=kind)
    assert contract.method is None and contract.path is None
    assert _project_api_contracts([contract.model_dump()])[0]["contract_type"] == kind


@pytest.mark.parametrize("verb", ["GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"])
def test_http_verbs_are_current(verb):
    assert ApiContract(id="c", method=verb, path="/x").method == verb


@pytest.mark.parametrize("token", ["CALL", "TOOL", "COMPONENT", "EVENT", "tool"])
@pytest.mark.parametrize("context", [None, {"on_write": True}])
def test_no_legacy_inference_on_read_or_write(token, context):
    data = {"id": "c", "method": token, "path": "/x"}
    before = deepcopy(data)
    with pytest.raises(ValidationError):
        ApiContract.model_validate(data, context=context)
    assert data == before
    with pytest.raises(ValidationError):
        _project_api_contracts([data])
    assert data == before


@pytest.mark.parametrize("data", [{"id": "c"}, {"id": "c", "method": "GET"}, "unparsed-row"])
def test_malformed_read_is_refused_without_repair(data):
    with pytest.raises(ValidationError):
        _project_api_contracts([data])


def test_current_projection_is_defensive_and_homogeneous():
    rows = [{"id": "c", "contract_type": "event", "description": "Order created"}]
    before = deepcopy(rows)
    result = _project_api_contracts(rows)
    assert result[0]["contract_type"] == "event"
    assert rows == before
    result[0]["description"] = "Changed projection"
    assert rows == before


def test_na_requires_justification_without_changing_decision_statuses():
    assert "not_applicable" not in get_args(DecisionStatus)
    with pytest.raises(ValidationError):
        ApiContract(id="c", contract_type="in_process", status="not_applicable")
    assert ApiContract(id="c", contract_type="in_process", status="not_applicable", notes="waived").status == "not_applicable"

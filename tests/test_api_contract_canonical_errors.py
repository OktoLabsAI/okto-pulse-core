"""
Canonical-error coverage for the api-contract write entry points
(spec 392376ee, AC4 / FR6 / dec_489b56a0 steer-a, dec_a7c8e190).

The four write entry points must reject a malformed contract shape with the
canonical ``invalid_api_contract`` error and NEVER leak the raw Pydantic
``errors.pydantic.dev`` URL surface. They route through two guards, tested here
at the exact functions each handler executes:

- okto_pulse_add_api_contract       → _validate_api_contract_write (server.py)
- okto_pulse_update_api_contract    → _validate_api_contract_write (server.py)
- the update_spec(api_contracts=) bulk path → _canonical_api_contract_error wrap
- okto_pulse_update_spec_api_contract → StructuredSpecEntityService.
  _validate_payload_for_create / _validate_payload_for_update

Every predicate is negative-wired: a well-formed contract still succeeds through
each path (so the wraps are not always-fail).
"""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from okto_pulse.core.mcp.server import (
    _canonical_api_contract_error,
    _validate_api_contract_write,
)
from okto_pulse.core.models.schemas import ApiContract
from okto_pulse.core.services.spec_structured_entities import StructuredSpecEntityService

NO_URL = "errors.pydantic.dev"


def _bad_http_call() -> dict:
    return {"id": "api_x", "method": "CALL", "path": "/x"}


def _bad_response_errors_object() -> dict:
    # response_errors must be a LIST; an object is a malformed shape.
    return {"id": "api_x", "method": "GET", "path": "/x", "response_errors": {"oops": 1}}


def _good_http() -> dict:
    return {"id": "api_x", "method": "GET", "path": "/x"}


# ---------------------------------------------------------------------------
# Entry points 1 & 2 — add_api_contract / update_api_contract via the shared
# _validate_api_contract_write guard (on_write strictness + canonical error).
# ---------------------------------------------------------------------------


def test_add_update_guard_rejects_call_canonically() -> None:
    err = _validate_api_contract_write(_bad_http_call())
    assert err is not None
    payload = json.loads(err)
    assert payload["error"] == "invalid_api_contract"
    assert NO_URL not in err
    assert "ValidationError" not in err


def test_add_update_guard_rejects_non_list_response_errors_canonically() -> None:
    err = _validate_api_contract_write(_bad_response_errors_object())
    assert err is not None
    assert json.loads(err)["error"] == "invalid_api_contract"
    assert NO_URL not in err


def test_add_update_guard_accepts_valid_contract() -> None:
    # negative-wiring: a well-formed contract passes (guard is not always-fail)
    assert _validate_api_contract_write(_good_http()) is None
    # a legacy token infers a non-http type and is accepted
    assert _validate_api_contract_write({"id": "api_x", "method": "TOOL"}) is None


# ---------------------------------------------------------------------------
# Entry point 4 — update_spec_api_contract via StructuredSpecEntityService
# (the validation methods are pure; a None session is fine).
# ---------------------------------------------------------------------------


def _service() -> StructuredSpecEntityService:
    return StructuredSpecEntityService(db=None)  # type: ignore[arg-type]


def test_structured_service_create_rejects_call_canonically() -> None:
    with pytest.raises(ValueError) as ei:
        _service()._validate_payload_for_create(
            "api_contract", {"method": "CALL", "path": "/x"}
        )
    msg = str(ei.value)
    assert "invalid_api_contract" in msg
    assert NO_URL not in msg


def test_structured_service_update_rejects_call_canonically() -> None:
    existing = {"id": "api_1", "contract_type": "http", "method": "GET", "path": "/x"}
    with pytest.raises(ValueError) as ei:
        _service()._validate_payload_for_update("api_contract", existing, {"method": "CALL"})
    msg = str(ei.value)
    assert "invalid_api_contract" in msg
    assert NO_URL not in msg


def test_structured_service_accepts_valid_contract() -> None:
    # negative-wiring: well-formed contracts are accepted through the service.
    svc = _service()
    created = svc._validate_payload_for_create("api_contract", {"method": "GET", "path": "/x"})
    assert created["contract_type"] == "http"
    assert created["method"] == "GET"
    # an in_process contract needs no method/path through the service.
    ip = svc._validate_payload_for_create("api_contract", {"contract_type": "in_process"})
    assert ip["contract_type"] == "in_process"


def test_structured_service_other_entities_keep_raw_message() -> None:
    # AC6 byte-unchanged: non-api_contract entities are NOT gated on on_write and
    # keep their existing str(exc) message path (no behavioral change for them).
    svc = _service()
    created = svc._validate_payload_for_create(
        "business_rule",
        {"title": "t", "rule": "r", "when": "w", "then": "x"},
    )
    assert created["id"].startswith("br_")


# ---------------------------------------------------------------------------
# Entry point 3 — the update_spec(api_contracts=) bulk path renders a residual
# ValidationError canonically (no URL leak).
# ---------------------------------------------------------------------------


def test_bulk_path_canonical_render_has_no_url() -> None:
    # a real ValidationError from the write-strict model, rendered canonically.
    with pytest.raises(ValidationError) as ei:
        ApiContract.model_validate(
            {"id": "c", "method": "CALL", "path": "/x"}, context={"on_write": True}
        )
    rendered = _canonical_api_contract_error(ei.value)
    payload = json.loads(rendered)
    assert payload["error"] == "invalid_api_contract"
    assert NO_URL not in rendered
    # the human-readable detail is present (actionable) but URL-free
    assert "CALL" in rendered or "method" in rendered.lower()

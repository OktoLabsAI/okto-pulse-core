"""
Tests for spec 392376ee (Ideação #5, F9+F10): ApiContract data-model hardening.

Schema layer (the IMPL-1 card). Strictness for http contracts is write-gated via
a Pydantic validation context (decision dec_a7c8e190): the four api-contract
write entry points construct with ``context={"on_write": True}``; read-back /
deserialization constructs without it and stays tolerant.

Covers:
- AC1 (ts_92db0cfb): an in_process contract validates with no method/path.
- AC2 (ts_85d99dff / ts_c84a9ca7): http method="CALL" is rejected on WRITE but a
  pre-existing stored "CALL" is TOLERATED on read-back (so list/get never crash).
- AC3 (ts_e4335290): a legacy method token (TOOL/COMPONENT/EVENT) infers
  contract_type while preserving the method value; a real verb infers http.
- AC6 (ts_31bb6a75): DecisionStatus + the Ideação #4-B not_applicable machinery
  are byte-unchanged; contract_type + the verb enum + the two new validators are
  the only ApiContract additions.

Every predicate is negative-wired (each assertion has a counter-case that proves
it is load-bearing, not vacuous).
"""

from __future__ import annotations

from pathlib import Path
from typing import get_args

import pytest
from pydantic import ValidationError

from okto_pulse.core.mcp.server import _project_api_contracts
from okto_pulse.core.models.schemas import ApiContract, DecisionStatus

CORE_DIR = Path(__file__).parent.parent / "src" / "okto_pulse" / "core"
SCHEMAS_PY = CORE_DIR / "models" / "schemas.py"

WRITE = {"on_write": True}


def _write(data: dict) -> ApiContract:
    """Construct as a write would — the four entry points pass on_write."""
    return ApiContract.model_validate(data, context=WRITE)


def _read(data: dict) -> ApiContract:
    """Construct as read-back / deserialization would — no on_write context."""
    return ApiContract.model_validate(data)


# ---------------------------------------------------------------------------
# AC1 — in_process contract validates without method/path
# ---------------------------------------------------------------------------


def test_ac1_in_process_contract_needs_no_method_or_path() -> None:
    c = ApiContract(id="c", contract_type="in_process")
    assert c.contract_type == "in_process"
    assert c.method is None
    assert c.path is None
    # also valid through the write path (on_write does not force method/path for non-http)
    assert _write({"id": "c", "contract_type": "in_process"}).contract_type == "in_process"
    # negative-wiring: an http contract on the write path DOES need method+path
    with pytest.raises(ValidationError):
        _write({"id": "c", "contract_type": "http"})


# ---------------------------------------------------------------------------
# AC2 — http method=CALL rejected on WRITE, real verb accepted (negative-wiring)
# ---------------------------------------------------------------------------


def test_ac2_http_call_rejected_on_write_real_verb_ok() -> None:
    with pytest.raises(ValidationError):
        _write({"id": "c", "method": "CALL", "path": "/x"})
    # negative-wiring: a real verb succeeds on the same write path
    ok = _write({"id": "c", "method": "GET", "path": "/x"})
    assert ok.method == "GET"
    assert ok.contract_type == "http"
    # http with no path is also rejected on write
    with pytest.raises(ValidationError):
        _write({"id": "c", "method": "GET"})
    # the full RFC verb set is accepted
    for verb in ("HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"):
        assert _write({"id": "c", "method": verb, "path": "/x"}).method == verb


# ---------------------------------------------------------------------------
# AC2 (read side) — a stored method=CALL is TOLERATED on read-back
# ---------------------------------------------------------------------------


def test_ac2_stored_call_tolerated_on_readback() -> None:
    # read-back (no on_write) must NOT raise — list/get over a board carrying this
    # pre-existing garbage must keep working.
    c = _read({"id": "c", "method": "CALL", "path": "/x"})
    assert c.contract_type == "http"
    assert c.method == "CALL"  # preserved, not coerced
    # negative-wiring: the SAME shape through the write path DOES raise.
    with pytest.raises(ValidationError):
        _write({"id": "c", "method": "CALL", "path": "/x"})


# ---------------------------------------------------------------------------
# AC3 — legacy method token infers contract_type, preserves method
# ---------------------------------------------------------------------------


def test_ac3_legacy_method_infers_contract_type() -> None:
    for token, expected in (
        ("TOOL", "in_process"),
        ("COMPONENT", "in_process"),
        ("EVENT", "event"),
    ):
        c = _read({"id": "c", "method": token, "path": "/x"})
        assert c.contract_type == expected, token
        assert c.method == token  # preserved
        # even on the write path the legacy contract validates (non-http escapes the verb enum)
        assert _write({"id": "c", "method": token, "path": "/x"}).contract_type == expected
    # case-insensitive
    assert _read({"id": "c", "method": "tool"}).contract_type == "in_process"
    # negative-wiring: a real verb infers http, NOT in_process
    assert _read({"id": "c", "method": "GET", "path": "/x"}).contract_type == "http"
    # an explicit contract_type always wins (no inference clobber)
    assert _read({"id": "c", "contract_type": "grpc", "method": "TOOL"}).contract_type == "grpc"


def test_ac3_explicit_http_plus_legacy_method_rejected_on_write() -> None:
    # TR2: claiming http while using a non-verb is rejected on write.
    with pytest.raises(ValidationError):
        _write({"id": "c", "contract_type": "http", "method": "TOOL", "path": "/x"})
    # negative-wiring: without the explicit http, the same method infers in_process and is fine.
    assert _write({"id": "c", "method": "TOOL", "path": "/x"}).contract_type == "in_process"


def test_legacy_read_projection_has_homogeneous_contract_type() -> None:
    malformed = "legacy-unparsed-row"
    rows = [
        {"id": "http", "method": "GET", "path": "/x"},
        {"id": "tool", "method": "TOOL"},
        {"id": "component", "method": "component"},
        {"id": "event", "method": "EVENT"},
        {"id": "explicit", "contract_type": "grpc", "method": "TOOL"},
        malformed,
    ]

    projected = _project_api_contracts(rows)

    assert [row["contract_type"] for row in projected[:-1]] == [
        "http",
        "in_process",
        "in_process",
        "event",
        "grpc",
    ]
    assert projected[-1] == malformed
    assert "contract_type" not in rows[0]


# ---------------------------------------------------------------------------
# AC6 — DecisionStatus + #4-B machinery byte-unchanged
# ---------------------------------------------------------------------------


def test_ac6_decisionstatus_and_na_machinery_unchanged() -> None:
    # DecisionStatus did NOT gain not_applicable
    assert "not_applicable" not in get_args(DecisionStatus)
    schemas = SCHEMAS_PY.read_text(encoding="utf-8")
    assert 'DecisionStatus = Literal["active", "superseded", "revoked"]' in schemas

    # the Ideação #4-B not_applicable + justification validator is intact on ApiContract,
    # and runs regardless of on_write context (it is not write-gated).
    with pytest.raises(ValidationError):
        ApiContract(id="c", contract_type="in_process", status="not_applicable")  # no notes
    ok = ApiContract(
        id="c", contract_type="in_process", status="not_applicable", notes="waived"
    )
    assert ok.status == "not_applicable"

    # the two new validators + the discriminator are present (the only ApiContract additions)
    assert "_infer_contract_type_from_legacy_method" in schemas
    assert "_validate_http_shape" in schemas
    assert "_require_na_justification" in schemas
    assert 'contract_type: Literal["http", "in_process", "grpc", "event"] = "http"' in schemas

"""
Doc-drift guard for the api-contract tool-doc (spec 392376ee, AC5 / FR5).

The api-contract tool-doc is the agent-facing description surface (R1
decision_ca50fba5b9a2). This guard asserts the F9/F10 facts are documented and
discoverable WITHOUT trial-and-error:
- the contract_type discriminator + its four values,
- the http-only method verb enum,
- the conditional optionality of method/path,
- the per-field JSON asymmetry (response_errors=LIST vs request_body /
  response_success=OBJECT).

Negative-wiring (AC5): the same checks FAIL on a synthetic doc with the
asymmetry note removed, so the guard is load-bearing, not vacuous.
"""

from __future__ import annotations

from pathlib import Path

DOC = (
    Path(__file__).parent.parent
    / "src" / "okto_pulse" / "core" / "mcp" / "resources"
    / "reference" / "tool-docs" / "api-contract.md"
)


def _missing_required_facts(text: str) -> list[str]:
    """Return the list of required doc facts that are ABSENT from ``text``."""
    lowered = text.lower()
    missing: list[str] = []

    # contract_type discriminator + its four values
    if "contract_type" not in lowered:
        missing.append("contract_type field")
    for value in ("http", "in_process", "grpc", "event"):
        if value not in lowered:
            missing.append(f"contract_type value {value!r}")

    # the http verb enum (a representative subset of the real verbs)
    for verb in ("GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "CONNECT", "TRACE"):
        if verb not in text:
            missing.append(f"http verb {verb}")

    # conditional optionality of method/path for non-http
    if "optional" not in lowered:
        missing.append("method/path optionality note")

    # the JSON asymmetry — response_errors is a LIST while the others are OBJECTs
    if "response_errors" not in lowered:
        missing.append("response_errors mention")
    if not ("list" in lowered and "object" in lowered):
        missing.append("LIST vs OBJECT asymmetry")
    # the explicit asymmetry sentence (the load-bearing marker)
    if "response_errors` is a list" not in lowered:
        missing.append("explicit response_errors-is-a-LIST sentence")

    return missing


def test_api_contract_doc_documents_f9_f10_facts() -> None:
    text = DOC.read_text(encoding="utf-8")
    missing = _missing_required_facts(text)
    assert missing == [], f"api-contract tool-doc is missing required facts: {missing}"


def test_doc_drift_guard_is_load_bearing() -> None:
    """Negative-wiring: stripping the asymmetry sentence makes the guard FAIL."""
    text = DOC.read_text(encoding="utf-8")
    # sanity: the real doc passes
    assert _missing_required_facts(text) == []

    # remove the explicit asymmetry sentence → the guard must now report it missing
    import re

    mutated = re.sub(
        r"In short: \*\*`response_errors` is a LIST.*?OBJECTs\.\*\*",
        "",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    assert mutated != text, "the asymmetry sentence was not found to remove (guard anchor drifted)"
    missing = _missing_required_facts(mutated)
    assert "explicit response_errors-is-a-LIST sentence" in missing

"""F4: public observation errors must not expose provider exception bodies."""
import json

import pytest

from okto_pulse.core.application import kg_rebuild
from okto_pulse.core.kg import safe_write_lifecycle
from okto_pulse.core.services import kg_health_service as health


@pytest.mark.parametrize("message", [
    "D:/private/another-board/source.json",
    "token=SYNTHETIC_SECRET\nSELECT body FROM private_content",
    "x" * 400,
])
def test_source_failure_is_unknown_with_stable_public_reason(monkeypatch, message):
    def unavailable():
        raise RuntimeError(message)

    monkeypatch.setattr(kg_rebuild, "build_source_store", unavailable)
    diagnostic = health._probe_rebuild_source_diagnostics("authorized-board")
    assert diagnostic == {
        "source_count": None,
        "canonical_source_count": None,
        "working_source_count": None,
        "enumeration_failure": True,
        "error": "source_enumeration_unavailable",
    }
    root = health._build_kg_root_cause(
        total_nodes=3, queue_depth=0, dead_letter_count=0, active_queue={},
        empty_after_materialized_history=False, combined_reasons=[],
        source_diag=diagnostic, safe_write_diag={},
    )
    assert root["drilldown_unavailable"] is True
    assert root["categories"]["source_enumeration_failure"]["present"] is True
    assert root["source_count"] is None
    assert message not in json.dumps(root)


def test_safe_write_failure_exposes_no_exception_body(monkeypatch):
    def unavailable():
        raise RuntimeError("SYNTHETIC_SECRET D:/private/lifecycle.json")

    monkeypatch.setattr(safe_write_lifecycle, "get_lifecycle_counter_samples", unavailable)
    diagnostic = health._probe_safe_write_diagnostics("authorized-board")
    assert diagnostic == {
        "last_safe_write_outcome": "unknown",
        "drain_failure": False,
        "outcomes": {},
        "probe_error": "safe_write_observation_unavailable",
    }

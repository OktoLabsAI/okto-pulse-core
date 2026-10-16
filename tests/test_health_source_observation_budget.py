"""Health source counts require bounded reads and do not need cognitive hashing."""

from types import SimpleNamespace
import pytest

from okto_pulse.core.application.rebuild_ports import BoardSourceSnapshot, SourceObservationBudget
from okto_pulse.core.kg import interfaces, rebuild_sources
from okto_pulse.core.services import kg_health_service as health


def test_health_supplies_budget_and_preserves_counts_without_cognitive_io(monkeypatch):
    calls = []

    class Reader:
        def fetch(self, board, *, observation_budget):
            calls.append((board, observation_budget))
            return BoardSourceSnapshot(rows=({
                "artifact_type": "spec", "id": "spec", "status": "done",
                "content_hash": "a" * 64, "source_version": "1",
                "created_at": "2026-09-20T00:00:00Z",
            },))

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: SimpleNamespace(require_board_source_reader=lambda: Reader()))
    monkeypatch.setattr(rebuild_sources, "_cognitive_durable_digest", lambda _: pytest.fail("count-only probe loaded cognitive store"))
    result = health._probe_rebuild_source_diagnostics("board")
    assert result == {"source_count": 1, "canonical_source_count": 1,
                      "working_source_count": 0, "enumeration_failure": False, "error": None}
    assert calls[0][0] == "board"
    budget = calls[0][1]
    assert type(budget) is SourceObservationBudget
    assert budget.timeout_seconds == health._HEALTH_PROBE_BUDGET_S


def test_health_never_retries_an_edition_without_budget_support(monkeypatch):
    class LegacyReader:
        def fetch(self, board):
            pytest.fail("unbounded source read")

    monkeypatch.setattr(interfaces, "get_kg_registry", lambda: SimpleNamespace(require_board_source_reader=lambda: LegacyReader()))
    result = health._probe_rebuild_source_diagnostics("board")
    assert result["enumeration_failure"] is True
    assert result["source_count"] is None
    assert result["error"] == "source_enumeration_unavailable"


@pytest.mark.parametrize("patch", [
    {"max_rows": True}, {"max_rows": 0}, {"max_rows": 10001},
    {"max_bytes": 1}, {"max_bytes": 16777217},
    {"timeout_seconds": float("nan")}, {"timeout_seconds": True},
    {"timeout_seconds": 0}, {"timeout_seconds": 6},
])
def test_observation_budget_is_closed_and_bounded(patch):
    with pytest.raises(ValueError):
        SourceObservationBudget(**patch)

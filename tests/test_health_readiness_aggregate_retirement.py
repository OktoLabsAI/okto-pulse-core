"""F4: technical readiness is bounded independently of queue population."""
import json

import pytest

from okto_pulse.core.services import kg_health_readiness_service as readiness
from okto_pulse.core.services import kg_health_service as health_service


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["summary", "full", "legacy"])
@pytest.mark.parametrize("enforcement", [False, True])
@pytest.mark.parametrize("artifact_ref", [None, "spec:foreign-or-sensitive"])
async def test_aggregates_do_not_read_rows_or_expose_errors_and_preserve_policy(
    monkeypatch, profile, enforcement, artifact_ref,
):
    snapshot = {
        "overall_state": "degraded",
        "dead_letter_count": 300001,
        "global_outbox_dead_letter_count": 200002,
        "canonical_debt": {"open_count": 100003},
        "operational_domains": {
            "active_queue": {"count": 7},
            "policy_constraint_projection": {"dlq_count": 400004},
        },
        "root_cause": {"scope": "graph", "categories": {
            "wal_or_commit_errors": {"present": True},
        }},
    }

    async def get_health(board_id, db, **kwargs):
        assert board_id == "authorized-board"
        return snapshot

    async def active(db, board_id):
        return enforcement

    async def forbidden(*args, **kwargs):
        pytest.fail("Readiness enumerated technical records")

    from okto_pulse.core.services import canonical_debt_service, dead_letter_inspector_service, queue_health_service
    monkeypatch.setattr(canonical_debt_service, "list_canonical_debt", forbidden)
    monkeypatch.setattr(dead_letter_inspector_service, "list_dead_letter_rows", forbidden)
    monkeypatch.setattr(queue_health_service, "get_global_outbox_dead_letter_drilldown", forbidden)
    monkeypatch.setattr(health_service, "get_kg_health", get_health)
    monkeypatch.setattr(readiness, "_enforcement_active", active)
    result = await readiness.build_health_readiness(
        "authorized-board", object(), profile=profile, artifact_ref=artifact_ref,
    )
    items = result["non_maskable_items"]
    assert len(items) == 5
    assert all(i["artifact_ref"] == "board:authorized-board" for i in items)
    assert all(i["next_action"] == "none" and i["drill_down_tool"] is None for i in items)
    assert all("last_error" not in i and "error_text" not in i for i in items)
    assert "foreign-or-sensitive" not in json.dumps(result)
    assert {i["signal"]: i.get("count") for i in items} == {
        "technical_dlq": 300001, "global_outbox_dead_letter": 200002,
        "canonical_debt_open": 100003, "policy_constraint_projection_dlq": 400004,
        "persistence_error": None,
    }
    assert result["technical_signals"]["technical_dlq_count"] == 500003
    assert result["technical_signals"]["active_queue_count"] == 7
    assert result["readiness"]["blocking"] is True
    assert result["readiness"]["would_block_done"] is enforcement
    assert set(result["readiness"]["reasons"]) == {i["signal"] for i in items}


def test_persistence_error_text_is_not_copied_to_aggregate():
    items = readiness._non_maskable_items("board-a", {
        "root_cause": {"categories": {"safe_write_drain_failure": {
            "present": True, "error": "D:/private/data token=secret payload of another board",
        }}},
    })
    assert len(items) == 1 and items[0]["signal"] == "persistence_error"
    assert not any(word in json.dumps(items) for word in ("private", "secret", "another board"))


def test_no_aggregate_invents_a_signal_for_zero_counts():
    assert readiness._non_maskable_items("board-a", {
        "dead_letter_count": 0,
        "canonical_debt": {"open_count": 0},
        "operational_domains": {"global_outbox_dead_letter": {"count": 0}},
    }) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["summary", "full", "legacy"])
@pytest.mark.parametrize("enforcement", [False, True])
@pytest.mark.parametrize("known_dlq", [0, 2])
@pytest.mark.parametrize("summary", [None, {}, {"open_count": None},
    {"status": "unavailable", "open_count": 0}, {"open_count": -1}, {"open_count": True}])
async def test_unavailable_debt_never_becomes_zero_or_clears_known_blocker(
    monkeypatch, profile, enforcement, known_dlq, summary,
):
    async def get_health(board_id, db, **kwargs):
        assert board_id == "authorized-board"
        return {"overall_state": "healthy", "dead_letter_count": known_dlq,
                "canonical_debt": summary}

    async def active(db, board_id):
        return enforcement

    monkeypatch.setattr(health_service, "get_kg_health", get_health)
    monkeypatch.setattr(readiness, "_enforcement_active", active)
    result = await readiness.build_health_readiness("authorized-board", object(), profile=profile)
    assert result["technical_signals"]["canonical_debt_open_count"] is None
    assert result["operational_domains"]["canonical_debt"]["count"] is None
    assert result["operational_domains"]["canonical_debt"]["status"] == "unavailable"
    assert result["overall_state"] != "healthy"
    expected_blocking = True if known_dlq else None
    assert result["readiness"]["blocking"] is expected_blocking
    assert result["readiness"]["would_block_done"] is (expected_blocking if enforcement else False)
    assert result["readiness"]["canonical_debt_observation_status"] == "unavailable"
    assert "canonical_debt_observation_unavailable" in result["readiness"]["reasons"]
    assert result["readiness"]["policy_reason"] != "no open technical signal"

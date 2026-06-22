"""R2-TEST3 (card 3df72c75) — health stale-canonical parity + primary cause.

Scenarios: ts_fe9fe207 (AC4 — Health surfaces stale_canonical_parity with the
read-only drilldown contract incl. literal ``mutation_allowed=false``) and
ts_110b573e (AC13 — it is a DISTINCT signal that never masks a stronger cause:
deterministic primary_health_cause, no double-count vs canonical_debt_open /
cognitive pending / partition integrity).

Anti-test-theater: the stale state is materialized by the REAL worker pipeline
then regressed via the real SQL maturity signal. The diagnostic is proven
read-only (the canonical node remains canonical after Health runs).
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_r2t3_"))

from r2_scenario_helpers import (
    count_canonical,
    new_board,
    seed_done_spec_canonical,
    set_spec_status,
)

from okto_pulse.core.kg.stale_canonical_parity import list_stale_canonical_parity
from okto_pulse.core.services.kg_health_service import get_kg_health

SCP = "stale_canonical_parity"


@pytest.fixture(autouse=True)
def _tmp_rebuild_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(tmp_path))
    return tmp_path


async def _make_stale_spec(db_factory, board_id):
    spec_id, _ref = await seed_done_spec_canonical(db_factory, board_id)
    assert count_canonical(board_id, "Requirement") >= 1
    await set_spec_status(db_factory, spec_id, "draft")  # real maturity regression
    return spec_id


def _issues(health, code):
    return [i for i in health.get("health_issues", []) if i.get("code") == code]


# ===========================================================================
# ts_fe9fe207 — AC4: Health drilldown + literal mutation_allowed=false (read-only)
# ===========================================================================


@pytest.mark.asyncio
async def test_health_surfaces_stale_parity_with_mutation_allowed_false(db_factory):
    board_id = await new_board(db_factory, "r2t3")
    await _make_stale_spec(db_factory, board_id)
    canonical_before = count_canonical(board_id, "Requirement")
    assert canonical_before >= 1

    async with db_factory() as db:
        health = await get_kg_health(board_id, db)

    issues = _issues(health, SCP)
    assert len(issues) == 1, issues
    issue = issues[0]
    assert issue["drill_down_tool"] == "okto_pulse_kg_stale_canonical_parity_list"
    # Literal contract flag (codex guidance) — the diagnostic declares no-mutation.
    assert issue["mutation_allowed"] is False
    sample = issue["sample"]
    for field in (
        "board_graph_stale", "global_discovery_stale_digest", "expected_graph_layer",
        "expected_maturity_status", "current_source_status", "recommended_action",
    ):
        assert field in sample, field

    # The drilldown payload carries the same literal flag ...
    async with db_factory() as db:
        drill = await list_stale_canonical_parity(db, board_id=board_id)
    assert drill["mutation_allowed"] is False
    assert drill["count"] >= 1
    assert drill["items"][0]["board_graph_stale"] is True

    # ... and the diagnostic is genuinely READ-ONLY: the node is still canonical.
    assert count_canonical(board_id, "Requirement") == canonical_before


@pytest.mark.asyncio
async def test_stale_parity_precedence_documented_and_primary_is_deterministic(db_factory):
    """The stale_parity issue documents its precedence (below debt/cognitive/
    partition, above digest mismatch), and primary_health_cause is DETERMINISTIC
    (identical across repeated reads of the same state). Note: the always-present
    decay_scheduler_debt baseline outranks stale_parity, so stale_parity does not
    claim global primary here — which is exactly the no-mask ordering (AC13)."""
    board_id = await new_board(db_factory, "r2t3")
    await _make_stale_spec(db_factory, board_id)

    async with db_factory() as db:
        health1 = await get_kg_health(board_id, db)
    async with db_factory() as db:
        health2 = await get_kg_health(board_id, db)

    issues = _issues(health1, SCP)
    assert len(issues) == 1, issues
    assert "below" in issues[0]["precedence_explanation"].lower()
    # Deterministic primary cause: same state -> same primary across reads.
    assert health1["primary_health_cause"] == health2["primary_health_cause"]
    assert health1["primary_health_cause"] not in ("none", None)
    # stale_parity is a warning that never escalates to claim primary over a
    # stronger/earlier-ranked cause.
    assert health1["primary_health_cause"] != SCP


# ===========================================================================
# ts_110b573e — AC13: distinct signal, never masks a stronger cause
# ===========================================================================


@pytest.mark.asyncio
async def test_stale_parity_distinct_and_does_not_mask_canonical_debt(db_factory):
    from okto_pulse.core.kg.canonical_learning_partition import PARTITION_TARGET_STATUS
    from okto_pulse.core.services.canonical_debt_service import upsert_canonical_debt

    board_id = await new_board(db_factory, "r2t3")
    await _make_stale_spec(db_factory, board_id)  # a real stale_canonical_parity
    async with db_factory() as db:
        await upsert_canonical_debt(
            db, board_id=board_id, artifact_type="bug", artifact_id="bug-scp",
            source_ref=f"card:bug:{uuid.uuid4()}:learning:s",
            content_hash="r2t3_debt", target_status=PARTITION_TARGET_STATUS,
            canonical_state="pending", failure_reason="some_reason",
        )
        await db.commit()
        health = await get_kg_health(board_id, db)

    # Both present as DISTINCT categories (exactly one each — no double-count) ...
    assert len(_issues(health, SCP)) == 1
    assert len(_issues(health, "canonical_debt_open")) == 1
    # ... and stale_canonical_parity NEVER claims primary over the stronger cause.
    assert health["primary_health_cause"] != SCP

"""KG-02.1 — Internal RebuildPreflightService contracts.

Covers FR2 (preflight enumerates without mutation), TR13 (read-only),
TR17 (status composition), OR or_4494881d (counter labels).
"""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.rebuild_preflight import (
    PreflightBlockReason,
    PreflightOutcome,
    RebuildHealthSummary,
    RebuildPreflightResult,
    RebuildPreflightService,
    RebuildSourceSummary,
    get_preflight_counter_labels,
    get_preflight_samples,
    reset_preflight_counter,
)


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_preflight_counter()
    yield
    reset_preflight_counter()


def _health(state: str = "healthy", generation: str | None = None) -> RebuildHealthSummary:
    return RebuildHealthSummary(
        base_state=state,
        metric_status="available",
        current_kg_generation_id=generation,
    )


def _source(
    eligible: int = 10,
    skipped: int = 0,
    non_det: bool = False,
) -> RebuildSourceSummary:
    return RebuildSourceSummary(
        eligible_count=eligible,
        skipped_cancelled_count=skipped,
        has_non_deterministic_inputs=non_det,
    )


def _source_row() -> dict[str, str]:
    return {
        "artifact_type": "spec",
        "id": "spec-test",
        "source_ref": "spec:spec-test",
        "source_version": "1",
        "content_hash": "a" * 64,
        "created_at": "2026-05-01T00:00:00Z",
        "status": "validated",
    }


def _service(health=None, source=None, status_probe=None) -> RebuildPreflightService:
    return RebuildPreflightService(
        source_probe=lambda _b: source if source else _source(),
        health_probe=lambda _b: health if health else _health(),
        rebuild_status_probe=status_probe or (lambda _b: ("idle", "")),
    )


# --- Happy paths --------------------------------------------------------------


def test_healthy_board_returns_ready_not_required():
    svc = _service()
    result = svc.run(board_id="b1")
    assert isinstance(result, RebuildPreflightResult)
    assert result.outcome == PreflightOutcome.READY.value
    assert result.action_required == "not_required"
    assert result.reason is None
    assert result.base_state == "healthy"
    assert result.eligible_source_count == 10
    assert result.preflight_hash  # deterministic
    assert result.generated_at
    assert result.rebuild_status == "idle"


def test_at_risk_returns_ready_recommended():
    svc = _service(health=_health(state="at_risk"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.READY.value
    assert result.action_required == "recommended"


# --- Confirmation-required paths ---------------------------------------------


def test_recovery_needed_requires_confirmation():
    svc = _service(health=_health(state="recovery_needed"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.action_required == "required"
    assert result.reason == PreflightBlockReason.HEALTH_RECOVERY_NEEDED.value


def test_backpressure_requires_confirmation():
    svc = _service(health=_health(state="backpressure"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.action_required == "recommended"
    assert result.reason == PreflightBlockReason.HEALTH_BACKPRESSURE_WITHOUT_ADMIN.value


def test_non_deterministic_sources_require_confirmation():
    svc = _service(source=_source(non_det=True))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.reason == PreflightBlockReason.NON_DETERMINISTIC_SOURCES.value


# --- Blocked paths ------------------------------------------------------------


def test_quarantined_is_blocked():
    svc = _service(health=_health(state="quarantined"))
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.BLOCKED.value
    assert result.reason == PreflightBlockReason.HEALTH_QUARANTINED.value


def test_source_store_failure_is_blocked():
    def boom(_b):
        raise RuntimeError("source store down")

    svc = RebuildPreflightService(
        source_probe=boom,
        health_probe=lambda _b: _health(),
    )
    result = svc.run(board_id="b1")
    assert result.outcome == PreflightOutcome.BLOCKED.value
    assert result.reason == PreflightBlockReason.SOURCE_STORE_UNAVAILABLE.value
    # Defaults applied so the response remains coherent.
    assert result.eligible_source_count == 0


# --- TR13 read-only invariant -------------------------------------------------


def test_preflight_never_calls_mutation_apis():
    """Static-source proof: rebuild_preflight.py MUST NOT reference any
    primitive that mutates KG storage."""
    import inspect
    import okto_pulse.core.kg.rebuild_preflight as module

    src = inspect.getsource(module)
    # Forbidden mutation calls.
    for forbidden in (
        "KGSingleWriterLock.acquire",
        "KGQuarantineService(",
        "purge_board_graph_storage",
        "purge_global_discovery_storage",
        "shutil.move",
        "os.unlink",
    ):
        assert forbidden not in src, (
            f"preflight must be read-only; found forbidden: {forbidden}"
        )


# --- TR15-aligned hash determinism --------------------------------------------


def test_preflight_hash_is_deterministic_across_runs():
    svc = _service(
        health=_health(state="at_risk", generation="gen-fixed"),
        source=_source(eligible=42, skipped=3),
    )
    a = svc.run(board_id="b1")
    b = svc.run(board_id="b1")
    assert a.preflight_hash == b.preflight_hash


def test_preflight_hash_changes_when_input_changes():
    svc_a = _service(source=_source(eligible=10))
    svc_b = _service(source=_source(eligible=11))
    a = svc_a.run(board_id="b1")
    b = svc_b.run(board_id="b1")
    assert a.preflight_hash != b.preflight_hash


# --- OR or_4494881d counter labels -------------------------------------------


def test_counter_labels_match_required_or_shape():
    assert get_preflight_counter_labels() == ("board_id", "outcome", "reason")


def test_counter_bumps_per_outcome():
    svc_ready = _service()
    svc_ready.run(board_id="b1")
    svc_block = _service(health=_health(state="quarantined"))
    svc_block.run(board_id="b1")
    svc_confirm = _service(health=_health(state="recovery_needed"))
    svc_confirm.run(board_id="b1")

    samples = get_preflight_samples()
    outcomes = {s["outcome"] for s in samples}
    assert PreflightOutcome.READY.value in outcomes
    assert PreflightOutcome.BLOCKED.value in outcomes
    assert PreflightOutcome.CONFIRMATION_REQUIRED.value in outcomes

    for s in samples:
        for label in get_preflight_counter_labels():
            assert label in s
            assert isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int) and s["count"] >= 1


# --- TR17 status composition --------------------------------------------------


def test_rebuild_status_is_composed_from_probe():
    svc = _service(status_probe=lambda _b: ("in_progress", "rebuild_running"))
    result = svc.run(board_id="b1")
    assert result.rebuild_status == "in_progress"
    assert result.operational_substatus == "rebuild_running"


def test_rebuild_status_default_is_idle():
    svc = RebuildPreflightService(
        source_probe=lambda _b: _source(),
        health_probe=lambda _b: _health(),
    )
    result = svc.run(board_id="b1")
    assert result.rebuild_status == "idle"


# --- Input validation --------------------------------------------------------


def test_empty_board_id_raises():
    svc = _service()
    with pytest.raises(ValueError):
        svc.run(board_id="")


# --- val_7a768ee2 rework: route registered + TestClient integration ---------




# --- SPEC4 card 8d77d45d / scenario ts_f49b7d37 ------------------------------
# Preflight on a recovery_needed board is NON-MUTATING (the graph.lbug store's
# timestamp + content hash are unchanged) AND diagnostic (returns root-cause +
# source/layer counts). This is a behavioural file-level proof on a REAL board
# graph, complementing the static "never calls a mutation api" check above.


def _graph_store_fingerprint(path) -> dict:
    """Map {relpath: (mtime, size, sha256|None)} for the board graph store on
    disk — handles graph.lbug being a single file OR a directory. mtime+size
    come from os.stat (never opens the file, so a Grafx lock can't hide a
    mutation); sha256 is added when the file is readable (the cache is closed
    before this is called)."""
    import hashlib
    import os

    def _one(fp: str) -> tuple:
        st = os.stat(fp)
        digest = None
        try:
            with open(fp, "rb") as fh:
                digest = hashlib.sha256(fh.read()).hexdigest()
        except OSError:
            digest = None  # still locked → mtime+size guard the mutation
        return (st.st_mtime, st.st_size, digest)

    p = str(path)
    out: dict[str, tuple] = {}
    if os.path.isdir(p):
        for root, _dirs, files in os.walk(p):
            for fname in files:
                fp = os.path.join(root, fname)
                out[os.path.relpath(fp, p)] = _one(fp)
    elif os.path.exists(p):
        out["."] = _one(p)
    return out


def test_preflight_is_non_mutating_and_diagnostic_on_recovery_needed():
    import uuid

    from kg_schema_testing import (
        board_graph_path,
        bootstrap_board_graph,
        close_board_db_cache,
        open_board_connection,
    )

    board_id = f"preflight-nonmut-{uuid.uuid4().hex[:8]}"
    # Materialize a REAL board graph with content so there is a store to guard.
    bootstrap_board_graph(board_id)
    with open_board_connection(board_id) as (_db, conn):
        conn.execute(
            "CREATE (n:Decision {id:$id, title:$t, graph_layer:'canonical', "
            "source_confidence:1.0})",
            {"id": "d1", "t": "seed"},
        )
    # Release the Grafx handle so the on-disk store is fully flushed + readable
    # for a content hash (preflight itself never opens the board graph).
    close_board_db_cache(board_id)

    graph_path = board_graph_path(board_id)
    before = _graph_store_fingerprint(graph_path)
    assert before, "expected a materialized board graph store on disk"

    # Run the REAL preflight on the recovery_needed board (root-cause) with a
    # source set that carries per-layer counts.
    source = RebuildSourceSummary(
        eligible_count=3,
        skipped_cancelled_count=0,
        has_non_deterministic_inputs=False,
        canonical_source_count=2,
        working_source_count=1,
        layer_counts={"canonical": 2, "working": 1},
    )
    svc = _service(health=_health(state="recovery_needed"), source=source)
    result = svc.run(board_id=board_id)

    # NON-MUTATION: the board graph store is byte-for-byte + timestamp identical.
    after = _graph_store_fingerprint(graph_path)
    assert after == before, "preflight mutated the board graph store (timestamp/hash changed)"

    # DIAGNOSTIC: recovery_needed root-cause + source/layer counts + hash.
    assert result.outcome == PreflightOutcome.CONFIRMATION_REQUIRED.value
    assert result.reason == PreflightBlockReason.HEALTH_RECOVERY_NEEDED.value
    assert result.base_state == "recovery_needed"
    assert result.canonical_source_count == 2
    assert result.eligible_source_count == 3
    assert result.layer_counts == {"canonical": 2, "working": 1}
    assert len(result.preflight_hash) == 64  # deterministic source-set hash

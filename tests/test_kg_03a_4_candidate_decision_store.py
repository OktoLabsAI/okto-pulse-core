"""Tests for KG-03A.4 — CandidateDecisionStore + listing API.

Coverage map (per spec card bd4346df):

* ``record`` persists candidate_id/board_id/source_ref/source_generation_id/
  consolidation_session_id/title/rationale/evidence_refs/status/
  created_by_agent_id/created_at/updated_at and never mutates
  ``spec.decisions`` on the Board DB.
* Initial status MUST be ``proposed``.
* Provenance validation rejects missing board_id / source_ref / generation /
  session / title / rationale / agent_id (bounded reason codes).
* Unsafe payload guard rejects oversized title/rationale, raw-token shapes,
  oversized evidence_refs list/entries.
* ``list`` filters by status and source_ref and applies limit/offset with
  stable ordering by created_at.
* GET /api/v1/kg/cognitive-pending/candidate-decisions returns contract
  shape with counts + items + readonly true.
* GET errors: invalid_status (400), candidate_decisions_unavailable (503).
* Counter ``kg_candidate_decision_event_total`` labels are bounded
  (board_id_hash, action, outcome, reason_code).
* The KG-03A.4 endpoint NEVER pokes the Board DB.
"""

from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.router import api_router
from okto_pulse.core.infra.auth import require_user
from okto_pulse.core.kg.candidate_decision_store import (
    CANDIDATE_DECISIONS_DIRNAME,
    CandidateDecisionAction,
    CandidateDecisionError,
    CandidateDecisionOutcome,
    CandidateDecisionReasonCode,
    CandidateDecisionStatus,
    CandidateDecisionStore,
    generate_candidate_id,
    get_candidate_counter_labels,
    get_candidate_event_count,
    get_candidate_samples,
    reset_candidate_counter,
)
from okto_pulse.core.kg.rebuild_audit import require_rebuild_audit_artifact_store


BOARD = "board-kg03a-4"
AGENT = "agent-kg03a-4"
SESSION = "sess_kg03a4"
GENERATION = "gen_kg03a4"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg-03a-4"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture
def store(base_dir: Path) -> CandidateDecisionStore:
    return CandidateDecisionStore(base_dir=base_dir)


@pytest.fixture(autouse=True)
def _reset_counter() -> Iterator[None]:
    reset_candidate_counter()
    yield
    reset_candidate_counter()


@pytest.fixture
def isolated_base_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    target = tmp_path / "kg-03a-4-rest"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(target))
    return target


@pytest.fixture
def client() -> TestClient:
    app = FastAPI()
    app.include_router(api_router)

    async def _fake_user() -> str:
        return "user-kg03a-4-test"

    app.dependency_overrides[require_user] = _fake_user
    return TestClient(app)


def _record(
    store: CandidateDecisionStore,
    *,
    title: str = "Adopt SQLite WAL for analytics ledger",
    rationale: str = (
        "Concurrent analytics writes are bottlenecked by the default "
        "journal mode; switching to WAL removes contention."
    ),
    source_ref: str = "spec:abc",
    source_generation_id: str = GENERATION,
    consolidation_session_id: str = SESSION,
    evidence_refs: list[str] | None = None,
    created_by_agent_id: str = AGENT,
):
    return store.record(
        board_id=BOARD,
        source_ref=source_ref,
        source_generation_id=source_generation_id,
        consolidation_session_id=consolidation_session_id,
        title=title,
        rationale=rationale,
        evidence_refs=evidence_refs,
        created_by_agent_id=created_by_agent_id,
    )


# ---------------------------------------------------------------------------
# Store — happy path / persistence
# ---------------------------------------------------------------------------


def test_record_persists_full_provenance_with_proposed_status(
    base_dir: Path, store: CandidateDecisionStore
) -> None:
    record = _record(
        store, evidence_refs=["spec:abc#section-3", "card:xyz"]
    )
    assert record.candidate_id.startswith("cand_")
    assert record.board_id == BOARD
    assert record.source_ref == "spec:abc"
    assert record.source_generation_id == GENERATION
    assert record.consolidation_session_id == SESSION
    assert record.title.startswith("Adopt SQLite WAL")
    assert record.rationale.startswith("Concurrent analytics")
    assert record.evidence_refs == (
        "spec:abc#section-3", "card:xyz",
    )
    assert record.status == CandidateDecisionStatus.PROPOSED.value
    assert record.created_by_agent_id == AGENT
    assert record.created_at
    assert record.updated_at == record.created_at
    assert record.formal_decision_ref is None
    assert record.dismissed_reason_code is None
    path = (
        base_dir
        / CANDIDATE_DECISIONS_DIRNAME
        / BOARD
        / f"{record.candidate_id}.json"
    )
    assert path.exists()


def test_record_generates_unique_candidate_ids(
    store: CandidateDecisionStore,
) -> None:
    seen: set[str] = set()
    for _ in range(20):
        record = _record(store, source_ref=f"spec:abc#{len(seen)}")
        assert record.candidate_id not in seen
        seen.add(record.candidate_id)


def test_record_never_touches_board_db_spec_decisions(
    tmp_path: Path, store: CandidateDecisionStore
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE spec_decisions (
                spec_id TEXT, decision_id TEXT,
                PRIMARY KEY(spec_id, decision_id)
            )
            """
        )
        conn.execute(
            "INSERT INTO spec_decisions VALUES ('spec:abc','dec:pre-existing')"
        )
        conn.commit()
    record = _record(store)
    assert record.status == CandidateDecisionStatus.PROPOSED.value
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM spec_decisions").fetchall()
    assert rows == [("spec:abc", "dec:pre-existing")]


def test_record_returns_none_when_get_unknown(
    store: CandidateDecisionStore,
) -> None:
    assert store.get(BOARD, "cand_does_not_exist") is None


def test_generate_candidate_id_is_prefixed_uuid() -> None:
    cid = generate_candidate_id()
    assert cid.startswith("cand_")
    assert len(cid) == len("cand_") + 32


# ---------------------------------------------------------------------------
# Store — provenance validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field, expected_reason",
    [
        ("board_id", CandidateDecisionReasonCode.MISSING_BOARD_ID.value),
        ("source_ref", CandidateDecisionReasonCode.MISSING_SOURCE_REF.value),
        (
            "source_generation_id",
            CandidateDecisionReasonCode.MISSING_GENERATION_ID.value,
        ),
        (
            "consolidation_session_id",
            CandidateDecisionReasonCode.MISSING_SESSION_ID.value,
        ),
        ("title", CandidateDecisionReasonCode.MISSING_TITLE.value),
        ("rationale", CandidateDecisionReasonCode.MISSING_RATIONALE.value),
        (
            "created_by_agent_id",
            CandidateDecisionReasonCode.MISSING_AGENT_ID.value,
        ),
    ],
)
def test_record_rejects_missing_provenance_field(
    store: CandidateDecisionStore, field: str, expected_reason: str
) -> None:
    kwargs = dict(
        board_id=BOARD,
        source_ref="spec:abc",
        source_generation_id=GENERATION,
        consolidation_session_id=SESSION,
        title="A title",
        rationale="A rationale",
        evidence_refs=None,
        created_by_agent_id=AGENT,
    )
    kwargs[field] = ""
    with pytest.raises(CandidateDecisionError) as excinfo:
        store.record(**kwargs)
    assert excinfo.value.outcome == (
        CandidateDecisionOutcome.VALIDATION_ERROR.value
    )
    assert excinfo.value.reason_code == expected_reason
    assert get_candidate_event_count(
        action=CandidateDecisionAction.RECORD.value,
        outcome=CandidateDecisionOutcome.VALIDATION_ERROR.value,
        reason_code=expected_reason,
    ) == 1


def test_record_rejects_whitespace_only_provenance(
    store: CandidateDecisionStore,
) -> None:
    with pytest.raises(CandidateDecisionError) as excinfo:
        store.record(
            board_id=BOARD,
            source_ref="   ",
            source_generation_id=GENERATION,
            consolidation_session_id=SESSION,
            title="title",
            rationale="rationale",
            evidence_refs=None,
            created_by_agent_id=AGENT,
        )
    assert excinfo.value.reason_code == (
        CandidateDecisionReasonCode.MISSING_SOURCE_REF.value
    )


# ---------------------------------------------------------------------------
# Store — unsafe payload guard
# ---------------------------------------------------------------------------


def test_record_rejects_oversized_title(
    store: CandidateDecisionStore,
) -> None:
    huge_title = "x" * 5000
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, title=huge_title)
    assert excinfo.value.outcome == (
        CandidateDecisionOutcome.UNSAFE_PAYLOAD.value
    )
    assert excinfo.value.unsafe_field == "title"


def test_record_rejects_oversized_rationale(
    store: CandidateDecisionStore,
) -> None:
    huge = "y" * 5000
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, rationale=huge)
    assert excinfo.value.unsafe_field == "rationale"


def test_record_rejects_token_shape_rationale(
    store: CandidateDecisionStore,
) -> None:
    token = "tok_" + "A" * 32
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, rationale=token)
    assert excinfo.value.outcome == (
        CandidateDecisionOutcome.UNSAFE_PAYLOAD.value
    )
    assert excinfo.value.unsafe_field == "rationale"


def test_record_rejects_evidence_refs_too_many(
    store: CandidateDecisionStore,
) -> None:
    refs = [f"spec:abc#{i}" for i in range(60)]
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, evidence_refs=refs)
    assert excinfo.value.unsafe_field == "evidence_refs"


def test_record_rejects_evidence_refs_entry_too_long(
    store: CandidateDecisionStore,
) -> None:
    refs = ["spec:abc#section", "x" * 300]
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, evidence_refs=refs)
    assert excinfo.value.unsafe_field == "evidence_refs"


def test_record_rejects_evidence_refs_token_shape(
    store: CandidateDecisionStore,
) -> None:
    refs = ["spec:abc#ok", "tok_" + "A" * 32]
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, evidence_refs=refs)
    assert excinfo.value.unsafe_field == "evidence_refs"


def test_record_rejects_non_string_evidence_entry(
    store: CandidateDecisionStore,
) -> None:
    refs = ["spec:abc#ok", 123]  # type: ignore[list-item]
    with pytest.raises(CandidateDecisionError) as excinfo:
        _record(store, evidence_refs=refs)  # type: ignore[arg-type]
    assert excinfo.value.unsafe_field == "evidence_refs"


def test_record_rejects_string_evidence_refs_field(
    store: CandidateDecisionStore,
) -> None:
    with pytest.raises(CandidateDecisionError) as excinfo:
        store.record(
            board_id=BOARD,
            source_ref="spec:abc",
            source_generation_id=GENERATION,
            consolidation_session_id=SESSION,
            title="t",
            rationale="r",
            evidence_refs="not-a-list",  # type: ignore[arg-type]
            created_by_agent_id=AGENT,
        )
    assert excinfo.value.unsafe_field == "evidence_refs"


def test_record_accepts_empty_evidence_refs(
    store: CandidateDecisionStore,
) -> None:
    record = _record(store, evidence_refs=[])
    assert record.evidence_refs == ()


# ---------------------------------------------------------------------------
# Store — list / filter / pagination
# ---------------------------------------------------------------------------


def test_list_returns_empty_for_unknown_board(
    store: CandidateDecisionStore,
) -> None:
    assert store.list("unknown-board") == []


def test_list_filters_by_status(store: CandidateDecisionStore) -> None:
    r1 = _record(store, source_ref="spec:a")
    r2 = _record(store, source_ref="spec:b")
    store.promote(
        board_id=BOARD,
        candidate_id=r2.candidate_id,
        formal_decision_ref="spec:b:dec_42",
    )
    proposed = store.list(
        BOARD, status_filter=CandidateDecisionStatus.PROPOSED.value
    )
    assert [r.candidate_id for r in proposed] == [r1.candidate_id]
    promoted = store.list(
        BOARD, status_filter=CandidateDecisionStatus.PROMOTED.value
    )
    assert [r.candidate_id for r in promoted] == [r2.candidate_id]
    assert promoted[0].formal_decision_ref == "spec:b:dec_42"


def test_list_filters_by_source_ref(store: CandidateDecisionStore) -> None:
    _record(store, source_ref="spec:a")
    target = _record(store, source_ref="spec:b")
    found = store.list(BOARD, source_ref_filter="spec:b")
    assert [r.candidate_id for r in found] == [target.candidate_id]


def test_list_applies_limit_and_offset(
    store: CandidateDecisionStore,
) -> None:
    for i in range(5):
        _record(store, source_ref=f"spec:abc#{i}")
    page1 = store.list(BOARD, limit=2, offset=0)
    page2 = store.list(BOARD, limit=2, offset=2)
    assert len(page1) == 2
    assert len(page2) == 2
    assert page1[0].candidate_id != page2[0].candidate_id


def test_list_orders_by_created_at_stable(
    store: CandidateDecisionStore,
) -> None:
    records = [
        _record(store, source_ref=f"spec:abc#{i}") for i in range(3)
    ]
    listed = store.list(BOARD)
    ordered_ids = [r.candidate_id for r in listed]
    assert sorted(ordered_ids) == sorted(r.candidate_id for r in records)


# ---------------------------------------------------------------------------
# Store — lifecycle methods (promote / link_existing / dismiss / no_action)
# ---------------------------------------------------------------------------


def test_promote_persists_status_and_formal_ref(
    store: CandidateDecisionStore,
) -> None:
    record = _record(store)
    updated = store.promote(
        board_id=BOARD,
        candidate_id=record.candidate_id,
        formal_decision_ref="spec:abc:dec_99",
    )
    assert updated.status == CandidateDecisionStatus.PROMOTED.value
    assert updated.formal_decision_ref == "spec:abc:dec_99"
    refetched = store.get(BOARD, record.candidate_id)
    assert refetched is not None
    assert refetched.status == CandidateDecisionStatus.PROMOTED.value
    assert refetched.formal_decision_ref == "spec:abc:dec_99"
    assert get_candidate_event_count(
        action=CandidateDecisionAction.PROMOTE.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


def test_link_existing_persists_status_and_formal_ref(
    store: CandidateDecisionStore,
) -> None:
    record = _record(store)
    updated = store.link_existing(
        board_id=BOARD,
        candidate_id=record.candidate_id,
        formal_decision_ref="spec:abc:dec_old",
    )
    assert updated.status == CandidateDecisionStatus.LINKED.value
    assert updated.formal_decision_ref == "spec:abc:dec_old"
    assert get_candidate_event_count(
        action=CandidateDecisionAction.LINK_EXISTING.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


def test_dismiss_persists_status_and_reason_code(
    store: CandidateDecisionStore,
) -> None:
    record = _record(store)
    updated = store.dismiss(
        board_id=BOARD,
        candidate_id=record.candidate_id,
        reason_code="duplicate",
    )
    assert updated.status == CandidateDecisionStatus.DISMISSED.value
    assert updated.dismissed_reason_code == "duplicate"
    assert get_candidate_event_count(
        action=CandidateDecisionAction.DISMISS.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


def test_mark_no_action_required_persists_status_and_reason_code(
    store: CandidateDecisionStore,
) -> None:
    record = _record(store)
    updated = store.mark_no_action_required(
        board_id=BOARD,
        candidate_id=record.candidate_id,
        reason_code="acknowledged_only",
    )
    assert updated.status == CandidateDecisionStatus.NO_ACTION_REQUIRED.value
    assert updated.dismissed_reason_code == "acknowledged_only"
    assert get_candidate_event_count(
        action=CandidateDecisionAction.NO_ACTION_REQUIRED.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


def test_transition_unknown_candidate_emits_not_found_and_raises(
    store: CandidateDecisionStore,
) -> None:
    with pytest.raises(CandidateDecisionError) as excinfo:
        store.promote(
            board_id=BOARD,
            candidate_id="cand_does_not_exist",
            formal_decision_ref="spec:abc:dec_x",
        )
    assert excinfo.value.outcome == CandidateDecisionOutcome.NOT_FOUND.value
    assert excinfo.value.reason_code == (
        CandidateDecisionReasonCode.NOT_FOUND.value
    )
    assert get_candidate_event_count(
        action=CandidateDecisionAction.PROMOTE.value,
        outcome=CandidateDecisionOutcome.NOT_FOUND.value,
        reason_code=CandidateDecisionReasonCode.NOT_FOUND.value,
    ) == 1


def test_transition_invalid_action_does_not_write_and_emits_validation_error(
    base_dir: Path, store: CandidateDecisionStore
) -> None:
    record = _record(store)
    path = (
        base_dir
        / CANDIDATE_DECISIONS_DIRNAME
        / BOARD
        / f"{record.candidate_id}.json"
    )
    mtime_before = path.stat().st_mtime
    with pytest.raises(CandidateDecisionError) as excinfo:
        store._transition(
            board_id=BOARD,
            candidate_id=record.candidate_id,
            action="totally_invalid",
            formal_decision_ref=None,
            dismissed_reason_code=None,
        )
    assert excinfo.value.outcome == (
        CandidateDecisionOutcome.VALIDATION_ERROR.value
    )
    assert excinfo.value.reason_code == (
        CandidateDecisionReasonCode.INVALID_STATUS.value
    )

    # File NOT rewritten and status NOT mutated.
    assert path.stat().st_mtime == mtime_before
    refetched = store.get(BOARD, record.candidate_id)
    assert refetched is not None
    assert refetched.status == CandidateDecisionStatus.PROPOSED.value

    # The raw caller-supplied action MUST NOT be emitted as a metric
    # label. Only the bounded sentinel "invalid" is allowed.
    samples = get_candidate_samples()
    assert not any(s["action"] == "totally_invalid" for s in samples), (
        "raw caller-supplied action leaked into the counter label: "
        f"{samples!r}"
    )
    invalid_samples = [
        s for s in samples
        if s["action"] == CandidateDecisionAction.INVALID.value
        and s["outcome"] == CandidateDecisionOutcome.VALIDATION_ERROR.value
        and s["reason_code"] == (
            CandidateDecisionReasonCode.INVALID_STATUS.value
        )
    ]
    assert len(invalid_samples) == 1

    # All emitted action labels must belong to the bounded enum.
    bounded_action_values = {a.value for a in CandidateDecisionAction}
    for sample in samples:
        assert sample["action"] in bounded_action_values, (
            f"unbounded action label {sample['action']!r} in samples"
        )

    assert get_candidate_event_count(
        action=CandidateDecisionAction.INVALID.value,
        outcome=CandidateDecisionOutcome.VALIDATION_ERROR.value,
        reason_code=CandidateDecisionReasonCode.INVALID_STATUS.value,
    ) == 1


def test_transition_record_action_rejected(
    store: CandidateDecisionStore,
) -> None:
    record = _record(store)
    with pytest.raises(CandidateDecisionError) as excinfo:
        store._transition(
            board_id=BOARD,
            candidate_id=record.candidate_id,
            action=CandidateDecisionAction.RECORD.value,
            formal_decision_ref=None,
            dismissed_reason_code=None,
        )
    assert excinfo.value.reason_code == (
        CandidateDecisionReasonCode.INVALID_STATUS.value
    )
    # The bounded "invalid" sentinel MUST be used — "record" itself must
    # NOT show up as a transition-action label.
    samples = get_candidate_samples()
    transition_samples = [
        s for s in samples
        if s["outcome"] == CandidateDecisionOutcome.VALIDATION_ERROR.value
        and s["reason_code"] == (
            CandidateDecisionReasonCode.INVALID_STATUS.value
        )
    ]
    assert transition_samples
    assert all(
        s["action"] == CandidateDecisionAction.INVALID.value
        for s in transition_samples
    )


# ---------------------------------------------------------------------------
# Counter — bounded labels
# ---------------------------------------------------------------------------


def test_counter_labels_are_bounded() -> None:
    assert get_candidate_counter_labels() == (
        "board_id_hash", "action", "outcome", "reason_code",
    )


def test_counter_emits_success_on_record(
    store: CandidateDecisionStore,
) -> None:
    _record(store)
    samples = get_candidate_samples()
    assert any(
        s["action"] == CandidateDecisionAction.RECORD.value
        and s["outcome"] == CandidateDecisionOutcome.SUCCESS.value
        and s["reason_code"] == CandidateDecisionReasonCode.NONE.value
        for s in samples
    )
    for sample in samples:
        assert len(sample["board_id_hash"]) == 16


def test_counter_emits_unsafe_payload_outcome(
    store: CandidateDecisionStore,
) -> None:
    huge = "x" * 5000
    with pytest.raises(CandidateDecisionError):
        _record(store, title=huge)
    assert get_candidate_event_count(
        action=CandidateDecisionAction.RECORD.value,
        outcome=CandidateDecisionOutcome.UNSAFE_PAYLOAD.value,
        reason_code=CandidateDecisionReasonCode.UNSAFE_PAYLOAD.value,
    ) == 1


# ---------------------------------------------------------------------------
# REST — GET /api/v1/kg/cognitive-pending/candidate-decisions
# ---------------------------------------------------------------------------


def test_rest_returns_empty_when_no_candidates(
    isolated_base_dir: Path, client: TestClient
) -> None:
    resp = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["board_id"] == BOARD
    assert payload["readonly"] is True
    assert payload["counts"] == {
        "proposed": 0,
        "promoted": 0,
        "linked": 0,
        "dismissed": 0,
        "no_action_required": 0,
        "total": 0,
    }
    assert payload["items"] == []


def test_rest_returns_contract_shape(
    isolated_base_dir: Path, client: TestClient
) -> None:
    store = CandidateDecisionStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    record = _record(
        store,
        source_ref="spec:abc",
        evidence_refs=["card:xyz"],
    )
    resp = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD},
    )
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["counts"] == {
        "proposed": 1,
        "promoted": 0,
        "linked": 0,
        "dismissed": 0,
        "no_action_required": 0,
        "total": 1,
    }
    assert payload["items"][0]["candidate_id"] == record.candidate_id
    assert payload["items"][0]["status"] == (
        CandidateDecisionStatus.PROPOSED.value
    )
    assert payload["items"][0]["evidence_refs"] == ["card:xyz"]
    assert payload["items"][0]["formal_decision_ref"] is None


def test_rest_counts_distinguish_all_five_statuses(
    isolated_base_dir: Path, client: TestClient
) -> None:
    store = CandidateDecisionStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    proposed = _record(store, source_ref="spec:a")
    promoted = _record(store, source_ref="spec:b")
    linked = _record(store, source_ref="spec:c")
    dismissed = _record(store, source_ref="spec:d")
    noop = _record(store, source_ref="spec:e")
    store.promote(
        board_id=BOARD,
        candidate_id=promoted.candidate_id,
        formal_decision_ref="spec:b:dec_p",
    )
    store.link_existing(
        board_id=BOARD,
        candidate_id=linked.candidate_id,
        formal_decision_ref="spec:c:dec_l",
    )
    store.dismiss(
        board_id=BOARD,
        candidate_id=dismissed.candidate_id,
        reason_code="duplicate",
    )
    store.mark_no_action_required(
        board_id=BOARD,
        candidate_id=noop.candidate_id,
        reason_code="acknowledged_only",
    )
    resp = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD},
    )
    assert resp.status_code == 200
    counts = resp.json()["counts"]
    assert counts == {
        "proposed": 1,
        "promoted": 1,
        "linked": 1,
        "dismissed": 1,
        "no_action_required": 1,
        "total": 5,
    }
    statuses = {item["status"] for item in resp.json()["items"]}
    assert statuses == {
        CandidateDecisionStatus.PROPOSED.value,
        CandidateDecisionStatus.PROMOTED.value,
        CandidateDecisionStatus.LINKED.value,
        CandidateDecisionStatus.DISMISSED.value,
        CandidateDecisionStatus.NO_ACTION_REQUIRED.value,
    }
    assert proposed.candidate_id  # keep var used


def test_rest_rejects_invalid_status(
    isolated_base_dir: Path, client: TestClient
) -> None:
    resp = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD, "status": "totally_made_up"},
    )
    assert resp.status_code == 400
    body = resp.json()
    assert body["detail"]["code"] == "invalid_status"


def test_rest_filters_by_status_and_source_ref(
    isolated_base_dir: Path, client: TestClient
) -> None:
    store = CandidateDecisionStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    r1 = _record(store, source_ref="spec:a")
    r2 = _record(store, source_ref="spec:b")
    store.promote(
        board_id=BOARD,
        candidate_id=r2.candidate_id,
        formal_decision_ref="spec:b:dec_99",
    )

    resp_proposed = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={
            "board_id": BOARD,
            "status": CandidateDecisionStatus.PROPOSED.value,
        },
    )
    assert resp_proposed.status_code == 200
    items = resp_proposed.json()["items"]
    assert [i["candidate_id"] for i in items] == [r1.candidate_id]

    resp_source = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD, "source_ref": "spec:b"},
    )
    items = resp_source.json()["items"]
    assert [i["candidate_id"] for i in items] == [r2.candidate_id]
    assert items[0]["formal_decision_ref"] == "spec:b:dec_99"


def test_rest_pagination_limits_and_offsets(
    isolated_base_dir: Path, client: TestClient
) -> None:
    store = CandidateDecisionStore(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    for i in range(4):
        _record(store, source_ref=f"spec:abc#{i}")
    resp = client.get(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD, "limit": 2, "offset": 1},
    )
    assert resp.status_code == 200
    assert len(resp.json()["items"]) == 2


def test_rest_requires_board_id(
    isolated_base_dir: Path, client: TestClient
) -> None:
    resp = client.get("/api/v1/kg/cognitive-pending/candidate-decisions")
    assert resp.status_code == 422


def test_rest_endpoint_does_not_expose_mutating_verbs(
    isolated_base_dir: Path, client: TestClient
) -> None:
    for verb in ("post", "put", "patch"):
        method = getattr(client, verb)
        resp = method(
            "/api/v1/kg/cognitive-pending/candidate-decisions",
            params={"board_id": BOARD},
            json={},
        )
        assert resp.status_code in (404, 405, 422), (
            f"verb {verb} should not be allowed on the read-only endpoint, "
            f"got {resp.status_code}"
        )
    resp = client.delete(
        "/api/v1/kg/cognitive-pending/candidate-decisions",
        params={"board_id": BOARD},
    )
    assert resp.status_code in (404, 405, 422), (
        f"DELETE should not be allowed on the read-only endpoint, "
        f"got {resp.status_code}"
    )

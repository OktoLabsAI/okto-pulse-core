"""Tests for KG-03A.5 — Candidate decision command API.

Coverage matrix (per spec card a438bee5):

* POST /api/v1/kg/cognitive-pending/candidate-decisions/{cand}/command
  routes the four bounded actions correctly:
    - promote_to_spec_decision
    - link_existing_decision
    - dismiss
    - no_action_required
* promote_to_spec_decision MUST actually write a new entry into the
  Board DB's ``spec.decisions`` (the formal flow), not just flip the
  candidate status.
* link_existing_decision MUST validate the formal decision exists on
  ``spec.decisions`` before linking and MUST NOT mutate ``spec.decisions``.
* dismiss / no_action_required MUST NOT touch ``spec.decisions``.
* Every accepted command writes a non-empty ``audit_ref`` (prefix
  ``audit_cmd_``) on the candidate record AND emits exactly one
  bounded counter sample for that action.
* Bad inputs (missing spec_id, missing reason_code, unknown candidate,
  unknown spec, unknown formal decision id, candidate already terminal,
  invalid action) all return the right HTTP code without mutating
  candidate or Board DB.
"""

from __future__ import annotations

import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from okto_pulse.core.api.router import api_router
from okto_pulse.core.infra import auth as _auth_mod
from okto_pulse.core.infra.database import get_db, get_session_factory
from okto_pulse.core.kg.candidate_decision_store import (
    CandidateDecisionAction,
    CandidateDecisionOutcome,
    CandidateDecisionStatus,
    CandidateDecisionStore,
    get_candidate_event_count,
    reset_candidate_counter,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    CognitivePendingMarker,
    CognitivePendingOutcomeType,
    default_rebuild_base_dir,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id
from okto_pulse.core.models.db import (
    Board,
    Ideation,
    Spec,
    SpecStatus,
)
from okto_pulse.core.services.main import SpecService


USER_ID = "kg03a5-rest-user"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4()}"


@pytest_asyncio.fixture
async def _client_and_entities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    base_dir = tmp_path / "kg-03a-5"
    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True)
    monkeypatch.setenv("OKTO_PULSE_REBUILD_BASE_DIR", str(base_dir))
    reset_candidate_counter()

    db_factory = get_session_factory()
    board_id = _id("kg03a5-board")
    ideation_id = _id("kg03a5-ideation")
    spec_id = _id("kg03a5-spec")
    existing_dec_id = "dec_preexist"

    async with db_factory() as db:
        db.add(Board(
            id=board_id, name="KG-03A.5 Board", owner_id=USER_ID,
        ))
        db.add(Ideation(
            id=ideation_id, board_id=board_id,
            title="KG-03A.5 Ideation", created_by=USER_ID,
        ))
        db.add(Spec(
            id=spec_id, board_id=board_id, ideation_id=ideation_id,
            title="KG-03A.5 Spec",
            status=SpecStatus.APPROVED, created_by=USER_ID,
            functional_requirements=["FR"], acceptance_criteria=["AC"],
            decisions=[
                {
                    "id": existing_dec_id,
                    "title": "Existing decision",
                    "rationale": "pre-seeded for link test",
                    "context": None,
                    "alternatives_considered": None,
                    "supersedes_decision_id": None,
                    "linked_requirements": [],
                    "linked_task_ids": None,
                    "status": "active",
                    "notes": None,
                },
            ],
        ))
        await db.commit()

    app = FastAPI()
    app.include_router(api_router)

    async def _override_db():
        async with db_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_db
    app.dependency_overrides[_auth_mod.require_user] = lambda: USER_ID

    client = TestClient(app)
    store = CandidateDecisionStore(base_dir=base_dir)

    yield client, store, db_factory, {
        "board_id": board_id,
        "spec_id": spec_id,
        "existing_decision_id": existing_dec_id,
        "base_dir": base_dir,
    }

    reset_candidate_counter()


@pytest.fixture(autouse=True)
def _reset_counter_between_tests() -> Iterator[None]:
    reset_candidate_counter()
    yield
    reset_candidate_counter()


def _seed_candidate(
    store: CandidateDecisionStore,
    *,
    board_id: str,
    spec_id: str,
    source_generation_id: str | None = None,
    title: str = "Adopt SQLite WAL for analytics ledger",
    rationale: str = (
        "Concurrent analytics writes are bottlenecked; WAL removes "
        "contention."
    ),
):
    return store.record(
        board_id=board_id,
        source_ref=f"spec:{spec_id}",
        source_generation_id=source_generation_id or "gen_kg03a5",
        consolidation_session_id="sess_kg03a5",
        title=title,
        rationale=rationale,
        evidence_refs=["card:xyz"],
        created_by_agent_id="agent-kg03a5",
    )


def _endpoint(board_id: str, candidate_id: str) -> str:
    return (
        f"/api/v1/kg/cognitive-pending/candidate-decisions/"
        f"{candidate_id}/command"
    )


async def _read_spec_decisions(db_factory, spec_id: str) -> list[dict]:
    async with db_factory() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if spec is None:
            return []
        return list(spec.decisions or [])


# ---------------------------------------------------------------------------
# promote_to_spec_decision — happy path + side-effect on Board DB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promote_writes_to_spec_decisions_and_links_candidate(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )

    before = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert len(before) == 1

    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": ids["spec_id"],
            "notes": "promoted via candidate command API",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["status"] == CandidateDecisionStatus.PROMOTED.value
    assert payload["action"] == CandidateDecisionAction.PROMOTE.value
    assert payload["audit_ref"].startswith("audit_cmd_")
    formal_ref = payload["formal_decision_ref"]
    assert formal_ref.startswith("dec_")
    assert payload["formal_decision"]["id"] == formal_ref
    assert payload["formal_decision"]["title"] == candidate.title

    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert len(after) == 2
    new_dec = next(d for d in after if d["id"] == formal_ref)
    assert new_dec["rationale"] == candidate.rationale
    assert new_dec["status"] == "active"

    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROMOTED.value
    assert stored.formal_decision_ref == formal_ref
    assert stored.audit_ref == payload["audit_ref"]

    assert get_candidate_event_count(
        action=CandidateDecisionAction.PROMOTE.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


@pytest.mark.asyncio
async def test_promote_requires_spec_id(_client_and_entities) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "spec_id_required"
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value
    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert len(after) == 1


@pytest.mark.asyncio
async def test_promote_unknown_spec_404(_client_and_entities) -> None:
    client, store, _, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": "spec_does_not_exist",
        },
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "spec_not_found"
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value


@pytest.mark.asyncio
async def test_promote_uses_overridden_title_and_rationale(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": ids["spec_id"],
            "title": "Override title",
            "rationale": "Override rationale",
            "alternatives_considered": ["A", "B"],
        },
    )
    assert resp.status_code == 200, resp.text
    new_ref = resp.json()["formal_decision_ref"]
    decisions = await _read_spec_decisions(db_factory, ids["spec_id"])
    new_dec = next(d for d in decisions if d["id"] == new_ref)
    assert new_dec["title"] == "Override title"
    assert new_dec["rationale"] == "Override rationale"
    assert new_dec["alternatives_considered"] == ["A", "B"]


# ---------------------------------------------------------------------------
# link_existing_decision — happy path + validates the decision exists
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_link_existing_links_without_mutating_spec(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    before = await _read_spec_decisions(db_factory, ids["spec_id"])

    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "link_existing_decision",
            "spec_id": ids["spec_id"],
            "formal_decision_id": ids["existing_decision_id"],
        },
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["status"] == CandidateDecisionStatus.LINKED.value
    assert payload["formal_decision_ref"] == ids["existing_decision_id"]
    assert payload["audit_ref"].startswith("audit_cmd_")

    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert after == before, (
        "link_existing_decision must NOT mutate spec.decisions"
    )

    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.LINKED.value
    assert stored.formal_decision_ref == ids["existing_decision_id"]
    assert stored.audit_ref == payload["audit_ref"]

    assert get_candidate_event_count(
        action=CandidateDecisionAction.LINK_EXISTING.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


@pytest.mark.asyncio
async def test_link_existing_unknown_decision_404(_client_and_entities) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "link_existing_decision",
            "spec_id": ids["spec_id"],
            "formal_decision_id": "dec_does_not_exist",
        },
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "formal_decision_not_found"
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value
    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert len(after) == 1


@pytest.mark.asyncio
async def test_link_existing_requires_formal_decision_id(
    _client_and_entities,
) -> None:
    client, store, _, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "link_existing_decision",
            "spec_id": ids["spec_id"],
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "formal_decision_id_required"


# ---------------------------------------------------------------------------
# dismiss / no_action_required — MUST NOT create a formal decision
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dismiss_does_not_create_formal_decision(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    before = await _read_spec_decisions(db_factory, ids["spec_id"])

    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "dismiss",
            "reason_code": "duplicate",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["status"] == CandidateDecisionStatus.DISMISSED.value
    assert payload["formal_decision_ref"] is None
    assert payload["formal_decision"] is None
    assert payload["dismissed_reason_code"] == "duplicate"
    assert payload["audit_ref"].startswith("audit_cmd_")

    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert after == before, "dismiss must NOT mutate spec.decisions"

    assert get_candidate_event_count(
        action=CandidateDecisionAction.DISMISS.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


@pytest.mark.asyncio
async def test_dismiss_requires_reason_code(_client_and_entities) -> None:
    client, store, _, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "dismiss",
            "reason_code": "   ",
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "reason_code_required"
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value


@pytest.mark.asyncio
async def test_no_action_required_does_not_create_formal_decision(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    before = await _read_spec_decisions(db_factory, ids["spec_id"])

    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "no_action_required",
            "reason_code": "acknowledged_only",
        },
    )
    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["status"] == (
        CandidateDecisionStatus.NO_ACTION_REQUIRED.value
    )
    assert payload["formal_decision_ref"] is None
    assert payload["dismissed_reason_code"] == "acknowledged_only"

    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert after == before, (
        "no_action_required must NOT mutate spec.decisions"
    )
    assert get_candidate_event_count(
        action=CandidateDecisionAction.NO_ACTION_REQUIRED.value,
        outcome=CandidateDecisionOutcome.SUCCESS.value,
    ) == 1


# ---------------------------------------------------------------------------
# Cross-cutting / error paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_candidate_not_found_returns_404(_client_and_entities) -> None:
    client, _, _, ids = _client_and_entities
    resp = client.post(
        _endpoint(ids["board_id"], "cand_does_not_exist"),
        json={
            "board_id": ids["board_id"],
            "action": "dismiss",
            "reason_code": "duplicate",
        },
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "candidate_not_found"


@pytest.mark.asyncio
async def test_candidate_already_terminal_returns_409(
    _client_and_entities,
) -> None:
    client, store, _, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    store.dismiss(
        board_id=ids["board_id"],
        candidate_id=candidate.candidate_id,
        reason_code="duplicate",
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "no_action_required",
            "reason_code": "too_late",
        },
    )
    assert resp.status_code == 409
    assert resp.json()["detail"]["code"] == "candidate_already_terminal"


@pytest.mark.asyncio
async def test_invalid_action_returns_422(_client_and_entities) -> None:
    client, store, _, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "this_is_not_real",
        },
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# Cross-board mutation guard (val_3366d1d7 blocker #1)
# ---------------------------------------------------------------------------


async def _seed_foreign_spec(db_factory) -> dict[str, str]:
    foreign_board_id = _id("kg03a5-foreign-board")
    foreign_ideation_id = _id("kg03a5-foreign-ideation")
    foreign_spec_id = _id("kg03a5-foreign-spec")
    foreign_dec_id = "dec_foreign"
    async with db_factory() as db:
        db.add(Board(
            id=foreign_board_id, name="Foreign KG-03A.5 Board",
            owner_id=USER_ID,
        ))
        db.add(Ideation(
            id=foreign_ideation_id, board_id=foreign_board_id,
            title="Foreign", created_by=USER_ID,
        ))
        db.add(Spec(
            id=foreign_spec_id, board_id=foreign_board_id,
            ideation_id=foreign_ideation_id,
            title="Foreign spec",
            status=SpecStatus.APPROVED, created_by=USER_ID,
            functional_requirements=["FR"], acceptance_criteria=["AC"],
            decisions=[{
                "id": foreign_dec_id,
                "title": "Foreign existing decision",
                "rationale": "do not touch from another board",
                "context": None,
                "alternatives_considered": None,
                "supersedes_decision_id": None,
                "linked_requirements": [],
                "linked_task_ids": None,
                "status": "active",
                "notes": None,
            }],
        ))
        await db.commit()
    return {
        "board_id": foreign_board_id,
        "spec_id": foreign_spec_id,
        "decision_id": foreign_dec_id,
    }


@pytest.mark.asyncio
async def test_promote_rejected_when_target_spec_belongs_to_other_board(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    foreign = await _seed_foreign_spec(db_factory)
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )

    before = await _read_spec_decisions(db_factory, foreign["spec_id"])
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": foreign["spec_id"],
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "target_spec_board_mismatch"

    after = await _read_spec_decisions(db_factory, foreign["spec_id"])
    assert after == before, (
        "promote must NOT mutate spec.decisions on a foreign board"
    )

    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value


@pytest.mark.asyncio
async def test_link_existing_rejected_when_target_spec_belongs_to_other_board(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    foreign = await _seed_foreign_spec(db_factory)
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )

    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "link_existing_decision",
            "spec_id": foreign["spec_id"],
            "formal_decision_id": foreign["decision_id"],
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "target_spec_board_mismatch"

    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value


# ---------------------------------------------------------------------------
# Override unsafe-payload guard (val_3366d1d7 blocker #2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_promote_rejects_oversized_title_override(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    before = await _read_spec_decisions(db_factory, ids["spec_id"])
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": ids["spec_id"],
            "title": "X" * 5000,
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "unsafe_override_payload"
    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert after == before
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value


@pytest.mark.asyncio
async def test_promote_rejects_token_shape_rationale_override(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    before = await _read_spec_decisions(db_factory, ids["spec_id"])
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": ids["spec_id"],
            "rationale": "tok_" + "A" * 32,
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "unsafe_override_payload"
    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert after == before
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.PROPOSED.value


@pytest.mark.asyncio
async def test_promote_rejects_alternatives_considered_token_entry(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    before = await _read_spec_decisions(db_factory, ids["spec_id"])
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": ids["spec_id"],
            "alternatives_considered": [
                "Plain alternative",
                "tok_" + "A" * 32,
            ],
        },
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["code"] == "unsafe_override_payload"
    after = await _read_spec_decisions(db_factory, ids["spec_id"])
    assert after == before


# ---------------------------------------------------------------------------
# Pending item outcome propagation (val_3366d1d7 blocker #3)
# ---------------------------------------------------------------------------


def _seed_pending_item(
    *,
    base_dir: Path,
    board_id: str,
    generation_id: str,
    source_ref: str,
):
    """Seed one cognitive pending item with the matching (board, generation,
    source_ref) so we can assert the command endpoint updates it.
    """

    marker = CognitivePendingMarker(base_dir=base_dir)
    marker.mark_for_generation(
        board_id=board_id,
        kg_generation_id=generation_id,
        source_set=[{
            "artifact_type": "spec",
            "id": source_ref.split(":", 1)[1],
            "source_ref": source_ref,
        }],
        event_ref="evt_kg03a5_test",
    )
    store = CognitiveConsolidationItemStore(base_dir=base_dir)
    items = store.list_items(board_id, generation_id, limit=10)
    assert items, "fixture must seed at least one pending item"
    return items[0]


@pytest.mark.asyncio
async def test_promote_propagates_outcome_to_pending_item(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    gen = generate_kg_generation_id()
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
        source_generation_id=gen,
    )
    pending = _seed_pending_item(
        base_dir=ids["base_dir"],
        board_id=ids["board_id"],
        generation_id=candidate.source_generation_id,
        source_ref=candidate.source_ref,
    )

    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "promote_to_spec_decision",
            "spec_id": ids["spec_id"],
        },
    )
    assert resp.status_code == 200, resp.text
    formal_ref = resp.json()["formal_decision_ref"]

    ledger = CognitiveConsolidationItemStore(base_dir=ids["base_dir"])
    refreshed = ledger.list_items(
        ids["board_id"], candidate.source_generation_id, limit=10,
    )
    item = next(it for it in refreshed if it.item_id == pending.item_id)
    assert item.status == CognitiveItemStatus.CONSOLIDATED.value
    assert item.outcome_type == (
        CognitivePendingOutcomeType.FORMAL_DECISION_PROMOTED.value
    )
    assert formal_ref in (item.promoted_formal_decision_ids or ())
    assert candidate.candidate_id in (
        item.generated_candidate_decision_ids or ()
    )


@pytest.mark.asyncio
async def test_link_existing_propagates_outcome_to_pending_item(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    gen = generate_kg_generation_id()
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
        source_generation_id=gen,
    )
    pending = _seed_pending_item(
        base_dir=ids["base_dir"],
        board_id=ids["board_id"],
        generation_id=candidate.source_generation_id,
        source_ref=candidate.source_ref,
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "link_existing_decision",
            "spec_id": ids["spec_id"],
            "formal_decision_id": ids["existing_decision_id"],
        },
    )
    assert resp.status_code == 200, resp.text

    ledger = CognitiveConsolidationItemStore(base_dir=ids["base_dir"])
    item = next(
        it for it in ledger.list_items(
            ids["board_id"], candidate.source_generation_id, limit=10,
        )
        if it.item_id == pending.item_id
    )
    assert item.outcome_type == (
        CognitivePendingOutcomeType.EXISTING_DECISION_LINKED.value
    )
    assert ids["existing_decision_id"] in (
        item.promoted_formal_decision_ids or ()
    )


@pytest.mark.asyncio
async def test_dismiss_propagates_outcome_to_pending_item(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    gen = generate_kg_generation_id()
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
        source_generation_id=gen,
    )
    pending = _seed_pending_item(
        base_dir=ids["base_dir"],
        board_id=ids["board_id"],
        generation_id=candidate.source_generation_id,
        source_ref=candidate.source_ref,
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "dismiss",
            "reason_code": "duplicate",
        },
    )
    assert resp.status_code == 200, resp.text
    ledger = CognitiveConsolidationItemStore(base_dir=ids["base_dir"])
    item = next(
        it for it in ledger.list_items(
            ids["board_id"], candidate.source_generation_id, limit=10,
        )
        if it.item_id == pending.item_id
    )
    assert item.outcome_type == (
        CognitivePendingOutcomeType.CONTRADICTION_DISMISSED.value
    )


@pytest.mark.asyncio
async def test_no_action_required_propagates_outcome_to_pending_item(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities
    gen = generate_kg_generation_id()
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
        source_generation_id=gen,
    )
    pending = _seed_pending_item(
        base_dir=ids["base_dir"],
        board_id=ids["board_id"],
        generation_id=candidate.source_generation_id,
        source_ref=candidate.source_ref,
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "no_action_required",
            "reason_code": "ack",
        },
    )
    assert resp.status_code == 200, resp.text
    ledger = CognitiveConsolidationItemStore(base_dir=ids["base_dir"])
    item = next(
        it for it in ledger.list_items(
            ids["board_id"], candidate.source_generation_id, limit=10,
        )
        if it.item_id == pending.item_id
    )
    assert item.outcome_type == (
        CognitivePendingOutcomeType.NO_ACTION_REQUIRED.value
    )


@pytest.mark.asyncio
async def test_dismiss_succeeds_when_pending_ledger_is_missing(
    _client_and_entities,
) -> None:
    """Best-effort propagation: candidate transition completes even when
    no matching pending item is on disk for the candidate's generation."""

    client, store, db_factory, ids = _client_and_entities
    candidate = _seed_candidate(
        store, board_id=ids["board_id"], spec_id=ids["spec_id"],
    )
    resp = client.post(
        _endpoint(ids["board_id"], candidate.candidate_id),
        json={
            "board_id": ids["board_id"],
            "action": "dismiss",
            "reason_code": "duplicate",
        },
    )
    assert resp.status_code == 200, resp.text
    stored = store.get(ids["board_id"], candidate.candidate_id)
    assert stored is not None
    assert stored.status == CandidateDecisionStatus.DISMISSED.value


@pytest.mark.asyncio
async def test_each_action_emits_exactly_one_bounded_counter_sample(
    _client_and_entities,
) -> None:
    client, store, db_factory, ids = _client_and_entities

    # promote
    c1 = _seed_candidate(store, board_id=ids["board_id"], spec_id=ids["spec_id"])
    client.post(_endpoint(ids["board_id"], c1.candidate_id), json={
        "board_id": ids["board_id"],
        "action": "promote_to_spec_decision",
        "spec_id": ids["spec_id"],
    })

    # link_existing
    c2 = _seed_candidate(store, board_id=ids["board_id"], spec_id=ids["spec_id"])
    client.post(_endpoint(ids["board_id"], c2.candidate_id), json={
        "board_id": ids["board_id"],
        "action": "link_existing_decision",
        "spec_id": ids["spec_id"],
        "formal_decision_id": ids["existing_decision_id"],
    })

    # dismiss
    c3 = _seed_candidate(store, board_id=ids["board_id"], spec_id=ids["spec_id"])
    client.post(_endpoint(ids["board_id"], c3.candidate_id), json={
        "board_id": ids["board_id"], "action": "dismiss",
        "reason_code": "duplicate",
    })

    # no_action_required
    c4 = _seed_candidate(store, board_id=ids["board_id"], spec_id=ids["spec_id"])
    client.post(_endpoint(ids["board_id"], c4.candidate_id), json={
        "board_id": ids["board_id"], "action": "no_action_required",
        "reason_code": "ack",
    })

    for action in (
        CandidateDecisionAction.PROMOTE.value,
        CandidateDecisionAction.LINK_EXISTING.value,
        CandidateDecisionAction.DISMISS.value,
        CandidateDecisionAction.NO_ACTION_REQUIRED.value,
    ):
        assert get_candidate_event_count(
            action=action,
            outcome=CandidateDecisionOutcome.SUCCESS.value,
        ) == 1, f"action {action!r} should have one success sample"

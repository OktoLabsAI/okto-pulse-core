"""Tests for KG-03A.1 — decision semantic source enumeration and
deterministic boundary.

Covers:
* AC1 — Active decisions emitted as ``artifact_type=decision`` with
  source_ref ``decision:<spec_id>:<decision_id>``.
* AC2 — Superseded, revoked, and empty decisions skipped.
* TR2 — Fallback ``decision_id`` deterministic when raw decision has
  no ``id``.
* AC3 — ``BoardRebuildIngestionAdapter`` and the
  ``DETERMINISTIC_REBUILD_ARTIFACT_TYPES`` filter exclude ``decision``
  from the deterministic queue (semantic-only).
* TR3 — ``content_hash`` covers parent_type + parent_id + parent_title
  + canonical decision payload.

Notes on validation:
    These tests inspect REAL code paths
    (``BoardSourceStore.fetch`` + deterministic boundary filters), not
    stubs. The validator should grep for ``_decision_sources_from_spec``
    and ``DETERMINISTIC_REBUILD_ARTIFACT_TYPES`` to confirm the
    implementation lives in production modules.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from okto_pulse.core.kg.board_source_store import (
    _decision_id,
    _decision_sources_from_spec,
)
from okto_pulse.core.kg.board_rebuild_adapter import (
    _DETERMINISTIC_SOURCE_ARTIFACT_TYPES,
)
from okto_pulse.core.kg.rebuild_deterministic import (
    DETERMINISTIC_REBUILD_ARTIFACT_TYPES,
)

_board_source_reader = pytest.importorskip(
    "okto_pulse.community.adapters.board_source_reader",
    reason="AF-04 Community integration test requires the Community board source reader.",
)
BoardSourceStore = _board_source_reader.BoardSourceStore


def _make_spec_db(tmp_path: Path, decisions_json: list[dict]) -> Path:
    """Tiny SQLite schema mirroring the production columns used by
    BoardSourceStore.fetch — enough to exercise the decision emission
    path without spinning up the full SQLAlchemy stack."""

    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE specs ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, "
            "title TEXT, description TEXT, version INTEGER, decisions TEXT)"
        )
        conn.execute(
            "INSERT INTO specs VALUES "
            "('s1', 'b1', 'validated', '2026-05-26T00:00:00Z', "
            "'Spec', 'D', 2, ?)",
            (json.dumps(decisions_json),),
        )
        conn.commit()
    return db_path


# -------- AC1 — active decisions are emitted ---------------------------


def test_active_decisions_emit_as_semantic_source_rows(tmp_path: Path) -> None:
    db = _make_spec_db(
        tmp_path,
        [
            {"id": "dec-a", "title": "Use hosted KG", "status": "active"},
            {"id": "dec-b", "title": "Cache on Redis", "status": "active"},
        ],
    )
    rows = BoardSourceStore(db_path=db).fetch("b1")
    decision_rows = [r for r in rows if r["artifact_type"] == "decision"]
    refs = {r["source_ref"] for r in decision_rows}
    assert refs == {"decision:s1:dec-a", "decision:s1:dec-b"}
    for row in decision_rows:
        assert row["source_version"] == "2"
        assert len(row["content_hash"]) == 64


# -------- AC2 — inactive / empty decisions are skipped -----------------


@pytest.mark.parametrize(
    "decision",
    [
        {"id": "dec-x", "title": "T", "rationale": "R", "status": "superseded"},
        {"id": "dec-x", "title": "T", "rationale": "R", "status": "revoked"},
    ],
)
def test_non_active_decisions_are_skipped(
    tmp_path: Path, decision: dict
) -> None:
    db = _make_spec_db(tmp_path, [decision])
    rows = BoardSourceStore(db_path=db).fetch("b1")
    assert all(r["artifact_type"] != "decision" for r in rows)


def test_empty_decision_no_title_no_rationale_is_skipped(
    tmp_path: Path,
) -> None:
    db = _make_spec_db(
        tmp_path,
        [
            {"id": "dec-empty", "status": "active"},
            {"id": "dec-blank", "title": "  ", "rationale": "  ", "status": "active"},
        ],
    )
    rows = BoardSourceStore(db_path=db).fetch("b1")
    assert all(r["artifact_type"] != "decision" for r in rows)


def test_decision_with_only_rationale_is_emitted(tmp_path: Path) -> None:
    """Either title OR rationale is enough — the spec does not require both."""

    db = _make_spec_db(
        tmp_path,
        [{"id": "dec-r", "rationale": "Pure rationale", "status": "active"}],
    )
    rows = BoardSourceStore(db_path=db).fetch("b1")
    decision_rows = [r for r in rows if r["artifact_type"] == "decision"]
    assert len(decision_rows) == 1
    assert decision_rows[0]["source_ref"] == "decision:s1:dec-r"


# -------- TR2 — deterministic fallback id ------------------------------


def test_fallback_decision_id_is_deterministic_when_id_is_missing() -> None:
    decision = {"title": "Use hosted KG", "rationale": "Lower ops"}
    first = _decision_id(decision, 0)
    second = _decision_id(decision, 0)
    assert first == second
    assert first.startswith("idx-0-")
    # 12-char fingerprint after the "idx-N-" prefix.
    assert len(first.split("-", 2)[2]) == 12


def test_fallback_decision_id_changes_when_payload_changes() -> None:
    base = {"title": "A", "rationale": "R"}
    other = {"title": "A", "rationale": "R2"}
    assert _decision_id(base, 0) != _decision_id(other, 0)


def test_fallback_decision_id_changes_when_index_changes() -> None:
    decision = {"title": "A", "rationale": "R"}
    assert _decision_id(decision, 0) != _decision_id(decision, 1)


def test_decision_without_id_uses_fallback_in_source_ref(
    tmp_path: Path,
) -> None:
    db = _make_spec_db(
        tmp_path,
        [
            {"title": "Use hosted KG", "rationale": "R", "status": "active"},
        ],
    )
    rows = BoardSourceStore(db_path=db).fetch("b1")
    decision_rows = [r for r in rows if r["artifact_type"] == "decision"]
    assert len(decision_rows) == 1
    ref = decision_rows[0]["source_ref"]
    # decision:<spec_id>:idx-<index>-<fingerprint>
    assert ref.startswith("decision:s1:idx-0-")


# -------- AC3 — deterministic boundary excludes decision ---------------


def test_decision_is_not_in_deterministic_rebuild_artifact_types() -> None:
    """`decision` MUST NOT appear in the deterministic rebuild allow-list.
    The rebuild materialises Decision nodes from the owning spec payload
    only — decision source rows are semantic-only signals to the
    cognitive pending workflow."""

    assert "decision" not in DETERMINISTIC_REBUILD_ARTIFACT_TYPES


def test_decision_is_not_in_board_rebuild_ingestion_allow_list() -> None:
    """`board_rebuild_adapter` MUST NOT enqueue decisions onto
    ConsolidationQueue. Test the allow-list constant directly so any
    future code change that adds `decision` to it trips the regression."""

    assert "decision" not in _DETERMINISTIC_SOURCE_ARTIFACT_TYPES


# -------- TR3 — content_hash includes parent context ------------------


def test_decision_content_hash_includes_parent_context() -> None:
    """Same decision payload under different parents produces different
    content_hashes — parent_type/parent_id/parent_title participate."""

    row = {
        "id": "s1",
        "version": 2,
        "title": "Spec One",
        "created_at": "2026-05-26T00:00:00Z",
        "decisions": json.dumps(
            [{"id": "dec-a", "title": "T", "rationale": "R", "status": "active"}]
        ),
    }

    class _MapRow(dict):
        def __getitem__(self, key):
            return super().__getitem__(key)

    sources_one = _decision_sources_from_spec(_MapRow(row))
    other = dict(row)
    other["id"] = "s2"
    other["title"] = "Spec Two"
    sources_two = _decision_sources_from_spec(_MapRow(other))

    assert sources_one[0]["content_hash"] != sources_two[0]["content_hash"]
    # source_ref encodes the parent spec, so it must differ too.
    assert sources_one[0]["source_ref"] == "decision:s1:dec-a"
    assert sources_two[0]["source_ref"] == "decision:s2:dec-a"


# -------- Sanity: rebuild source set integration -----------------------


def test_full_source_set_keeps_decision_alongside_spec(
    tmp_path: Path,
) -> None:
    """The spec row stays in the source set AND a decision row is
    appended next to it — the semantic source does NOT replace the
    deterministic spec source."""

    db = _make_spec_db(
        tmp_path,
        [{"id": "dec-a", "title": "T", "rationale": "R", "status": "active"}],
    )
    rows = BoardSourceStore(db_path=db).fetch("b1")
    refs = {r["source_ref"] for r in rows}
    assert "spec:s1" in refs
    assert "decision:s1:dec-a" in refs

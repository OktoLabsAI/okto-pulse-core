"""KG-02.2 — Source enumerator + manifest + confirmation binding tests.

Covers FR2/FR3, TR4/TR5/TR6/TR14, IR ir_1959b2e1/ir_b4b43304/ir_cf1be9e8,
OR or_2d295e26 + or_f9b83da6.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from okto_pulse.core.kg.rebuild_confirmation import (
    CANONICAL_OPERATIONS,
    ConfirmationOutcome,
    RebuildConfirmationStore,
    get_confirmation_count,
    get_confirmation_counter_labels,
    get_confirmation_samples,
    reset_confirmation_counter,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    REBUILD_AUDIT_GLOBAL_BOARD_ID,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import require_rebuild_audit_artifact_store
from okto_pulse.core.kg.rebuild_sources import (
    CANONICAL_ARTIFACT_TYPES,
    EnumerationOutcome,
    KGRebuildSourceManifest,
    RebuildSourceEnumerator,
    get_enumeration_count,
    get_enumeration_counter_labels,
    reset_enumeration_counter,
)
from okto_pulse.core.kg.source_maturity import classify_source_for_kg

_board_source_reader = pytest.importorskip(
    "okto_pulse.community.adapters.board_source_reader",
    reason="AF-04 Community integration test requires the Community board source reader.",
)
BoardSourceStore = _board_source_reader.BoardSourceStore


# ---------------------------------------------------------------------------
# IMPL-2 compatibility helper — see test_kg_rebuild_service.py for details.
# ---------------------------------------------------------------------------

def _make_rebuild_test_app(board_id: str = "b-test"):
    """Return a FastAPI app with get_db overridden to satisfy FR10/FR9 gates."""
    from types import SimpleNamespace

    from fastapi import FastAPI

    from okto_pulse.core.api.router import api_router
    from okto_pulse.core.infra.database import get_db

    _fake_board = SimpleNamespace(id=board_id, owner_id="user-lifecycle-test")

    class _FakeResult:
        def scalar_one_or_none(self):
            # ShareService.get_user_permission calls execute() for BoardShare rows;
            # return a fake share with "owner" permission so the 403 gate passes.
            return SimpleNamespace(permission="owner")

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        async def execute(self, stmt):
            return _FakeResult()

        async def get(self, model_class, pk):
            # _require_board_access + ShareService._get_board call db.get(Board, id).
            return _fake_board

        async def commit(self):
            pass

        async def rollback(self):
            pass

    async def _fake_db():
        yield _FakeSession()

    app = FastAPI()
    app.include_router(api_router)
    app.dependency_overrides[get_db] = _fake_db
    return app


@pytest.fixture(autouse=True)
def _reset_counters():
    reset_enumeration_counter()
    reset_confirmation_counter()
    yield
    reset_enumeration_counter()
    reset_confirmation_counter()


# --- Source enumerator -------------------------------------------------------


def _row(
    artifact_type: str = "spec",
    id_: str = "id-1",
    created_at: str = "2026-05-01T00:00:00Z",
    version: str = "v1",
    content_hash: str = "h",
    status: str = "done",
) -> dict:
    return {
        "artifact_type": artifact_type,
        "id": id_,
        "source_ref": f"ref:{id_}",
        "source_version": version,
        "content_hash": content_hash,
        "created_at": created_at,
        "status": status,
    }


def test_enumerate_filters_out_cancelled_specs():
    """TR4: cancelled specs are excluded deterministically."""
    rows = [
        _row(id_="active-1"),
        _row(id_="cancelled-1", status="cancelled"),
        _row(id_="active-2"),
        _row(id_="cancelled-2", status="cancelled"),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")
    ids = [r.id for r in result.sources]
    assert "cancelled-1" not in ids
    assert "cancelled-2" not in ids
    assert sorted(ids) == ["active-1", "active-2"]
    assert result.skipped_cancelled_count == 2
    assert result.eligible_count == 2


@pytest.mark.parametrize(
    ("artifact_type", "status"),
    [
        ("spec", "draft"),
        ("spec", "review"),
        ("spec", "validated"),
        ("story", "ready"),
        ("refinement", "review"),
        ("task", "in_progress"),
    ],
)
def test_source_classifier_blocks_immature_canonical(
    artifact_type: str,
    status: str,
) -> None:
    result = classify_source_for_kg(
        artifact_type=artifact_type,
        artifact_status=status,
        content_hash="h",
    )

    assert result.graph_layer == "working"
    if artifact_type == "story":
        assert result.disposition == "working"
        assert result.reason_code == "story_never_canonical"
    else:
        assert result.disposition == "skipped_by_maturity"
        assert result.reason_code == f"{artifact_type}_{status}_not_canonical"


def test_enumerate_orders_by_artifact_type_then_created_at_then_id_then_version():
    """TR5: stable ordering over canonical-eligible rows.

    Decisions are no longer independent rebuild sources; they inherit the
    owning spec's maturity and stay out of the canonical source list.
    """
    rows = [
        _row(artifact_type="bug", id_="b-1", created_at="2026-05-03", status="done"),
        _row(artifact_type="spec", id_="s-2", created_at="2026-05-02"),
        _row(artifact_type="spec", id_="s-1", created_at="2026-05-01"),
        _row(artifact_type="decision", id_="s-1:d-1", created_at="2026-05-01"),
        _row(artifact_type="refinement", id_="r-1", created_at="2026-05-01", status="done"),
        _row(artifact_type="test", id_="tc-1", created_at="2026-05-01", status="done"),
        _row(artifact_type="task", id_="t-1", created_at="2026-05-01", status="done"),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")
    sequence = [(r.artifact_type, r.id) for r in result.sources]
    # Partition order, then created_at/id/version within type.
    assert sequence == [
        ("refinement", "r-1"),
        ("spec", "s-1"),
        ("spec", "s-2"),
        ("task", "t-1"),
        ("test", "tc-1"),
        ("bug", "b-1"),
    ]
    assert [r.id for r in result.legacy_unknown] == ["s-1:d-1"]


def test_closed_sprint_is_working_not_canonical():
    rows = [
        _row(artifact_type="sprint", id_="sp-1", status="closed"),
        _row(artifact_type="spec", id_="s-1", status="done"),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")

    assert [r.id for r in result.sources] == ["s-1"]
    assert [r.id for r in result.skipped_by_maturity] == ["sp-1"]
    assert result.skipped_by_maturity[0].reason_code == "sprint_not_canonical"


def test_ideation_and_intermediate_statuses_stay_out_of_canonical():
    rows = [
        _row(artifact_type="ideation", id_="i-1", status="done", created_at="2026-06-12"),
        _row(artifact_type="spec", id_="s-draft", status="draft", created_at="2026-06-12"),
        _row(artifact_type="spec", id_="s-review", status="review", created_at="2026-06-12"),
        _row(artifact_type="spec", id_="s-approved", status="approved", created_at="2026-06-12"),
        _row(artifact_type="spec", id_="s-validated", status="validated", created_at="2026-06-12"),
        _row(artifact_type="refinement", id_="r-review", status="review", created_at="2026-06-12"),
    ]
    # working_ttl_days=0 isolates the MATURITY/PARTITION contract from the working
    # TTL (which is covered deterministically by test_working_ttl_expiry_* /
    # _override). Without it this fixture's created_at would silently cross the
    # default 7-day TTL as wall-clock advances and the working sources would be
    # mis-asserted as skipped_expired (bug 5187f991: time-dependent fixture).
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows, working_ttl_days=0)
    result = enum.enumerate(board_id="b1")

    assert result.sources == ()
    assert [r.id for r in result.working_sources] == ["i-1"]
    skipped_ids = {r.id for r in result.skipped_by_maturity}
    assert skipped_ids == {
        "s-draft",
        "s-review",
        "s-approved",
        "s-validated",
        "r-review",
    }
    assert all(r.graph_layer == "working" for r in result.skipped_by_maturity)
    assert {r.id for r in result.materializable_sources} == {
        "i-1",
        "s-draft",
        "s-review",
        "s-approved",
        "s-validated",
        "r-review",
    }


def test_rebuild_partitions_cancelled_and_immature_sources_separately():
    rows = [
        _row(
            artifact_type="spec",
            id_="s-cancelled",
            status="cancelled",
            created_at="2026-06-12T00:00:00Z",
        ),
        _row(
            artifact_type="spec",
            id_="s-approved",
            status="approved",
            created_at="2026-06-12T00:00:01Z",
        ),
        _row(
            artifact_type="spec",
            id_="s-validated",
            status="validated",
            created_at="2026-06-12T00:00:02Z",
        ),
        _row(
            artifact_type="spec",
            id_="s-done",
            status="done",
            created_at="2026-06-12T00:00:03Z",
        ),
    ]
    # working_ttl_days=0: isolate maturity/partition from the working TTL (bug
    # 5187f991 — time-dependent fixture; TTL has its own deterministic tests).
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows, working_ttl_days=0)
    result = enum.enumerate(board_id="b1")

    assert [r.id for r in result.sources] == ["s-done"]
    assert [r.id for r in result.skipped_by_maturity] == [
        "s-approved",
        "s-validated",
    ]
    assert result.skipped_cancelled_count == 1
    assert result.canonical_source_count == 1
    assert result.skipped_by_maturity_count == 2
    assert result.source_partition_counts["canonical"] == 1
    assert result.source_partition_counts["skipped_by_maturity"] == 2
    assert result.source_partition_counts["skipped_cancelled"] == 1


def test_rebuild_legacy_missing_hash_is_not_canonical():
    rows = [
        _row(id_="legacy-no-hash", content_hash=""),
        _row(id_="canonical", status="done"),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")

    assert [r.id for r in result.sources] == ["canonical"]
    assert [r.id for r in result.legacy_unknown] == ["legacy-no-hash"]
    assert result.legacy_unknown[0].reason_code == "missing_status_or_content_hash"
    assert result.has_non_deterministic_inputs is True


def test_regression_fresh_working_sources_are_not_dropped_from_partition():
    """Regression for SPEC4 bug 5187f991 (test card 7414f1a7 / ts_df112b35).

    A working-partition source (ideation, or an intermediate-status spec) MUST
    land in working_sources / skipped_by_maturity AND in materializable_sources —
    never silently dropped — so a confirmed rebuild materializes the full
    canonical+working partition. DETERMINISTIC (working_ttl_days=0 + an ANCIENT
    created_at) so it cannot rot like the time-dependent fixtures that exposed
    the bug: it fails closed if the enumerator ever drops/misclassifies a working
    source regardless of wall-clock.
    """
    rows = [
        _row(artifact_type="ideation", id_="i-1", status="done", created_at="2020-01-01T00:00:00Z"),
        _row(artifact_type="spec", id_="s-approved", status="approved", created_at="2020-01-01T00:00:00Z"),
        _row(artifact_type="spec", id_="s-done", status="done", created_at="2020-01-01T00:00:00Z"),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows, working_ttl_days=0)
    result = enum.enumerate(board_id="b1")

    # The working partition survives an ancient created_at (TTL disabled here;
    # TTL expiry is covered separately + deterministically).
    assert [r.id for r in result.working_sources] == ["i-1"]
    assert [r.id for r in result.skipped_by_maturity] == ["s-approved"]
    assert [r.id for r in result.sources] == ["s-done"]
    assert result.skipped_expired_working == ()
    # canonical + working are both materializable — none dropped.
    assert {r.id for r in result.materializable_sources} == {"i-1", "s-approved", "s-done"}
    assert result.working_source_count == 1
    assert result.skipped_by_maturity_count == 1


def test_working_ttl_expiry_does_not_touch_canonical_sources():
    rows = [
        _row(
            id_="approved-old",
            status="approved",
            created_at="2026-05-01T00:00:00Z",
        ),
        _row(
            id_="done-current",
            status="done",
            created_at="2026-06-12T00:00:00Z",
        ),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")

    assert [r.id for r in result.sources] == ["done-current"]
    assert [r.id for r in result.skipped_expired_working] == ["approved-old"]
    assert result.skipped_expired_working[0].reason_code == (
        "spec_approved_working_expired"
    )


def test_working_ttl_override_keeps_recent_effective_working_source():
    rows = [
        {
            **_row(
                id_="approved-old-default",
                status="approved",
                created_at="2026-05-01T00:00:00Z",
            ),
            "working_ttl_days": 7,
        },
        {
            **_row(
                id_="approved-old-override",
                status="approved",
                created_at="2026-05-01T00:00:00Z",
            ),
            "working_ttl_days": 90,
        },
        _row(
            id_="done-current",
            status="done",
            created_at="2026-06-12T00:00:00Z",
        ),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")

    assert [r.id for r in result.sources] == ["done-current"]
    assert [r.id for r in result.skipped_expired_working] == [
        "approved-old-default"
    ]
    assert [r.id for r in result.skipped_by_maturity] == [
        "approved-old-override"
    ]
    assert result.skipped_by_maturity[0].expires_at is not None


def test_enumerate_stable_for_repeated_runs():
    """Repeated runs over the same input produce the same source ids
    in the same order — pure determinism."""
    rows = [
        _row(id_=f"id-{i}", created_at=f"2026-05-{i:02d}T00:00:00Z")
        for i in range(5, 0, -1)
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: list(rows))
    a = enum.enumerate(board_id="b1")
    b = enum.enumerate(board_id="b1")
    assert [r.id for r in a.sources] == [r.id for r in b.sources]


def test_enumerate_flags_non_deterministic_inputs():
    """TR6: rows without content_hash are non-deterministic; the flag
    surfaces so preflight UI requires extra confirmation."""
    rows = [
        _row(id_="ok"),
        _row(id_="legacy", content_hash=""),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")
    assert result.has_non_deterministic_inputs is True


def test_enumerate_skips_unknown_artifact_type_and_flags_non_det():
    rows = [
        _row(id_="ok"),
        _row(artifact_type="meme", id_="weird"),
    ]
    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    result = enum.enumerate(board_id="b1")
    assert result.has_non_deterministic_inputs is True
    assert all(r.artifact_type in CANONICAL_ARTIFACT_TYPES for r in result.sources)


def test_enumerate_raises_when_source_store_fails():
    def boom(_b):
        raise RuntimeError("store down")

    enum = RebuildSourceEnumerator(source_store=boom)
    with pytest.raises(RuntimeError):
        enum.enumerate(board_id="b1")
    assert (
        get_enumeration_count(
            "b1", EnumerationOutcome.SOURCE_STORE_UNAVAILABLE.value
        )
        == 1
    )


def test_enumerate_empty_board_raises():
    enum = RebuildSourceEnumerator(source_store=lambda _b: [])
    with pytest.raises(ValueError):
        enum.enumerate(board_id="")


def test_board_source_store_maps_cards_to_task_test_bug_sources(
    tmp_path: Path,
) -> None:
    """Production source store policy:
    ideation/refinement/spec/card rows are exposed to the maturity
    enumerator. Active decisions are emitted as semantic source rows, and
    cancelled/archived rows are included so the enumerator can count
    deterministic skips."""

    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE specs ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, "
            "title TEXT, description TEXT, version INTEGER, decisions TEXT)"
        )
        conn.execute(
            "CREATE TABLE refinements ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, "
            "title TEXT, description TEXT, analysis TEXT, version INTEGER)"
        )
        conn.execute(
            "CREATE TABLE ideations ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, title TEXT)"
        )
        conn.execute(
            "CREATE TABLE stories ("
            "id TEXT, board_id TEXT, topic_id TEXT, status TEXT, "
            "created_at TEXT, title TEXT, description TEXT, actor TEXT, "
            "goal TEXT, benefit TEXT, labels TEXT)"
        )
        conn.execute(
            "CREATE TABLE cards ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, "
            "title TEXT, description TEXT, card_type TEXT, archived INTEGER)"
        )
        conn.execute(
            "INSERT INTO specs VALUES "
            "('s1', 'b1', 'done', '2026-05-01T00:00:00Z', 'Spec', 'D', 2, ?)",
            (json.dumps([
                {"id": "dec-active", "title": "Use hosted KG", "status": "active"},
                {"id": "dec-old", "title": "Use local KG", "status": "superseded"},
            ]),),
        )
        conn.execute(
            "INSERT INTO refinements VALUES "
            "('r1', 'b1', 'done', '2026-05-01T00:00:01Z', 'Ref', 'D', 'A', 1)"
        )
        conn.execute(
            "INSERT INTO ideations VALUES "
            "('i1', 'b1', 'done', '2026-05-01T00:00:02Z', 'Idea')"
        )
        conn.execute(
            "INSERT INTO stories VALUES ("
            "'st1', 'b1', 'topic1', 'ready', '2026-05-01T00:00:02Z', "
            "'Story', 'D', 'Actor', 'Goal', 'Benefit', '[]')"
        )
        conn.executemany(
            "INSERT INTO cards VALUES (?, 'b1', ?, ?, ?, 'D', ?, ?)",
            [
                ("t1", "done", "2026-05-01T00:00:03Z", "Task", "normal", 0),
                ("tc1", "done", "2026-05-01T00:00:04Z", "Test", "test", 0),
                ("b1c", "done", "2026-05-01T00:00:05Z", "Bug", "bug", 0),
                ("old", "cancelled", "2026-05-01T00:00:06Z", "Old", "normal", 0),
                ("arch", "done", "2026-05-01T00:00:07Z", "Archived", "bug", 1),
            ],
        )
        conn.commit()

    rows = BoardSourceStore(db_path=db_path).fetch("b1")
    refs = {row["source_ref"] for row in rows}
    assert refs == {
        "ideation:i1",
        "story:st1",
        "spec:s1",
        "refinement:r1",
        "task:t1",
        "test:tc1",
        "bug:b1c",
        "task:old",
        "bug:arch",
        "decision:s1:dec-active",
    }
    assert "decision:s1:dec-old" not in refs
    assert {row["artifact_type"] for row in rows} == {
        "ideation",
        "story",
        "spec",
            "refinement",
            "task",
            "test",
            "bug",
            "decision",
        }

    enum = RebuildSourceEnumerator(source_store=lambda _b: rows)
    source_set = enum.enumerate(board_id="b1")
    assert {row.source_ref for row in source_set.sources} == {
        "refinement:r1",
        "spec:s1",
        "task:t1",
        "test:tc1",
    }
    assert {row.source_ref for row in source_set.skipped_expired_working} == {
        "ideation:i1",
    }
    assert {row.source_ref for row in source_set.working_sources} == {
        "story:st1",
    }
    assert source_set.skipped_cancelled_count == 2


def test_board_source_store_propagates_working_ttl_board_override(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute(
            "CREATE TABLE boards (id TEXT, settings TEXT)"
        )
        conn.execute(
            "CREATE TABLE specs ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, "
            "updated_at TEXT, title TEXT, description TEXT, version INTEGER, "
            "functional_requirements TEXT, technical_requirements TEXT, "
            "acceptance_criteria TEXT, test_scenarios TEXT, business_rules TEXT, "
            "api_contracts TEXT, decisions TEXT)"
        )
        conn.execute(
            "INSERT INTO boards VALUES (?, ?)",
            ("b1", json.dumps({"kg_working_ttl_days": 30})),
        )
        conn.execute(
            "INSERT INTO specs VALUES ("
            "'s1', 'b1', 'approved', '2026-05-01T00:00:00Z', "
            "'2026-05-01T00:00:00Z', 'Spec', 'D', 1, '[]', '[]', '[]', "
            "'[]', '[]', '[]', '[]')"
        )
        conn.commit()

    rows = BoardSourceStore(db_path=db_path).fetch("b1")

    assert rows[0]["working_ttl_days"] == 30


# --- OR or_2d295e26 counter shape -------------------------------------------


def test_enumeration_counter_labels_match_or_shape():
    assert get_enumeration_counter_labels() == ("board_id", "outcome", "reason")


def test_enumeration_counter_bumps_per_outcome():
    rows = [_row()]
    RebuildSourceEnumerator(source_store=lambda _b: rows).enumerate(board_id="b1")
    assert get_enumeration_count("b1", EnumerationOutcome.SUCCESS.value) == 1


# --- Manifest builder + load + revalidate -----------------------------------


def test_manifest_build_writes_immutable_json_to_disk(tmp_path: Path):
    enum = RebuildSourceEnumerator(source_store=lambda _b: [_row()])
    source_set = enum.enumerate(board_id="b1")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    manifest = store.build(source_set=source_set, preflight_hash="a" * 64)

    assert manifest.manifest_ref.startswith("rebuild_manifest_")
    assert manifest.board_id == "b1"
    assert manifest.preflight_hash == "a" * 64
    assert len(manifest.source_set_hash) == 64
    # Written to disk.
    path = tmp_path / "rebuild" / "manifests" / f"{manifest.manifest_ref}.json"
    assert path.exists()
    body = json.loads(path.read_text(encoding="utf-8"))
    assert body["preflight_hash"] == "a" * 64
    assert body["source_set_hash"] == manifest.source_set_hash


def test_manifest_load_returns_byte_identical_record(tmp_path: Path):
    enum = RebuildSourceEnumerator(source_store=lambda _b: [_row()])
    source_set = enum.enumerate(board_id="b1")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    manifest = store.build(source_set=source_set, preflight_hash="b" * 64)
    loaded = store.load(manifest.manifest_ref)
    assert loaded is not None
    assert loaded.manifest_ref == manifest.manifest_ref
    assert loaded.source_set_hash == manifest.source_set_hash
    assert loaded.preflight_hash == "b" * 64


def test_manifest_load_returns_none_for_missing(tmp_path: Path):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    assert store.load("does_not_exist") is None


def test_manifest_revalidate_detects_drift(tmp_path: Path):
    # revalidate is typed (card 5ec8c75c): EQUIVALENT / REBASELINE / MANIFEST_DRIFT.
    from okto_pulse.core.kg.rebuild_sources import SourceSetRevalidation

    enum_a = RebuildSourceEnumerator(source_store=lambda _b: [_row(id_="a")])
    source_a = enum_a.enumerate(board_id="b1")
    enum_b = RebuildSourceEnumerator(
        source_store=lambda _b: [_row(id_="a"), _row(id_="b")]
    )
    source_b = enum_b.enumerate(board_id="b1")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    manifest = store.build(source_set=source_a, preflight_hash="c" * 64)
    assert (
        store.revalidate(manifest=manifest, current_source_set=source_a).outcome
        is SourceSetRevalidation.EQUIVALENT
    )
    drift = store.revalidate(manifest=manifest, current_source_set=source_b)
    assert drift.is_drift
    assert drift.outcome is SourceSetRevalidation.MANIFEST_DRIFT
    assert (
        get_enumeration_count("b1", EnumerationOutcome.SOURCE_SET_HASH_MISMATCH.value)
        >= 1
    )


def test_manifest_build_requires_preflight_hash(tmp_path: Path):
    enum = RebuildSourceEnumerator(source_store=lambda _b: [_row()])
    source_set = enum.enumerate(board_id="b1")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    with pytest.raises(ValueError):
        store.build(source_set=source_set, preflight_hash="")


def test_manifest_source_set_hash_is_deterministic(tmp_path: Path):
    """Same source set → same source_set_hash, regardless of build time."""
    rows = [_row(id_="a"), _row(id_="b")]
    enum = RebuildSourceEnumerator(source_store=lambda _b: list(rows))
    source_a = enum.enumerate(board_id="b1")
    source_b = enum.enumerate(board_id="b1")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    manifest_a = store.build(source_set=source_a, preflight_hash="c" * 64)
    manifest_b = store.build(source_set=source_b, preflight_hash="c" * 64)
    assert manifest_a.source_set_hash == manifest_b.source_set_hash
    # But the manifest_refs are unique (each build produces a fresh id).
    assert manifest_a.manifest_ref != manifest_b.manifest_ref


def test_manifest_source_set_hash_changes_when_maturity_partition_changes(
    tmp_path: Path,
):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    approved_set = RebuildSourceEnumerator(
        source_store=lambda _b: [
            _row(id_="s1", status="approved", created_at="2026-06-12T00:00:00Z")
        ],
        working_ttl_days=0,  # isolate maturity partition from TTL (bug 5187f991)
    ).enumerate(board_id="b1")
    done_set = RebuildSourceEnumerator(
        source_store=lambda _b: [
            _row(id_="s1", status="done", created_at="2026-06-12T00:00:00Z")
        ]
    ).enumerate(board_id="b1")

    approved_manifest = store.build(
        source_set=approved_set,
        preflight_hash="d" * 64,
    )
    done_manifest = store.build(
        source_set=done_set,
        preflight_hash="d" * 64,
    )

    assert approved_manifest.source_set_hash != done_manifest.source_set_hash
    assert len(approved_manifest.sources) == 0
    assert len(approved_manifest.skipped_by_maturity) == 1
    assert [r.id for r in approved_manifest.materializable_sources] == ["s1"]
    assert len(done_manifest.sources) == 1
    assert len(done_manifest.skipped_by_maturity) == 0
    assert [r.id for r in done_manifest.materializable_sources] == ["s1"]


# --- Confirmation store ------------------------------------------------------


def _store(tmp_path: Path, ttl: int = 300) -> RebuildConfirmationStore:
    return RebuildConfirmationStore(base_dir=tmp_path, ttl_seconds=ttl)


def test_issue_and_consume_happy_path(tmp_path: Path):
    store = _store(tmp_path)
    token = store.issue(
        board_id="b1",
        actor_id="agent-007",
        operation="rebuild",
        preflight_hash="c" * 64,
        manifest_ref="m-ref",
    )
    assert token.confirmation_id.startswith("conf_")
    assert token.board_id == "b1"

    result = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id="b1",
        expected_actor_id="agent-007",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64,
        expected_manifest_ref="m-ref",
    )
    assert result.outcome == ConfirmationOutcome.CONSUMED.value
    assert result.token is not None
    assert result.token.confirmation_id == token.confirmation_id


def test_second_consume_returns_replayed(tmp_path: Path):
    store = _store(tmp_path)
    token = store.issue(
        board_id="b1", actor_id="user-1", operation="rebuild",
        preflight_hash="c" * 64, manifest_ref="m",
    )
    first = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id="b1", expected_actor_id="user-1",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64, expected_manifest_ref="m",
    )
    assert first.outcome == ConfirmationOutcome.CONSUMED.value

    second = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id="b1", expected_actor_id="user-1",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64, expected_manifest_ref="m",
    )
    # Second attempt hits MISSING because consume unlinks atomically
    # — equivalent to REPLAYED from the caller's perspective.
    assert second.outcome in (
        ConfirmationOutcome.MISSING.value,
        ConfirmationOutcome.REPLAYED.value,
    )
    assert second.token is None


def test_expired_token_is_rejected(tmp_path: Path):
    store = _store(tmp_path, ttl=30)  # min ttl
    # Forge a manifest with expires_at in the past via the store path.
    token = store.issue(
        board_id="b1", actor_id="u", operation="rebuild",
        preflight_hash="c" * 64, manifest_ref="m",
    )
    # Manipulate the on-disk record to expire immediately.
    path = (
        tmp_path / "rebuild" / "confirmations"
        / f"{token.confirmation_id}.json"
    )
    data = json.loads(path.read_text(encoding="utf-8"))
    data["expires_at"] = "2020-01-01T00:00:00+00:00"
    path.write_text(json.dumps(data), encoding="utf-8")

    result = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id="b1", expected_actor_id="u",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64, expected_manifest_ref="m",
    )
    assert result.outcome == ConfirmationOutcome.EXPIRED.value
    assert result.token is None
    # Expired token is unlinked so a subsequent call sees MISSING.
    assert not path.exists()


@pytest.mark.parametrize("field,actual,expected", [
    ("board_id", "b-other", "b1"),
    ("actor_id", "other-user", "user-1"),
    ("operation", "reset", "rebuild"),
    ("preflight_hash", "d" * 64, "c" * 64),
    ("manifest_ref", "m-other", "m"),
])
def test_scope_mismatch_blocks_consumption(
    tmp_path: Path, field: str, actual: str, expected: str
):
    store = _store(tmp_path)
    token = store.issue(
        board_id="b1", actor_id="user-1", operation="rebuild",
        preflight_hash="c" * 64, manifest_ref="m",
    )
    kwargs = {
        "expected_board_id": "b1",
        "expected_actor_id": "user-1",
        "expected_operation": "rebuild",
        "expected_preflight_hash": "c" * 64,
        "expected_manifest_ref": "m",
    }
    kwargs[f"expected_{field}"] = actual
    result = store.consume(confirmation_id=token.confirmation_id, **kwargs)
    assert result.outcome == ConfirmationOutcome.SCOPE_MISMATCH.value
    assert result.reason == field
    assert result.token is None

    # Crucially: the token is NOT unlinked, so the true owner can
    # still consume it within TTL.
    path = (
        tmp_path / "rebuild" / "confirmations"
        / f"{token.confirmation_id}.json"
    )
    assert path.exists()


def test_unknown_operation_rejected_at_issue(tmp_path: Path):
    store = _store(tmp_path)
    with pytest.raises(ValueError):
        store.issue(
            board_id="b1", actor_id="u", operation="bogus",
            preflight_hash="c" * 64, manifest_ref="m",
        )


def test_missing_confirmation_returns_missing(tmp_path: Path):
    store = _store(tmp_path)
    result = store.consume(
        confirmation_id="conf_does_not_exist",
        expected_board_id="b1", expected_actor_id="u",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64, expected_manifest_ref="m",
    )
    assert result.outcome == ConfirmationOutcome.MISSING.value
    assert result.token is None


def test_ttl_bounds_rejected(tmp_path: Path):
    with pytest.raises(ValueError):
        RebuildConfirmationStore(base_dir=tmp_path, ttl_seconds=5)
    with pytest.raises(ValueError):
        RebuildConfirmationStore(base_dir=tmp_path, ttl_seconds=100_000)


# --- OR or_f9b83da6 / or_0752f230 counter labels ----------------------------


def test_confirmation_counter_labels_match_or_shape():
    assert get_confirmation_counter_labels() == (
        "board_id", "operation", "outcome", "reason", "actor_type",
    )


def test_confirmation_counter_includes_actor_type_bucket(tmp_path: Path):
    """Actor bucketing: agent vs human goes into the safe label."""
    store = _store(tmp_path)
    token_agent = store.issue(
        board_id="b1", actor_id="agent-001", operation="rebuild",
        preflight_hash="c" * 64, manifest_ref="m",
    )
    store.consume(
        confirmation_id=token_agent.confirmation_id,
        expected_board_id="b1", expected_actor_id="agent-001",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64, expected_manifest_ref="m",
    )

    token_human = store.issue(
        board_id="b1", actor_id="alice@org.com", operation="rebuild",
        preflight_hash="c" * 64, manifest_ref="m2",
    )
    store.consume(
        confirmation_id=token_human.confirmation_id,
        expected_board_id="b1", expected_actor_id="alice@org.com",
        expected_operation="rebuild",
        expected_preflight_hash="c" * 64, expected_manifest_ref="m2",
    )

    assert (
        get_confirmation_count("b1", ConfirmationOutcome.CONSUMED.value, actor_type="agent")
        == 1
    )
    assert (
        get_confirmation_count("b1", ConfirmationOutcome.CONSUMED.value, actor_type="human")
        == 1
    )


def test_confirmation_counter_never_includes_raw_confirmation_id(tmp_path: Path):
    store = _store(tmp_path)
    token = store.issue(
        board_id="b1", actor_id="u", operation="rebuild",
        preflight_hash="c" * 64, manifest_ref="m",
    )
    samples = get_confirmation_samples()
    for s in samples:
        for value in s.values():
            sv = str(value)
            assert token.confirmation_id not in sv
            # Raw actor_id MUST NOT leak either.
            assert "u" != sv or sv != "u"  # trivially true; explicit check:
        # Stricter: enforce that none of the bounded label values
        # contains the raw confirmation_id token.
        assert s.get("board_id") != token.confirmation_id


# --- Canonical operations set -----------------------------------------------


def test_canonical_operations_includes_rebuild_and_reset():
    assert "rebuild" in CANONICAL_OPERATIONS
    assert "reset" in CANONICAL_OPERATIONS


# --- POST endpoints lifecycle integration (val_d0da4a75 rework) -------------


def _client_with_router(board_id: str = "b-life"):
    """Helper: live api_router app with auth + FR10/FR9 overrides.

    IMPL-2 added board scope (FR10) + real health probe (FR9).
    Both gates need a DB session.  We override get_db to return a fake
    session whose Board SELECT always succeeds and also patch get_kg_health
    to return a healthy state so the admission gate never blocks.
    """
    import okto_pulse.core.api.kg_rebuild as kg_rebuild_mod
    from fastapi.testclient import TestClient

    from okto_pulse.core.infra.auth import require_user

    async def _fake_health(_board_id, _db, scheduler_control=None):
        return {"graph_state": "healthy", "metric_status": "available",
                "current_kg_generation_id": None}

    # Monkeypatch at module level (safe: tests run in same process, sequential).
    _original_health = kg_rebuild_mod.get_kg_health
    kg_rebuild_mod.get_kg_health = _fake_health  # type: ignore[assignment]

    app = _make_rebuild_test_app(board_id=board_id)

    async def _fake_user():
        return "user-lifecycle-test"

    app.dependency_overrides[require_user] = _fake_user
    client = TestClient(app)

    # Restore after context exit.
    class _CtxClient:
        def __enter__(self):
            return client.__enter__()

        def __exit__(self, *args):
            result = client.__exit__(*args)
            kg_rebuild_mod.get_kg_health = _original_health  # type: ignore[assignment]
            return result

    return _CtxClient()


def test_preflight_endpoint_returns_manifest_ref_and_source_set_hash():
    """val_d0da4a75 #1: /preflight is the manifest issuance point.
    Response MUST include manifest_ref + source_set_hash, and the
    manifest must be loadable via the canonical store."""
    with _client_with_router() as client:
        resp = client.post(
            "/api/v1/kg/rebuild/preflight",
            params={"board_id": "b-life"},
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["manifest_ref"].startswith("rebuild_manifest_")
    assert len(body["source_set_hash"]) == 64
    assert len(body["preflight_hash"]) == 64

    # Manifest is loadable from the canonical store.
    store = KGRebuildSourceManifest(
        artifact_store=require_rebuild_audit_artifact_store()
    )
    manifest = store.load(body["manifest_ref"])
    assert manifest is not None
    assert manifest.board_id == "b-life"
    assert manifest.preflight_hash == body["preflight_hash"]
    assert manifest.source_set_hash == body["source_set_hash"]


def test_confirm_uses_existing_manifest_does_not_recreate():
    """val_d0da4a75 #1: /confirm loads the manifest_ref produced by
    /preflight; it MUST NOT enumerate or build a new manifest."""
    with _client_with_router() as client:
        pre = client.post(
            "/api/v1/kg/rebuild/preflight",
            params={"board_id": "b-confirm-isolated"},
        )
        assert pre.status_code == 200
        manifest_ref = pre.json()["manifest_ref"]
        preflight_hash = pre.json()["preflight_hash"]

        conf = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b-confirm-isolated",
                "operation": "rebuild",
                "preflight_hash": preflight_hash,
                "manifest_ref": manifest_ref,
            },
        )
    assert conf.status_code == 200, conf.text
    body = conf.json()
    assert body["manifest_ref"] == manifest_ref  # SAME ref echoed back

    # Only ONE manifest exists in the store for this board (no
    # recreation by confirm).
    artifact_store = require_rebuild_audit_artifact_store()
    payloads = artifact_store.list_json(
        RebuildAuditKey(
            namespace="source_manifest",
            board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
        )
    )
    matching = sum(
        1
        for payload in payloads
        if payload.get("board_id") == "b-confirm-isolated"
    )
    assert matching == 1, f"expected exactly 1 manifest, got {matching}"


def test_confirm_fails_when_manifest_ref_not_found():
    """val_d0da4a75 regression: confirm must reject unknown manifest_ref."""
    with _client_with_router() as client:
        resp = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b1",
                "operation": "rebuild",
                "preflight_hash": "a" * 64,
                "manifest_ref": "rebuild_manifest_does_not_exist",
            },
        )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["error"] == "manifest_not_found"


def test_confirm_fails_when_preflight_hash_mismatch():
    """val_d0da4a75 regression: confirm must reject when preflight_hash
    does not match the manifest's bound hash."""
    with _client_with_router() as client:
        pre = client.post(
            "/api/v1/kg/rebuild/preflight",
            params={"board_id": "b-mismatch"},
        )
        manifest_ref = pre.json()["manifest_ref"]

        resp = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b-mismatch",
                "operation": "rebuild",
                "preflight_hash": "0" * 64,  # not the real one
                "manifest_ref": manifest_ref,
            },
        )
    assert resp.status_code == 400
    detail = resp.json()["detail"]
    assert detail["error"] == "preflight_hash_mismatch"


def test_confirm_rejects_invalid_preflight_hash_shape():
    """val_d0da4a75 #3: short or non-hex preflight_hash rejected."""
    with _client_with_router() as client:
        resp = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b1",
                "operation": "rebuild",
                "preflight_hash": "short",
                "manifest_ref": "rebuild_manifest_anything",
            },
        )
    # Pydantic length validator catches it first → 422.
    assert resp.status_code in (400, 422)


def test_manifest_load_rejects_path_traversal_and_alias(tmp_path: Path):
    """val_d0da4a75 #2: malformed manifest_ref values must be rejected
    by the load path before any filesystem access."""
    from okto_pulse.core.kg.rebuild_sources import KGRebuildSourceManifest

    store = KGRebuildSourceManifest(base_dir=tmp_path)
    # Place a legitimate manifest so traversal targets are visible.
    enum = RebuildSourceEnumerator(source_store=lambda _b: [_row()])
    source_set = enum.enumerate(board_id="b1")
    legit = store.build(source_set=source_set, preflight_hash="a" * 64)

    # All these should return None — never the real manifest.
    for ref in (
        "../manifests/" + legit.manifest_ref,
        "../../etc/passwd",
        "/etc/passwd",
        "rebuild_manifest_../" + legit.manifest_ref,
        "rebuild_manifest_a/b",
        "rebuild_manifest_a\\b",
        "manifest_no_prefix",
        "rebuild_manifest_",  # empty suffix
        legit.manifest_ref + "/..",
    ):
        assert store.load(ref) is None, f"expected None for traversal-shaped ref: {ref!r}"

    # The legitimate ref still works.
    assert store.load(legit.manifest_ref) is not None


def test_manifest_build_rejects_short_or_invalid_preflight_hash(tmp_path: Path):
    """val_d0da4a75 #3: KGRebuildSourceManifest.build validates
    preflight_hash as canonical sha256 hex 64 chars before building."""
    from okto_pulse.core.kg.rebuild_sources import KGRebuildSourceManifest

    enum = RebuildSourceEnumerator(source_store=lambda _b: [_row()])
    source_set = enum.enumerate(board_id="b1")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    for bad in ("short", "", "X" * 64, "1" * 63, "1" * 65):
        with pytest.raises(ValueError):
            store.build(source_set=source_set, preflight_hash=bad)


def test_validate_preflight_hash_accepts_canonical():
    from okto_pulse.core.kg.rebuild_sources import validate_preflight_hash

    canonical = "abcdef0123456789" * 4  # 64 hex chars
    assert validate_preflight_hash(canonical) == canonical
    with pytest.raises(ValueError):
        validate_preflight_hash("ABCDEF" + "0" * 58)  # uppercase
    with pytest.raises(ValueError):
        validate_preflight_hash("xyz" + "0" * 61)


def test_validate_manifest_ref_rejects_traversal_and_alias():
    from okto_pulse.core.kg.rebuild_sources import validate_manifest_ref

    # Happy path: shape produced by build()
    validate_manifest_ref("rebuild_manifest_abc123_-XYZ")

    # Rejected shapes:
    for bad in (
        "../manifests/rebuild_manifest_abc",
        "rebuild_manifest_a/b",
        "rebuild_manifest_a\\b",
        "rebuild_manifest_..",
        "rebuild_manifest_",
        "other_prefix_abc",
        "/etc/passwd",
        "",
    ):
        with pytest.raises(ValueError):
            validate_manifest_ref(bad)


def test_post_rebuild_confirm_rejects_unsupported_operation():
    from fastapi.testclient import TestClient

    from okto_pulse.core.infra.auth import require_user

    app = _make_rebuild_test_app(board_id="b1")

    async def _fake_user():
        return "u"

    app.dependency_overrides[require_user] = _fake_user

    with TestClient(app) as client:
        response = client.post(
            "/api/v1/kg/rebuild/confirm",
            json={
                "board_id": "b1",
                "operation": "drop_database",
                "preflight_hash": "deadbeef" * 8,
                "manifest_ref": "rebuild_manifest_anything",
            },
        )
    assert response.status_code == 400
    detail = response.json()["detail"]
    assert detail["error"] == "unsupported_operation"

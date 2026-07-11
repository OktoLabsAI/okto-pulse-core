"""R10A — BoardSourceReader port and Community SQLite adapter."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from kg_registry_testing import configure_test_kg_registry
from okto_pulse.core.application.boundary.source_read_consumer_gate import (
    SourceReadConsumerGate,
)
from okto_pulse.core.kg.board_source_store import (
    SPEC_CONTENT_COLUMNS_V1,
    SPEC_CONTENT_COLUMNS_V2,
    SPEC_SOURCE_MANIFEST_VERSION,
    _canonical_content_hash,
)
from okto_pulse.core.kg.interfaces.board_source_reader import SourceReadError

_board_source_reader = pytest.importorskip(
    "okto_pulse.community.adapters.board_source_reader",
    reason="AF-04 Community integration test requires the Community board source reader.",
)
CommunityBoardSourceReader = _board_source_reader.CommunityBoardSourceReader


def _source_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE boards (id TEXT, settings TEXT)")
        conn.execute(
            "CREATE TABLE specs ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, updated_at TEXT, "
            "title TEXT, description TEXT, context TEXT, version INTEGER, "
            "functional_requirements TEXT, technical_requirements TEXT, "
            "acceptance_criteria TEXT, test_scenarios TEXT, business_rules TEXT, "
            "api_contracts TEXT, decisions TEXT, integration_requirements TEXT, "
            "observability_requirements TEXT)"
        )
        conn.execute(
            "CREATE TABLE cards ("
            "id TEXT, board_id TEXT, status TEXT, created_at TEXT, updated_at TEXT, "
            "title TEXT, description TEXT, details TEXT, priority TEXT, "
            "card_type TEXT, spec_id TEXT, sprint_id TEXT, test_scenario_ids TEXT, "
            "conclusions TEXT, screen_mockups TEXT, knowledge_bases TEXT, "
            "validations TEXT, origin_task_id TEXT, severity TEXT, "
            "expected_behavior TEXT, observed_behavior TEXT, "
            "steps_to_reproduce TEXT, action_plan TEXT, linked_test_task_ids TEXT)"
        )
        conn.execute(
            "CREATE TABLE amendment_hotfix_revisions ("
            "id TEXT, board_id TEXT, created_at TEXT, updated_at TEXT, "
            "original_spec_id TEXT, origin_bug_id TEXT, origin_task_ids TEXT, "
            "affected_task_ids TEXT, revision_spec_id TEXT, "
            "regression_scenario_ids TEXT, regression_test_task_ids TEXT, "
            "automated_regression_refs TEXT, status TEXT, lineage_state TEXT)"
        )
        conn.execute(
            "INSERT INTO boards VALUES (?, ?)",
            ("b1", json.dumps({"kg_working_ttl_days": 7})),
        )
        conn.execute(
            "INSERT INTO specs VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "s1",
                "b1",
                "done",
                "2026-06-01T00:00:00Z",
                "2026-06-02T00:00:00Z",
                "Spec",
                "Description",
                "Context",
                2,
                '["FR"]',
                '["TR"]',
                '["AC"]',
                '["TS"]',
                '["BR"]',
                '[{"path": "/x"}]',
                '[{"id": "dec1", "title": "Decision", "status": "active"}]',
                '[ { "id" : "ir1" } ]',
                '[{"id":"or1"}]',
            ),
        )
        conn.execute(
            "INSERT INTO cards VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "bug1",
                "b1",
                "done",
                "2026-06-03T00:00:00Z",
                "2026-06-04T00:00:00Z",
                "Bug",
                "Bug desc",
                "",
                "high",
                "bug",
                "s1",
                None,
                "[]",
                "[]",
                "[]",
                "[]",
                "[]",
                None,
                "major",
                "expected",
                "observed",
                "step",
                "plan",
                '["test1"]',
            ),
        )
        conn.execute(
            "INSERT INTO amendment_hotfix_revisions VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "am1",
                "b1",
                "2026-06-05T00:00:00Z",
                "2026-06-06T00:00:00Z",
                "s1",
                "bug1",
                '["task1"]',
                '["task2"]',
                "s2",
                '["scenario1"]',
                '["test1"]',
                '["tests/test_bug.py::test_regression"]',
                "done",
                "complete",
            ),
        )
        conn.commit()
    return db_path


def test_community_reader_preserves_source_contract_fields(tmp_path: Path) -> None:
    rows = CommunityBoardSourceReader(_source_db(tmp_path)).fetch("b1")

    spec = next(row for row in rows if row["artifact_type"] == "spec")
    assert spec["source_ref"] == "spec:s1"
    assert spec["source_version"] == "2"
    assert spec["source_artifact_status"] == "done"
    assert spec["source_manifest_version"] == SPEC_SOURCE_MANIFEST_VERSION
    assert spec["working_ttl_days"] == 7
    assert spec["content_hash_v1"] == _canonical_content_hash(
        _spec_hash_row(), SPEC_CONTENT_COLUMNS_V1
    )
    assert spec["content_hash"] == _canonical_content_hash(
        _spec_hash_row(), SPEC_CONTENT_COLUMNS_V2
    )

    decision = next(row for row in rows if row["artifact_type"] == "decision")
    assert decision["source_ref"] == "decision:s1:dec1"

    bug = next(row for row in rows if row["artifact_type"] == "bug")
    assert bug["source_ref"] == "bug:bug1"
    assert bug["has_minimal_evidence"] is True

    amendment = next(
        row for row in rows if row["artifact_type"] == "amendment_hotfix_revision"
    )
    assert amendment["source_ref"] == "amendment_hotfix_revision:am1"
    assert amendment["source_artifact_status"] == "done"
    assert amendment["lineage_complete"] is True


def _spec_hash_row() -> dict[str, object]:
    return {
        "title": "Spec",
        "description": "Description",
        "context": "Context",
        "version": 2,
        "functional_requirements": '["FR"]',
        "technical_requirements": '["TR"]',
        "acceptance_criteria": '["AC"]',
        "test_scenarios": '["TS"]',
        "business_rules": '["BR"]',
        "api_contracts": '[{"path": "/x"}]',
        "decisions": '[{"id": "dec1", "title": "Decision", "status": "active"}]',
        "integration_requirements": '[ { "id" : "ir1" } ]',
        "observability_requirements": '[{"id":"or1"}]',
    }


def test_community_reader_translates_sqlite_errors_to_structured_source_error(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bad.db"
    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE specs (id TEXT, board_id TEXT)")
        conn.commit()

    with pytest.raises(SourceReadError) as exc:
        CommunityBoardSourceReader(db_path).fetch("b1")

    assert exc.value.code == "read_error"
    assert exc.value.cause_type == "OperationalError"
    assert "sqlite3" not in str(exc.value)

    unavailable = tmp_path / "unavailable.db"
    unavailable.mkdir()
    with pytest.raises(SourceReadError) as unavailable_exc:
        CommunityBoardSourceReader(unavailable).fetch("b1")

    assert unavailable_exc.value.code == "source_unavailable"
    assert "sqlite3" not in str(unavailable_exc.value)


def test_kg_rebuild_build_source_store_uses_registry_reader() -> None:
    class _Reader:
        def fetch(self, board_id: str) -> list[dict[str, str]]:
            return [{"artifact_type": "spec", "id": board_id}]

    configure_test_kg_registry(board_source_reader=_Reader())

    from okto_pulse.community.api.kg_rebuild import _build_source_store

    assert _build_source_store()("b-reg") == [{"artifact_type": "spec", "id": "b-reg"}]


def test_source_read_gate_passes_real_core_and_blocks_direct_consumer(
    tmp_path: Path,
) -> None:
    real_root = Path(__file__).resolve().parents[1] / "src"
    assert SourceReadConsumerGate().run(source_root=real_root).status == "passed"

    target = tmp_path / "src" / "okto_pulse" / "core" / "kg"
    target.mkdir(parents=True)
    (target / "canonical_debt_replay.py").write_text(
        "from okto_pulse.core.kg.board_source_store import BoardSourceStore\n"
        "def _pulse_db_path():\n"
        "    return 'pulse.db'\n"
        "def f():\n"
        "    return BoardSourceStore().fetch('b')\n",
        encoding="utf-8",
    )

    report = SourceReadConsumerGate().run(source_root=tmp_path)
    assert report.status == "blocking"
    assert report.evidence["offenders"]


def test_source_read_gate_blocks_all_historical_raw_consumers(
    tmp_path: Path,
) -> None:
    files = (
        "okto_pulse/core/api/kg_rebuild.py",
        "okto_pulse/core/kg/canonical_debt_replay.py",
        "okto_pulse/core/kg/canonical_stale_reconciler.py",
        "okto_pulse/core/kg/stale_canonical_parity.py",
        "okto_pulse/core/services/kg_health_service.py",
    )
    for rel in files:
        path = tmp_path / "src" / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            "from okto_pulse.core.kg.board_source_store import BoardSourceStore\n"
            "def _build_source_index():\n"
            "    return BoardSourceStore().fetch('b1')\n",
            encoding="utf-8",
        )

    report = SourceReadConsumerGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    offender_files = {offender["file"] for offender in report.evidence["offenders"]}
    assert set(files).issubset(offender_files)


def test_source_read_gate_discovers_new_raw_source_read_consumer(
    tmp_path: Path,
) -> None:
    target = tmp_path / "src" / "okto_pulse" / "core" / "kg"
    target.mkdir(parents=True)
    (target / "new_canonical_thing.py").write_text(
        "import sqlite3\n"
        "from okto_pulse.core.db import get_engine\n"
        "from okto_pulse.core.kg.board_source_store import BoardSourceStore\n"
        "def _build_source_index():\n"
        "    sqlite3.connect('pulse.db')\n"
        "    BoardSourceStore().fetch('b1')\n"
        "    return str(get_engine().url)\n",
        encoding="utf-8",
    )

    report = SourceReadConsumerGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    offenders = report.evidence["offenders"]
    assert any(
        offender["file"].endswith("okto_pulse/core/kg/new_canonical_thing.py")
        and offender["kind"] == "forbidden_sqlite_connect"
        for offender in offenders
    )
    assert any(
        offender["file"].endswith("okto_pulse/core/kg/new_canonical_thing.py")
        and offender["kind"] == "forbidden_import"
        for offender in offenders
    )
    assert any(
        offender["file"].endswith("okto_pulse/core/kg/new_canonical_thing.py")
        and offender["kind"] == "forbidden_engine_url_path_read"
        for offender in offenders
    )

"""R10A — BoardSourceReader port and Community SQLite adapter."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from kg_registry_testing import configure_test_kg_registry
from source_reader_schema_testing import (
    create_complete_source_catalog,
    insert_source_board,
)
from okto_pulse.core.application.boundary.source_read_consumer_gate import (
    SourceReadConsumerGate,
)
from okto_pulse.core.kg.board_source_store import (
    REFINEMENT_CONTENT_COLUMNS,
    SPEC_CONTENT_COLUMNS_V1,
    SPEC_CONTENT_COLUMNS_V2,
    SPEC_SOURCE_MANIFEST_VERSION,
    _canonical_content_hash,
    projected_root_content_hash,
)
from okto_pulse.core.kg.interfaces.board_source_reader import (
    BoardSourceSnapshot,
    SourceReadError,
)
from okto_pulse.core.services.quality_projection_currentness import (
    legacy_spec_validation_digest_set_v1,
    legacy_spec_validation_versions_v1,
)

_board_source_reader = pytest.importorskip(
    "okto_pulse.community.adapters.board_source_reader",
    reason="AF-04 Community integration test requires the Community board source reader.",
)
CommunityBoardSourceReader = _board_source_reader.CommunityBoardSourceReader
read_realm_source_snapshot = _board_source_reader.read_realm_source_snapshot


def _source_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "pulse.db"
    with sqlite3.connect(str(db_path)) as conn:
        create_complete_source_catalog(conn)
        insert_source_board(conn, "b1")
        conn.execute(
            "UPDATE boards SET settings = ? WHERE id = ?",
            (json.dumps({"kg_working_ttl_days": 7}), "b1"),
        )
        conn.execute(
            "INSERT INTO specs "
            "(id, board_id, status, created_at, updated_at, title, description, "
            "context, version, edition, functional_requirements, technical_requirements, "
            "acceptance_criteria, test_scenarios, business_rules, api_contracts, "
            "decisions, integration_requirements, observability_requirements) VALUES ("
            "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                1,
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
            "INSERT INTO cards "
            "(id, board_id, status, created_at, updated_at, title, description, "
            "details, priority, card_type, spec_id, sprint_id, test_scenario_ids, "
            "conclusions, screen_mockups, knowledge_bases, validations, "
            "origin_task_id, severity, expected_behavior, observed_behavior, "
            "steps_to_reproduce, action_plan, linked_test_task_ids) VALUES ("
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
            "INSERT INTO amendment_hotfix_revisions "
            "(id, board_id, created_at, updated_at, original_spec_id, origin_bug_id, "
            "origin_task_ids, affected_task_ids, revision_spec_id, "
            "regression_scenario_ids, regression_test_task_ids, "
            "automated_regression_refs, status, lineage_state) VALUES ("
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
    content_hash_v2 = _canonical_content_hash(
        _spec_hash_row(), SPEC_CONTENT_COLUMNS_V2
    )
    assert spec["content_hash_v2"] == content_hash_v2
    assert spec["content_hash"] == projected_root_content_hash(content_hash_v2)

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


def _insert_mapping(
    connection: sqlite3.Connection,
    table_name: str,
    values: dict[str, object],
) -> None:
    columns = tuple(values)
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    placeholders = ", ".join("?" for _ in columns)
    connection.execute(
        f'INSERT INTO "{table_name}" ({quoted_columns}) '
        f"VALUES ({placeholders})",
        tuple(values[column] for column in columns),
    )


def _seed_current_projection_heads(
    connection: sqlite3.Connection,
) -> None:
    quality_subject = {
        field_name: (
            json.loads(value)
            if isinstance(value, str)
            and value.lstrip().startswith(("[", "{"))
            else value
        )
        for field_name, value in _spec_hash_row().items()
    }
    quality_digests = legacy_spec_validation_digest_set_v1(
        subject=quality_subject,
        qa_items=(),
    )
    quality_versions = legacy_spec_validation_versions_v1()
    refinement = {
        column: None for column in REFINEMENT_CONTENT_COLUMNS
    }
    refinement.update(
        {
            "id": "r1",
            "board_id": "b1",
            "created_at": "2026-06-07T00:00:00Z",
            "updated_at": "2026-06-07T00:00:00Z",
            "archived": 0,
            "title": "Refinement",
            "description": "Refinement description",
            "in_scope": '["scope"]',
            "out_of_scope": "[]",
            "analysis": "Analysis",
            "decisions": "[]",
            "status": "done",
            "version": 2,
            "edition": 1,
            "labels": "[]",
        }
    )
    _insert_mapping(connection, "refinements", refinement)

    for receipt_id, subject_type, subject_id, score in (
        ("quality-current", "spec", "s1", 2.0),
        ("quality-history", "spec", "s1", 3.0),
    ):
        _insert_mapping(
            connection,
            "quality_assessment_receipts",
            {
                "id": receipt_id,
                "board_id": "b1",
                "subject_type": subject_type,
                "subject_id": subject_id,
                "subject_version": 2,
                "subject_edition": 1,
                "assessment_kind": "spec_validation",
                "origin": "legacy_import",
                "source": "legacy_migration",
                "channel": "legacy_import",
                "outcome": "recorded",
                "scale_kind": "percentage",
                "scale_minimum": 0.0,
                "scale_maximum": 100.0,
                "scale_direction": "lower_better",
                "score": score,
                "justification": f"Receipt {receipt_id}",
                "content_digest": quality_digests.content_digest,
                "clarification_digest": quality_digests.clarification_digest,
                "ruleset_digest": quality_digests.ruleset_digest,
                "taxonomy_digest": quality_digests.taxonomy_digest,
                "policy_digest": quality_digests.policy_digest,
                "input_digest": quality_digests.input_digest,
                "canonicalization_version": (
                    quality_digests.canonicalization_version
                ),
                "ruleset_version": quality_versions.ruleset_version,
                "taxonomy_version": quality_versions.taxonomy_version,
                "analyzer_version": quality_versions.analyzer_version,
                "policy_version": quality_versions.policy_version,
                "run_identity_digest": "1" * 64,
                "authority_digest": "2" * 64,
                "created_by": "agent",
                "created_at": "2026-06-08T00:00:00Z",
                "predecessor_receipt_id": None,
                "contract_version": "quality-assessment/v1",
                "head_revision": (
                    1 if receipt_id == "quality-current" else 2
                ),
            },
        )
    _insert_mapping(
        connection,
        "quality_assessment_heads",
        {
            "board_id": "b1",
            "subject_type": "spec",
            "subject_id": "s1",
            "assessment_kind": "spec_validation",
            "receipt_id": "quality-current",
            "revision": 1,
            "updated_at": "2026-06-08T00:00:00Z",
        },
    )

    for entry_id, rationale in (
        ("rdl-current", "Current rationale"),
        ("rdl-history", "Historical rationale"),
    ):
        _insert_mapping(
            connection,
            "research_decision_entries",
            {
                "id": entry_id,
                "ledger_id": "ledger-1",
                "board_id": "b1",
                "refinement_id": "r1",
                "refinement_version": 2,
                "predecessor_entry_id": None,
                "unknown": "Which retry strategy?",
                "status": "resolved",
                "anchor_type": "functional_requirement",
                "anchor_ref": "fr_retry",
                "evidence_refs": '["evidence:1"]',
                "alternatives": '["bounded", "fixed"]',
                "decision": "bounded",
                "rationale": rationale,
                "confidence": 0.9,
                "evidence_absence_justification": None,
                "created_by": "agent",
                "created_at": "2026-06-08T00:00:00Z",
            },
        )
    _insert_mapping(
        connection,
        "research_decision_heads",
        {
            "ledger_id": "ledger-1",
            "board_id": "b1",
            "refinement_id": "r1",
            "current_entry_id": "rdl-current",
            "revision": 1,
            "refinement_version": 2,
            "status": "resolved",
            "updated_by": "agent",
            "updated_at": "2026-06-08T00:00:00Z",
        },
    )


def test_current_heads_bind_roots_without_pseudo_rows_and_readers_match(
    tmp_path: Path,
) -> None:
    db_path = _source_db(tmp_path)
    with sqlite3.connect(db_path) as connection:
        _seed_current_projection_heads(connection)
        connection.commit()

    per_board = CommunityBoardSourceReader(db_path).fetch("b1")
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        _boards, realm_rows = read_realm_source_snapshot(
            connection,
            realm_id="local",
        )

    assert per_board.complete is True
    assert tuple(per_board.rows) == realm_rows["b1"]
    assert sum(
        "FROM quality_assessment_heads AS head" in statement
        for statement in statements
    ) == 1
    assert sum(
        "FROM research_decision_heads AS head" in statement
        for statement in statements
    ) == 1
    assert not any(
        row["artifact_type"]
        in {"quality_assessment", "research_decision"}
        for row in per_board.rows
    )

    baseline = {
        row["source_ref"]: row["content_hash"]
        for row in per_board.rows
        if row["source_ref"] in {"spec:s1", "refinement:r1"}
    }
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "UPDATE quality_assessment_receipts SET score = 4 "
            "WHERE id = 'quality-history'"
        )
        connection.execute(
            "UPDATE research_decision_entries SET rationale = 'Changed history' "
            "WHERE id = 'rdl-history'"
        )
        connection.commit()
    history_only = {
        row["source_ref"]: row["content_hash"]
        for row in CommunityBoardSourceReader(db_path).fetch("b1").rows
        if row["source_ref"] in baseline
    }
    assert history_only == baseline

    with sqlite3.connect(db_path) as connection:
        connection.execute(
            "UPDATE quality_assessment_heads "
            "SET receipt_id = 'quality-history', revision = 2 "
            "WHERE board_id = 'b1' AND subject_type = 'spec' "
            "AND subject_id = 's1' "
            "AND assessment_kind = 'spec_validation'"
        )
        connection.execute(
            "UPDATE research_decision_heads "
            "SET current_entry_id = 'rdl-history', revision = 2 "
            "WHERE ledger_id = 'ledger-1'"
        )
        connection.commit()
    promoted = {
        row["source_ref"]: row["content_hash"]
        for row in CommunityBoardSourceReader(db_path).fetch("b1").rows
        if row["source_ref"] in baseline
    }
    assert promoted["spec:s1"] != baseline["spec:s1"]
    assert promoted["refinement:r1"] != baseline["refinement:r1"]


def test_projection_catalog_is_required_for_board_and_realm_reads(
    tmp_path: Path,
) -> None:
    db_path = _source_db(tmp_path)
    with sqlite3.connect(db_path) as connection:
        connection.execute("DROP TABLE quality_assessment_heads")
        connection.commit()

    snapshot = CommunityBoardSourceReader(db_path).fetch("b1")
    assert snapshot.complete is False
    assert snapshot.cause == "table_missing"
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        with pytest.raises(
            sqlite3.DatabaseError,
            match="source catalog is incomplete",
        ):
            read_realm_source_snapshot(connection, realm_id="local")


def test_community_reader_translates_sqlite_errors_to_structured_source_error(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "bad.db"
    with sqlite3.connect(str(db_path)) as conn:
        create_complete_source_catalog(conn)
        insert_source_board(conn, "b1")
        conn.commit()

    class _ReaderWithFailingPostPreflightRead(CommunityBoardSourceReader):
        def _fetch_conn(
            self,
            conn: sqlite3.Connection,
            board_id: str,
        ) -> BoardSourceSnapshot:
            snapshot = super()._fetch_conn(conn, board_id)
            assert snapshot.complete is True
            conn.execute("SELECT missing_source_column FROM specs").fetchall()
            return snapshot

    with pytest.raises(SourceReadError) as exc:
        _ReaderWithFailingPostPreflightRead(db_path).fetch("b1")

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
        def fetch(self, board_id: str) -> BoardSourceSnapshot:
            return BoardSourceSnapshot(
                rows=({"artifact_type": "spec", "id": board_id},),
            )

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

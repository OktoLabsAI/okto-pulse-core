"""SQLite fixtures for the authoritative BoardSourceReader catalog."""

from __future__ import annotations

import sqlite3

from okto_pulse.core.kg.board_source_store import (
    AMENDMENT_CONTENT_COLUMNS,
    CARD_CONTENT_COLUMNS,
    IDEATION_CONTENT_COLUMNS,
    REFINEMENT_CONTENT_COLUMNS,
    SPEC_CONTENT_COLUMNS_V2,
    SPRINT_CONTENT_COLUMNS,
    STORY_CONTENT_COLUMNS,
)


_SOURCE_TABLE_COLUMNS: dict[str, set[str]] = {
    "stories": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        *STORY_CONTENT_COLUMNS,
    },
    "ideations": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        *IDEATION_CONTENT_COLUMNS,
    },
    "specs": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        "status",
        *SPEC_CONTENT_COLUMNS_V2,
    },
    "refinements": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        *REFINEMENT_CONTENT_COLUMNS,
    },
    "sprints": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        *SPRINT_CONTENT_COLUMNS,
    },
    "cards": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        "status",
        *CARD_CONTENT_COLUMNS,
    },
    "amendment_hotfix_revisions": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "status",
        *AMENDMENT_CONTENT_COLUMNS,
    },
}


def create_complete_source_catalog(connection: sqlite3.Connection) -> None:
    """Create the smallest schema that proves a complete source snapshot."""

    connection.execute(
        "CREATE TABLE IF NOT EXISTS boards ("
        "id TEXT PRIMARY KEY, name TEXT, description TEXT, owner_id TEXT, "
        "realm_id TEXT, settings TEXT)"
    )
    for table, columns in sorted(_SOURCE_TABLE_COLUMNS.items()):
        declarations = ["id TEXT PRIMARY KEY"]
        declarations.extend(
            f'"{column}" {"INTEGER" if column == "archived" else "TEXT"}'
            for column in sorted(columns - {"id"})
        )
        connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(declarations)})'
        )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS quality_assessment_receipts ("
        "id TEXT PRIMARY KEY, board_id TEXT, subject_type TEXT, subject_id TEXT, "
        "subject_version INTEGER, assessment_kind TEXT, origin TEXT, source TEXT, "
        "channel TEXT, outcome TEXT, scale_kind TEXT, scale_minimum REAL, "
        "scale_maximum REAL, scale_direction TEXT, score REAL, justification TEXT, "
        "content_digest TEXT, clarification_digest TEXT, ruleset_digest TEXT, "
        "taxonomy_digest TEXT, policy_digest TEXT, input_digest TEXT, "
        "canonicalization_version TEXT, ruleset_version TEXT, taxonomy_version TEXT, "
        "analyzer_version TEXT, policy_version TEXT, run_identity_digest TEXT, "
        "authority_digest TEXT, created_by TEXT, created_at TEXT, "
        "predecessor_receipt_id TEXT, contract_version TEXT, head_revision INTEGER)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS quality_assessment_heads ("
        "board_id TEXT, subject_type TEXT, subject_id TEXT, assessment_kind TEXT, "
        "receipt_id TEXT, revision INTEGER, updated_at TEXT, "
        "PRIMARY KEY (board_id, subject_type, subject_id, assessment_kind))"
    )
    for table_name, parent_column in (
        ("ideation_qa_items", "ideation_id"),
        ("refinement_qa_items", "refinement_id"),
        ("spec_qa_items", "spec_id"),
    ):
        connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{table_name}" ('
            "id TEXT PRIMARY KEY, "
            f'"{parent_column}" TEXT, '
            "question TEXT, question_type TEXT, choices TEXT, "
            "allow_free_text INTEGER, answer TEXT, selected TEXT, "
            "answered_at TEXT, revision INTEGER, lifecycle TEXT, "
            "tombstoned INTEGER)"
        )
        # Some endpoint tests use the process-wide local SQLite database, whose
        # Q&A tables may predate the SK-A clarification identity columns.
        # CREATE TABLE IF NOT EXISTS cannot bring those existing fixtures
        # forward, so make the test catalog converge to the production schema.
        existing_columns = {
            str(row[1])
            for row in connection.execute(
                f'PRAGMA table_info("{table_name}")'
            ).fetchall()
        }
        missing_columns = (
            ("revision", "INTEGER NOT NULL DEFAULT 1"),
            ("lifecycle", "TEXT NOT NULL DEFAULT 'active'"),
            ("tombstoned", "INTEGER NOT NULL DEFAULT 0"),
        )
        for column_name, declaration in missing_columns:
            if column_name not in existing_columns:
                connection.execute(
                    f'ALTER TABLE "{table_name}" '
                    f'ADD COLUMN "{column_name}" {declaration}'
                )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS research_decision_entries ("
        "id TEXT PRIMARY KEY, ledger_id TEXT, board_id TEXT, refinement_id TEXT, "
        "refinement_version INTEGER, predecessor_entry_id TEXT, unknown TEXT, "
        "status TEXT, anchor_type TEXT, anchor_ref TEXT, evidence_refs TEXT, "
        "alternatives TEXT, decision TEXT, rationale TEXT, confidence REAL, "
        "evidence_absence_justification TEXT, created_by TEXT, created_at TEXT)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS research_decision_heads ("
        "ledger_id TEXT PRIMARY KEY, board_id TEXT, refinement_id TEXT, "
        "current_entry_id TEXT, revision INTEGER, refinement_version INTEGER, "
        "status TEXT, updated_by TEXT, updated_at TEXT)"
    )


def insert_source_board(
    connection: sqlite3.Connection,
    board_id: str,
) -> bool:
    """Insert a board row compatible with both minimal and ORM test schemas."""

    available = {
        str(row[1])
        for row in connection.execute("PRAGMA table_info(boards)").fetchall()
    }
    candidates: tuple[tuple[str, str], ...] = (
        ("id", board_id),
        ("name", f"Source board {board_id}"),
        ("description", f"Source board summary {board_id}"),
        ("owner_id", "source-reader-test"),
        ("realm_id", "local"),
        ("settings", "{}"),
    )
    values = tuple((column, value) for column, value in candidates if column in available)
    columns_sql = ", ".join(f'"{column}"' for column, _ in values)
    placeholders = ", ".join("?" for _ in values)
    cursor = connection.execute(
        f"INSERT OR IGNORE INTO boards ({columns_sql}) VALUES ({placeholders})",
        tuple(value for _, value in values),
    )
    return cursor.rowcount > 0

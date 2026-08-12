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
        "edition",
        *IDEATION_CONTENT_COLUMNS,
    },
    "specs": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        "status",
        "edition",
        *SPEC_CONTENT_COLUMNS_V2,
    },
    "refinements": {
        "id",
        "board_id",
        "created_at",
        "updated_at",
        "archived",
        "edition",
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


# Closed Code Traceability source catalog consumed by the Community
# BoardSourceReader.  These fixtures intentionally model only relational shape;
# individual Code Traceability persistence tests own constraints and types.
_CODE_TRACEABILITY_SOURCE_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "code_investigation_requests": (
        "id", "board_id", "subject_type", "subject_id", "subject_version",
        "issued_to_actor_id", "source_ref", "required_capabilities",
        "selector_scope_digest", "expected_head_generation",
        "expected_predecessor_receipt_id", "canonicalization_profile",
        "limits_profile", "challenge_key_id", "challenge_token_hash",
        "single_use", "status", "expires_at", "requested_by", "created_at",
        "consumed_at", "request_payload_sha256", "idempotency_key",
    ),
    "code_investigation_receipts": (
        "id", "request_id", "board_id", "subject_type", "subject_id",
        "subject_version", "attestor_actor_id", "generation",
        "predecessor_receipt_id", "trust_level", "acceptance_status",
        "outcome", "capabilities", "source_ref", "source_identity_digest",
        "canonicalization_profile", "limits_profile", "selector_scope_digest",
        "declared_revision", "workspace_state_id", "declared_dirty",
        "reproducibility_claim", "fingerprint_algorithm", "manifest_digest",
        "manifest_entry_count", "omission_manifest", "omission_digest",
        "omission_count", "tooling", "observed_at", "received_at",
        "expires_at", "observation_sha256", "payload_sha256", "idempotency_key",
    ),
    "code_investigation_receipt_revocations": (
        "id", "receipt_id", "board_id", "reason_code", "justification",
        "revoked_by", "revoked_at",
    ),
    "code_investigation_heads": (
        "board_id", "source_ref", "generation", "latest_receipt_id",
        "current_receipt_id", "state", "revision", "updated_at",
    ),
    "code_evidence": (
        "id", "board_id", "investigation_receipt_id", "source_ref",
        "parent_type", "refinement_id", "spec_id", "card_id", "parent_version",
        "evidence_type", "claim", "declared_revision", "workspace_state_id",
        "declared_dirty", "reproducibility_claim", "selector_kind",
        "relative_path", "language", "symbol_kind", "qualified_symbol",
        "symbol_signature", "snapshot_line_start", "snapshot_line_end", "excerpt",
        "excerpt_sha256", "excerpt_omitted_reason", "declared_file_blob_sha256",
        "declared_source_content_sha256", "attestation_state",
        "attestation_basis", "lifecycle_status", "supersedes_evidence_id",
        "revocation_reason", "submitted_by", "received_at", "payload_sha256",
        "idempotency_key",
    ),
    "code_evidence_spec_links": (
        "id", "board_id", "spec_id", "evidence_id", "entity_type", "entity_id",
        "relation_type", "rationale", "evidence_content_sha256",
        "source_refinement_version", "spec_version", "created_by", "created_at",
    ),
    "code_evidence_dispositions": (
        "id", "board_id", "spec_id", "evidence_id", "disposition",
        "justification", "spec_version", "active", "created_by", "created_at",
        "cleared_by", "cleared_at",
    ),
    "implementation_targets": (
        "id", "board_id", "card_id", "source_ref", "selector_kind",
        "relative_path_hint", "language", "symbol_kind", "qualified_symbol",
        "symbol_signature", "role", "intent", "required", "source_spec_version",
        "baseline_evidence_id", "lifecycle_status", "revision",
        "current_resolution_id", "last_change_reason_sha256", "created_by",
        "created_at", "updated_at",
    ),
    "implementation_target_spec_links": (
        "id", "target_id", "spec_id", "entity_type", "entity_id", "created_by",
        "created_at",
    ),
    "implementation_target_evidence_links": (
        "id", "target_id", "evidence_id", "relation_type", "created_by",
        "created_at",
    ),
    "implementation_target_resolutions": (
        "id", "board_id", "target_id", "investigation_receipt_id", "source_ref",
        "receipt_generation", "subject_version", "target_revision",
        "declared_revision", "workspace_state_id", "declared_dirty", "state",
        "resolved_relative_path", "resolved_language", "resolved_symbol_kind",
        "resolved_qualified_symbol", "resolved_symbol_signature",
        "resolved_line_start", "resolved_line_end", "symbol_fingerprint",
        "declared_file_blob_sha256", "selector_fingerprint", "confidence",
        "reason_code", "candidate_count", "candidates", "declared_tool_id",
        "declared_tool_version", "submitted_by", "agent_observed_at", "received_at",
        "payload_sha256", "idempotency_key",
    ),
    "implementation_target_execution_records": (
        "id", "board_id", "card_id", "target_id", "target_revision",
        "result_investigation_receipt_id", "source_ref", "disposition",
        "result_declared_revision", "result_workspace_state_id",
        "actual_relative_path", "actual_qualified_symbol", "replacement_target_id",
        "justification", "submitted_by", "received_at", "payload_sha256",
        "idempotency_key",
    ),
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
            f'"{column}" '
            + (
                "INTEGER NOT NULL DEFAULT 1"
                if column == "edition"
                else "INTEGER"
                if column == "archived"
                else "TEXT"
            )
            for column in sorted(columns - {"id"})
        )
        connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(declarations)})'
        )
    for table, columns in sorted(
        _CODE_TRACEABILITY_SOURCE_TABLE_COLUMNS.items()
    ):
        declarations = [f'"{column}" TEXT' for column in columns]
        connection.execute(
            f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(declarations)})'
        )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS quality_assessment_receipts ("
        "id TEXT PRIMARY KEY, board_id TEXT, subject_type TEXT, subject_id TEXT, "
        "subject_version INTEGER, subject_edition INTEGER, assessment_kind TEXT, "
        "origin TEXT, source TEXT, "
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

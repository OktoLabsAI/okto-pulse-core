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
        "id TEXT PRIMARY KEY, name TEXT, owner_id TEXT, realm_id TEXT, settings TEXT)"
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
        ("owner_id", "source-reader-test"),
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

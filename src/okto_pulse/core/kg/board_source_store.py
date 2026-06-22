"""Production source store for KG rebuild (bug b4c6920c).

Reads SDLC artifacts (ideations, refinements, specs, sprints and
task/test/bug cards) from the SQLite-backed SQLAlchemy store and returns the
raw rows expected by ``RebuildSourceEnumerator``. The enumerator owns maturity
filtering; this store deliberately includes cancelled/archived rows so rebuild
preflight can report deterministic skip counts.
list expected by ``RebuildSourceEnumerator`` (KG-02.2).

Replaces the ``_empty_source_store`` stub that ``core/api/kg_rebuild.py``
was wiring in production — that stub caused preflight to always report
``eligible_source_count=0`` even for boards with content.

Design choices:

* **Sync interface** — ``RebuildSourceEnumerator.enumerate`` calls the
  source_store synchronously inside ``preflight``. We use the same
  SQLite file the async engine uses but with the stdlib ``sqlite3``
  module (read-only). This is safe because:
  - rebuild preflight is administrative + not on a hot path,
  - the read is row-by-row, takes <100ms even for boards with thousands
    of artifacts,
  - SQLite supports concurrent readers without blocking the async writer.

* **content_hash deterministic** — there is no ``content_hash`` column
  on the source tables. We compute a stable SHA-256 over the canonical
  JSON of the load-bearing fields (title + description + version + any
  structured JSON arrays). KG-02.5 ``EXCLUDED_HASH_FIELDS`` excludes
  timestamps + ids from the structural hash, so the content_hash here
  feeds into the source-side hash only (not the structural hash).

* **source_version** — uses the existing ``version`` integer column
  when present (specs/refinements), while task/test/bug cards fall back
  to '1'.

* **source_ref** — uses ``<artifact_type>:<id>`` to match the convention
  used by the cognitive badge surface. Formal decisions are emitted as
  semantic source rows for cognitive pending, while deterministic rebuild
  materializes decision nodes through the owning spec payload to avoid
  duplicates. Card rows are typed as task/test/bug even though the deterministic
  worker still consumes them via the legacy ConsolidationQueue ``card``
  artifact type.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger("okto_pulse.kg.board_source_store")


# Spec source manifest versioning (spec eaf185c9 / card 5ec8c75c). V2 adds
# integration_requirements + observability_requirements to the spec content
# hash. V1 is kept so a legacy board's stored baseline can be PROVEN
# schema-rebaseline (the v1-compatible hash is unchanged) versus genuinely
# drifted (real content change). This module only EXPOSES both hashes + the
# version; the drift-vs-rebaseline decision lives in the rebuild pipeline
# (rebuild_sources/rebuild_service), never here or in the deterministic worker.
SPEC_SOURCE_MANIFEST_VERSION = 2
_SPEC_CONTENT_COLUMNS_BASE: tuple[str, ...] = (
    "title", "description", "context", "version",
    "functional_requirements", "technical_requirements",
    "acceptance_criteria", "test_scenarios",
    "business_rules", "api_contracts", "decisions",
)
SPEC_CONTENT_COLUMNS_V1: tuple[str, ...] = _SPEC_CONTENT_COLUMNS_BASE
SPEC_CONTENT_COLUMNS_V2: tuple[str, ...] = _SPEC_CONTENT_COLUMNS_BASE + (
    "integration_requirements",
    "observability_requirements",
)


# Artifact types read directly from dedicated tables. Cards are read below
# through a polymorphic query that maps card_type normal/test/bug to
# source artifact_type task/test/bug.
ARTIFACT_QUERIES: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    # (artifact_type, table, status_column, content_columns_for_hash)
    (
        "story",
        "stories",
        "status",
        (
            "title", "description", "actor", "goal", "benefit",
            "topic_id", "status", "labels",
        ),
    ),
    (
        "ideation",
        "ideations",
        "status",
        (
            "title", "description", "problem_statement", "proposed_approach",
            "scope_assessment", "complexity", "status", "version", "labels",
        ),
    ),
    (
        "spec",
        "specs",
        "status",
        SPEC_CONTENT_COLUMNS_V2,
    ),
    (
        "refinement",
        "refinements",
        "status",
        (
            "title", "description", "in_scope", "out_of_scope", "analysis",
            "decisions", "status", "version", "labels",
        ),
    ),
    (
        "sprint",
        "sprints",
        "status",
        (
            "title", "description", "spec_id", "spec_version", "status",
            "lane_type", "objective", "expected_outcome",
            "test_scenario_ids", "business_rule_ids", "evaluations",
            "version", "labels",
        ),
    ),
)


CARD_CONTENT_COLUMNS: tuple[str, ...] = (
    "title",
    "description",
    "details",
    "status",
    "priority",
    "card_type",
    "spec_id",
    "sprint_id",
    "test_scenario_ids",
    "conclusions",
    "screen_mockups",
    "knowledge_bases",
    "validations",
    "origin_task_id",
    "severity",
    "expected_behavior",
    "observed_behavior",
    "steps_to_reproduce",
    "action_plan",
    "linked_test_task_ids",
)


#: Load-bearing fields for the Path B amendment content hash (spec 7ea1e4be).
#: Includes the exact-membership lineage sets (origin/affected task ids) so a
#: lineage change re-hashes and re-enqueues the amendment (stale edges/semantics
#: would otherwise survive a rebuild). ``validation_metadata`` is intentionally
#: EXCLUDED: it is external audit/evidence-pointer data, not edge-bearing lineage
#: — the canonical node + its belongs_to edges derive from the refs + status +
#: lineage_state below, so audit changes must not force re-materialization.
AMENDMENT_CONTENT_COLUMNS: tuple[str, ...] = (
    "original_spec_id",
    "origin_bug_id",
    "origin_task_ids",
    "affected_task_ids",
    "revision_spec_id",
    "regression_scenario_ids",
    "regression_test_task_ids",
    "automated_regression_refs",
    "status",
    "lineage_state",
)


def _canonical_content_hash(row: sqlite3.Row, columns: tuple[str, ...]) -> str:
    """SHA-256 over a canonical-JSON encoding of the load-bearing fields.

    Stable across runs as long as the underlying values don't change.
    Uses sorted keys + tight separators (matches KG-02.5
    `_canonical_json` style)."""

    payload: dict[str, Any] = {}
    for col in columns:
        value: Any
        try:
            value = row[col]
        except (IndexError, KeyError):
            value = None
        if isinstance(value, (bytes, bytearray)):
            value = value.decode("utf-8", errors="replace")
        if isinstance(value, str):
            # Most JSON columns are stored as TEXT — try to parse so the
            # hash is invariant to whitespace re-formatting.
            try:
                value = json.loads(value)
            except (json.JSONDecodeError, ValueError):
                pass
        payload[col] = value
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _to_iso(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return {str(row["name"]) for row in rows}


def _board_working_ttl_days(
    conn: sqlite3.Connection,
    board_id: str,
) -> int | None:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='boards'"
    ).fetchone()
    if not exists:
        return None
    columns = _table_columns(conn, "boards")
    if "settings" not in columns:
        return None
    row = conn.execute(
        "SELECT settings FROM boards WHERE id = ?",
        (board_id,),
    ).fetchone()
    if row is None:
        return None
    raw = row["settings"]
    if not raw:
        return None
    try:
        settings = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(settings, dict):
        return None
    for key in (
        "kg_working_ttl_days",
        "kg_working_source_ttl_days",
        "working_graph_ttl_days",
    ):
        value = settings.get(key)
        if value is None:
            continue
        try:
            ttl = int(value)
        except (TypeError, ValueError):
            continue
        if ttl >= 0:
            return ttl
    return None


def _card_artifact_type(row: sqlite3.Row) -> str:
    try:
        raw_value = row["card_type"]
    except (IndexError, KeyError):
        raw_value = "normal"
    raw = str(raw_value or "normal").lower()
    if raw == "test":
        return "test"
    if raw == "bug":
        return "bug"
    return "task"


def _row_status(row: sqlite3.Row, status_col: str = "status") -> str:
    try:
        archived = row["archived"]
    except (IndexError, KeyError):
        archived = 0
    if bool(archived):
        return "archived"
    try:
        return str(row[status_col] or "")
    except (IndexError, KeyError):
        return ""


def _updated_at(row: sqlite3.Row) -> str:
    try:
        return _to_iso(row["updated_at"])
    except (IndexError, KeyError):
        return _to_iso(row["created_at"])


def _bug_has_minimal_evidence(row: sqlite3.Row) -> bool:
    if _card_artifact_type(row) != "bug":
        return True
    evidence_fields = ("observed_behavior", "expected_behavior", "steps_to_reproduce")
    has_text = False
    for field in evidence_fields:
        try:
            if str(row[field] or "").strip():
                has_text = True
                break
        except (IndexError, KeyError):
            continue
    try:
        linked_tests = _load_json_array(row["linked_test_task_ids"])
    except (IndexError, KeyError):
        linked_tests = []
    try:
        conclusions = _load_json_array(row["conclusions"])
    except (IndexError, KeyError):
        conclusions = []
    return has_text and (bool(linked_tests) or bool(conclusions))


def _load_json_array(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _decision_id(decision: dict[str, Any], index: int) -> str:
    explicit = decision.get("id")
    if explicit:
        return str(explicit)
    fingerprint = _canonical_payload_hash(decision)[:12]
    return f"idx-{index}-{fingerprint}"


def _decision_sources_from_spec(row: sqlite3.Row) -> list[dict[str, Any]]:
    try:
        raw_decisions = row["decisions"]
    except (IndexError, KeyError):
        return []

    decisions = _load_json_array(raw_decisions)
    if not decisions:
        return []

    spec_id = str(row["id"])
    try:
        spec_version = row["version"]
    except (IndexError, KeyError):
        spec_version = 1
    source_version = str(spec_version if spec_version is not None else 1)
    try:
        spec_title = row["title"]
    except (IndexError, KeyError):
        spec_title = ""
    try:
        created_at = row["created_at"]
    except (IndexError, KeyError):
        created_at = ""

    out: list[dict[str, Any]] = []
    for index, raw in enumerate(decisions):
        if not isinstance(raw, dict):
            continue
        status = str(raw.get("status", "active") or "active").lower()
        if status != "active":
            continue
        title = str(raw.get("title") or "").strip()
        rationale = str(raw.get("rationale") or raw.get("description") or "").strip()
        if not title and not rationale:
            continue
        local_id = _decision_id(raw, index)
        payload = {
            "parent_type": "spec",
            "parent_id": spec_id,
            "parent_title": spec_title,
            "decision": raw,
        }
        out.append({
            "artifact_type": "decision",
            "id": f"{spec_id}:{local_id}",
            "source_ref": f"decision:{spec_id}:{local_id}",
            "source_version": source_version,
            "content_hash": _canonical_payload_hash(payload),
            "created_at": _to_iso(created_at),
        })
    return out


@dataclass(frozen=True, slots=True)
class BoardSourceStore:
    """File-backed source store reading directly from the pulse SQLite.

    Wire as the ``source_store`` callable of ``RebuildSourceEnumerator``::

        store = BoardSourceStore(db_path=Path("~/.okto-pulse/data/pulse.db"))
        enumerator = RebuildSourceEnumerator(source_store=store.fetch)

    Returns ``list[dict]`` (not list[RebuildSourceRow]) because the
    enumerator coerces dicts internally and accepting dicts keeps the
    test seam wide.
    """

    db_path: Path

    def fetch(self, board_id: str) -> list[dict[str, Any]]:
        """Return raw source rows for a board.

        Best-effort: if a table doesn't exist (e.g. the schema is older
        than the queries here), the entry is skipped silently with a
        warning so the rebuild can still proceed with partial sources.
        """

        if not self.db_path.exists():
            logger.warning(
                "kg.board_source_store.db_missing path=%s — returning empty",
                self.db_path,
            )
            return []

        out: list[dict[str, Any]] = []
        conn = sqlite3.connect(
            f"file:{self.db_path}?mode=ro&immutable=0",
            uri=True,
            timeout=5.0,
        )
        conn.row_factory = sqlite3.Row
        try:
            working_ttl_days = _board_working_ttl_days(conn, board_id)
            for artifact_type, table, status_col, content_cols in ARTIFACT_QUERIES:
                # Skip table if the schema doesn't have it yet.
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                if not exists:
                    logger.warning(
                        "kg.board_source_store.table_missing table=%s — skipped",
                        table,
                    )
                    continue
                # Stable ordering: created_at first (lex on ISO date),
                # then id for tie-breaking. Do not filter by status here;
                # maturity/cancelled policy belongs to RebuildSourceEnumerator.
                rows = conn.execute(
                    f"SELECT * FROM {table} "
                    f"WHERE board_id = ? "
                    f"ORDER BY created_at ASC, id ASC",
                    (board_id,),
                ).fetchall()
                for row in rows:
                    row_id = str(row["id"])
                    try:
                        version_raw = row["version"]
                    except (IndexError, KeyError):
                        version_raw = 1
                    source_version = str(version_raw if version_raw is not None else 1)
                    content_hash = _canonical_content_hash(row, content_cols)
                    source_row = {
                        "artifact_type": artifact_type,
                        "id": row_id,
                        "source_ref": f"{artifact_type}:{row_id}",
                        "source_version": source_version,
                        "content_hash": content_hash,
                        "created_at": _to_iso(row["created_at"]),
                        "updated_at": _updated_at(row),
                        "status": _row_status(row, status_col),
                        "source_artifact_status": _row_status(row, status_col),
                        "has_minimal_evidence": True,
                    }
                    if artifact_type == "spec":
                        # content_hash above is the manifest-v2 hash (incl.
                        # IR/OR). Also carry the v1-compatible hash + version so
                        # the rebuild pipeline can PROVE schema-rebaseline vs
                        # real content drift (card 5ec8c75c, dec_c8e418e7).
                        source_row["content_hash_v1"] = _canonical_content_hash(
                            row, SPEC_CONTENT_COLUMNS_V1
                        )
                        source_row["source_manifest_version"] = (
                            SPEC_SOURCE_MANIFEST_VERSION
                        )
                    if working_ttl_days is not None:
                        source_row["working_ttl_days"] = working_ttl_days
                    out.append(source_row)
                    if artifact_type == "spec":
                        out.extend(_decision_sources_from_spec(row))
            cards_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='cards'"
            ).fetchone()
            if not cards_exists:
                logger.warning(
                    "kg.board_source_store.table_missing table=cards — skipped",
                )
            else:
                rows = conn.execute(
                    "SELECT * FROM cards "
                    "WHERE board_id = ? "
                    "ORDER BY created_at ASC, id ASC",
                    (board_id,),
                ).fetchall()
                for row in rows:
                    row_id = str(row["id"])
                    artifact_type = _card_artifact_type(row)
                    content_hash = _canonical_content_hash(
                        row,
                        CARD_CONTENT_COLUMNS,
                    )
                    source_row = {
                        "artifact_type": artifact_type,
                        "id": row_id,
                        "source_ref": f"{artifact_type}:{row_id}",
                        "source_version": "1",
                        "content_hash": content_hash,
                        "created_at": _to_iso(row["created_at"]),
                        "updated_at": _updated_at(row),
                        "status": _row_status(row),
                        "source_artifact_status": _row_status(row),
                        "has_minimal_evidence": _bug_has_minimal_evidence(row),
                    }
                    if working_ttl_days is not None:
                        source_row["working_ttl_days"] = working_ttl_days
                    out.append(source_row)
            # Path B amendment revisions (spec 7ea1e4be). Existence-guarded so
            # boards on an older schema (no table) emit zero amendment sources —
            # backward compatibility (TR4): absence is silent, never fatal.
            amendments_exists = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name='amendment_hotfix_revisions'"
            ).fetchone()
            if not amendments_exists:
                # Silent on legacy boards: the Path B table is absent on every
                # board created before this feature, so a warning would be pure
                # operational noise. debug-only (TR4 backward compatibility).
                logger.debug(
                    "kg.board_source_store.table_missing "
                    "table=amendment_hotfix_revisions — skipped (no Path B sources)",
                )
            else:
                rows = conn.execute(
                    "SELECT * FROM amendment_hotfix_revisions "
                    "WHERE board_id = ? "
                    "ORDER BY created_at ASC, id ASC",
                    (board_id,),
                ).fetchall()
                for row in rows:
                    row_id = str(row["id"])
                    content_hash = _canonical_content_hash(
                        row, AMENDMENT_CONTENT_COLUMNS
                    )
                    try:
                        lineage_raw = row["lineage_state"]
                    except (IndexError, KeyError):
                        lineage_raw = None
                    source_row = {
                        "artifact_type": "amendment_hotfix_revision",
                        "id": row_id,
                        "source_ref": f"amendment_hotfix_revision:{row_id}",
                        "source_version": "1",
                        "content_hash": content_hash,
                        "created_at": _to_iso(row["created_at"]),
                        "updated_at": _updated_at(row),
                        "status": _row_status(row, "status"),
                        "source_artifact_status": _row_status(row, "status"),
                        # Path B: canonical only at done AND complete lineage.
                        "lineage_complete": str(lineage_raw or "").strip().lower()
                        == "complete",
                    }
                    if working_ttl_days is not None:
                        source_row["working_ttl_days"] = working_ttl_days
                    out.append(source_row)
        finally:
            conn.close()
        return out


__all__ = [
    "BoardSourceStore",
    "ARTIFACT_QUERIES",
    "CARD_CONTENT_COLUMNS",
    "SPEC_CONTENT_COLUMNS_V1",
    "SPEC_CONTENT_COLUMNS_V2",
    "SPEC_SOURCE_MANIFEST_VERSION",
]

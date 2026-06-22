"""Legacy-board hardening for the ``graph_layer``/``maturity_status`` schema.

Spec eaf185c9 — FR6 / TR6 / AC6 (card 81a96a49), observability requirement
``or_1f52d4fd`` (metric ``kg_rebuild_schema_layer_migration_failure``).

A board graph created before the v0.3.0 maturity columns landed has no
``graph_layer`` (and possibly no ``maturity_status``) property. When a
rebuild/reprocess runs a query or commit that reads/writes those columns the
embedded graph engine raises a binder error such as
``Cannot find property graph_layer for n``. Left unhandled that raw string is
the ONLY diagnostic that reaches the dead-letter queue — opaque and not
actionable.

This module gives the rebuild/reprocess pipeline a single, behavioral
chokepoint:

* :func:`is_graph_layer_schema_error` — recognise the "missing layer column"
  signature (and ONLY that — a duplicate-column / already-present error is not
  a missing-schema error).
* :func:`ensure_graph_layer_schema` — attempt the idempotent schema
  migration + backfill (the same public surface the operator is told to run)
  and report whether the columns were actually added (``migrated``), were
  already present (``already_present``), or could not be repaired
  (``migration_failed``). Emits one bounded counter sample per attempt.
* :func:`build_structured_schema_layer_error` — turn the raw binder error into
  a structured, actionable diagnostic that names the operational action, so a
  board that genuinely cannot be migrated dead-letters an actionable message
  instead of the raw ``Cannot find property`` string.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# The two maturity columns a legacy graph may be missing.
SCHEMA_LAYER_COLUMNS: tuple[str, ...] = ("graph_layer", "maturity_status")

# Tokens an engine binder error uses when a property/column is absent. We
# require one of these AND a layer column name to be present, so a generic
# "property" mention never trips the guard on its own.
_MISSING_PROPERTY_TOKENS: tuple[str, ...] = (
    "cannot find property",
    "does not exist",
    "no such property",
    "unknown property",
    "not found",
    "undefined property",
    "invalid property",
)

# Tokens that mean the column is ALREADY there (benign idempotent ADD). If the
# error is one of these it is NOT a missing-schema error even though it names a
# layer column — mirrors schema._is_duplicate_column_error.
_ALREADY_PRESENT_TOKENS: tuple[str, ...] = (
    "already exists",
    "already has property",
    "duplicate",
)


class SchemaLayerOutcome:
    """Bounded ``outcome`` label values for ``kg_rebuild_schema_layer_migration_failure``."""

    MIGRATED = "migrated"
    ALREADY_PRESENT = "already_present"
    MIGRATION_FAILED = "migration_failed"


@dataclass(frozen=True)
class SchemaLayerRemediation:
    """Result of attempting to repair the missing maturity schema on a board."""

    board_id: str
    outcome: str
    columns_added: dict[str, list[str]] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    structured_message: str | None = None

    @property
    def recovered(self) -> bool:
        """True only when this attempt actually ADDED the missing columns, so a
        retry of the failed operation can now succeed. ``already_present`` is
        NOT a recovery (the original error has a different root cause) and
        ``migration_failed`` is not either."""
        return self.outcome == SchemaLayerOutcome.MIGRATED

    @property
    def needs_structured_error(self) -> bool:
        """True when the schema could not be repaired and the caller should
        dead-letter the structured diagnostic instead of the raw error."""
        return self.outcome == SchemaLayerOutcome.MIGRATION_FAILED


def is_graph_layer_schema_error(error: Any) -> bool:
    """Recognise a "missing graph_layer/maturity_status column" engine error.

    Matches when the message names one of the maturity columns together with a
    missing-property token, and is NOT a benign already-present/duplicate ADD.
    """
    text = str(error).lower()
    if not any(col in text for col in SCHEMA_LAYER_COLUMNS):
        return False
    if any(tok in text for tok in _ALREADY_PRESENT_TOKENS):
        return False
    return any(tok in text for tok in _MISSING_PROPERTY_TOKENS)


def _layer_columns_were_added(columns_added: dict[str, list[str]]) -> bool:
    """True when the migration's ``columns_added`` map records a freshly-added
    ``graph_layer`` or ``maturity_status`` column for any node type."""
    for cols in columns_added.values():
        for col in cols:
            if any(layer in str(col) for layer in SCHEMA_LAYER_COLUMNS):
                return True
    return False


def build_structured_schema_layer_error(
    board_id: str,
    *,
    raw_error: Any | None = None,
    migration_errors: list[str] | None = None,
) -> str:
    """Build the actionable diagnostic for a board whose maturity schema could
    not be migrated automatically.

    The message names the operational action (the migrate-schema tripleta) so a
    dead-letter row is self-explanatory instead of leaking the raw
    ``Cannot find property graph_layer for n``.
    """
    action = (
        f"run MCP `okto_pulse_kg_migrate_schema --board {board_id}` "
        f"(CLI: `python -m okto_pulse.tools.kg_migrate_schema --board {board_id}`) "
        "to add the graph_layer/maturity_status columns and backfill defaults, "
        "then retry the rebuild/reprocess. Do NOT delete the graph store."
    )
    parts = [
        f"kg_rebuild_schema_layer_migration_failure board={board_id}: the KG "
        "graph is missing the graph_layer/maturity_status schema required for "
        "rebuild/reprocess and automatic migration did not resolve it. "
        f"Operational action: {action}"
    ]
    if migration_errors:
        joined = "; ".join(str(e) for e in migration_errors)[:240]
        parts.append(f"migration_errors=[{joined}]")
    if raw_error is not None:
        parts.append(f"underlying_error={str(raw_error)[:200]}")
    return " | ".join(parts)


def ensure_graph_layer_schema(
    board_id: str,
    *,
    raw_error: Any | None = None,
) -> SchemaLayerRemediation:
    """Attempt to repair a board missing the graph_layer/maturity_status schema.

    Runs the idempotent :func:`migrate_schema_for_board` (ALTER ADD +
    ``_backfill_kg_layer_defaults``) and classifies the result:

    * ``migrated`` — the maturity columns were added now; the failed
      query/commit can be retried and should succeed.
    * ``already_present`` — migration ran clean but the columns already
      existed; the original error has a different root cause, so the caller
      should let normal retry/backoff handle it (no structured override).
    * ``migration_failed`` — the migration reported errors; the caller should
      dead-letter the returned ``structured_message`` instead of the raw error.

    Emits exactly one ``kg_rebuild_schema_layer_migration_failure`` sample per
    call (or_1f52d4fd), labelled by outcome.
    """
    # Lazy import: schema.py is a low-level module and importing it at module
    # load time would risk an import cycle through the worker stack.
    from okto_pulse.core.kg.schema import migrate_schema_for_board

    try:
        result = migrate_schema_for_board(board_id)
    except Exception as exc:  # pragma: no cover - migrate is itself defensive
        message = build_structured_schema_layer_error(
            board_id, raw_error=raw_error, migration_errors=[f"{type(exc).__name__}: {exc}"]
        )
        _emit_schema_layer_sample(
            board_id=board_id, outcome=SchemaLayerOutcome.MIGRATION_FAILED
        )
        logger.warning(
            "kg.schema_layer_guard.migration_raised board=%s err=%s",
            board_id, exc,
            extra={"event": "kg.schema_layer_guard.migration_failed",
                   "board_id": board_id, "error": str(exc)},
        )
        return SchemaLayerRemediation(
            board_id=board_id,
            outcome=SchemaLayerOutcome.MIGRATION_FAILED,
            errors=[str(exc)],
            structured_message=message,
        )

    columns_added = dict(result.get("columns_added") or {})
    errors = list(result.get("errors") or [])
    migrated_ok = bool(result.get("migrated")) and not errors

    if migrated_ok and _layer_columns_were_added(columns_added):
        outcome = SchemaLayerOutcome.MIGRATED
        _emit_schema_layer_sample(board_id=board_id, outcome=outcome)
        logger.info(
            "kg.schema_layer_guard.migrated board=%s columns_added=%s",
            board_id, columns_added,
            extra={"event": "kg.schema_layer_guard.migrated",
                   "board_id": board_id},
        )
        return SchemaLayerRemediation(
            board_id=board_id,
            outcome=outcome,
            columns_added=columns_added,
            errors=errors,
        )

    if migrated_ok:
        # Migration ran clean but the maturity columns were already present —
        # the failing query/commit is not actually a missing-column problem.
        outcome = SchemaLayerOutcome.ALREADY_PRESENT
        _emit_schema_layer_sample(board_id=board_id, outcome=outcome)
        logger.info(
            "kg.schema_layer_guard.already_present board=%s",
            board_id,
            extra={"event": "kg.schema_layer_guard.already_present",
                   "board_id": board_id},
        )
        return SchemaLayerRemediation(
            board_id=board_id,
            outcome=outcome,
            columns_added=columns_added,
            errors=errors,
        )

    # Migration reported errors — produce an actionable diagnostic.
    outcome = SchemaLayerOutcome.MIGRATION_FAILED
    message = build_structured_schema_layer_error(
        board_id, raw_error=raw_error, migration_errors=errors
    )
    _emit_schema_layer_sample(board_id=board_id, outcome=outcome)
    logger.warning(
        "kg.schema_layer_guard.migration_failed board=%s errors=%s",
        board_id, errors,
        extra={"event": "kg.schema_layer_guard.migration_failed",
               "board_id": board_id},
    )
    return SchemaLayerRemediation(
        board_id=board_id,
        outcome=outcome,
        columns_added=columns_added,
        errors=errors,
        structured_message=message,
    )


# ---------------------------------------------------------------------------
# Counter for OR or_1f52d4fd — kg_rebuild_schema_layer_migration_failure
# ---------------------------------------------------------------------------
#
# Contract (or_1f52d4fd): one bounded sample per schema-layer remediation
# attempt during rebuild/reprocess. Labels are (board_id, outcome) only —
# outcome distinguishes a handled migration (``migrated``/``already_present``)
# from an UNHANDLED one (``migration_failed``). The threshold is "0 unhandled
# occurrences after migration/backfill on a legacy test board", i.e. the count
# of outcome=migration_failed must be 0 once the board has been migrated.

_SCHEMA_LAYER_LABELS = ("board_id", "outcome")
_schema_layer_samples: list[dict[str, Any]] = []
_schema_layer_lock = threading.Lock()


def _emit_schema_layer_sample(*, board_id: str, outcome: str) -> None:
    """Emit one sample on ``kg_rebuild_schema_layer_migration_failure``."""
    with _schema_layer_lock:
        _schema_layer_samples.append({"board_id": board_id, "outcome": outcome})


def get_schema_layer_migration_event_count(
    *,
    board_id: str | None = None,
    outcome: str | None = None,
) -> int:
    with _schema_layer_lock:
        return sum(
            1
            for sample in _schema_layer_samples
            if (board_id is None or sample["board_id"] == board_id)
            and (outcome is None or sample["outcome"] == outcome)
        )


def get_schema_layer_migration_counter_labels() -> tuple[str, ...]:
    return _SCHEMA_LAYER_LABELS


def get_schema_layer_migration_samples() -> list[dict[str, Any]]:
    with _schema_layer_lock:
        return [dict(sample) for sample in _schema_layer_samples]


def reset_schema_layer_migration_counter() -> None:
    with _schema_layer_lock:
        _schema_layer_samples.clear()


__all__ = [
    "SCHEMA_LAYER_COLUMNS",
    "SchemaLayerOutcome",
    "SchemaLayerRemediation",
    "is_graph_layer_schema_error",
    "ensure_graph_layer_schema",
    "build_structured_schema_layer_error",
    "get_schema_layer_migration_event_count",
    "get_schema_layer_migration_counter_labels",
    "get_schema_layer_migration_samples",
    "reset_schema_layer_migration_counter",
]

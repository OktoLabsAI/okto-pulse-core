"""RKG-05 — canonical, NON-MASKABLE KG health/readiness projection (api_1feb6875).

The single source the health/readiness/MCP/UI/report surfaces share
(tr_1e460a5d). The technical KG signals (technical_dlq, dead_letter_backlog,
canonical_debt_open, persistence_error) are exposed CONSISTENTLY — scalar counters
in ``technical_signals`` + per-item drill-down in ``non_maskable_items`` — in BOTH
the summary and full profiles (fr_3fbb564c / OR or_36e0cd85). active_queue,
dead_letter and canonical_debt stay SEPARATE domains; one count is never inferred
from another (tr_22d4434d). ``readiness`` keeps ``blocking`` (a technical problem
IS visible) and ``would_block_done`` (whether the gate would actually block,
enforcement-aware) DISTINCT (fr_b3e1fd1b / br_c4ef8e0a). The signals are derived
straight from health, not from any cognitive verdict, so a skip/no_action can
never hide an open technical signal (fr_85ff49df / br_aeb8f119).
"""

from __future__ import annotations

from typing import Any


from okto_pulse.core.kg.rebuild_audit import emit_cognitive_technical_signal_sample
from okto_pulse.core.ports.scheduler import SchedulerControl

_DLQ_TOOL = "okto_pulse_kg_dead_letter_list"
_DEBT_TOOL = "okto_pulse_kg_canonical_debt_list"
_HEALTH_TOOL = "okto_pulse_kg_health"

VALID_PROFILES = ("summary", "full", "legacy")


class InvalidProfileError(ValueError):
    """Raised when an unknown profile is requested (maps to invalid_profile/400)."""


def _is_full(profile: str) -> bool:
    return profile in ("full", "legacy")


def _persistence_present(health: dict) -> tuple[bool, str | None]:
    categories = (health.get("root_cause") or {}).get("categories") or {}
    wal = categories.get("wal_or_commit_errors") or {}
    drain = categories.get("safe_write_drain_failure") or {}
    present = bool(wal.get("present") or drain.get("present"))
    return present, (wal.get("error") or drain.get("error"))


def build_technical_signal_counters(health: dict) -> dict[str, int]:
    """The 4 scalar counters — SEPARATE operational domains (tr_22d4434d).
    ``active_queue_count`` is NOT inferred from ``dead_letter_count``."""
    domains = health.get("operational_domains") or {}
    dlq = int(health.get("dead_letter_count") or 0)
    cdebt_open = int((health.get("canonical_debt") or {}).get("open_count") or 0)
    active_queue = int((domains.get("active_queue") or {}).get("count") or 0)
    return {
        "dead_letter_count": dlq,
        "technical_dlq_count": dlq,
        "canonical_debt_open_count": cdebt_open,
        "active_queue_count": active_queue,
    }


async def _non_maskable_items(
    db: object, board_id: str, health: dict, *, artifact_ref: str | None,
) -> list[dict[str, Any]]:
    """Per-item drill-down for every OPEN technical signal — derived from the DLQ,
    the open canonical debt and the persistence root-cause (never from a cognitive
    verdict, so skip/no_action cannot drop them)."""
    from okto_pulse.core.services.canonical_debt_service import (
        OPEN_STATES,
        list_canonical_debt,
    )
    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    items: list[dict[str, Any]] = []

    dlq = await list_dead_letter_rows(db, board_id, limit=200)
    for row in dlq.get("rows", []):
        ref = f"{row.get('artifact_type')}:{row.get('artifact_id')}"
        items.append({
            "artifact_ref": ref,
            "source_ref": ref,
            "signal": "technical_dlq",
            "last_error": row.get("last_error"),
            "error_text": row.get("error_text"),
            "next_action": row.get("next_action"),
            "remediation": "diagnose then reprocess via "
                           "okto_pulse_kg_connectivity_dlq_reprocess / "
                           "okto_pulse_kg_dead_letter_reprocess after the root cause is fixed",
            "drill_down_tool": _DLQ_TOOL,
        })

    debt = await list_canonical_debt(db, board_id=board_id, limit=200)
    for row in getattr(debt, "items", []):
        if (row.get("canonical_state") or "") not in OPEN_STATES:
            continue
        ref = row.get("source_ref") or f"{row.get('artifact_type')}:{row.get('artifact_id')}"
        items.append({
            "artifact_ref": ref,
            "source_ref": row.get("source_ref") or ref,
            "signal": "canonical_debt_open",
            "last_error": row.get("last_error") or row.get("failure_reason"),
            "error_text": row.get("last_error") or row.get("failure_reason"),
            "next_action": row.get("next_action"),
            "remediation": "reconcile/retry the canonical debt for this artifact",
            "drill_down_tool": _DEBT_TOOL,
        })

    present, perr = _persistence_present(health)
    if present:
        items.append({
            "artifact_ref": f"board:{board_id}",
            "source_ref": f"board:{board_id}",
            "signal": "persistence_error",
            "last_error": perr,
            "error_text": perr,
            "next_action": "inspect root_cause then recover the board graph",
            "remediation": "okto_pulse_kg_rebuild_preflight / WAL recovery",
            "drill_down_tool": _HEALTH_TOOL,
        })

    if artifact_ref:
        items = [
            it for it in items
            if artifact_ref in (it["artifact_ref"], it["source_ref"])
        ]
    return items


async def _enforcement_active(db: object, board_id: str) -> bool:
    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory
    from okto_pulse.core.services.main import _cognitive_readiness_blocking_active

    # R01C IMP3 drain: resolve the board via the edition-owned repository port
    # (the R01B FR3 ``resolve_unit_of_work_factory().wrap`` seam) instead of the ORM
    # import. Pure existence get-by-id (no owner/permission predicate); the
    # ``board is not None`` guard is preserved — ``boards.get`` returns None for a
    # missing board, identical to ``db.get(Board, board_id)``.
    board = await resolve_unit_of_work_factory().wrap(db).boards.get(board_id)
    return bool(_cognitive_readiness_blocking_active(board)) if board is not None else False


async def build_health_readiness(
    board_id: str,
    db: object,
    *,
    profile: str = "summary",
    surface: str = "rest",
    artifact_ref: str | None = None,
    scheduler_control: SchedulerControl | None = None,
) -> dict[str, Any]:
    """api_1feb6875: the canonical health/readiness projection.

    NON-MASKABLE in BOTH summary and full: ``technical_signals`` (scalar counters),
    ``non_maskable_items`` (per-item drill-down), ``readiness`` (blocking vs
    would_block_done) and the top-level ``cognitive_enforcement_mode`` /
    ``enforcement_active``. The full profile only ADDS the prose ``health_issues``
    + ``root_cause``. Raises ``InvalidProfileError`` on an unknown profile."""
    if profile not in VALID_PROFILES:
        raise InvalidProfileError(f"invalid_profile: {profile}")

    from okto_pulse.core.services.kg_health_service import get_kg_health

    health = await get_kg_health(
        board_id,
        db,
        scheduler_control=scheduler_control,
    )
    counters = build_technical_signal_counters(health)
    items = await _non_maskable_items(db, board_id, health, artifact_ref=artifact_ref)

    present, _ = _persistence_present(health)
    blocking = bool(
        counters["technical_dlq_count"] > 0
        or counters["canonical_debt_open_count"] > 0
        or present
    )
    enforcement_active = await _enforcement_active(db, board_id)
    would_block_done = blocking and enforcement_active
    mode = "blocking" if enforcement_active else "advisory"
    reasons = sorted({it["signal"] for it in items})

    if would_block_done:
        policy_reason = (
            "open technical signal + enforcement_active=true → the gate blocks done")
    elif blocking:
        policy_reason = (
            "open technical signal but enforcement_active=false (advisory) → "
            "would_block_done=false; the artifact is NOT ready while the blocker is open")
    else:
        policy_reason = "no open technical signal"

    result: dict[str, Any] = {
        "board_id": board_id,
        "profile": "full" if _is_full(profile) else "summary",
        "overall_state": health.get("overall_state"),
        # top-level enforcement policy (fr_b3e1fd1b)
        "cognitive_enforcement_mode": mode,
        "enforcement_active": enforcement_active,
        # non-maskable in BOTH profiles
        "technical_signals": counters,
        "readiness": {
            "blocking": blocking,
            "would_block_done": would_block_done,
            "reasons": reasons,
            "policy_reason": policy_reason,
        },
        "non_maskable_items": items,
        # domain separation, additive (tr_22d4434d)
        "operational_domains": health.get("operational_domains"),
    }
    if _is_full(profile):
        result["health_issues"] = health.get("health_issues")
        result["root_cause"] = health.get("root_cause")

    # OR or_36e0cd85: one bounded sample per surfaced open technical signal.
    for signal in (reasons or (["persistence_error"] if present and not reasons else [])):
        emit_cognitive_technical_signal_sample(
            signal=signal, surface=surface, blocking=True,
            would_block_done=would_block_done, board_id=board_id)
    return result

"""RKG-05 — canonical, NON-MASKABLE KG health/readiness projection (api_1feb6875).

The single source the health/readiness/MCP/UI/report surfaces share
(tr_1e460a5d). The technical KG signals (technical_dlq, dead_letter_backlog,
canonical_debt_open, persistence_error) are exposed CONSISTENTLY — scalar counters
in ``technical_signals`` + bounded domain aggregates in ``non_maskable_items`` — in BOTH
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

_POLICY_PROJECTION_DLQ_SIGNAL = "policy_constraint_projection_dlq"

VALID_PROFILES = ("summary", "full", "legacy")


class InvalidProfileError(ValueError):
    """Raised when an unknown profile is requested (maps to invalid_profile/400)."""


def _is_full(profile: str) -> bool:
    return profile in ("full", "legacy")


def _persistence_present(health: dict) -> tuple[bool, str | None]:
    root_cause = health.get("root_cause") or {}
    if root_cause.get("scope", "graph") != "graph":
        return False, None
    categories = root_cause.get("categories") or {}
    wal = categories.get("wal_or_commit_errors") or {}
    drain = categories.get("safe_write_drain_failure") or {}
    present = bool(wal.get("present") or drain.get("present"))
    return present, (wal.get("error") or drain.get("error"))


def _canonical_debt_count(health: dict) -> int | None:
    summary = health.get("canonical_debt")
    if not isinstance(summary, dict) or summary.get("status") not in (None, "available", "ok"):
        return None
    value = summary.get("open_count")
    return value if type(value) is int and value >= 0 else None


def build_technical_signal_counters(health: dict) -> dict[str, int | None]:
    """Scalar counters with terminal queue domains kept explicitly separate.

    ``active_queue_count`` is NOT inferred from ``dead_letter_count``."""
    domains = health.get("operational_domains") or {}
    dlq = int(health.get("dead_letter_count") or 0)
    global_outbox_dlq = int(
        (
            domains.get("global_outbox_dead_letter")
            or {}
        ).get("count")
        or health.get("global_outbox_dead_letter_count")
        or 0
    )
    cdebt_open = _canonical_debt_count(health)
    active_queue = int((domains.get("active_queue") or {}).get("count") or 0)
    policy_projection = domains.get("policy_constraint_projection") or {}
    return {
        # Established values retain their exact meanings.  Policy
        # projection delivery has dedicated counters and is never folded into
        # consolidation/global-outbox DLQ or active queue.
        "dead_letter_count": dlq,
        "global_outbox_dead_letter_count": global_outbox_dlq,
        "technical_dlq_count": dlq + global_outbox_dlq,
        "canonical_debt_open_count": cdebt_open,
        "active_queue_count": active_queue,
        "policy_constraint_projection_pending_count": int(
            policy_projection.get("pending_count") or 0
        ),
        "policy_constraint_projection_processing_count": int(
            policy_projection.get("processing_count") or 0
        ),
        "policy_constraint_projection_retry_scheduled_count": int(
            policy_projection.get("retry_scheduled_count") or 0
        ),
        "policy_constraint_projection_dlq_count": int(
            policy_projection.get("dlq_count") or 0
        ),
        "policy_constraint_projection_max_attempt_count": int(
            policy_projection.get("max_attempt_count") or 0
        ),
    }


def _non_maskable_items(board_id: str, health: dict) -> list[dict[str, Any]]:
    """At most one aggregate per technical domain, from the same health snapshot.

    Never enumerate DLQ/debt records or expose their payloads, errors or IDs.
    Board-scoped signals cannot be hidden by an artifact filter or a cognitive
    verdict. The counters and enforcement policy remain the source of readiness.
    """
    counters = build_technical_signal_counters(health)
    domains = (
        ("technical_dlq", "dead_letter_count", "Affected graph delivery is unavailable; technical gates remain enforced."),
        ("global_outbox_dead_letter", "global_outbox_dead_letter_count", "Affected global discovery delivery is unavailable; technical gates remain enforced."),
        ("canonical_debt_open", "canonical_debt_open_count", "Canonical projection is pending automatic recovery."),
        (_POLICY_PROJECTION_DLQ_SIGNAL, "policy_constraint_projection_dlq_count", "Policy constraint projection delivery is unavailable."),
    )
    items = [
        {
            "artifact_ref": f"board:{board_id}",
            "source_ref": f"board:{board_id}",
            "signal": signal,
            "count": counters[counter],
            "next_action": "none",
            "limitation": limitation,
            "drill_down_tool": None,
        }
        for signal, counter, limitation in domains
        if counters[counter] is not None and counters[counter] > 0
    ]
    present, _ = _persistence_present(health)
    if present:
        items.append({
            "artifact_ref": f"board:{board_id}",
            "source_ref": f"board:{board_id}",
            "signal": "persistence_error",
            "next_action": "none",
            "limitation": "The affected graph operations are unavailable.",
            "drill_down_tool": None,
        })
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
    ``non_maskable_items`` (bounded Board aggregates), ``readiness`` (blocking vs
    would_block_done) and the top-level ``cognitive_enforcement_mode`` /
    ``enforcement_active``. The full profile only ADDS the prose ``health_issues``
    + ``root_cause``. Health 1.2 represents unavailable canonical debt as null;
    absent a known blocker, blocking is null and would_block_done is null under
    enforcement (false in advisory mode). This projection never runs or changes
    the authoritative completion gate. Raises ``InvalidProfileError`` on an
    unknown profile."""
    if profile not in VALID_PROFILES:
        raise InvalidProfileError(f"invalid_profile: {profile}")

    from okto_pulse.core.services.kg_health_service import get_kg_health

    health = await get_kg_health(
        board_id,
        db,
        scheduler_control=scheduler_control,
    )
    counters = build_technical_signal_counters(health)
    debt_unavailable = counters["canonical_debt_open_count"] is None
    # Deprecated compatibility input: artifact_ref no longer selects technical rows.
    # Infrastructure status is Board-scoped; semantic artifact queries remain separate.
    items = _non_maskable_items(board_id, health)

    present, _ = _persistence_present(health)
    blocking = bool(
        counters["technical_dlq_count"] > 0
        or counters["policy_constraint_projection_dlq_count"] > 0
        or (counters["canonical_debt_open_count"] or 0) > 0
        or present
    )
    enforcement_active = await _enforcement_active(db, board_id)
    # Observation is not the authoritative gate. Preserve a known blocker;
    # otherwise incomplete evidence is unknown, never an inferred passage.
    if debt_unavailable and not blocking:
        blocking = None
    would_block_done = blocking if enforcement_active else False
    mode = "blocking" if enforcement_active else "advisory"
    reasons_set = {it["signal"] for it in items}
    if counters["dead_letter_count"] > 0:
        reasons_set.add("technical_dlq")
    if counters["global_outbox_dead_letter_count"] > 0:
        reasons_set.add("global_outbox_dead_letter")
    if counters["policy_constraint_projection_dlq_count"] > 0:
        reasons_set.add(_POLICY_PROJECTION_DLQ_SIGNAL)
    if (counters["canonical_debt_open_count"] or 0) > 0:
        reasons_set.add("canonical_debt_open")
    if present:
        reasons_set.add("persistence_error")
    reasons = sorted(reasons_set)
    if debt_unavailable:
        reasons.append("canonical_debt_observation_unavailable")

    if would_block_done:
        policy_reason = (
            "open technical signal + enforcement_active=true → the gate blocks done")
    elif blocking:
        policy_reason = (
            "open technical signal but enforcement_active=false (advisory) → "
            "would_block_done=false; the artifact is NOT ready while the blocker is open")
    elif debt_unavailable:
        policy_reason = "canonical projection observation unavailable; readiness cannot be determined"
    else:
        policy_reason = "no open technical signal"

    result: dict[str, Any] = {
        "board_id": board_id,
        "health_schema_version": health.get("health_schema_version", "1.3"),
        "profile": "full" if _is_full(profile) else "summary",
        "overall_state": (
            "at_risk" if debt_unavailable and health.get("overall_state") == "healthy"
            else health.get("overall_state")
        ),
        # top-level enforcement policy (fr_b3e1fd1b)
        "cognitive_enforcement_mode": mode,
        "enforcement_active": enforcement_active,
        # non-maskable in BOTH profiles
        "technical_signals": counters,
        "readiness": {
            "canonical_debt_observation_status": "unavailable" if debt_unavailable else "available",
            "blocking": blocking,
            "would_block_done": would_block_done,
            "reasons": reasons,
            "policy_reason": policy_reason,
        },
        "non_maskable_items": items,
        # domain separation, additive (tr_22d4434d)
        "operational_domains": {
            **(health.get("operational_domains") or {}),
            "canonical_debt": {
                "domain": "canonical_debt",
                "semantics": "semantic_canonicality_pending",
                "count": counters["canonical_debt_open_count"],
                "status": "unavailable" if debt_unavailable else "available",
                "drill_down_tool": None,
            },
        },
    }
    if _is_full(profile):
        result["health_issues"] = health.get("health_issues")
        result["root_cause"] = health.get("root_cause")

    # OR or_36e0cd85: one bounded sample per surfaced open technical signal.
    for signal in (reasons or (["persistence_error"] if present and not reasons else [])):
        if signal == "canonical_debt_observation_unavailable":
            continue  # Unknown debt is not an observed open technical signal.
        emit_cognitive_technical_signal_sample(
            signal=signal, surface=surface, blocking=True,
            would_block_done=would_block_done, board_id=board_id)
    return result

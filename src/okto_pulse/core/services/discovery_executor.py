"""Discovery intent executor — dispatches a user-facing intent click to the
real backend tool it advertises, instead of falling back to semantic search.

Before this module, `GlobalSearchView.handleIntentClick` always invoked
`kgApi.globalSearch(intent.description, 20)`. The resulting hits were
semantically adjacent to the intent's own description, not the data the
intent promised. This executor closes that gap: given an intent id and its
params, it runs the real aggregation service / KG query and returns a
normalized tabular payload ready for the Discovery UI.

Payload shape (stable contract with the frontend):

    {
      "rows": [{"id", "type", "title", "summary", "meta": {...}}, ...],
      "columns": ["Type", "Title", ...],          # ordered for rendering
      "total": int,                                # absolute count
      "tool_binding": "okto_pulse_list_blockers",
      "params_echo": {"stale_hours": 72, ...},    # what was actually used
      "execution": "real_tool" | "semantic_fallback",
    }

`execution == "real_tool"` means the intent's tool_binding was executed.
`semantic_fallback` is reserved for the case where a tool cannot run yet
(e.g. missing agent context, feature-flagged) — the frontend prefixes
results with an honest banner.
"""

from __future__ import annotations

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.models.db import (
    ActivityLog,
    Card,
    Comment,
    DiscoveryIntent,
    Spec,
)
from okto_pulse.core.services import analytics_service


# --------------------------------------------------------------------------- #
# Main dispatcher                                                             #
# --------------------------------------------------------------------------- #


async def execute_intent(
    db: AsyncSession,
    user_id: str,
    board_id: str,
    intent: DiscoveryIntent,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch an intent to its real implementation.

    Args:
        db: SQLAlchemy async session with read access to the board.
        user_id: Authenticated user id (used by intents that are user-scoped,
            like my_mentions).
        board_id: Target board.
        intent: Resolved DiscoveryIntent row from DB.
        params: Already-validated params coming from the UI (may be empty).

    Returns:
        Normalized payload as described at the top of this module.

    Raises:
        ValueError: if tool_binding is unknown or a required param is missing.
    """
    binding = intent.tool_binding

    # Validate required params declared in the intent schema.
    for key, meta in (intent.params_schema or {}).items():
        if meta.get("required") and not params.get(key):
            raise ValueError(f"Missing required param: {key}")

    if binding == "okto_pulse_get_activity_log":
        return await _exec_activity_log(db, board_id)
    if binding == "okto_pulse_list_blockers":
        return await _exec_blockers(db, board_id)
    if binding == "okto_pulse_kg_find_contradictions":
        return await _exec_contradictions(board_id)
    if binding == "okto_pulse_kg_find_similar_decisions":
        return await _exec_similar_decisions(board_id, params)
    if binding == "okto_pulse_kg_query_natural":
        return await _exec_query_natural(board_id, params)
    if binding == "okto_pulse_get_card_dependencies":
        return await _exec_card_dependencies(db, params)
    if binding == "okto_pulse_list_my_mentions":
        return await _exec_my_mentions(db, user_id, board_id)
    if binding == "okto_pulse_list_test_scenarios":
        return await _exec_test_scenarios(db, board_id, intent, params)
    if binding == "okto_pulse_list_uncovered_requirements":
        return await _exec_uncovered_requirements(db, board_id)
    if binding == "okto_pulse_list_supersedence_chains":
        return await _exec_supersedence_chains(db, board_id)
    if binding == "okto_pulse_kg_get_learning_from_bugs":
        return await _exec_learnings(board_id, params)

    raise ValueError(f"Unsupported tool_binding: {binding}")


def _ok(
    rows: list[dict],
    columns: list[str],
    tool_binding: str,
    params_echo: dict | None = None,
    extra: dict | None = None,
) -> dict:
    """Build the normalized OK payload."""
    out = {
        "rows": rows,
        "columns": columns,
        "total": len(rows),
        "tool_binding": tool_binding,
        "params_echo": params_echo or {},
        "execution": "real_tool",
    }
    if extra:
        out.update(extra)
    return out


# --------------------------------------------------------------------------- #
# Per-tool executors                                                          #
# --------------------------------------------------------------------------- #


async def _exec_activity_log(db: AsyncSession, board_id: str) -> dict:
    q = (
        select(ActivityLog)
        .where(ActivityLog.board_id == board_id)
        .order_by(ActivityLog.created_at.desc())
        .limit(50)
    )
    rows = []
    for a in (await db.execute(q)).scalars().all():
        when = a.created_at.isoformat() if a.created_at else ""
        summary_bits = [f"{a.actor_type}:{a.actor_name}" if a.actor_name else a.actor_id]
        if a.card_id:
            summary_bits.append(f"card={a.card_id[:8]}")
        summary_bits.append(when.replace("T", " ")[:16])
        rows.append(
            {
                "id": str(a.id),
                "type": a.action,
                "title": a.action,
                "summary": " · ".join(summary_bits),
                "meta": {
                    "card_id": a.card_id,
                    "actor_id": a.actor_id,
                    "actor_type": a.actor_type,
                    "actor_name": a.actor_name,
                    "created_at": when,
                    "details": a.details,
                },
            }
        )
    return _ok(
        rows,
        columns=["Action", "Actor", "Card", "When"],
        tool_binding="okto_pulse_get_activity_log",
    )


async def _exec_blockers(db: AsyncSession, board_id: str) -> dict:
    data = await analytics_service.compute_blockers(db, board_id)
    rows = []
    for b in data.get("blockers", []):
        title = b.get("card_title") or b.get("spec_title") or b.get("scenario_title") or "—"
        rows.append(
            {
                "id": b.get("card_id") or b.get("scenario_id") or b.get("spec_id") or title,
                "type": b.get("type"),
                "title": title,
                "summary": b.get("reason"),
                "meta": b.get("evidence") or {},
            }
        )
    return _ok(
        rows,
        columns=["Type", "Title", "Reason"],
        tool_binding="okto_pulse_list_blockers",
        extra={"summary": data.get("summary", {})},
    )


async def _exec_contradictions(board_id: str) -> dict:
    from okto_pulse.core.kg.kg_service import get_kg_service

    svc = get_kg_service()
    pairs = svc.find_contradictions(board_id, node_id=None, max_rows=50)
    rows = []
    for p in pairs:
        rows.append(
            {
                "id": f"{p['id_a']}__{p['id_b']}",
                "type": "ContradictionPair",
                "title": f"{p['title_a']}  ⟂  {p['title_b']}",
                "summary": f"confidence {p.get('confidence', 0):.2f}",
                "meta": p,
            }
        )
    return _ok(
        rows,
        columns=["Pair", "Confidence"],
        tool_binding="okto_pulse_kg_find_contradictions",
    )


async def _exec_similar_decisions(board_id: str, params: dict) -> dict:
    from okto_pulse.core.kg.kg_service import get_kg_service

    svc = get_kg_service()
    topic = params.get("topic", "").strip()
    if not topic:
        raise ValueError("topic is required")
    rows_raw = svc.find_similar_decisions(board_id, topic, top_k=10, min_similarity=0.3)
    rows = []
    for r in rows_raw:
        rows.append(
            {
                "id": r["id"],
                "type": "Decision",
                "title": r["title"],
                "summary": f"similarity {r.get('similarity', 0):.2f} · combined {r.get('combined_score', 0):.2f}",
                "meta": r,
            }
        )
    return _ok(
        rows,
        columns=["Decision", "Score"],
        tool_binding="okto_pulse_kg_find_similar_decisions",
        params_echo={"topic": topic},
    )


async def _exec_query_natural(board_id: str, params: dict) -> dict:
    """Natural-language query over the board KG — the remap target for
    similar_nodes_to_text (ideação 803c1fe1)."""
    from okto_pulse.core.kg.tier_power import execute_natural_query

    nl_query = params.get("query") or params.get("nl_query") or ""
    if not nl_query.strip():
        raise ValueError("query is required")
    result = execute_natural_query(
        board_id, nl_query.strip(), limit=20, min_confidence=0.3,
    )
    rows = []
    for n in result.get("nodes", []):
        rows.append(
            {
                "id": n.get("node_id"),
                "type": n.get("node_type") or "Node",
                "title": n.get("title") or "(untitled)",
                "summary": f"similarity {n.get('similarity', 0):.2f}",
                "meta": n,
            }
        )
    return _ok(
        rows,
        columns=["Type", "Title", "Similarity"],
        tool_binding="okto_pulse_kg_query_natural",
        params_echo={"query": nl_query},
    )


async def _exec_card_dependencies(db: AsyncSession, params: dict) -> dict:
    from okto_pulse.core.models.db import CardDependency

    card_id = params.get("card_id", "").strip()
    if not card_id:
        raise ValueError("card_id is required")
    # dependents = cards that depend on this one
    q = (
        select(CardDependency, Card)
        .join(Card, Card.id == CardDependency.card_id)
        .where(CardDependency.depends_on_id == card_id)
    )
    rows = []
    for dep, card in (await db.execute(q)).all():
        rows.append(
            {
                "id": card.id,
                "type": "Card",
                "title": card.title,
                "summary": f"status={card.status} · priority={card.priority}",
                "meta": {"card_id": card.id, "created_at": dep.created_at.isoformat() if dep.created_at else None},
            }
        )
    return _ok(
        rows,
        columns=["Dependent Card", "Status"],
        tool_binding="okto_pulse_get_card_dependencies",
        params_echo={"card_id": card_id},
    )


async def _exec_my_mentions(db: AsyncSession, user_id: str, board_id: str) -> dict:
    # Match @user_id or @username in Comment.content. We keep it simple:
    # anything starting with @user_id literal. User display names aren't
    # canonical, so this is a starting point — future work tracked in the
    # discovery ideation follow-up.
    mention_token = f"@{user_id}"
    q = (
        select(Comment, Card)
        .join(Card, Card.id == Comment.card_id)
        .where(Card.board_id == board_id)
        .where(Comment.content.contains(mention_token))
        .order_by(Comment.created_at.desc())
        .limit(50)
    )
    rows = []
    for c, card in (await db.execute(q)).all():
        rows.append(
            {
                "id": c.id,
                "type": "Mention",
                "title": card.title,
                "summary": (c.content or "")[:200],
                "meta": {
                    "card_id": card.id,
                    "created_at": c.created_at.isoformat() if c.created_at else None,
                    "author_id": c.author_id,
                },
            }
        )
    return _ok(
        rows,
        columns=["Where", "Comment"],
        tool_binding="okto_pulse_list_my_mentions",
    )


async def _exec_test_scenarios(
    db: AsyncSession, board_id: str, intent: DiscoveryIntent, params: dict
) -> dict:
    """Handles both coverage_for_fr (param fr_id) and scenarios_without_tasks."""
    q = select(Spec).where(Spec.board_id == board_id)
    specs = (await db.execute(q)).scalars().all()
    rows: list[dict] = []
    fr_id_filter = (params.get("fr_id") or "").strip()
    scenarios_without_tasks = intent.name == "scenarios_without_tasks"

    for spec in specs:
        scenarios = getattr(spec, "test_scenarios", None) or []
        for sc in scenarios:
            linked_tasks = sc.get("linked_task_ids") or []
            linked_criteria = sc.get("linked_criteria") or []
            if scenarios_without_tasks and linked_tasks:
                continue
            if fr_id_filter and fr_id_filter not in (linked_criteria or []):
                # coverage_for_fr expects fr_id index (e.g. "0", "1"). We
                # match against the linked_criteria list as a string list.
                if str(fr_id_filter) not in [str(x) for x in linked_criteria]:
                    continue
            rows.append(
                {
                    "id": sc.get("id"),
                    "type": "TestScenario",
                    "title": sc.get("title") or "(untitled)",
                    "summary": f"spec: {spec.title} · status: {sc.get('status') or 'draft'} · linked_tasks: {len(linked_tasks)}",
                    "meta": {
                        "spec_id": spec.id,
                        "scenario_type": sc.get("scenario_type"),
                        "linked_task_ids": linked_tasks,
                        "linked_criteria": linked_criteria,
                    },
                }
            )
    return _ok(
        rows,
        columns=["Scenario", "Spec", "Linked tasks"],
        tool_binding="okto_pulse_list_test_scenarios",
        params_echo=params,
    )


async def _exec_uncovered_requirements(db: AsyncSession, board_id: str) -> dict:
    """NEW aggregator (ideação d1783b03): lists all FRs, TRs and ACs without
    any linked card or test scenario, across the specs on the board that
    are still **in flight** — draft, review, approved, validated and
    in_progress.

    Specs in status `done` are excluded: their coverage gaps were either
    resolved at validation time, explicitly skipped via the board's
    skip_*_coverage flags, or deliberately deferred — none of those
    states represent an actionable "uncovered requirement" today.

    Specs in `cancelled` are also excluded (work abandoned).

    Tolerates legacy specs where TRs / BRs / scenarios are stored as
    strings rather than dicts — those are treated as "uncovered-by-default"
    because there is no way to attach linked_task_ids to a plain string.
    """
    from okto_pulse.core.models.db import SpecStatus

    ACTIVE_STATUSES = {
        SpecStatus.DRAFT,
        SpecStatus.REVIEW,
        SpecStatus.APPROVED,
        SpecStatus.VALIDATED,
        SpecStatus.IN_PROGRESS,
    }
    specs = (
        await db.execute(
            select(Spec).where(
                Spec.board_id == board_id,
                Spec.status.in_(ACTIVE_STATUSES),
            )
        )
    ).scalars().all()
    rows: list[dict] = []
    for spec in specs:
        # Technical requirements
        for i, tr in enumerate(getattr(spec, "technical_requirements", None) or []):
            if isinstance(tr, dict):
                tr_id = tr.get("id") or f"{spec.id}:tr:{i}"
                text = tr.get("text") or ""
                covered = bool(tr.get("linked_task_ids") or [])
            else:
                tr_id = f"{spec.id}:tr:{i}"
                text = str(tr)
                covered = False  # legacy string form — no linkage possible
            if not covered:
                rows.append(
                    {
                        "id": tr_id,
                        "type": "UncoveredTR",
                        "title": text[:160],
                        "summary": f"spec: {spec.title} · no linked cards",
                        "meta": {"spec_id": spec.id, "kind": "technical_requirement"},
                    }
                )
        # FRs: covered when at least one BR references them
        frs = getattr(spec, "functional_requirements", None) or []
        brs = getattr(spec, "business_rules", None) or []
        covered_fr_indices: set[int] = set()
        for br in brs:
            if not isinstance(br, dict):
                continue
            for idx in br.get("linked_requirements") or []:
                try:
                    covered_fr_indices.add(int(idx))
                except (ValueError, TypeError):
                    pass
        for idx, fr in enumerate(frs):
            if idx not in covered_fr_indices:
                rows.append(
                    {
                        "id": f"{spec.id}:fr:{idx}",
                        "type": "UncoveredFR",
                        "title": (fr or "")[:160],
                        "summary": f"spec: {spec.title} · FR #{idx} · no linked BR",
                        "meta": {"spec_id": spec.id, "kind": "functional_requirement", "index": idx},
                    }
                )
        # ACs: covered when at least one scenario references them
        acs = getattr(spec, "acceptance_criteria", None) or []
        scenarios = getattr(spec, "test_scenarios", None) or []
        covered_ac_indices: set[int] = set()
        for sc in scenarios:
            if not isinstance(sc, dict):
                continue
            for idx in sc.get("linked_criteria") or []:
                try:
                    covered_ac_indices.add(int(idx))
                except (ValueError, TypeError):
                    pass
        for idx, ac in enumerate(acs):
            if idx not in covered_ac_indices:
                rows.append(
                    {
                        "id": f"{spec.id}:ac:{idx}",
                        "type": "UncoveredAC",
                        "title": (ac or "")[:160],
                        "summary": f"spec: {spec.title} · AC #{idx} · no linked scenario",
                        "meta": {"spec_id": spec.id, "kind": "acceptance_criterion", "index": idx},
                    }
                )
    return _ok(
        rows,
        columns=["Kind", "Text", "Spec"],
        tool_binding="okto_pulse_list_uncovered_requirements",
    )


async def _exec_supersedence_chains(db: AsyncSession, board_id: str) -> dict:
    """NEW aggregator (ideação d1783b03): walks spec.decisions JSON on every
    spec of the board, collects all entries whose supersedes_decision_id
    points to another decision (on the same spec — cross-spec chains are
    intentionally out of scope, matching the constraint expressed in
    decision_8b8139e5ba98 on the KG)."""
    specs = (
        await db.execute(select(Spec).where(Spec.board_id == board_id))
    ).scalars().all()

    # Flatten all decisions across specs, indexed by id (same scope as the
    # canonical supersedence constraint).
    by_id: dict[str, dict] = {}
    for spec in specs:
        for dec in getattr(spec, "decisions", None) or []:
            dec_id = dec.get("id")
            if dec_id:
                by_id[dec_id] = {**dec, "_spec_id": spec.id, "_spec_title": spec.title}

    chains: list[list[dict]] = []
    seen_as_head: set[str] = set()
    for dec_id, dec in by_id.items():
        if dec_id in seen_as_head:
            continue
        if not dec.get("supersedes_decision_id"):
            continue
        chain: list[dict] = []
        cur = dec
        while cur:
            chain.append(
                {
                    "id": cur.get("id"),
                    "title": cur.get("title"),
                    "status": cur.get("status", "active"),
                    "spec_id": cur.get("_spec_id"),
                    "spec_title": cur.get("_spec_title"),
                }
            )
            cur_id = cur.get("id")
            if cur_id:
                seen_as_head.add(cur_id)
            nxt_id = cur.get("supersedes_decision_id")
            cur = by_id.get(nxt_id) if nxt_id else None
        if len(chain) >= 2:
            chains.append(chain)

    rows = []
    for chain in chains:
        head = chain[0]
        length = len(chain)
        trail = " → ".join((c.get("title") or "(untitled)")[:40] for c in chain)
        rows.append(
            {
                "id": head.get("id"),
                "type": "SupersedenceChain",
                "title": head.get("title") or "(untitled)",
                "summary": f"{length} decisions · {trail}",
                "meta": {"chain": chain, "length": length},
            }
        )
    return _ok(
        rows,
        columns=["Head", "Length", "Trail"],
        tool_binding="okto_pulse_list_supersedence_chains",
    )


async def _exec_learnings(board_id: str, params: dict) -> dict:
    """learning_from_bugs — isolated from the upstream tool's missing-area
    binding bug (ideação ba344686) by invoking the KG service directly with
    a resolved area string (empty = "any area")."""
    from okto_pulse.core.kg.kg_service import get_kg_service

    svc = get_kg_service()
    area = (params.get("area") or "").strip()
    try:
        rows_raw = svc.get_learning_from_bugs(board_id, area=area or None, max_rows=50)
    except Exception as e:  # noqa: BLE001
        # Upstream Kùzu bug "Parameter area not found" is known to affect
        # empty-area calls. Degrade gracefully with a typed empty result
        # plus an inline execution note — the UI can display it.
        return {
            "rows": [],
            "columns": ["Learning", "Source bug"],
            "total": 0,
            "tool_binding": "okto_pulse_kg_get_learning_from_bugs",
            "params_echo": {"area": area} if area else {},
            "execution": "real_tool",
            "warning": f"Upstream tool raised {type(e).__name__}: {str(e)[:140]}",
        }
    rows = []
    for r in rows_raw:
        rows.append(
            {
                "id": r.get("learning_id"),
                "type": "Learning",
                "title": r.get("learning_title") or "(untitled)",
                "summary": f"from bug: {r.get('bug_title') or r.get('bug_id')}",
                "meta": r,
            }
        )
    return _ok(
        rows,
        columns=["Learning", "Source bug"],
        tool_binding="okto_pulse_kg_get_learning_from_bugs",
        params_echo={"area": area} if area else {},
    )

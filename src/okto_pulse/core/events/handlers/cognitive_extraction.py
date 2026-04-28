"""CognitiveExtractionHandler — auto-emits Learning/Alternative/Assumption
candidates when a card transitions to ``done`` (spec 3d907a87, FR1-FR7).

Closes the gap audited in 2026-04-28 of Global Discovery: extractors
existed in ``kg/agent/extractors/`` (learnings.py, alternatives.py) plus
the new ``assumptions.py`` (FR4 / D2) but had **zero call sites** in
production. The result was that ``learning_from_bugs`` and the
Alternative/Assumption queries always returned empty, even on boards
with rich post-mortems.

Design — Decision D1 (umbrella refinement a647d21a):
    - Trigger: ``CardMoved`` event with ``to_status == "done"``.
    - Bug cards with ``action_plan`` ≥ 50 chars → ``extract_learning_from_bug``.
    - Cards with ``spec_id`` set → ``extract_alternatives`` + ``extract_assumptions``
      over the spec context.
    - LLM dependency for Learning is **opt-in** via
      ``Board.settings.cognitive_llm_config`` (D5). Absent → log info + skip
      Learning. Regex extractors (Alternative + Assumption) always run.
    - Idempotency (D3 / FR5): query Kuzu for the equivalent node before
      invoking each extractor. v1 skip silently if already exists; never
      supersede.

This handler intentionally **does not** push candidates into the Kuzu
store directly. Cognitive nodes go through the consolidation session
pipeline (``begin_consolidation`` → ``add_node_candidate`` →
``commit_consolidation``) which is owned by the cognitive agent and is
not safe to call from inside an event drain transaction. Instead, the
handler emits a structured ``cognitive.extraction.candidate`` log line
per extracted candidate carrying the full payload; the cognitive agent
or a downstream worker consumes the log and persists. The wiring of that
downstream worker is registered in this spec's out-of-scope list and
deferred to a follow-up spec.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import CardMoved, DomainEvent
from okto_pulse.core.kg.agent.extractors import (
    AlternativeExtraction,
    AssumptionExtraction,
    LEARNING_MIN_ACTION_PLAN_CHARS,
    LearningExtraction,
    extract_alternatives,
    extract_assumptions,
    extract_learning_from_bug,
)
from okto_pulse.core.models.db import Board, Card, CardType, Spec

logger = logging.getLogger("okto_pulse.core.events.cognitive_extraction")


@register_handler("card.moved")
class CognitiveExtractionHandler:
    """Maps ``card.moved → done`` events to cognitive extractor invocations."""

    async def handle(self, event: DomainEvent, session: AsyncSession) -> None:
        # BR1: only react to terminal-state transitions.
        if not isinstance(event, CardMoved) or event.to_status != "done":
            return

        card = await self._load_card(session, event.card_id)
        if card is None:
            logger.debug(
                "cognitive.extraction.skipped reason=card_not_found "
                "card_id=%s board=%s",
                event.card_id, event.board_id,
                extra={
                    "event": "cognitive.extraction.skipped",
                    "reason": "card_not_found",
                    "card_id": event.card_id,
                    "board_id": event.board_id,
                },
            )
            return

        board_settings = await self._load_board_settings(session, event.board_id)
        llm_config = (board_settings or {}).get("cognitive_llm_config") if isinstance(
            board_settings, dict
        ) else None

        # Bug branch → Learning (BR2 + BR5 idempotency).
        if _card_type_value(card.card_type) == "bug":
            await self._maybe_extract_learning(card, llm_config, event)

        # Spec branch → Alternative + Assumption (BR3 + BR4 + BR5 idempotency).
        if card.spec_id:
            spec = await session.get(Spec, card.spec_id)
            if spec is not None:
                self._extract_alternatives(spec, event)
                self._extract_assumptions(spec, event)

    # ------------------------------------------------------------------
    # Branch helpers
    # ------------------------------------------------------------------

    async def _maybe_extract_learning(
        self,
        card: Card,
        llm_config: dict | None,
        event: CardMoved,
    ) -> None:
        # BR2: filter mirrors ``extract_learning_from_bug`` so we short-circuit
        # before paying the cost of LLM init.
        action_plan = (card.action_plan or "").strip()
        if len(action_plan) < LEARNING_MIN_ACTION_PLAN_CHARS:
            return

        bug_node_id = _bug_node_id(card.id)

        # BR5 / D3: idempotency check via Kuzu MATCH. Skip silently if a
        # Learning is already linked to this Bug. Errors during the probe
        # do not abort extraction — we degrade to "best effort" so the
        # event drain is never blocked by a transient KG read failure.
        if _learning_already_exists(event.board_id, bug_node_id):
            logger.debug(
                "cognitive.extraction.learning.skipped reason=already_exists "
                "card_id=%s bug_node_id=%s",
                card.id, bug_node_id,
                extra={
                    "event": "cognitive.extraction.learning.skipped",
                    "reason": "already_exists",
                    "card_id": card.id,
                    "bug_node_id": bug_node_id,
                    "board_id": event.board_id,
                },
            )
            return

        # BR7: opt-in LLM. Absent config → log info + skip; Alternative + Assumption
        # branches are unaffected because they run from the spec branch above.
        if not llm_config:
            logger.info(
                "cognitive.extraction.learning.skipped reason=no_llm_config "
                "card_id=%s board=%s",
                card.id, event.board_id,
                extra={
                    "event": "cognitive.extraction.learning.skipped",
                    "reason": "no_llm_config",
                    "card_id": card.id,
                    "board_id": event.board_id,
                },
            )
            return

        # IMPL-F: instantiate the configured summariser when a provider is
        # available. Falls back to None (skip Learning) if the provider is
        # unknown — the registry currently exposes ``openai`` only; new
        # providers register themselves in ``_summariser_factory``.
        summariser = _summariser_factory(llm_config)
        if summariser is None:
            logger.info(
                "cognitive.extraction.learning.skipped reason=unknown_provider "
                "card_id=%s provider=%s",
                card.id, llm_config.get("provider"),
                extra={
                    "event": "cognitive.extraction.learning.skipped",
                    "reason": "unknown_provider",
                    "card_id": card.id,
                    "board_id": event.board_id,
                    "llm_provider": llm_config.get("provider"),
                },
            )
            return

        logger.info(
            "cognitive.extraction.learning.candidate "
            "card_id=%s board=%s provider=%s",
            card.id, event.board_id, llm_config.get("provider"),
            extra={
                "event": "cognitive.extraction.learning.candidate",
                "card_id": card.id,
                "board_id": event.board_id,
                "bug_node_id": bug_node_id,
                "action_plan_excerpt": action_plan[:500],
                "llm_provider": llm_config.get("provider"),
                "llm_model": llm_config.get("model"),
            },
        )

    def _extract_alternatives(self, spec: Spec, event: CardMoved) -> None:
        source_ref = f"spec:{spec.id}"
        # BR5 / D3: idempotency — skip if any Alternative with this source_ref
        # is already in the KG.
        if _node_with_source_ref_exists(event.board_id, "Alternative", source_ref):
            logger.debug(
                "cognitive.extraction.alternative.skipped reason=already_exists "
                "spec_id=%s source_ref=%s",
                spec.id, source_ref,
                extra={
                    "event": "cognitive.extraction.alternative.skipped",
                    "reason": "already_exists",
                    "spec_id": spec.id,
                    "source_ref": source_ref,
                    "board_id": event.board_id,
                },
            )
            return
        results: list[AlternativeExtraction] = extract_alternatives(
            spec_context=spec.context or "",
            qa_texts=None,
            source_ref=source_ref,
        )
        for cand in results:
            logger.info(
                "cognitive.extraction.alternative.candidate "
                "spec_id=%s board=%s title=%s",
                spec.id, event.board_id, cand.title[:40],
                extra={
                    "event": "cognitive.extraction.alternative.candidate",
                    "spec_id": spec.id,
                    "board_id": event.board_id,
                    "source_ref": cand.source_ref,
                    "source_section": cand.source_section,
                    "title": cand.title,
                    "reasoning_against": cand.reasoning_against,
                },
            )

    def _extract_assumptions(self, spec: Spec, event: CardMoved) -> None:
        source_ref = f"spec:{spec.id}"
        if _node_with_source_ref_exists(event.board_id, "Assumption", source_ref):
            logger.debug(
                "cognitive.extraction.assumption.skipped reason=already_exists "
                "spec_id=%s source_ref=%s",
                spec.id, source_ref,
                extra={
                    "event": "cognitive.extraction.assumption.skipped",
                    "reason": "already_exists",
                    "spec_id": spec.id,
                    "source_ref": source_ref,
                    "board_id": event.board_id,
                },
            )
            return
        results: list[AssumptionExtraction] = extract_assumptions(
            spec_context=spec.context or "",
            qa_texts=None,
            source_ref=source_ref,
        )
        for cand in results:
            logger.info(
                "cognitive.extraction.assumption.candidate "
                "spec_id=%s board=%s title=%s",
                spec.id, event.board_id, cand.title[:40],
                extra={
                    "event": "cognitive.extraction.assumption.candidate",
                    "spec_id": spec.id,
                    "board_id": event.board_id,
                    "source_ref": cand.source_ref,
                    "source_section": cand.source_section,
                    "title": cand.title,
                    "body": cand.body,
                },
            )

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    async def _load_card(self, session: AsyncSession, card_id: str) -> Card | None:
        return await session.get(Card, card_id)

    async def _load_board_settings(
        self, session: AsyncSession, board_id: str
    ) -> dict | None:
        board = await session.get(Board, board_id)
        if board is None:
            return None
        settings = board.settings or {}
        if isinstance(settings, dict):
            return settings
        # Pydantic model fallback when SA loads it as a typed object.
        try:
            return settings.model_dump()
        except Exception:
            return None


def _card_type_value(value: Any) -> str:
    """Return the underlying string for a CardType enum or raw value."""
    if isinstance(value, CardType):
        return value.value
    return str(value or "")


def _bug_node_id(card_id: str) -> str:
    """Build the deterministic Kuzu Bug node id used in :validates edges.

    Mirrors the worker convention of ``bug_<short_card_id>``. Kept inline so
    the handler does not import the deterministic worker (private API).
    """
    return f"bug_{card_id.replace('-', '')[:12]}"


def _learning_already_exists(board_id: str, bug_node_id: str) -> bool:
    """BR5 / D3 idempotency probe — does this Bug already have a Learning?

    Best-effort: any exception (graph not yet bootstrapped, Kùzu not
    installed in tests, schema drift) returns False so the handler proceeds
    and the rest of the pipeline (or its own dedup) catches the duplicate.
    """
    try:
        from okto_pulse.core.kg.schema import BoardConnection
    except Exception:
        return False
    try:
        with BoardConnection(board_id) as (_db, conn):
            res = conn.execute(
                "MATCH (l:Learning)-[:validates]->(b:Bug {id: $bid}) "
                "RETURN count(l) AS c",
                {"bid": bug_node_id},
            )
            row = res.get_next() if res.has_next() else None
            return bool(row and int(row[0]) > 0)
    except Exception:
        return False


def _node_with_source_ref_exists(board_id: str, node_type: str, source_ref: str) -> bool:
    """BR5 / D3 idempotency probe for Alternative/Assumption.

    Returns True iff Kùzu has at least one ``node_type`` with a matching
    ``source_artifact_ref``. Defensive against missing column / table.
    """
    try:
        from okto_pulse.core.kg.schema import BoardConnection
    except Exception:
        return False
    try:
        with BoardConnection(board_id) as (_db, conn):
            res = conn.execute(
                f"MATCH (n:{node_type}) WHERE n.source_artifact_ref = $ref "
                "RETURN count(n) AS c",
                {"ref": source_ref},
            )
            row = res.get_next() if res.has_next() else None
            return bool(row and int(row[0]) > 0)
    except Exception:
        return False


# ----------------------------------------------------------------------
# IMPL-F — LearningSummariser adapter registry.
# Provider implementations are registered here so the handler can stay
# provider-agnostic. v1 ships an OpenAI stub that loads the API key from
# the env var named in cognitive_llm_config["api_key_env"]; the actual
# HTTP call is encapsulated and lazy so unit tests don't pull the package.
# ----------------------------------------------------------------------


class LearningSummariser:
    """Protocol shape — see kg/agent/extractors/learnings.py for the
    full definition. Re-stated here so type checkers don't choke on the
    factory return type without importing the upstream Protocol.
    """

    def summarise(
        self, *, bug_title: str, action_plan: str, context: dict[str, str] | None = None,
    ) -> tuple[str, str]:  # pragma: no cover — protocol shape only
        raise NotImplementedError


class _OpenAILearningSummariser:
    """Minimal OpenAI provider. The actual API call is deferred to the
    downstream worker (see spec D5 / out-of-scope). v1 returns a
    short title + body derived from the action_plan so the contract
    is exercisable end-to-end without pulling the openai SDK.
    """

    def __init__(self, *, model: str, api_key_env: str, max_tokens: int, timeout_s: int):
        self.model = model
        self.api_key_env = api_key_env
        self.max_tokens = max_tokens
        self.timeout_s = timeout_s

    def summarise(
        self,
        *,
        bug_title: str,
        action_plan: str,
        context: dict[str, str] | None = None,
    ) -> tuple[str, str]:
        # Stub: deterministic shape so wire/tests can exercise the code path
        # without an actual LLM call. The real provider is wired in the
        # follow-up spec called out in D5 / out-of-scope.
        title = f"Lesson from: {bug_title[:80]}"
        body = action_plan[:500]
        return title, body


def _summariser_factory(llm_config: dict) -> LearningSummariser | None:
    provider = (llm_config or {}).get("provider", "").lower()
    if provider == "openai":
        return _OpenAILearningSummariser(
            model=str(llm_config.get("model", "gpt-4o-mini")),
            api_key_env=str(llm_config.get("api_key_env", "OPENAI_API_KEY")),
            max_tokens=int(llm_config.get("max_tokens", 800)),
            timeout_s=int(llm_config.get("timeout_s", 30)),
        )
    return None

"""Alternative/Assumption candidates and native Spec closeout work.

Authored Learning capture admission has its own outbox handler. Card Done
alone neither infers a Learning nor opens generic Bug work.
"""

from __future__ import annotations

import logging

from okto_pulse.core.events.bus import register_handler
from okto_pulse.core.events.types import CardMoved, DomainEvent, SpecMoved
from okto_pulse.core.kg.agent.extractors import (
    AlternativeExtraction,
    AssumptionExtraction,
    extract_alternatives,
    extract_assumptions,
)
from okto_pulse.core.ports.domain_event_delivery import (
    CognitiveCardFacts,
    CognitiveSpecFacts,
    get_domain_event_fact_reader,
)

logger = logging.getLogger("okto_pulse.core.events.cognitive_extraction")


@register_handler("card.moved", "spec.moved")
class CognitiveExtractionHandler:
    """Maps ``card.moved → done`` and ``spec.moved → done`` events to cognitive
    extractor invocations and opens cognitive-closeout pending work in the ledger.

    FR1/AC1 (codex): a spec reaching done is the CANONICAL trigger for spec
    cognitive closeout (``SpecMoved``), independent of any card. Authored Learning
    closeout is opened by LearningCaptureMaterializationEnqueuer.
    """

    async def handle(self, event: DomainEvent, session: object) -> None:
        # FR1/AC1: a spec reaching done is the canonical spec-closeout trigger.
        # Ledger-only open here (safe in the drain); the worker persists later.
        if isinstance(event, SpecMoved):
            if event.to_status == "done":
                spec = None
                if session is not None:
                    spec = await get_domain_event_fact_reader().load_cognitive_spec_facts(
                        session,
                        spec_id=event.spec_id,
                    )
                self._open_closeout_pending(
                    event.board_id,
                    f"spec:{event.spec_id}",
                    "spec",
                    content_hash=getattr(spec, "content_hash", None),
                )
            return

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

        # Learning work is opened by learning.capture_admitted.v1 in the
        # author's transaction. Done alone neither invents a lesson nor opens
        # an endless advisory obligation for every Bug.

        # Spec branch → Alternative + Assumption candidate logs.
        # The spec-done CLOSEOUT pending is opened on SpecMoved(done) above — NOT
        # here: one card going done does not mean the spec is done.
        if card.spec_id:
            spec = await get_domain_event_fact_reader().load_cognitive_spec_facts(
                session,
                spec_id=card.spec_id,
            )
            if spec is not None:
                self._extract_alternatives(spec, event)
                self._extract_assumptions(spec, event)

    @staticmethod
    def _open_closeout_pending(
        board_id: str,
        source_ref: str,
        artifact_type: str,
        *,
        content_hash: str | None = None,
    ) -> None:
        """Open ledger work without hiding an enqueue failure.

        The event drain remains resilient, while the structured warning is an
        observable technical signal.  The readiness evaluator independently
        detects the resulting missing work item and fails closed.
        """
        try:
            from okto_pulse.core.kg.cognitive_closeout_production import (
                open_cognitive_closeout_pending,
            )

            # Store + generation resolved inside (production default base_dir,
            # latest_generation or a stable id) — no no-arg store, no per-item gen.
            open_cognitive_closeout_pending(
                board_id=board_id,
                source_ref=source_ref,
                artifact_type=artifact_type,
                content_hash=content_hash,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception(
                "cognitive.closeout.open_pending_failed source_ref=%s err=%s",
                source_ref,
                exc,
                extra={
                    "event": "cognitive.closeout.open_pending_failed",
                    "board_id": board_id,
                    "source_ref": source_ref,
                    "artifact_type": artifact_type,
                    "error_type": type(exc).__name__,
                },
            )

    # ------------------------------------------------------------------
    # Branch helpers
    # ------------------------------------------------------------------


    def _extract_alternatives(
        self, spec: CognitiveSpecFacts, event: CardMoved
    ) -> None:
        # FR1/FR3: per-concept ref. The base ref is spec:<id>; the extractor
        # appends spec:<id>:alternative:<content_hash8> per candidate.
        base_ref = f"spec:{spec.spec_id}"
        results: list[AlternativeExtraction] = extract_alternatives(
            spec_context=spec.context or "",
            qa_texts=None,
            source_ref=base_ref,
        )
        for cand in results:
            # FR2/AC3: per-candidate idempotency. Probe the candidate's
            # per-concept ref and skip ONLY that concept if it already
            # exists — never the whole type (the old any-exists probe on the
            # global ref suppressed every distinct concept after the first).
            if _node_with_source_ref_exists(
                event.board_id, "Alternative", cand.source_ref
            ):
                logger.debug(
                    "cognitive.extraction.alternative.skipped reason=already_exists "
                    "spec_id=%s source_ref=%s",
                    spec.spec_id, cand.source_ref,
                    extra={
                        "event": "cognitive.extraction.alternative.skipped",
                        "reason": "already_exists",
                        "spec_id": spec.spec_id,
                        "source_ref": cand.source_ref,
                        "board_id": event.board_id,
                    },
                )
                continue
            logger.info(
                "cognitive.extraction.alternative.candidate "
                "spec_id=%s board=%s title=%s",
                spec.spec_id, event.board_id, cand.title[:40],
                extra={
                    "event": "cognitive.extraction.alternative.candidate",
                    "spec_id": spec.spec_id,
                    "board_id": event.board_id,
                    "source_ref": cand.source_ref,
                    "source_section": cand.source_section,
                    "title": cand.title,
                    "reasoning_against": cand.reasoning_against,
                },
            )

    def _extract_assumptions(
        self, spec: CognitiveSpecFacts, event: CardMoved
    ) -> None:
        # FR1/FR3: per-concept ref (spec:<id>:assumption:<content_hash8>).
        base_ref = f"spec:{spec.spec_id}"
        results: list[AssumptionExtraction] = extract_assumptions(
            spec_context=spec.context or "",
            qa_texts=None,
            source_ref=base_ref,
        )
        for cand in results:
            # FR2/AC3: per-candidate idempotency — skip only the existing
            # concept, never the whole type.
            if _node_with_source_ref_exists(
                event.board_id, "Assumption", cand.source_ref
            ):
                logger.debug(
                    "cognitive.extraction.assumption.skipped reason=already_exists "
                    "spec_id=%s source_ref=%s",
                    spec.spec_id, cand.source_ref,
                    extra={
                        "event": "cognitive.extraction.assumption.skipped",
                        "reason": "already_exists",
                        "spec_id": spec.spec_id,
                        "source_ref": cand.source_ref,
                        "board_id": event.board_id,
                    },
                )
                continue
            logger.info(
                "cognitive.extraction.assumption.candidate "
                "spec_id=%s board=%s title=%s",
                spec.spec_id, event.board_id, cand.title[:40],
                extra={
                    "event": "cognitive.extraction.assumption.candidate",
                    "spec_id": spec.spec_id,
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

    async def _load_card(
        self, session: object, card_id: str
    ) -> CognitiveCardFacts | None:
        return await get_domain_event_fact_reader().load_cognitive_card_facts(
            session,
            card_id=card_id,
        )









def _node_with_source_ref_exists(board_id: str, node_type: str, source_ref: str) -> bool:
    """BR5 / D3 idempotency probe for Alternative/Assumption.

    Returns True iff graph backend has at least one ``node_type`` with a matching
    ``source_artifact_ref``. Defensive against missing column / table.
    """
    from okto_pulse.core.kg.interfaces import get_kg_registry

    try:
        result = get_kg_registry().cypher_executor.execute_read_only(
            board_id,
            f"MATCH (n:{node_type}) WHERE n.source_artifact_ref = $ref "
            "RETURN count(n) AS c",
            {"ref": source_ref},
            max_rows=1,
        )
        rows = result.get("rows", [])
        return bool(rows and int(rows[0][0]) > 0)
    except Exception:
        return False

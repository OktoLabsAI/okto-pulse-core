"""RKG-03 — the dedicated cognitive-closeout worker (production path, #1 codex).

Started in ``core/app.py`` alongside the deterministic consolidation worker, it
periodically drains PENDING cognitive-closeout work from the ledger
(opened by the CognitiveExtractionHandler) and persists Alternative/Assumption/
Learning to graph.lbug OUTSIDE the event drain, advancing the SAME ledger
pending→consolidated/skipped/failed.

The worker NEVER writes the graph inside the event drain and uses a cognitive
(non-``system:``) agent id so it stays on the cognitive writer boundary.
"""

from __future__ import annotations

import asyncio
import logging

from okto_pulse.core.kg.cognitive_closeout_production import (
    drain_cognitive_closeout_pending,
)
from okto_pulse.core.kg.rebuild_audit import (
    AUDIT_DIRNAME,
    COGNITIVE_PENDING_DIRNAME,
    REBUILD_DIRNAME,
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
    default_rebuild_base_dir,
)

# Statuses a drain should (re)process: fresh pending + items left in_progress by
# a worker that crashed mid-tick (recoverable).
_DRAINABLE_STATUSES = frozenset({
    CognitiveItemStatus.PENDING.value, CognitiveItemStatus.IN_PROGRESS.value})

logger = logging.getLogger("okto_pulse.core.kg.workers.cognitive_closeout")

AGENT_ID = "cognitive_closeout_worker"
_DEFAULT_INTERVAL_S = 30.0


def _lookup_spec_decision_node(board_id: str, spec_id: str) -> str | None:
    """Find the spec's related canonical Decision node id (for the Alternative's
    relates_to edge), or None when the spec has no materialised Decision."""
    from okto_pulse.core.kg.cognitive_source_ref_resolver import strip_concept_suffix
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    target = normalize_cognitive_artifact_id(f"spec:{spec_id}")
    try:
        result = get_kg_registry().cypher_executor.execute_read_only(
            board_id,
            "MATCH (d:Decision) WHERE d.graph_layer = 'canonical' "
            "RETURN d.id, d.source_artifact_ref",
            {},
            max_rows=5000,
        )
        for nid, sref in result.get("rows") or []:
            if sref and normalize_cognitive_artifact_id(
                    strip_concept_suffix(str(sref))) == target:
                return str(nid)
    except Exception as exc:
        logger.debug("cognitive_closeout.decision_lookup_failed spec=%s err=%s", spec_id, exc)
    return None


def _graph_bug_probe(board_id: str):
    """A canonical-bug probe backed by the live board graph (RKG-02)."""
    from okto_pulse.core.kg.cognitive_source_ref_resolver import strip_concept_suffix
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    keys: set[str] = set()
    loaded = False

    def _ensure_loaded() -> None:
        nonlocal loaded
        if loaded:
            return
        loaded = True
        try:
            result = get_kg_registry().cypher_executor.execute_read_only(
                board_id,
                "MATCH (b:Bug) WHERE b.graph_layer = 'canonical' "
                "RETURN b.id, b.source_artifact_ref",
                {},
                max_rows=10000,
            )
            for bid, bsref in result.get("rows") or []:
                if bid:
                    keys.add(normalize_cognitive_artifact_id(f"card:{bid}"))
                if bsref:
                    keys.add(normalize_cognitive_artifact_id(
                        strip_concept_suffix(str(bsref))
                    ))
        except Exception:
            pass

    def _probe(uuid: str) -> bool:
        _ensure_loaded()
        return normalize_cognitive_artifact_id(f"card:{uuid}") in keys

    return _probe


def build_closeout_input_loader(session_factory):
    """The production input loader: derives the closeout inputs for a pending
    ledger item from SQL (Card/Spec/Board settings) + the live graph (bug probe,
    related Decision)."""
    from okto_pulse.core.events.handlers.cognitive_extraction import _summariser_factory
    from okto_pulse.core.models.db import Board, Card, Spec

    async def _loader(board_id: str, item) -> dict:
        kind = (item.source_ref.split(":", 1)[0] or "").lower()
        ident = item.source_ref.split(":", 1)[-1]
        if item.artifact_type == "bug" or kind == "bug":
            async with session_factory() as db:
                card = await db.get(Card, ident)
                board = await db.get(Board, board_id)
            settings = (board.settings or {}) if board is not None else {}
            llm_config = settings.get("cognitive_llm_config")
            summariser = _summariser_factory(llm_config) if llm_config else None
            return {
                "bug_card_id": ident,
                "bug_title": getattr(card, "title", "") or "",
                "bug_action_plan": getattr(card, "action_plan", "") or "",
                "llm_config": llm_config,
                "summariser": summariser,
                "bug_probe": _graph_bug_probe(board_id),
            }
        if item.artifact_type == "spec" or kind == "spec":
            async with session_factory() as db:
                spec = await db.get(Spec, ident)
            return {
                "spec_context": getattr(spec, "context", "") or "",
                "decision_ref": _lookup_spec_decision_node(board_id, ident),
            }
        return {}

    return _loader


class CognitiveCloseoutWorker:
    """Periodic worker draining cognitive-closeout pending per board."""

    def __init__(self, session_factory, *, interval_s: float = _DEFAULT_INTERVAL_S,
                 store: CognitiveConsolidationItemStore | None = None) -> None:
        self._session_factory = session_factory
        self._interval_s = interval_s
        self._running = False
        self._task: asyncio.Task | None = None
        self._loader = build_closeout_input_loader(session_factory)
        self._store = store or CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="kg.cognitive_closeout_worker")
        logger.info("kg.cognitive_closeout_worker.started interval_s=%s", self._interval_s)

    async def stop(self, timeout: float = 10.0) -> None:
        self._running = False
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
            self._task = None

    def _scan_ledger_records(self) -> list[tuple[str, str]]:
        """Bounded scan of the cognitive_pending ledger directory for (board,
        generation) records that still hold drainable items. The LEDGER is the
        canonical work source (codex: NO full SQL board scan — SQL is only used
        to load an item's payload once a real pending item is found)."""
        root = (self._store.base_dir / REBUILD_DIRNAME / AUDIT_DIRNAME
                / COGNITIVE_PENDING_DIRNAME)
        records: list[tuple[str, str]] = []
        if not root.is_dir():
            return records
        for board_dir in sorted(root.iterdir()):
            if not board_dir.is_dir():
                continue
            board_id = board_dir.name
            for gen_file in sorted(board_dir.glob("*.json")):
                gen = gen_file.stem
                try:
                    items = self._store.list_items(board_id, gen)
                except Exception:
                    continue
                if any(i.status in _DRAINABLE_STATUSES for i in items):
                    records.append((board_id, gen))
        return records

    async def drain_once(self) -> int:
        """Drain every ledger record with drainable items once; returns the
        number of items processed."""
        processed = 0
        for board_id, gen in self._scan_ledger_records():
            try:
                results = await drain_cognitive_closeout_pending(
                    self._session_factory, board_id, input_loader=self._loader,
                    store=self._store, agent_id=AGENT_ID, kg_generation_id=gen,
                )
                processed += len(results)
            except Exception as exc:
                logger.warning(
                    "kg.cognitive_closeout_worker.record_failed board=%s gen=%s err=%s",
                    board_id, gen, exc)
        return processed

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.drain_once()
            except Exception as exc:  # pragma: no cover - loop must survive
                logger.warning("kg.cognitive_closeout_worker.tick_failed err=%s", exc)
            await asyncio.sleep(self._interval_s)


_worker: CognitiveCloseoutWorker | None = None


def get_cognitive_closeout_worker(session_factory=None) -> CognitiveCloseoutWorker:
    global _worker
    if _worker is None:
        if session_factory is None:
            from okto_pulse.core.infra.database import get_session_factory
            session_factory = get_session_factory()
        _worker = CognitiveCloseoutWorker(session_factory)
    return _worker

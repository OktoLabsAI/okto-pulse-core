"""RKG-03 — the dedicated cognitive-closeout worker (production path, #1 codex).

Started in ``core/app.py`` alongside the deterministic consolidation worker, it
periodically drains PENDING cognitive-closeout work from the ledger
(opened by the CognitiveExtractionHandler) and persists Alternative/Assumption/
Learning to board graph OUTSIDE the event drain, advancing the SAME ledger
pending→consolidated/skipped/failed.

The worker NEVER writes the graph inside the event drain and uses a cognitive
(non-``system:``) agent id so it stays on the cognitive writer boundary.
"""

from __future__ import annotations

from okto_pulse.core.runtime_context import register_runtime_value, resolve_runtime_value

import asyncio
import logging

from okto_pulse.core.kg.cognitive_closeout_production import (
    drain_cognitive_closeout_pending,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from okto_pulse.core.kg.interfaces.cognitive_pending_work import (
    CognitivePendingWorkProvider,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitiveConsolidationItemStore,
    CognitiveItemStatus,
)

# Statuses a drain should (re)process: fresh pending + items left in_progress by
# a worker that crashed mid-tick (recoverable).
_DRAINABLE_STATUSES = frozenset({
    CognitiveItemStatus.PENDING.value, CognitiveItemStatus.IN_PROGRESS.value})

logger = logging.getLogger("okto_pulse.core.kg.workers.cognitive_closeout")

AGENT_ID = "cognitive_closeout_worker"
_DEFAULT_INTERVAL_S = 30.0


def _default_item_store() -> CognitiveConsolidationItemStore:
    return CognitiveConsolidationItemStore(
        artifact_store=get_kg_registry().require_rebuild_audit_artifact_store()
    )


def _default_pending_work_provider() -> CognitivePendingWorkProvider:
    return get_kg_registry().require_cognitive_pending_work_provider()


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


def build_closeout_input_loader(relational_scope_factory):
    """The production input loader: derives the closeout inputs for a pending
    ledger item from SQL (Card/Spec/Board settings) + the live graph (bug probe,
    related Decision)."""
    from okto_pulse.core.events.handlers.cognitive_extraction import _summariser_factory
    from okto_pulse.core.ports.domain_event_delivery import (
        get_domain_event_fact_reader,
    )
    from okto_pulse.core.ports.bug_cognitive_context import (
        resolve_bug_cognitive_context_assembler,
    )

    async def _loader(board_id: str, item) -> dict:
        kind = (item.source_ref.split(":", 1)[0] or "").lower()
        ident = item.source_ref.split(":", 1)[-1]
        reader = get_domain_event_fact_reader()
        if item.artifact_type == "bug" or kind == "bug":
            assembler = resolve_bug_cognitive_context_assembler()
            if assembler is None:
                raise RuntimeError("bug_cognitive_context_assembler_not_configured")
            async with relational_scope_factory() as db:
                bug_context = await assembler.assemble(
                    db,
                    board_id=board_id,
                    bug_id=ident,
                )
                settings = await reader.load_board_settings(db, board_id=board_id)
            settings = settings or {}
            llm_config = settings.get("cognitive_llm_config")
            summariser = _summariser_factory(llm_config) if llm_config else None
            return {
                "bug_card_id": ident,
                "bug_title": bug_context.title or "",
                "bug_action_plan": bug_context.action_plan or "",
                "bug_context": bug_context,
                "llm_config": llm_config,
                "summariser": summariser,
                "bug_probe": lambda uuid: (
                    uuid == ident and bug_context.canonical_bug_present is True
                ),
            }
        if item.artifact_type == "spec" or kind == "spec":
            async with relational_scope_factory() as db:
                spec = await reader.load_cognitive_spec_facts(db, spec_id=ident)
            return {
                "spec_context": spec.context if spec and spec.context else "",
                "decision_ref": _lookup_spec_decision_node(board_id, ident),
            }
        return {}

    return _loader


class CognitiveCloseoutWorker:
    """Periodic worker draining cognitive-closeout pending per board."""

    def __init__(self, relational_scope_factory=None, *, interval_s: float = _DEFAULT_INTERVAL_S,
                 store: CognitiveConsolidationItemStore | None = None,
                 pending_work_provider: CognitivePendingWorkProvider | None = None) -> None:
        if relational_scope_factory is None:
            from okto_pulse.core.ports.relational_runtime import get_db_session

            relational_scope_factory = get_db_session
        self._relational_scope_factory = relational_scope_factory
        self._interval_s = interval_s
        self._running = False
        self._task: asyncio.Task | None = None
        self._loader = build_closeout_input_loader(relational_scope_factory)
        self._store = store or _default_item_store()
        self._pending_work_provider = (
            pending_work_provider or _default_pending_work_provider()
        )

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
        """Bounded discovery of cognitive_pending ledger records for (board,
        generation) records that still hold drainable items. The LEDGER is the
        canonical work source (codex: NO full SQL board scan — SQL is only used
        to load an item's payload once a real pending item is found). Runtime
        editions own the durable-storage enumeration mechanics."""
        records: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for ref in self._pending_work_provider.list_records():
            board_id = str(ref.board_id)
            gen = str(ref.kg_generation_id)
            key = (board_id, gen)
            if not board_id or not gen or key in seen:
                continue
            seen.add(key)
            try:
                items = self._store.list_items(board_id, gen)
            except Exception:
                continue
            if any(i.status in _DRAINABLE_STATUSES for i in items):
                records.append(key)
        return records

    async def drain_once(self) -> int:
        """Drain every ledger record with drainable items once; returns the
        number of items processed."""
        processed = 0
        for board_id, gen in self._scan_ledger_records():
            try:
                results = await drain_cognitive_closeout_pending(
                    self._relational_scope_factory,
                    board_id,
                    input_loader=self._loader,
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


_RUNTIME_KEY = "kg.workers.cognitive_closeout"


def get_cognitive_closeout_worker(
    relational_scope_factory=None,
) -> CognitiveCloseoutWorker:
    worker = resolve_runtime_value(_RUNTIME_KEY)
    if worker is None:
        worker = CognitiveCloseoutWorker(relational_scope_factory)
        register_runtime_value(_RUNTIME_KEY, worker)
    return worker

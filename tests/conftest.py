"""Shared fixtures for the KG foundation test suite.

Provides:
- Structured logging with file capture per test
- Test lifecycle hooks (start/teardown/end)
- KG operation tracing
- Timeout handling (default 120s per test)
- Fresh environment per test (complete isolation)
"""

import logging
import json
import os
import sys
import tempfile
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from importlib import util as importlib_util
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import event, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# ---------------------------------------------------------------------------
# Path setup — must happen before any okto_pulse import
# ---------------------------------------------------------------------------

_CORE_SRC = Path(__file__).parent / ".." / "src"
sys.path.insert(0, str(_CORE_SRC))

# ---------------------------------------------------------------------------
# Test logging infrastructure (must be imported early)
# ---------------------------------------------------------------------------

from test_logging import (  # noqa: E402
    TestLifecycleLogger,
    TimeoutTracker,
    cleanup_test_logging,
    get_test_logger,
    log_kg_event,
    setup_test_logging,
)

# ---------------------------------------------------------------------------
# Environment setup — MUST happen before any okto_pulse import
# ---------------------------------------------------------------------------

_tmpdb = tempfile.mktemp(suffix=".db")
_kg_dir = tempfile.mkdtemp(prefix="okto_kg_test_")
os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{_tmpdb}"
os.environ["KG_BASE_DIR"] = _kg_dir
os.environ["KG_CLEANUP_INTERVAL_SECONDS"] = "1"
os.environ["KG_CLEANUP_ENABLED"] = "false"
# Force the deterministic stub embedding provider for unit tests so we
# don't reach for sentence-transformers (slow + non-deterministic on first
# load). The community edition flips this to "sentence-transformers" in
# production via CommunitySettings.
os.environ["KG_EMBEDDING_MODE"] = "stub"

# ---------------------------------------------------------------------------
# Application imports
# ---------------------------------------------------------------------------

from okto_pulse.core.infra.database import create_database, get_session_factory, init_db  # noqa: E402
from okto_pulse.core.infra import database as _database_mod  # noqa: E402
from okto_pulse.core.infra import schema_lifecycle as _schema_lifecycle  # noqa: E402
from okto_pulse.core.kg.embedding import reset_embedding_provider_cache  # noqa: E402
from okto_pulse.core.kg.schema import bootstrap_board_graph  # noqa: E402
from okto_pulse.core.kg.session_manager import reset_session_manager_for_tests  # noqa: E402
from okto_pulse.core.kg.workers import reset_cleanup_worker_for_tests  # noqa: E402
from okto_pulse.core.models import db as _models  # noqa: E402, F401
from okto_pulse.core.models.db import (  # noqa: E402
    Board,
    Card,
    CanonicalDebt,
    ConsolidationAudit,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    GlobalUpdateOutbox,
    Ideation,
    KGTickRun,
    KuzuNodeRef,
    Refinement,
    Spec,
    Sprint,
)
from okto_pulse.core.ports.kg_operational import (  # noqa: E402
    KGCanonicalDebtSignal,
    KGDeadLetterSignal,
    KGOperationalReadModelPort,
    KGOutboxCounts,
    KGQueueEntrySnapshot,
    KGWorkerAuditPort,
    KGWorkerQueuePort,
    register_kg_operational_ports,
    reset_kg_operational_ports_for_tests,
)
from okto_pulse.core.ports.relational_effects import (  # noqa: E402
    ConsolidationQueueUpsert,
    KGTickRunUpsert,
    RelationalEffectsPort,
    register_relational_effects_port,
    reset_relational_effects_port_for_tests,
)
# Ensure AppSetting (0.1.4) is registered with Base before init_db() runs;
# otherwise the app_settings table is missing and runtime settings tests fail.
from okto_pulse.core.services import settings_service as _settings_svc  # noqa: E402, F401


def _build_test_relational_runtime(url: str, *, echo: bool = False):
    engine_kwargs: dict = {
        "echo": echo,
        "future": True,
    }
    if url.startswith("postgresql"):
        engine_kwargs.update({
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True,
        })
    elif url.startswith("sqlite"):
        engine_kwargs.update({
            "pool_size": 20,
            "max_overflow": 30,
            "pool_timeout": 10,
            "pool_recycle": 1800,
            "pool_pre_ping": True,
        })

    engine = create_async_engine(url, **engine_kwargs)
    if engine.url.get_backend_name() == "sqlite":
        @event.listens_for(engine.sync_engine, "connect")
        def _install_test_sqlite_pragmas(dbapi_conn, _conn_record):  # noqa: ANN001
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.execute("PRAGMA synchronous=NORMAL")
            finally:
                cursor.close()

    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return engine, session_factory


from okto_pulse.core.runtime_registry import register_relational_runtime_factory  # noqa: E402

register_relational_runtime_factory(_build_test_relational_runtime)


class _CoreTestSchemaLifecycle:
    async def initialize_schema(self) -> None:
        async with _database_mod.get_engine().begin() as conn:
            await conn.run_sync(_database_mod.Base.metadata.create_all)


class _CoreTestRelationalEffects(RelationalEffectsPort):
    async def count_active_consolidation_queue(
        self,
        session,
        *,
        board_id: str,
    ) -> int:
        depth = await session.scalar(
            select(func.count()).where(
                ConsolidationQueue.board_id == board_id,
                ConsolidationQueue.status.in_(["pending", "claimed"]),
            )
        )
        return int(depth or 0)

    async def upsert_consolidation_queue(
        self,
        session,
        upsert: ConsolidationQueueUpsert,
    ) -> None:
        insert = _upsert_insert_for_session(session)
        stmt = (
            insert(ConsolidationQueue)
            .values(
                board_id=upsert.board_id,
                artifact_type=upsert.artifact_type,
                artifact_id=upsert.artifact_id,
                priority=upsert.priority,
                source=upsert.source,
                triggered_by_event=upsert.triggered_by_event,
                status="pending",
            )
            .on_conflict_do_update(
                index_elements=["board_id", "artifact_type", "artifact_id"],
                set_={
                    "status": "pending",
                    "attempts": 0,
                    "last_error": None,
                    "priority": upsert.priority,
                    "source": upsert.source,
                    "triggered_by_event": upsert.triggered_by_event,
                    "claimed_by_session_id": None,
                    "claimed_at": None,
                    "worker_id": None,
                    "claim_timeout_at": None,
                    "next_retry_at": None,
                },
                where=ConsolidationQueue.status.notin_(("pending", "claimed")),
            )
        )
        await session.execute(stmt)

    async def list_board_ids(self, session) -> list[str]:
        result = await session.execute(select(Board.id))
        return list(result.scalars().all())

    async def read_latest_kg_tick_completed_at(self, session):
        return (
            await session.execute(
                select(KGTickRun.completed_at)
                .where(KGTickRun.completed_at.is_not(None))
                .order_by(KGTickRun.completed_at.desc())
                .limit(1)
            )
        ).scalars().first()

    async def upsert_kg_tick_run(
        self,
        session,
        upsert: KGTickRunUpsert,
    ) -> None:
        insert = _upsert_insert_for_session(session)
        stmt = (
            insert(KGTickRun)
            .values(
                tick_id=upsert.tick_id,
                started_at=upsert.started_at,
                completed_at=upsert.completed_at,
                nodes_recomputed=upsert.nodes_recomputed,
                duration_ms=upsert.duration_ms,
                boards_processed=upsert.boards_processed,
                boards_failed=upsert.boards_failed,
                error=upsert.error,
            )
            .on_conflict_do_update(
                index_elements=["tick_id"],
                set_={
                    "completed_at": upsert.completed_at,
                    "nodes_recomputed": upsert.nodes_recomputed,
                    "duration_ms": upsert.duration_ms,
                    "boards_processed": upsert.boards_processed,
                    "boards_failed": upsert.boards_failed,
                    "error": upsert.error,
                },
            )
        )
        await session.execute(stmt)


class _CoreTestKGOperationalReadModel(KGOperationalReadModelPort):
    async def list_consolidation_audit(
        self,
        context,
        *,
        board_id: str,
        limit: int,
    ) -> list[dict]:
        query = (
            select(ConsolidationAudit)
            .where(
                ConsolidationAudit.board_id == board_id,
                ConsolidationAudit.committed_at.is_not(None),
            )
            .order_by(ConsolidationAudit.committed_at.desc())
            .limit(limit)
        )
        rows = (await context.execute(query)).scalars().all()
        return [
            {
                "session_id": r.session_id,
                "board_id": r.board_id,
                "artifact_id": r.artifact_id,
                "artifact_type": getattr(r, "artifact_type", ""),
                "agent_id": r.agent_id,
                "committed_at": r.committed_at.isoformat() if r.committed_at else None,
                "nodes_added": r.nodes_added or 0,
                "nodes_updated": r.nodes_updated or 0,
                "nodes_superseded": r.nodes_superseded or 0,
                "edges_added": r.edges_added or 0,
                "summary_text": r.summary_text,
                "undo_status": r.undo_status or "none",
            }
            for r in rows
        ]

    async def list_all_board_ids(self, context, *, limit: int = 100) -> list[str]:
        result = await context.execute(select(Board).limit(limit))
        return [b.id for b in result.scalars().all()]

    async def list_pending_entries(self, context, *, board_id: str) -> list[dict]:
        query = (
            select(ConsolidationQueue)
            .where(ConsolidationQueue.board_id == board_id)
            .order_by(ConsolidationQueue.triggered_at.desc())
            .limit(100)
        )
        rows = (await context.execute(query)).scalars().all()
        return [
            {
                "id": r.id,
                "board_id": r.board_id,
                "artifact_id": r.artifact_id,
                "artifact_type": r.artifact_type,
                "priority": r.priority,
                "source": r.source,
                "status": r.status,
                "triggered_at": r.triggered_at.isoformat() if r.triggered_at else None,
                "claimed_by_session_id": r.claimed_by_session_id,
            }
            for r in rows
        ]

    async def build_pending_tree(
        self,
        context,
        *,
        board_id: str,
        depth: int = 5,
    ) -> dict:
        q_rows = (
            await context.execute(
                select(ConsolidationQueue).where(
                    ConsolidationQueue.board_id == board_id
                )
            )
        ).scalars().all()
        q_by_artifact: dict[tuple[str, str], ConsolidationQueue] = {
            (r.artifact_type, r.artifact_id): r for r in q_rows
        }

        def _queue_meta(art_type: str, art_id: str) -> dict:
            entry = q_by_artifact.get((art_type, art_id))
            if entry is None:
                return {
                    "status": "not_queued",
                    "queued_age_seconds": None,
                    "retry_count": 0,
                    "layer": None,
                    "last_error": None,
                }
            age = None
            if entry.triggered_at is not None:
                triggered_at = entry.triggered_at
                if triggered_at.tzinfo is None:
                    triggered_at = triggered_at.replace(tzinfo=timezone.utc)
                age = (datetime.now(timezone.utc) - triggered_at).total_seconds()
            return {
                "status": entry.status,
                "queued_age_seconds": int(age) if age is not None else None,
                "retry_count": 0,
                "layer": entry.source or "unknown",
                "last_error": None,
            }

        ideas = (
            await context.execute(select(Ideation).where(Ideation.board_id == board_id))
        ).scalars().all()
        refs = (
            await context.execute(
                select(Refinement).where(Refinement.board_id == board_id)
            )
        ).scalars().all()
        specs = (
            await context.execute(select(Spec).where(Spec.board_id == board_id))
        ).scalars().all()
        sprints = (
            await context.execute(select(Sprint).where(Sprint.board_id == board_id))
        ).scalars().all()
        cards = (
            await context.execute(select(Card).where(Card.board_id == board_id))
        ).scalars().all()

        refs_by_ideation: dict[str, list] = defaultdict(list)
        for row in refs:
            refs_by_ideation[row.ideation_id or ""].append(row)
        specs_by_refinement: dict[str, list] = defaultdict(list)
        specs_orphan: list = []
        for row in specs:
            if row.refinement_id:
                specs_by_refinement[row.refinement_id].append(row)
            else:
                specs_orphan.append(row)
        sprints_by_spec: dict[str, list] = defaultdict(list)
        for row in sprints:
            sprints_by_spec[row.spec_id].append(row)
        cards_by_sprint: dict[str, list] = defaultdict(list)
        cards_by_spec_direct: dict[str, list] = defaultdict(list)
        for row in cards:
            if getattr(row, "sprint_id", None):
                cards_by_sprint[row.sprint_id].append(row)
            else:
                cards_by_spec_direct[row.spec_id].append(row)

        levels_counter = {
            lvl: {
                "pending": 0,
                "in_progress": 0,
                "done": 0,
                "failed": 0,
                "not_queued": 0,
            }
            for lvl in ("ideations", "refinements", "specs", "sprints", "cards")
        }

        def _tally(level: str, art_type: str, art_id: str) -> None:
            status = _queue_meta(art_type, art_id)["status"]
            levels_counter[level][status] = levels_counter[level].get(status, 0) + 1

        def _card_node(row) -> dict:
            meta = _queue_meta("card", row.id)
            _tally("cards", "card", row.id)
            return {
                "id": row.id,
                "type": "card",
                "title": row.title,
                "card_type": (
                    str(row.card_type) if getattr(row, "card_type", None) else "normal"
                ),
                **meta,
                "children": [],
            }

        def _sprint_node(row) -> dict:
            meta = _queue_meta("sprint", row.id)
            _tally("sprints", "sprint", row.id)
            children = [_card_node(c) for c in cards_by_sprint.get(row.id, [])]
            if depth < 5:
                children = []
            return {
                "id": row.id,
                "type": "sprint",
                "title": row.title,
                **meta,
                "children": children,
            }

        def _spec_node(row) -> dict:
            meta = _queue_meta("spec", row.id)
            _tally("specs", "spec", row.id)
            sp_children = [_sprint_node(sp) for sp in sprints_by_spec.get(row.id, [])]
            direct_cards = [_card_node(c) for c in cards_by_spec_direct.get(row.id, [])]
            if depth < 4:
                sp_children = []
                direct_cards = []
            return {
                "id": row.id,
                "type": "spec",
                "title": row.title,
                **meta,
                "children": sp_children + direct_cards,
            }

        def _refinement_node(row) -> dict:
            meta = _queue_meta("refinement", row.id)
            _tally("refinements", "refinement", row.id)
            spec_children = [_spec_node(s) for s in specs_by_refinement.get(row.id, [])]
            if depth < 3:
                spec_children = []
            return {
                "id": row.id,
                "type": "refinement",
                "title": row.title,
                **meta,
                "children": spec_children,
            }

        tree: list[dict] = []
        for row in ideas:
            meta = _queue_meta("ideation", row.id)
            _tally("ideations", "ideation", row.id)
            ref_children = [_refinement_node(r) for r in refs_by_ideation.get(row.id, [])]
            if depth < 2:
                ref_children = []
            tree.append({
                "id": row.id,
                "type": "ideation",
                "title": row.title,
                **meta,
                "children": ref_children,
            })
        for row in specs_orphan:
            tree.append(_spec_node(row))

        total_pending = sum(
            sum(v for k, v in counts.items() if k in ("pending", "in_progress"))
            for counts in levels_counter.values()
        )
        return {
            "board_id": board_id,
            "depth": depth,
            "total_pending": total_pending,
            "levels": levels_counter,
            "tree": tree,
        }

    async def queue_status_counts(self, context, *, board_id: str) -> dict[str, int]:
        rows = (
            await context.execute(
                select(ConsolidationQueue.status, func.count())
                .where(ConsolidationQueue.board_id == board_id)
                .group_by(ConsolidationQueue.status)
            )
        ).all()
        return {str(status): int(count) for status, count in rows}

    async def kuzu_node_ref_operation_counts(
        self,
        context,
        *,
        board_id: str,
    ) -> dict[str, int]:
        rows = (
            await context.execute(
                select(KuzuNodeRef.operation, func.count())
                .where(KuzuNodeRef.board_id == board_id)
                .group_by(KuzuNodeRef.operation)
            )
        ).all()
        return {str(op): int(count) for op, count in rows}

    async def global_outbox_counts(
        self,
        context,
        *,
        board_id: str,
        max_retries: int,
        dead_letter_retry_sentinel: int,
    ) -> KGOutboxCounts:
        pending = await context.scalar(
            select(func.count()).where(
                GlobalUpdateOutbox.board_id == board_id,
                GlobalUpdateOutbox.processed_at.is_(None),
                GlobalUpdateOutbox.retry_count >= 0,
                GlobalUpdateOutbox.retry_count < max_retries,
            )
        )
        dead_letter = await context.scalar(
            select(func.count()).where(
                GlobalUpdateOutbox.board_id == board_id,
                GlobalUpdateOutbox.processed_at.is_(None),
                (GlobalUpdateOutbox.retry_count >= max_retries)
                | (GlobalUpdateOutbox.retry_count == dead_letter_retry_sentinel),
            )
        )
        processed = await context.scalar(
            select(func.count()).where(
                GlobalUpdateOutbox.board_id == board_id,
                GlobalUpdateOutbox.processed_at.is_not(None),
            )
        )
        return KGOutboxCounts(
            pending=int(pending or 0),
            dead_letter=int(dead_letter or 0),
            processed=int(processed or 0),
        )

    async def list_canonical_debt_signals(
        self,
        context,
        *,
        board_id: str,
    ) -> list[KGCanonicalDebtSignal]:
        rows = (
            await context.execute(
                select(CanonicalDebt).where(CanonicalDebt.board_id == board_id)
            )
        ).scalars().all()
        return [
            KGCanonicalDebtSignal(
                artifact_type=str(row.artifact_type or ""),
                artifact_id=str(row.artifact_id or ""),
                source_ref=row.source_ref,
                canonical_state=row.canonical_state,
                failure_reason=row.failure_reason,
                last_error=row.last_error,
            )
            for row in rows
        ]

    async def list_dead_letter_signals(
        self,
        context,
        *,
        board_id: str,
    ) -> list[KGDeadLetterSignal]:
        rows = (
            await context.execute(
                select(ConsolidationDeadLetter).where(
                    ConsolidationDeadLetter.board_id == board_id
                )
            )
        ).scalars().all()
        return [
            KGDeadLetterSignal(
                artifact_type=str(row.artifact_type or ""),
                artifact_id=str(row.artifact_id or ""),
            )
            for row in rows
        ]


class _CoreTestKGWorkerQueue(KGWorkerQueuePort):
    async def route_to_dead_letter(
        self,
        context,
        *,
        queue_entry: KGQueueEntrySnapshot,
        errors,
    ):
        dlq_row = ConsolidationDeadLetter(
            id=str(uuid.uuid4()),
            board_id=queue_entry.board_id,
            artifact_type=queue_entry.artifact_type,
            artifact_id=queue_entry.artifact_id,
            original_queue_id=queue_entry.id,
            attempts=queue_entry.attempts or 0,
            errors=list(errors),
        )
        context.add(dlq_row)
        existing = await context.get(ConsolidationQueue, queue_entry.id)
        if existing is not None:
            await context.delete(existing)
        return dlq_row

    async def list_dead_letter(
        self,
        context,
        *,
        board_id: str,
        limit: int = 100,
    ):
        result = await context.execute(
            select(ConsolidationDeadLetter)
            .where(ConsolidationDeadLetter.board_id == board_id)
            .order_by(ConsolidationDeadLetter.dead_lettered_at.desc())
            .limit(limit)
        )
        return list(result.scalars().all())

    async def retry_pending_entry(
        self,
        context,
        *,
        board_id: str,
        queue_entry_id: str,
        recursive: bool = False,
    ):
        entry = await context.get(ConsolidationQueue, queue_entry_id)
        if entry is None or entry.board_id != board_id:
            return None
        entry.status = "pending"
        entry.next_retry_at = datetime.now(timezone.utc)
        entry.claim_timeout_at = None
        entry.worker_id = None
        entry.claimed_at = None
        entry.claimed_by_session_id = None
        return {
            "id": entry.id,
            "board_id": entry.board_id,
            "artifact_type": entry.artifact_type,
            "artifact_id": entry.artifact_id,
            "recursive": recursive,
        }


class _CoreTestKGWorkerAudit(KGWorkerAuditPort):
    async def emit_outbox_event(
        self,
        context,
        *,
        event_id: str,
        board_id: str,
        session_id: str,
        event_type: str,
        payload,
    ) -> None:
        context.add(
            GlobalUpdateOutbox(
                event_id=event_id,
                board_id=board_id,
                session_id=session_id,
                event_type=event_type,
                payload=dict(payload),
            )
        )

    async def record_audit_event(self, context, *, payload) -> None:
        return None


def _upsert_insert_for_session(session):
    dialect_name = session.bind.dialect.name if session.bind else None
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.sqlite import insert

    return insert


AGENT_ID = "agent-test-001"
BOARD_ID = "board-test-001"


def _community_package_available() -> bool:
    try:
        return importlib_util.find_spec("okto_pulse.community") is not None
    except (ImportError, ModuleNotFoundError, ValueError):
        return False


def pytest_collection_modifyitems(config, items):
    """Skip reviewed Community-runtime tests explicitly in core-only runs."""
    from okto_pulse.core.application.boundary.core_test_suite_import_gate import (
        community_runtime_dependency_skip_reason,
    )

    community_available = _community_package_available()
    for item in items:
        reason = community_runtime_dependency_skip_reason(
            item.nodeid,
            community_available=community_available,
        )
        if reason:
            item.add_marker(pytest.mark.skip(reason=reason))


# ============================================================================
# Session-scoped database init (unchanged from original)
# ============================================================================

@pytest_asyncio.fixture(scope="session", autouse=True, loop_scope="session")
async def _db_init():
    """Create the SQLite schema once per session.

    Pinned to ``loop_scope="session"`` so the connection pool warmed up here
    is reused by every async test (function-scoped per-test loops would re-bind
    aiosqlite handles and race the worker singletons).
    """
    create_database(f"sqlite+aiosqlite:///{_tmpdb}", echo=False)
    _schema_lifecycle.register_relational_schema_lifecycle_orchestrator(
        _CoreTestSchemaLifecycle()
    )
    await init_db()
    yield


# ============================================================================
# Test lifecycle logging fixture (autouse — applies to ALL tests)
# ============================================================================

@pytest.fixture(autouse=True)
def _register_test_telemetry_state_carrier():
    """R12: explicit test carrier for the full telemetry state dict.

    Production state persistence is Community-owned. Core tests register this
    sanctioned fake so telemetry settings/service tests can exercise the same
    registry path without creating a runtime fallback.
    """
    from okto_pulse.core.telemetry.telemetry_state_registry import (
        register_telemetry_state_carrier,
        reset_telemetry_state_carrier_for_tests,
    )

    class _TestTelemetryStateCarrier:
        def load_state(self, metrics_dir: Path) -> dict:
            path = Path(metrics_dir) / "state.json"
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {}
            return data if isinstance(data, dict) else {}

        def save_state(self, metrics_dir: Path, state: dict) -> None:
            base = Path(metrics_dir)
            base.mkdir(parents=True, exist_ok=True)
            tmp = (base / "state.json").with_suffix(".tmp")
            tmp.write_text(json.dumps(dict(state), indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(base / "state.json")

    reset_telemetry_state_carrier_for_tests()
    register_telemetry_state_carrier(_TestTelemetryStateCarrier())
    yield
    reset_telemetry_state_carrier_for_tests()


@pytest.fixture(autouse=True)
def _reset_telemetry_effect_config_provider():
    """Keep telemetry effect defaults explicit per test."""
    from okto_pulse.core.telemetry.effect_config_registry import (
        reset_telemetry_effect_config_provider_for_tests,
    )

    reset_telemetry_effect_config_provider_for_tests()
    yield
    reset_telemetry_effect_config_provider_for_tests()


@pytest.fixture(autouse=True)
def _test_logging(request: pytest.FixtureRequest):
    """Set up structured logging for every test.

    Logs go to both stdout and a dedicated file under ``.test-logs/<test_name>.log``.
    Lifecycle events are captured: START, fixture setup, body, teardown, END.
    Even if a test crashes, logs are flushed to disk.
    """
    test_id = request.node.nodeid
    logger = setup_test_logging(test_id=test_id)

    # Log test metadata
    func_name = getattr(request.node, "function_name", None) or getattr(request.node, "name", request.node.nodeid)
    logger.info(f"TEST_METADATA class={request.node.cls.__name__ if request.node.cls else 'N/A'} "
                f"function={func_name} "
                f"params={dict(request.node.callspec.params) if hasattr(request.node, 'callspec') and request.node.callspec else '{}'}")

    # Track KG operation loggers
    kg_logger = logging.getLogger(f"test.kg.{test_id}")

    # Start the test lifecycle logger
    ll = TestLifecycleLogger(test_id, logger)
    ll.__enter__()

    # Attach KG logger as child of test logger
    kg_logger.parent = logger

    yield

    # Teardown: flush and clean up
    ll.__exit__(None, None, None)
    cleanup_test_logging(test_id)


# ============================================================================
# Timeout fixture (custom — wraps each test with a heartbeat tracker)
# ============================================================================

_DEFAULT_TIMEOUT = 120.0  # seconds


def _get_timeout(request: pytest.FixtureRequest) -> float:
    """Extract timeout from pytest.mark.timeout or use default."""
    mark = request.node.get_closest_marker("timeout")
    if mark:
        return float(mark.args[0]) if mark.args else float(mark.kwargs.get("seconds", _DEFAULT_TIMEOUT))
    return _DEFAULT_TIMEOUT


@pytest.fixture(autouse=True)
def _test_timeout(request: pytest.FixtureRequest):
    """Enforce per-test timeout with structured logging.

    Tests can override via ``@pytest.mark.timeout(30)``.
    Default is {_DEFAULT_TIMEOUT}s.
    """
    timeout = _get_timeout(request)
    tracker = TimeoutTracker(max_seconds=timeout)

    # Log the timeout setting
    logger = get_test_logger(request.node.nodeid)
    logger.debug(f"TIMEOUT set to {timeout}s for this test")

    # Start the heartbeat
    tracker.heartbeat()

    yield

    # Final heartbeat check on teardown
    tracker.heartbeat()
    reason = tracker.check()
    if reason:
        logger.warning(reason)


# ============================================================================
# Test isolation — fresh environment per test (extends original)
# ============================================================================

@pytest.fixture(autouse=True)
def _isolation_reset(request: pytest.FixtureRequest):
    """Ensure complete test isolation.

    Resets all singleton state, clears KG sessions, and flushes caches
    before each test. This prevents state leakage between tests.
    """
    # Pre-test: reset all singletons
    reset_session_manager_for_tests()
    reset_cleanup_worker_for_tests()
    reset_embedding_provider_cache()
    _reset_commit_health_cache()

    logger = get_test_logger(request.node.nodeid)
    logger.debug("ISOLATION: singletons reset (session_mgr, cleanup_worker, embedding_cache)")

    yield

    # Post-test: one more reset to ensure clean state for next test
    reset_session_manager_for_tests()
    reset_cleanup_worker_for_tests()
    reset_embedding_provider_cache()
    _reset_commit_health_cache()
    logger.debug("ISOLATION: singletons reset after teardown")


@pytest.fixture(autouse=True)
def _kg_registry_test_fakes():
    """R-P2-03: the KG registry no longer lazy-builds implicit Onda A defaults.

    The test suite configures the embedded fakes EXPLICITLY via ``defaults_factory``
    (the sanctioned test/fake route) so it is literal that tests run on fakes; a
    test that needs a specific composition just reconfigures the registry. Real
    runtime must supply a ``base_registry`` (Community adapters) instead.
    """
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()
    configure_test_kg_registry()
    yield
    reset_registry_for_tests()


@pytest.fixture(autouse=True)
def _reset_rebuild_audit_artifact_store_state(_kg_registry_test_fakes):
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    def _reset_store() -> None:
        try:
            store = get_kg_registry().require_rebuild_audit_artifact_store()
        except Exception:
            return
        reset = getattr(store, "reset_for_tests", None)
        if callable(reset):
            reset()

    _reset_store()
    yield
    _reset_store()


@pytest.fixture(scope="module", autouse=True)
def _kg_registry_module_bootstrap_seed():
    """Seed the registry for legacy module-scoped KG fixtures.

    A few older integration modules bootstrap the global discovery graph from a
    module-scoped fixture, before the function-scoped registry fixture above can
    run. Keep a minimal explicit test composition available for those setup
    paths; each individual test still gets a fresh registry from
    ``_kg_registry_test_fakes``.
    """
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    reset_registry_for_tests()
    configure_test_kg_registry()
    yield
    reset_registry_for_tests()


def _reset_commit_health_cache() -> None:
    """O resolver de health do write-path cacheia por board (TTL 5s) e o
    health cacheia a projeção de órfãos (TTL 300s); o board_id das fixtures
    é compartilhado entre testes, então os caches vazariam estado de um
    teste para o seguinte."""
    from okto_pulse.core.kg.primitives import reset_commit_health_cache_for_tests
    from okto_pulse.core.services.kg_health_service import (
        reset_orphan_projection_cache_for_tests,
    )

    reset_commit_health_cache_for_tests()
    reset_orphan_projection_cache_for_tests()


# ============================================================================
# Standard fixtures (from original, unchanged)
# ============================================================================

@pytest.fixture
def board_id():
    return BOARD_ID


@pytest.fixture
def agent_id():
    return AGENT_ID


@pytest.fixture
def db_factory():
    return get_session_factory()


# Modules SANCTIONED to swap the process-global engine/env during their own
# tests (each restores it on teardown). Every other test runs under the
# backstop below.
_ENGINE_SWAP_SANCTIONED = frozenset({
    "test_kg_governance.py",
    "test_kg_dedup_nc8.py",
    "test_kg_dedup_migration.py",
    "test_kg_pipeline_e2e.py",
    "test_kg_real_integration.py",
})


@pytest.fixture(autouse=True)
def _database_isolation_backstop(request):
    """FU-2 fail-fast backstop: every test must see the session temp database.

    Two past leak channels motivated this guard: a module-level
    ``os.environ["DATABASE_URL"] = <real db>`` executed during COLLECTION
    (test_kg_real_integration), and a ``create_app(CoreSettings(), ...)``
    without a create_database monkeypatch re-registering the process-global
    engine from that poisoned env (test_r_p2_06b) — silently pointing every
    later ``db_factory`` at the user's real ~/.okto-pulse data. This fixture
    turns either leak into an attributable failure at the FIRST victim test
    instead of mysterious order-dependent breakage."""
    if request.node.path.name in _ENGINE_SWAP_SANCTIONED:
        yield
        return

    from okto_pulse.core.infra.database import get_engine

    env_url = os.environ.get("DATABASE_URL", "")
    if env_url != f"sqlite+aiosqlite:///{_tmpdb}":
        pytest.fail(
            f"DATABASE_URL env poisoned before {request.node.nodeid}: "
            f"{env_url!r} (expected the session temp db {_tmpdb!r}). "
            "Some earlier test/module mutated os.environ without restoring it."
        )
    engine_db = get_engine().url.database
    if engine_db != _tmpdb:
        pytest.fail(
            f"process-global engine hijacked before {request.node.nodeid}: "
            f"bound to {engine_db!r} (expected the session temp db {_tmpdb!r}). "
            "Some earlier test called create_database()/create_app() without "
            "restoring the conftest engine."
        )
    yield


@pytest.fixture(autouse=True)
def _register_test_unit_of_work_factory():
    """R01B FR3: EXPLICIT test/transitional-compat wiring of the relational
    UnitOfWorkFactory seam (``okto_pulse.core.runtime_registry``).

    Production registers the Community factory via the composition root; the core
    no longer self-constructs one (fail-closed). The test harness registers a
    CORE-ONLY provider over the global session factory so the migrated REST/MCP
    consumers resolve a provider. This is test wiring, NOT a runtime fallback —
    reset on teardown so the negative fail-closed test can prove the empty seam."""
    from okto_pulse.core.repositories import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.runtime_registry import (
        register_unit_of_work_factory,
        reset_unit_of_work_factory,
    )

    register_unit_of_work_factory(SQLAlchemyUnitOfWorkFactory(get_session_factory()))
    yield
    reset_unit_of_work_factory()


@pytest.fixture(autouse=True)
def _test_relational_runtime_factory_is_stable():
    """Keep the explicit test relational runtime factory registered."""
    register_relational_runtime_factory(_build_test_relational_runtime)
    yield
    register_relational_runtime_factory(_build_test_relational_runtime)


@pytest.fixture(autouse=True)
def _register_test_relational_effects_port():
    """Register the SQLAlchemy-backed test implementation of relational effects."""
    reset_relational_effects_port_for_tests()
    register_relational_effects_port(_CoreTestRelationalEffects())
    yield
    reset_relational_effects_port_for_tests()


@pytest.fixture(autouse=True)
def _register_test_kg_operational_read_model_port():
    """Register the SQLAlchemy-backed test implementation of KG operational reads."""
    reset_kg_operational_ports_for_tests()
    register_kg_operational_ports(
        read_model=_CoreTestKGOperationalReadModel(),
        worker_queue=_CoreTestKGWorkerQueue(),
        worker_audit=_CoreTestKGWorkerAudit(),
    )
    yield
    reset_kg_operational_ports_for_tests()


@pytest.fixture
def board_handle():
    return bootstrap_board_graph(BOARD_ID)


# ============================================================================
# KG operation tracing helpers (available as fixtures for tests that need them)
# ============================================================================

@pytest.fixture
def kg_tracer(board_id: str):
    """Provide a KG operation tracer that logs all KG operations.

    Usage::

        def test_something(kg_tracer):
            kg_tracer.log("consolidation_begin", {"artifact_id": "spec-123"})
    """
    logger = logging.getLogger(f"test.kg.{board_id}")

    class Tracer:
        def log(self, operation: str, details: dict | None = None) -> None:
            log_kg_event(logger, operation, board_id=board_id, **(details or {}))

        def begin(self, session_id: str, artifact_type: str, artifact_id: str) -> None:
            logger.info(f"KG_BEGIN session={session_id} type={artifact_type} artifact={artifact_id}")

        def commit(self, session_id: str, nodes_added: int) -> None:
            logger.info(f"KG_COMMIT session={session_id} nodes_added={nodes_added}")

        def abort(self, session_id: str, reason: str) -> None:
            logger.warning(f"KG_ABORT session={session_id} reason={reason}")

    return Tracer()


# ============================================================================
# Utility fixtures for testing with timeouts
# ============================================================================

@pytest.fixture
def heartbeat():
    """Provide a heartbeat callback for tests that need to signal progress.

    Usage::

        def test_with_heartbeat(heartbeat):
            # Periodically call heartbeat() to prevent timeout
            for i in range(10):
                do_something()
                heartbeat()
    """
    tracker = TimeoutTracker(max_seconds=_DEFAULT_TIMEOUT)

    def _heartbeat():
        tracker.heartbeat()

    return _heartbeat

"""Shared fixtures for the KG foundation test suite.

Provides:
- Structured logging with file capture per test
- Test lifecycle hooks (start/teardown/end)
- KG operation tracing
- Timeout handling (default 120s per test)
- Fresh environment per test (complete isolation)
"""

import asyncio
import contextlib
from contextlib import asynccontextmanager
import hashlib
import logging
import copy
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
from sqlalchemy import and_, asc, event, exists, func, literal, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

# ---------------------------------------------------------------------------
# Path setup — must happen before any okto_pulse import
# ---------------------------------------------------------------------------

_CORE_SRC = (Path(__file__).parent / ".." / "src").resolve()
_WORKSPACE_ROOT = _CORE_SRC.parent.parent

# Bootstrap the current Core tree before importing the shared checkout resolver.
# The resolver then owns paired-edition precedence and removes stale okto_labs
# roots from both sys.path and PYTHONPATH, so multiprocessing spawn children
# inherit the same authoritative source trees.
_core_source_text = str(_CORE_SRC)
while _core_source_text in sys.path:
    sys.path.remove(_core_source_text)
sys.path.insert(0, _core_source_text)

from okto_pulse.core.application.boundary.repository_checkout import (  # noqa: E402
    activate_repository_checkout_paths,
)

_REPOSITORY_PATHS = activate_repository_checkout_paths(
    anchor_repo=_CORE_SRC.parent,
    required=False,
)
_COMMUNITY_SRC = next(
    (
        checkout.source_root
        for checkout in _REPOSITORY_PATHS.checkouts
        if checkout.edition == "community"
    ),
    None,
)

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

from okto_pulse.core.infra import config as _core_config_module  # noqa: E402
from okto_pulse.core.infra.config import CoreSettings, configure_settings  # noqa: E402


def _require_source_origin(module, source_root: Path, *, edition: str) -> None:
    origin = Path(module.__file__).resolve()
    if not origin.is_relative_to(source_root):
        raise RuntimeError(
            f"{edition} test module resolved outside its local source tree: "
            f"{origin} (expected under {source_root})"
        )


_require_source_origin(_core_config_module, _CORE_SRC, edition="Core")

try:
    from okto_pulse.community import config as _community_config_module  # noqa: E402
    from okto_pulse.community.config import CommunitySettings  # noqa: E402
except ModuleNotFoundError:
    if _COMMUNITY_SRC is not None:
        raise
    _initial_settings = CoreSettings()
else:
    if _COMMUNITY_SRC is not None:
        _require_source_origin(
            _community_config_module,
            _COMMUNITY_SRC,
            edition="Community",
        )
    # Real Community adapters require their edition-owned operational fields.
    # Pure-Core runs without the Community package keep the policy-only model.
    _initial_settings = CommunitySettings()

configure_settings(_initial_settings)

from okto_pulse.core.infra.database import create_database, get_session_factory, init_db  # noqa: E402
from okto_pulse.core.infra import database as _database_mod  # noqa: E402
from okto_pulse.core.ports import schema_lifecycle as _schema_lifecycle  # noqa: E402
from okto_pulse.core.kg.embedding import reset_embedding_provider_cache  # noqa: E402
from kg_schema_testing import bootstrap_board_graph  # noqa: E402
from okto_pulse.core.kg.session_manager import reset_session_manager_for_tests  # noqa: E402
import sqlalchemy_test_models as _models  # noqa: E402, F401
from sqlalchemy_test_models import (  # noqa: E402
    Board,
    Card,
    Agent,
    AgentBoard,
    CanonicalDebt,
    ConsolidationAudit,
    ConsolidationDeadLetter,
    ConsolidationQueue,
    ArtifactDeletionTombstone,
    GlobalUpdateOutbox,
    Ideation,
    KGTickRun,
    KuzuNodeRef,
    Refinement,
    PermissionPreset,
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
from okto_pulse.core.ports.kg_events import (  # noqa: E402
    HISTORICAL_PROGRESS_SETTINGS_KEY,
    KGEventsPoll,
    KGOutboxEvent,
    register_kg_events_reader_port,
    reset_kg_events_reader_port_for_tests,
)
from okto_pulse.core.ports.relational_effects import (  # noqa: E402
    ConsolidationQueueUpsert,
    KGTickRunUpsert,
    RelationalEffectsPort,
    register_relational_effects_port,
    reset_relational_effects_port_for_tests,
)
from okto_pulse.core.ports.relational_application import (  # noqa: E402
    AgentPermissionContext,
    EffectivePermissions,
    PermissionPresetView,
    register_relational_application_adapter,
    reset_relational_application_adapter_for_tests,
)

# Register test-owned relational service adapters. Production Core never builds
# or imports these SQLAlchemy implementations.
import sqlalchemy_test_runtime_settings_service as _settings_svc  # noqa: E402
import sqlalchemy_test_traceability_read_model as _traceability_adapter  # noqa: E402
from sqlalchemy_test_resource_gate_service import (  # noqa: E402
    TestSqlAlchemyResourceGateAdapter,
)
from okto_pulse.core.ports.relational_services import (  # noqa: E402
    register_resource_gate_adapter_factory,
    register_runtime_settings_adapter,
    register_traceability_adapter,
)

register_resource_gate_adapter_factory(TestSqlAlchemyResourceGateAdapter)
register_runtime_settings_adapter(_settings_svc)
register_traceability_adapter(_traceability_adapter)


class _CoreTestDatabaseRuntime:
    """Test-owned SQLAlchemy implementation of the relational runtime port."""

    def __init__(self, engine, session_factory):
        self.engine = engine
        self.session_factory = session_factory
        self._pending_closes = set()

    async def _await_cleanup(self, awaitable):
        task = asyncio.ensure_future(awaitable)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            task.add_done_callback(self._consume_cleanup_exception)
            raise

    @staticmethod
    def _consume_cleanup_exception(task):
        if task.cancelled():
            return
        with contextlib.suppress(BaseException):
            task.exception()

    async def _quiet_cleanup(self, awaitable):
        with contextlib.suppress(BaseException):
            await self._await_cleanup(awaitable)

    async def _cancel_safe_close(self, awaitable):
        task = asyncio.ensure_future(awaitable)
        self._pending_closes.add(task)
        task.add_done_callback(self._pending_closes.discard)
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            raise
        except Exception:
            logging.getLogger(__name__).exception("db.session.cancel_safe_close_failed")

    @asynccontextmanager
    async def transactional_session(self):
        session = self.session_factory()
        try:
            yield session
            await session.commit()
        except BaseException:
            await self._quiet_cleanup(session.rollback())
            raise
        finally:
            await self._quiet_cleanup(session.close())

    @asynccontextmanager
    async def cancel_safe_session_scope(self, session_factory=None):
        scope = (session_factory or self.session_factory)()
        enter = getattr(scope, "__aenter__", None)
        exit_ = getattr(scope, "__aexit__", None)
        if callable(enter) and callable(exit_):
            session = await enter()
        else:
            session = scope
            exit_ = None
        exc_info = (None, None, None)
        try:
            yield session
        except BaseException as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            raise
        finally:
            if exit_ is not None:
                await self._cancel_safe_close(exit_(*exc_info))
            else:
                await self._cancel_safe_close(session.close())

    async def close(self):
        await self._await_cleanup(self.engine.dispose())

    def pool_status(self):
        return self.engine.sync_engine.pool.status()

    def local_database_path(self):
        url = self.engine.url
        if url.get_backend_name() != "sqlite":
            return None
        database = url.database
        if not database or str(database) in {":memory:", "file::memory:"}:
            return None
        return Path(str(database))


def _build_test_relational_runtime(url: str, *, echo: bool = False):
    engine_kwargs: dict = {
        "echo": echo,
        "future": True,
    }
    if url.startswith("postgresql"):
        engine_kwargs.update(
            {
                "pool_size": 10,
                "max_overflow": 20,
                "pool_pre_ping": True,
            }
        )
    elif url.startswith("sqlite"):
        engine_kwargs.update(
            {
                "pool_size": 20,
                "max_overflow": 30,
                "pool_timeout": 10,
                "pool_recycle": 1800,
                "pool_pre_ping": True,
            }
        )

    engine = create_async_engine(url, **engine_kwargs)
    if engine.url.get_backend_name() == "sqlite":

        @event.listens_for(engine.sync_engine, "connect")
        def _install_test_sqlite_pragmas(dbapi_conn, _conn_record):  # noqa: ANN001
            cursor = dbapi_conn.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
                cursor.execute("PRAGMA busy_timeout=30000")
                cursor.execute("PRAGMA synchronous=NORMAL")
                cursor.execute("PRAGMA foreign_keys=ON")
            finally:
                cursor.close()

    session_factory = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return _CoreTestDatabaseRuntime(engine, session_factory)


from okto_pulse.core.runtime_registry import register_relational_runtime_factory  # noqa: E402

register_relational_runtime_factory(_build_test_relational_runtime)


class _CoreTestSchemaLifecycle:
    async def initialize_schema(self) -> None:
        async with _database_mod.get_engine().begin() as conn:
            await conn.run_sync(_models.Base.metadata.create_all)


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

    async def upsert_consolidation_queue_unless_tombstoned(
        self,
        session,
        upsert: ConsolidationQueueUpsert,
    ) -> bool:
        insert = _upsert_insert_for_session(session)
        candidate_id = str(uuid.uuid4())
        admitted_values = select(
            literal(candidate_id),
            literal(upsert.board_id),
            literal(upsert.artifact_type),
            literal(upsert.artifact_id),
            literal("consolidate"),
            literal(0),
            literal(upsert.priority),
            literal(upsert.source),
            literal(upsert.triggered_by_event),
            literal("pending"),
            literal(0),
        ).where(
            ~exists(
                select(1).where(
                    ArtifactDeletionTombstone.board_id == upsert.board_id,
                    ArtifactDeletionTombstone.artifact_type == upsert.artifact_type,
                    ArtifactDeletionTombstone.artifact_id == upsert.artifact_id,
                )
            )
        )
        stmt = (
            insert(ConsolidationQueue)
            .from_select(
                (
                    "id",
                    "board_id",
                    "artifact_type",
                    "artifact_id",
                    "work_kind",
                    "generation",
                    "priority",
                    "source",
                    "triggered_by_event",
                    "status",
                    "attempts",
                ),
                admitted_values,
                include_defaults=False,
            )
            .on_conflict_do_update(
                index_elements=["board_id", "artifact_type", "artifact_id"],
                index_where=ConsolidationQueue.work_kind == "consolidate",
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
            .returning(ConsolidationQueue.id)
        )
        return (await session.execute(stmt)).scalar_one_or_none() is not None

    async def list_board_ids(self, session) -> list[str]:
        result = await session.execute(select(Board.id))
        return list(result.scalars().all())

    async def is_global_recovery_active(self, session) -> bool:
        del session
        return False

    async def fence_kg_tick_publication(self, session) -> bool:
        del session
        return False

    async def read_latest_kg_tick_completed_at(self, session):
        return (
            (
                await session.execute(
                    select(KGTickRun.completed_at)
                    .where(KGTickRun.completed_at.is_not(None))
                    .order_by(KGTickRun.completed_at.desc())
                    .limit(1)
                )
            )
            .scalars()
            .first()
        )

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


def _test_preset_view(preset):
    return PermissionPresetView(
        id=preset.id,
        owner_id=preset.owner_id,
        name=preset.name,
        description=preset.description,
        is_builtin=bool(preset.is_builtin),
        base_preset_id=preset.base_preset_id,
        flags=copy.deepcopy(preset.flags) if preset.flags else preset.flags,
        created_at=preset.created_at,
        updated_at=preset.updated_at,
    )


class _CoreTestPermissionPresetGateway:
    """SQLAlchemy test double for the Community permission-preset adapter."""

    def __init__(self, session):
        self._session = session

    async def get_effective_permissions(self, *, user_id, board_id):
        from okto_pulse.core.infra.permissions import (
            _match_builtin_preset_name,
            map_legacy_permissions,
            resolve_permissions,
        )

        result = await self._session.execute(
            select(Agent).where(Agent.created_by == user_id).limit(1)
        )
        agent = result.scalar_one_or_none()
        agent_flags = None
        preset_flags = None
        preset_name = None
        if agent is not None:
            if isinstance(agent.permission_flags, dict) and agent.permission_flags:
                agent_flags = agent.permission_flags
            elif isinstance(agent.permissions, list) and agent.permissions:
                agent_flags = map_legacy_permissions(agent.permissions)
            if agent.preset_id:
                preset = await self._session.get(PermissionPreset, agent.preset_id)
                if preset is not None and preset.flags:
                    preset_flags = preset.flags
                    preset_name = preset.name
        effective = resolve_permissions(agent_flags, preset_flags, None)
        return EffectivePermissions(
            board_id=board_id,
            preset_name=preset_name or _match_builtin_preset_name(effective.flags),
            flags=effective.flags,
        )

    async def list_presets(self, *, user_id):
        result = await self._session.execute(
            select(PermissionPreset)
            .where(
                (PermissionPreset.is_builtin.is_(True))
                | (PermissionPreset.owner_id == user_id)
            )
            .order_by(PermissionPreset.is_builtin.desc(), PermissionPreset.name)
        )
        return [_test_preset_view(row) for row in result.scalars().all()]

    async def create_preset(self, *, user_id, name, description, flags):
        preset = PermissionPreset(
            id=str(uuid.uuid4()),
            owner_id=user_id,
            name=name,
            description=description or None,
            is_builtin=False,
            flags=copy.deepcopy(flags) if flags else flags,
        )
        self._session.add(preset)
        await self._session.flush()
        await self._session.refresh(preset)
        return _test_preset_view(preset)

    async def clone_preset(
        self, *, source_preset_id, user_id, name, description, flags
    ):
        from okto_pulse.core.infra.permissions import (
            _flatten_registry,
            _get_nested,
            _set_nested,
        )

        source = await self._session.get(PermissionPreset, source_preset_id)
        if source is None:
            return None
        cloned_flags = copy.deepcopy(source.flags) if source.flags else {}
        if flags:
            for path in _flatten_registry(flags):
                value = _get_nested(flags, path)
                if value is not None:
                    _set_nested(cloned_flags, path, value)
        preset = PermissionPreset(
            id=str(uuid.uuid4()),
            owner_id=user_id,
            name=name,
            description=description or source.description,
            is_builtin=False,
            base_preset_id=source_preset_id,
            flags=cloned_flags,
        )
        self._session.add(preset)
        await self._session.flush()
        await self._session.refresh(preset)
        return _test_preset_view(preset)

    async def update_preset(self, *, preset_id, user_id, name, description, flags):
        preset = await self._session.get(PermissionPreset, preset_id)
        if preset is None:
            return None
        if preset.is_builtin:
            raise PermissionError("Built-in presets cannot be modified or deleted")
        if preset.owner_id != user_id:
            raise PermissionError("You can only modify your own presets")
        if name is not None:
            preset.name = name
        if description is not None:
            preset.description = description
        if flags is not None:
            preset.flags = copy.deepcopy(flags)
        await self._session.flush()
        await self._session.refresh(preset)
        return _test_preset_view(preset)

    async def delete_preset(self, *, preset_id, user_id):
        preset = await self._session.get(PermissionPreset, preset_id)
        if preset is None:
            return False
        if preset.is_builtin:
            raise PermissionError("Built-in presets cannot be modified or deleted")
        if preset.owner_id != user_id:
            raise PermissionError("You can only delete your own presets")
        await self._session.delete(preset)
        await self._session.flush()
        return True


class _CoreTestAgentAuthenticationGateway:
    """SQLAlchemy test double for the edition-owned agent lookup port."""

    def __init__(self, session):
        self._session = session

    async def authenticate_agent_by_api_key(self, api_key, *, credential_source):
        from okto_pulse.core.ports import AgentAuthSession

        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        result = await self._session.execute(
            select(Agent).where(
                Agent.api_key_hash == key_hash,
                Agent.is_active.is_(True),
            )
        )
        agent = result.scalar_one_or_none()
        if agent is None:
            return None
        return AgentAuthSession(
            agent_id=agent.id,
            agent_name=agent.name,
            is_active=True,
            metadata={
                "credential_source": credential_source,
                "realm_id": "local",
            },
        )

    async def list_accessible_board_ids_for_agent(self, agent_id):
        result = await self._session.execute(
            select(Board.id)
            .join(AgentBoard, AgentBoard.board_id == Board.id)
            .where(AgentBoard.agent_id == agent_id)
            .order_by(Board.name)
        )
        return list(result.scalars().all())

    async def agent_has_board_access(self, agent_id, board_id):
        result = await self._session.execute(
            select(AgentBoard.id).where(
                AgentBoard.agent_id == agent_id,
                AgentBoard.board_id == board_id,
            )
        )
        return result.scalar_one_or_none() is not None

    async def resolve_agent_permission_context(self, agent_id, *, board_id=None):
        from okto_pulse.core.infra.permissions import resolve_permissions

        agent = await self._session.get(Agent, agent_id)
        if agent is None or not bool(getattr(agent, "is_active", True)):
            return None
        agent_board = None
        if board_id:
            result = await self._session.execute(
                select(AgentBoard).where(
                    AgentBoard.agent_id == agent.id,
                    AgentBoard.board_id == board_id,
                )
            )
            agent_board = result.scalar_one_or_none()
            if agent_board is None:
                return None
        agent_flags = getattr(agent, "permission_flags", None)
        if agent_flags is None:
            permissions = agent.permissions
        else:
            preset_flags = None
            if agent.preset_id:
                preset = await self._session.get(PermissionPreset, agent.preset_id)
                if preset is not None:
                    preset_flags = preset.flags
            board_overrides = (
                agent_board.permission_overrides if agent_board is not None else None
            )
            permissions = resolve_permissions(
                agent_flags,
                preset_flags,
                board_overrides,
            )
        return AgentPermissionContext(
            agent_id=agent.id,
            agent_name=agent.name,
            permissions=permissions,
        )


class _CoreTestAmendmentRevisionApiBackend:
    """SQLAlchemy test double for the Community amendment backend."""

    def __init__(self, session):
        from okto_pulse.core.services.amendment_revision import AmendmentRevisionService

        self._session = session
        self._store = AmendmentRevisionService(session)

    async def get_bug(self, board_id, bug_id):
        return await self._session.get(Card, bug_id)

    async def get_spec(self, board_id, spec_id):
        return await self._session.get(Spec, spec_id)

    async def create_amendment(self, **kwargs):
        return await self._store.create(validation_metadata=None, **kwargs)

    async def get_amendment(self, amendment_id):
        return await self._store.get(amendment_id)

    async def list_amendments_for_bug(
        self, *, board_id, original_spec_id, origin_bug_id
    ):
        return await self._store.list_for_bug(
            board_id=board_id,
            original_spec_id=original_spec_id,
            origin_bug_id=origin_bug_id,
        )

    async def associate_artifacts(self, amendment_id, *, actor, **kwargs):
        return await self._store.associate_artifacts(
            amendment_id,
            actor=actor,
            **kwargs,
        )

    async def set_lineage_state(self, amendment_id, lineage_state, actor):
        return await self._store.set_lineage_state(amendment_id, lineage_state, actor)

    async def set_status(self, amendment_id, new_status, actor):
        return await self._store.set_status(amendment_id, new_status, actor)

    async def path_b_resolution(self, *, board_id, bug_id, candidate_scenario_ids):
        from okto_pulse.core.services.bug_regression_preview import (
            BugRegressionScenarioPreviewError,
            BugRegressionScenarioPreviewService,
        )

        try:
            payload = await BugRegressionScenarioPreviewService(self._session).resolve(
                board_id=board_id,
                bug_id=bug_id,
                candidate_scenario_ids=candidate_scenario_ids or None,
            )
        except BugRegressionScenarioPreviewError as exc:
            return {"available": False, **exc.to_dict()}
        return {
            "available": True,
            "coverage_state": payload.get("coverage_state"),
            "coverage_pending_scenarios": payload.get("coverage_pending_scenarios"),
            "missing_links": payload.get("missing_links"),
            "safe_next_actions": payload.get("safe_next_actions"),
            "next_action": payload.get("next_action"),
            "eligible_regression_artifacts": payload.get(
                "eligible_regression_artifacts"
            ),
            "rejected_regression_artifacts": payload.get(
                "rejected_regression_artifacts"
            ),
            "rejected_scenarios": payload.get("rejected_scenarios"),
            "amendment_revision_id": payload.get("amendment_revision_id"),
        }

    def eligibility(self, amendment):
        from okto_pulse.core.services.amendment_revision import AmendmentRevisionService

        return AmendmentRevisionService.eligibility(amendment)


class _CoreTestRelationalApplicationAdapter:
    """Explicit test-only relational adapter bundle for Core use cases."""

    def permission_presets(self, session):
        return _CoreTestPermissionPresetGateway(session)

    def quality_assessments(self, session):
        _ = session
        from okto_pulse.core.ports.quality_assessment import (
            QualityAssessmentAdapterMissing,
        )

        raise QualityAssessmentAdapterMissing(
            "The Core test adapter has no quality-assessment persistence."
        )

    def quality_assessment_lifecycle(self, session):
        _ = session

        class _QualityAssessmentLifecycleStub:
            async def load_lifecycle_state(self, **_kwargs):
                return (), ()

            async def apply_lifecycle_plan(self, _plan):
                return None

            async def apply_purge_plan(self, plan):
                from datetime import datetime, timezone

                from okto_pulse.core.domain.quality_assessment_lifecycle import (
                    AssessmentPurgePostcondition,
                    AssessmentPurgeResidual,
                )

                return AssessmentPurgePostcondition(
                    target=plan.target,
                    residuals=tuple(
                        AssessmentPurgeResidual(resource=resource, count=0)
                        for resource in plan.deletion_order
                    ),
                    zero_orphans=True,
                    projections_reconciled=True,
                    outbox_reconciled=True,
                    epoch_consistency_preserved=True,
                    verified_at=datetime.now(timezone.utc),
                )

        return _QualityAssessmentLifecycleStub()

    def research_decisions(self, session):
        """Empty RDL port for Core scenarios that do not seed ledger entries."""

        _ = session

        class _ResearchDecisionLedgerStub:
            async def get_snapshot_for_version(self, **_kwargs):
                return None

            async def list_current_entries_with_heads(self, **_kwargs):
                return ()

            async def save_snapshot(self, snapshot):
                return snapshot

            async def save_derivation(self, derivation):
                return derivation

        return _ResearchDecisionLedgerStub()

    def checklists(self, session):
        class _CreateBoardChecklistStub:
            async def apply_binding_cas(
                self,
                binding,
                *,
                expected_version,
                expected_digest,
            ):
                assert expected_version == 0
                assert expected_digest is None
                return binding

            async def get_spec_snapshot(self, *, board_id, spec_id):
                from okto_pulse.core.domain.checklist import ChecklistSpecSnapshot

                spec = await session.get(Spec, spec_id)
                if spec is None or spec.board_id != board_id:
                    return None
                return ChecklistSpecSnapshot(
                    board_id=board_id,
                    spec_id=spec_id,
                    spec_version=spec.version,
                    content_digest="0" * 64,
                    input_digest="1" * 64,
                    status=spec.status.value,
                    archived=bool(spec.archived),
                )

            async def get_binding(self, **_kwargs):
                return None

            async def get_current(self, **_kwargs):
                return None

        return _CreateBoardChecklistStub()

    def amendment_revision_backend(self, session):
        return _CoreTestAmendmentRevisionApiBackend(session)

    def agent_authentication(self, session):
        return _CoreTestAgentAuthenticationGateway(session)


class _CoreTestKGEventsReader:
    """Explicit SQLAlchemy test double for the edition-owned events reader."""

    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def poll(
        self,
        *,
        board_id,
        after,
        limit,
        after_event_id=None,
    ):
        from okto_pulse.core.infra.database import cancel_safe_session_scope

        async with cancel_safe_session_scope(self._session_factory) as session:
            events = await self._query_outbox_rows(
                session,
                board_id=board_id,
                after=after,
                limit=limit,
                after_event_id=after_event_id,
            )
            progress = await self._query_queue_snapshot(session, board_id=board_id)
        return KGEventsPoll(events=events, progress=progress)

    async def replay(
        self,
        *,
        board_id,
        after,
        limit,
        after_event_id=None,
    ):
        from okto_pulse.core.infra.database import cancel_safe_session_scope

        async with cancel_safe_session_scope(self._session_factory) as session:
            return await self._query_outbox_rows(
                session,
                board_id=board_id,
                after=after,
                limit=limit,
                after_event_id=after_event_id,
            )

    async def _query_outbox_rows(
        self,
        session,
        *,
        board_id,
        after,
        limit,
        after_event_id=None,
    ):
        cursor_filter = GlobalUpdateOutbox.created_at > after
        if after_event_id is not None:
            cursor_filter = or_(
                GlobalUpdateOutbox.created_at > after,
                and_(
                    GlobalUpdateOutbox.created_at == after,
                    GlobalUpdateOutbox.event_id > after_event_id,
                ),
            )
        rows = (
            (
                await session.execute(
                    select(GlobalUpdateOutbox)
                    .where(
                        and_(
                            GlobalUpdateOutbox.board_id == board_id,
                            cursor_filter,
                        )
                    )
                    .order_by(
                        asc(GlobalUpdateOutbox.created_at),
                        asc(GlobalUpdateOutbox.event_id),
                    )
                    .limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return [
            KGOutboxEvent(
                event_id=row.event_id,
                session_id=row.session_id,
                event_type=row.event_type,
                created_at=row.created_at,
                payload=dict(row.payload) if isinstance(row.payload, dict) else {},
            )
            for row in rows
        ]

    async def _query_queue_snapshot(self, session, *, board_id):
        rows = (
            await session.execute(
                select(
                    ConsolidationQueue.status, ConsolidationQueue.source, func.count()
                )
                .where(ConsolidationQueue.board_id == board_id)
                .group_by(ConsolidationQueue.status, ConsolidationQueue.source)
            )
        ).all()
        snapshot = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
        historical = {"pending": 0, "claimed": 0, "done": 0, "failed": 0, "paused": 0}
        for status, source, count in rows:
            if status in snapshot:
                snapshot[status] += int(count)
                if source == "historical_backfill":
                    historical[status] += int(count)

        live_total = sum(snapshot.values())
        historical_active = (
            historical["pending"] + historical["claimed"] + historical["paused"]
        )
        historical_total = 0
        if historical_active > 0:
            board = await session.get(Board, board_id)
            if board is not None and isinstance(board.settings, dict):
                state = board.settings.get(HISTORICAL_PROGRESS_SETTINGS_KEY)
                if isinstance(state, dict):
                    try:
                        historical_total = int(state.get("total") or 0)
                    except (TypeError, ValueError):
                        historical_total = 0

        non_historical_total = live_total - sum(historical.values())
        if historical_active > 0 and historical_total > 0:
            snapshot["total"] = max(live_total, historical_total + non_historical_total)
            snapshot["processed"] = max(
                0,
                snapshot["total"]
                - (snapshot["pending"] + snapshot["claimed"] + snapshot["paused"]),
            )
        else:
            snapshot["total"] = live_total
            snapshot["processed"] = snapshot["done"]
        return snapshot


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
            (
                await context.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.board_id == board_id
                    )
                )
            )
            .scalars()
            .all()
        )
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
            (
                await context.execute(
                    select(Ideation).where(Ideation.board_id == board_id)
                )
            )
            .scalars()
            .all()
        )
        refs = (
            (
                await context.execute(
                    select(Refinement).where(Refinement.board_id == board_id)
                )
            )
            .scalars()
            .all()
        )
        specs = (
            (await context.execute(select(Spec).where(Spec.board_id == board_id)))
            .scalars()
            .all()
        )
        sprints = (
            (await context.execute(select(Sprint).where(Sprint.board_id == board_id)))
            .scalars()
            .all()
        )
        cards = (
            (await context.execute(select(Card).where(Card.board_id == board_id)))
            .scalars()
            .all()
        )

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
            ref_children = [
                _refinement_node(r) for r in refs_by_ideation.get(row.id, [])
            ]
            if depth < 2:
                ref_children = []
            tree.append(
                {
                    "id": row.id,
                    "type": "ideation",
                    "title": row.title,
                    **meta,
                    "children": ref_children,
                }
            )
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

    async def graph_node_ref_operation_counts(
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
            (
                await context.execute(
                    select(CanonicalDebt).where(CanonicalDebt.board_id == board_id)
                )
            )
            .scalars()
            .all()
        )
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
            (
                await context.execute(
                    select(ConsolidationDeadLetter).where(
                        ConsolidationDeadLetter.board_id == board_id
                    )
                )
            )
            .scalars()
            .all()
        )
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

    async def list_dead_letter_page(
        self,
        context,
        *,
        board_id: str,
        limit: int,
        offset: int,
    ):
        total = int(
            await context.scalar(
                select(func.count())
                .select_from(ConsolidationDeadLetter)
                .where(ConsolidationDeadLetter.board_id == board_id)
            )
            or 0
        )
        rows = (
            (
                await context.execute(
                    select(ConsolidationDeadLetter)
                    .where(ConsolidationDeadLetter.board_id == board_id)
                    .order_by(ConsolidationDeadLetter.id)
                    .limit(limit)
                    .offset(offset)
                )
            )
            .scalars()
            .all()
        )
        return total, list(rows)

    async def reprocess_dead_letter_rows(
        self,
        context,
        *,
        board_id: str,
        dead_letter_ids,
        limit: int,
    ):
        query = select(ConsolidationDeadLetter).where(
            ConsolidationDeadLetter.board_id == board_id
        )
        if dead_letter_ids:
            query = query.where(ConsolidationDeadLetter.id.in_(dead_letter_ids))
        rows = list(
            (
                await context.execute(
                    query.order_by(
                        ConsolidationDeadLetter.dead_lettered_at.asc()
                    ).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        requeued = []
        already_queued = []
        now = datetime.now(timezone.utc)
        for row in rows:
            existing = (
                await context.execute(
                    select(ConsolidationQueue).where(
                        ConsolidationQueue.board_id == row.board_id,
                        ConsolidationQueue.artifact_type == row.artifact_type,
                        ConsolidationQueue.artifact_id == row.artifact_id,
                    )
                )
            ).scalar_one_or_none()
            target = existing
            if target is None:
                target = ConsolidationQueue(
                    board_id=row.board_id,
                    artifact_type=row.artifact_type,
                    artifact_id=row.artifact_id,
                    priority="high",
                    source="dead_letter_reprocess",
                    status="pending",
                    triggered_at=now,
                    triggered_by_event="dlq_reprocess",
                    attempts=0,
                )
                context.add(target)
                await context.flush()
                bucket = requeued
            else:
                target.status = "pending"
                target.attempts = 0
                target.last_error = None
                target.next_retry_at = None
                target.claimed_at = None
                target.claim_timeout_at = None
                target.worker_id = None
                target.claimed_by_session_id = None
                bucket = already_queued
            bucket.append(
                {
                    "dead_letter_id": row.id,
                    "queue_id": target.id,
                    "artifact_type": row.artifact_type,
                    "artifact_id": row.artifact_id,
                }
            )
            await context.delete(row)
        return {
            "success": True,
            "requested": len(dead_letter_ids) if dead_letter_ids else None,
            "selected": len(rows),
            "requeued": requeued,
            "already_queued": already_queued,
            "requeued_count": len(requeued),
            "already_queued_count": len(already_queued),
        }

    async def retry_pending_entry(
        self,
        context,
        *,
        board_id: str,
        queue_entry_id: str,
        recursive: bool = False,
    ):
        from okto_pulse.community.adapters.kg_operational import (
            CommunitySqlAlchemyKGWorkerQueue,
        )

        return await CommunitySqlAlchemyKGWorkerQueue().retry_pending_entry(
            context,
            board_id=board_id,
            queue_entry_id=queue_entry_id,
            recursive=recursive,
        )


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


@pytest_asyncio.fixture
async def preserve_relational_runtime():
    """Restore a test-swapped relational runtime without leaking its engine."""

    from okto_pulse.core.ports.relational_runtime import (
        configure_database_runtime,
        resolve_database_runtime,
    )

    original = resolve_database_runtime()
    yield
    try:
        current = resolve_database_runtime()
    except RuntimeError:
        current = None
    if current is original:
        return
    try:
        close = getattr(current, "close", None)
        if callable(close):
            await close()
    finally:
        configure_database_runtime(runtime=original)


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
            tmp.write_text(
                json.dumps(dict(state), indent=2, sort_keys=True), encoding="utf-8"
            )
            tmp.replace(base / "state.json")

    reset_telemetry_state_carrier_for_tests()
    register_telemetry_state_carrier(_TestTelemetryStateCarrier())
    yield
    reset_telemetry_state_carrier_for_tests()


@pytest.fixture(autouse=True)
def _reset_telemetry_effect_config_provider(tmp_path: Path):
    """Provide an explicit test-only telemetry runtime adapter."""
    from okto_pulse.core.telemetry.effect_config_registry import (
        register_telemetry_effect_config_provider,
        reset_telemetry_effect_config_provider_for_tests,
    )

    class _TestTelemetryEffectConfigProvider:
        def state_ref(self, settings) -> str:
            raw = getattr(settings, "metrics_dir", "") or str(tmp_path / "metrics")
            return str(Path(raw).resolve())

        def delivery_target(self, settings) -> str:
            return str(getattr(settings, "metrics_beacon_url", "") or "")

    reset_telemetry_effect_config_provider_for_tests()
    register_telemetry_effect_config_provider(_TestTelemetryEffectConfigProvider())
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
    func_name = getattr(request.node, "function_name", None) or getattr(
        request.node, "name", request.node.nodeid
    )
    logger.info(
        f"TEST_METADATA class={request.node.cls.__name__ if request.node.cls else 'N/A'} "
        f"function={func_name} "
        f"params={dict(request.node.callspec.params) if hasattr(request.node, 'callspec') and request.node.callspec else '{}'}"
    )

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
_KG_HEALTH_PROBE_TEST_DRAIN_TIMEOUT_S = 30.0


def _assert_kg_health_probes_drained(*, nodeid: str, phase: str) -> None:
    """Fail closed when a test leaves runtime-owned KG health work behind.

    The public application lifecycle facade is intentional here: tests exercise
    the same ownership boundary used by an edition before it closes graph
    handles. A finite deadline keeps a broken probe from hanging the suite,
    while the explicit failure prevents the deadline from hiding a leak.
    """

    from okto_pulse.core.services.application_kg import drain_kg_health_probes

    try:
        remaining = drain_kg_health_probes(
            timeout_s=_KG_HEALTH_PROBE_TEST_DRAIN_TIMEOUT_S,
        )
    except Exception as exc:
        pytest.fail(
            "kg_health_probe_test_isolation_failed:"
            f"phase={phase}:nodeid={nodeid}:error={type(exc).__name__}",
            pytrace=False,
        )
    if remaining:
        pytest.fail(
            "kg_health_probe_test_isolation_timeout:"
            f"phase={phase}:nodeid={nodeid}:remaining={remaining}:"
            f"timeout_s={_KG_HEALTH_PROBE_TEST_DRAIN_TIMEOUT_S}",
            pytrace=False,
        )

    logging.getLogger("test.kg.health_probe_isolation").debug(
        "KG_HEALTH_PROBE_DRAIN phase=%s nodeid=%s remaining=0",
        phase,
        nodeid,
    )


def _get_timeout(request: pytest.FixtureRequest) -> float:
    """Extract timeout from pytest.mark.timeout or use default."""
    mark = request.node.get_closest_marker("timeout")
    if mark:
        return (
            float(mark.args[0])
            if mark.args
            else float(mark.kwargs.get("seconds", _DEFAULT_TIMEOUT))
        )
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
    reset_embedding_provider_cache()
    _reset_commit_health_cache()

    logger = get_test_logger(request.node.nodeid)
    logger.debug("ISOLATION: singletons reset (session_mgr, embedding_cache)")

    yield

    # Post-test: one more reset to ensure clean state for next test
    reset_session_manager_for_tests()
    reset_embedding_provider_cache()
    _reset_commit_health_cache()
    logger.debug("ISOLATION: singletons reset after teardown")


@pytest.fixture(autouse=True)
def _coordination_test_fakes():
    """Install explicit coordination fakes used by processor tests."""
    from okto_pulse.core.ports.coordination import (
        register_coordination_providers,
        reset_coordination_providers_for_tests,
    )
    from coordination_fakes import (
        FakeClaimRepository,
        FakeLeaseProvider,
        FakeWriteLockPort,
    )

    register_coordination_providers(
        claim_repository=FakeClaimRepository(),
        lease_provider=FakeLeaseProvider(),
        write_lock_port=FakeWriteLockPort(),
    )
    yield
    reset_coordination_providers_for_tests()


@pytest.fixture(autouse=True)
def _knowledge_propagation_legacy_test_port(request: pytest.FixtureRequest):
    """Make legacy Knowledge reads explicit in Core-only test compositions.

    Production composition remains fail-closed when the edition-owned port is
    absent.  Tests that exercise the port registry itself own that registry and
    therefore intentionally opt out of this compatibility fake.
    """

    from okto_pulse.core.ports.knowledge_propagation import (
        KnowledgePropagationScope,
        register_knowledge_propagation_port,
        reset_knowledge_propagation_port_for_tests,
    )

    if (
        request.node.path.name == "test_knowledge_propagation_port.py"
        or "_register_legacy_knowledge_scope_port" in request.fixturenames
    ):
        yield
        return

    class _ExplicitLegacyKnowledgePropagationPort:
        async def load_scope(self, _context, lookup):
            return KnowledgePropagationScope(
                target=lookup.target,
                scope_revision=0,
                v2_active=False,
                selection_state=None,
            )

        async def get_idempotency_entry(self, _context, _lookup):
            raise AssertionError(
                "legacy Knowledge test port does not support mutations"
            )

        async def load_parent_evidence(self, _context, _lookup):
            raise AssertionError(
                "legacy Knowledge test port does not support mutations"
            )

        async def stage_mutation(self, _context, _plan):
            raise AssertionError(
                "legacy Knowledge test port does not support mutations"
            )

        async def stage_attempt(self, _context, _attempt):
            raise AssertionError(
                "legacy Knowledge test port does not support mutations"
            )

    reset_knowledge_propagation_port_for_tests()
    register_knowledge_propagation_port(_ExplicitLegacyKnowledgePropagationPort())
    try:
        yield
    finally:
        reset_knowledge_propagation_port_for_tests()


@pytest.fixture(autouse=True)
def _domain_event_publisher_test_fake():
    """Install the explicit relational publisher used by integration tests."""
    from okto_pulse.core.ports.domain_event_delivery import (
        register_domain_event_fact_reader,
        register_domain_event_publisher,
        reset_domain_event_publisher_for_tests,
    )
    from sqlalchemy_domain_event_delivery_store import (
        TestSqlAlchemyDomainEventFactReader,
        TestSqlAlchemyDomainEventPublisher,
    )

    register_domain_event_publisher(TestSqlAlchemyDomainEventPublisher())
    register_domain_event_fact_reader(TestSqlAlchemyDomainEventFactReader())
    yield
    reset_domain_event_publisher_for_tests()


@pytest.fixture(autouse=True)
def _queue_health_test_reader():
    from okto_pulse.core.ports.queue_health import (
        register_queue_health_read_port,
        reset_queue_health_read_port_for_tests,
    )
    from sqlalchemy_queue_health_reader import TestSqlAlchemyQueueHealthReader

    register_queue_health_read_port(TestSqlAlchemyQueueHealthReader())
    yield
    reset_queue_health_read_port_for_tests()


@pytest.fixture(autouse=True)
def _canonical_debt_test_store():
    from okto_pulse.core.ports.canonical_debt import (
        register_canonical_debt_store,
        reset_canonical_debt_store_for_tests,
    )
    from sqlalchemy_canonical_debt_store import TestSqlAlchemyCanonicalDebtStore

    register_canonical_debt_store(TestSqlAlchemyCanonicalDebtStore())
    yield
    reset_canonical_debt_store_for_tests()


@pytest.fixture(autouse=True)
def _cognitive_effectiveness_test_reader():
    from okto_pulse.core.ports.cognitive_effectiveness import (
        register_cognitive_effectiveness_read_port,
        reset_cognitive_effectiveness_read_port_for_tests,
    )
    from sqlalchemy_cognitive_effectiveness_reader import (
        TestSqlAlchemyCognitiveEffectivenessReader,
    )

    register_cognitive_effectiveness_read_port(
        TestSqlAlchemyCognitiveEffectivenessReader()
    )
    yield
    reset_cognitive_effectiveness_read_port_for_tests()


@pytest.fixture(autouse=True)
def _skip_override_test_reader():
    from okto_pulse.core.ports.skip_overrides import (
        register_skip_override_read_port,
        reset_skip_override_read_port_for_tests,
    )
    from sqlalchemy_skip_override_reader import TestSqlAlchemySkipOverrideReader

    register_skip_override_read_port(TestSqlAlchemySkipOverrideReader())
    yield
    reset_skip_override_read_port_for_tests()


@pytest.fixture(autouse=True)
def _discovery_catalog_test_reader():
    from okto_pulse.core.ports.discovery_catalog import (
        register_discovery_catalog_read_port,
        reset_discovery_catalog_read_port_for_tests,
    )
    from sqlalchemy_discovery_catalog_reader import (
        TestSqlAlchemyDiscoveryCatalogReader,
    )

    register_discovery_catalog_read_port(TestSqlAlchemyDiscoveryCatalogReader())
    yield
    reset_discovery_catalog_read_port_for_tests()


@pytest.fixture(autouse=True)
def _amendment_revision_test_store():
    from okto_pulse.core.ports.amendment_revision import (
        register_amendment_revision_store,
        reset_amendment_revision_store_for_tests,
    )
    from sqlalchemy_amendment_revision_store import (
        TestSqlAlchemyAmendmentRevisionStore,
    )

    register_amendment_revision_store(TestSqlAlchemyAmendmentRevisionStore())
    yield
    reset_amendment_revision_store_for_tests()


@pytest.fixture(autouse=True)
def _parent_artifact_test_reader():
    from okto_pulse.core.ports.parent_artifact import (
        register_parent_artifact_read_port,
        reset_parent_artifact_read_port_for_tests,
    )
    from sqlalchemy_parent_artifact_reader import TestSqlAlchemyParentArtifactReader

    register_parent_artifact_read_port(TestSqlAlchemyParentArtifactReader())
    yield
    reset_parent_artifact_read_port_for_tests()


@pytest.fixture(autouse=True)
def _architecture_legacy_test_reader():
    from okto_pulse.core.ports.architecture_legacy import (
        register_architecture_legacy_snapshot_read_port,
        reset_architecture_legacy_snapshot_read_port_for_tests,
    )
    from sqlalchemy_architecture_legacy_reader import (
        TestSqlAlchemyArchitectureLegacySnapshotReader,
    )

    register_architecture_legacy_snapshot_read_port(
        TestSqlAlchemyArchitectureLegacySnapshotReader()
    )
    yield
    reset_architecture_legacy_snapshot_read_port_for_tests()


@pytest.fixture(autouse=True)
def _bug_regression_preview_test_reader():
    from okto_pulse.core.ports.bug_regression_preview import (
        register_bug_regression_preview_read_port,
        reset_bug_regression_preview_read_port_for_tests,
    )
    from sqlalchemy_bug_regression_preview_reader import (
        TestSqlAlchemyBugRegressionPreviewReader,
    )

    register_bug_regression_preview_read_port(
        TestSqlAlchemyBugRegressionPreviewReader()
    )
    yield
    reset_bug_regression_preview_read_port_for_tests()


@pytest.fixture(autouse=True)
def _discovery_selector_test_reader():
    from okto_pulse.core.ports.discovery_selector import (
        register_discovery_selector_read_port,
        reset_discovery_selector_read_port_for_tests,
    )
    from sqlalchemy_discovery_selector_reader import (
        TestSqlAlchemyDiscoverySelectorReader,
    )

    register_discovery_selector_read_port(TestSqlAlchemyDiscoverySelectorReader())
    yield
    reset_discovery_selector_read_port_for_tests()


@pytest.fixture(autouse=True)
def _board_relational_cleanup_test_port():
    from okto_pulse.core.ports.board_relational_cleanup import (
        register_board_relational_cleanup_port,
        reset_board_relational_cleanup_port_for_tests,
    )
    from sqlalchemy_board_cleanup import TestSqlAlchemyBoardRelationalCleanup

    register_board_relational_cleanup_port(TestSqlAlchemyBoardRelationalCleanup())
    yield
    reset_board_relational_cleanup_port_for_tests()


@pytest.fixture(autouse=True)
def _structured_spec_test_store():
    from okto_pulse.core.ports.structured_spec import (
        register_structured_spec_store,
        reset_structured_spec_store_for_tests,
    )
    from sqlalchemy_structured_spec_store import TestSqlAlchemyStructuredSpecStore

    register_structured_spec_store(TestSqlAlchemyStructuredSpecStore())
    yield
    reset_structured_spec_store_for_tests()


@pytest.fixture(autouse=True)
def _requirement_lint_writer_hook():
    from okto_pulse.core.ports.requirement_lint import (
        RequirementLintWriteResult,
        register_requirement_lint_writer_hook,
        reset_requirement_lint_writer_hook_for_tests,
    )

    class _ContractTestRequirementLintHook:
        async def stage_requirement_lint(self, context, command):  # noqa: ANN001
            del context
            return RequirementLintWriteResult(
                receipt_id=(
                    f"qar_test_{command.spec_id}_{command.spec_version}_"
                    f"{command.writer.value}"
                ),
                head_revision=command.spec_version,
                evaluated_rule_count=1,
                finding_count=0,
            )

    register_requirement_lint_writer_hook(_ContractTestRequirementLintHook())
    yield
    reset_requirement_lint_writer_hook_for_tests()


@pytest.fixture(autouse=True)
def _effective_resource_test_port():
    from okto_pulse.core.ports.effective_resource import (
        register_effective_resource_persistence_port,
        reset_effective_resource_persistence_port_for_tests,
    )
    from sqlalchemy_effective_resource import (
        TestSqlAlchemyEffectiveResourcePersistence,
    )

    register_effective_resource_persistence_port(
        TestSqlAlchemyEffectiveResourcePersistence()
    )
    yield
    reset_effective_resource_persistence_port_for_tests()


@pytest.fixture(autouse=True)
def _spec_resource_propagation_test_store():
    from okto_pulse.core.ports.spec_resource_propagation import (
        register_spec_resource_propagation_store,
        reset_spec_resource_propagation_store_for_tests,
    )
    from sqlalchemy_spec_resource_propagation_store import (
        TestSqlAlchemySpecResourcePropagationStore,
    )

    register_spec_resource_propagation_store(
        TestSqlAlchemySpecResourcePropagationStore()
    )
    yield
    reset_spec_resource_propagation_store_for_tests()


@pytest.fixture(autouse=True)
def _critical_context_test_reader():
    from okto_pulse.core.ports.critical_context import (
        register_critical_context_read_port,
        reset_critical_context_read_port_for_tests,
    )
    from sqlalchemy_critical_context_reader import (
        TestSqlAlchemyCriticalContextReader,
    )

    register_critical_context_read_port(TestSqlAlchemyCriticalContextReader())
    yield
    reset_critical_context_read_port_for_tests()


@pytest.fixture(autouse=True)
def _default_board_configuration_test_store():
    from okto_pulse.core.ports.default_board_configuration import (
        register_default_board_configuration_store,
        reset_default_board_configuration_store_for_tests,
    )
    from sqlalchemy_default_board_configuration_store import (
        TestSqlAlchemyDefaultBoardConfigurationStore,
    )

    register_default_board_configuration_store(
        TestSqlAlchemyDefaultBoardConfigurationStore()
    )
    yield
    reset_default_board_configuration_store_for_tests()


@pytest.fixture(autouse=True)
def _design_system_test_store():
    from okto_pulse.core.ports.design_system import (
        register_design_system_store,
        reset_design_system_store_for_tests,
    )
    from sqlalchemy_design_system_store import TestSqlAlchemyDesignSystemStore

    register_design_system_store(TestSqlAlchemyDesignSystemStore())
    yield
    reset_design_system_store_for_tests()


@pytest.fixture(autouse=True)
def _global_outbox_test_store():
    from okto_pulse.core.ports.global_outbox import (
        register_global_outbox_store,
        reset_global_outbox_store_for_tests,
    )
    from sqlalchemy_global_outbox_store import TestSqlAlchemyGlobalOutboxStore

    register_global_outbox_store(TestSqlAlchemyGlobalOutboxStore())
    yield
    reset_global_outbox_store_for_tests()


@pytest.fixture(autouse=True)
def _consolidation_test_store():
    from okto_pulse.core.ports.consolidation import (
        register_consolidation_persistence_port,
        reset_consolidation_persistence_port_for_tests,
    )
    from okto_pulse.core.ports.reconcile_intent import (
        register_reconcile_intent_port,
        reset_reconcile_intent_port_for_tests,
    )
    from okto_pulse.core.ports.tombstone import (
        register_tombstone_port,
        reset_tombstone_port_for_tests,
    )
    from sqlalchemy_consolidation_store import (
        TestSqlAlchemyConsolidationPersistence,
    )

    adapter = TestSqlAlchemyConsolidationPersistence()
    register_consolidation_persistence_port(adapter)
    register_tombstone_port(adapter)
    register_reconcile_intent_port(adapter)
    yield
    reset_consolidation_persistence_port_for_tests()
    reset_tombstone_port_for_tests()
    reset_reconcile_intent_port_for_tests()


@pytest.fixture(autouse=True)
def _stale_sweep_test_port_reset():
    from okto_pulse.core.ports.stale_sweep import (
        reset_stale_sweep_port_for_tests,
    )

    yield
    reset_stale_sweep_port_for_tests()


@pytest.fixture(autouse=True)
def _kg_health_test_reader():
    from okto_pulse.core.ports.kg_health import (
        register_kg_health_read_port,
        reset_kg_health_read_port_for_tests,
    )
    from sqlalchemy_kg_health_reader import TestSqlAlchemyKGHealthReader

    register_kg_health_read_port(TestSqlAlchemyKGHealthReader())
    yield
    reset_kg_health_read_port_for_tests()


@pytest.fixture(autouse=True)
def _kg_governance_test_store():
    from okto_pulse.core.ports.kg_governance import (
        register_kg_governance_store,
        reset_kg_governance_store_for_tests,
    )
    from sqlalchemy_kg_governance_store import TestSqlAlchemyKGGovernanceStore

    register_kg_governance_store(TestSqlAlchemyKGGovernanceStore())
    yield
    reset_kg_governance_store_for_tests()


@pytest.fixture(autouse=True)
def _discovery_execution_test_reader():
    from okto_pulse.core.ports.discovery_execution import (
        register_discovery_execution_read_port,
        reset_discovery_execution_read_port_for_tests,
    )
    from sqlalchemy_discovery_execution_reader import (
        TestSqlAlchemyDiscoveryExecutionReader,
    )

    register_discovery_execution_read_port(TestSqlAlchemyDiscoveryExecutionReader())
    yield
    reset_discovery_execution_read_port_for_tests()


@pytest.fixture(autouse=True)
def _analytics_test_reader():
    from okto_pulse.core.ports.analytics_read import (
        register_analytics_read_port,
        reset_analytics_read_port_for_tests,
    )
    from sqlalchemy_analytics_read import TestSqlAlchemyAnalyticsReader

    register_analytics_read_port(TestSqlAlchemyAnalyticsReader())
    yield
    reset_analytics_read_port_for_tests()


@pytest.fixture(autouse=True)
def _architecture_test_persistence():
    from okto_pulse.core.ports.architecture_persistence import (
        register_architecture_persistence_port,
        reset_architecture_persistence_port_for_tests,
    )
    from sqlalchemy_architecture_persistence import (
        TestSqlAlchemyArchitecturePersistence,
    )

    register_architecture_persistence_port(TestSqlAlchemyArchitecturePersistence())
    yield
    reset_architecture_persistence_port_for_tests()


@pytest.fixture(autouse=True)
def _application_test_persistence():
    from okto_pulse.core.ports.application_persistence import (
        register_application_persistence_port,
        reset_application_persistence_port_for_tests,
    )
    from sqlalchemy_application_persistence import (
        TestSqlAlchemyApplicationPersistence,
    )

    register_application_persistence_port(TestSqlAlchemyApplicationPersistence())
    yield
    reset_application_persistence_port_for_tests()


@pytest.fixture(autouse=True)
def _kg_registry_test_fakes(request: pytest.FixtureRequest):
    """R-P2-03: the KG registry no longer lazy-builds implicit Onda A defaults.

    The test suite configures the embedded fakes EXPLICITLY via ``defaults_factory``
    (the sanctioned test/fake route) so it is literal that tests run on fakes; a
    test that needs a specific composition just reconfigures the registry. Real
    runtime must supply a ``base_registry`` (Community adapters) instead.

    Health jobs are drained on both sides of every test before this fixture
    replaces or discards the graph registry. This ordering prevents a probe
    spawned by one test from opening or holding a graph handle during the next
    test, without turning a stuck job into a silent cleanup success.
    """
    from kg_registry_testing import configure_test_kg_registry
    from okto_pulse.core.kg.interfaces.registry import reset_registry_for_tests

    _assert_kg_health_probes_drained(
        nodeid=request.node.nodeid,
        phase="before_registry_setup",
    )
    reset_registry_for_tests()
    configure_test_kg_registry()
    yield
    _assert_kg_health_probes_drained(
        nodeid=request.node.nodeid,
        phase="before_registry_teardown",
    )
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
_ENGINE_SWAP_SANCTIONED = frozenset(
    {
        "test_kg_governance.py",
        "test_kg_dedup_nc8.py",
        "test_kg_dedup_migration.py",
        "test_kg_pipeline_e2e.py",
        "test_kg_real_integration.py",
    }
)


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
    from sqlalchemy_test_unit_of_work import SQLAlchemyUnitOfWorkFactory
    from okto_pulse.core.runtime_registry import (
        register_unit_of_work_factory,
        reset_unit_of_work_factory,
    )

    register_unit_of_work_factory(SQLAlchemyUnitOfWorkFactory(get_session_factory()))
    yield
    reset_unit_of_work_factory()


@pytest.fixture(autouse=True)
def _register_test_relational_application_adapter():
    """Compose the test-only relational application adapter explicitly."""

    reset_relational_application_adapter_for_tests()
    register_relational_application_adapter(_CoreTestRelationalApplicationAdapter())
    yield
    reset_relational_application_adapter_for_tests()


@pytest.fixture(autouse=True)
def _register_test_mcp_host_provider():
    """Use an explicit test host while Core exercises the command catalog."""

    from okto_pulse.core.ports.mcp_host import (
        register_mcp_host_provider,
        reset_mcp_host_provider_for_tests,
    )

    class _CoreTestMcpHost:
        def active_credential(self):
            return None

        def build_asgi_app(
            self,
            catalog,
            *,
            resource_catalog,
            projection_identity,
            trace_sink=None,
        ):
            _ = (catalog, resource_catalog, projection_identity, trace_sink)

            async def _catalog_unavailable(scope, receive, send):
                _ = (scope, receive, send)

            return self.wrap_session_middleware(_catalog_unavailable)

        def mount(
            self,
            app,
            catalog,
            *,
            mount_path,
            resource_catalog,
            projection_identity,
            trace_sink=None,
        ):
            app.mount(
                mount_path,
                self.build_asgi_app(
                    catalog,
                    resource_catalog=resource_catalog,
                    projection_identity=projection_identity,
                    trace_sink=trace_sink,
                ),
            )

        def wrap_session_middleware(self, app):
            from okto_pulse.core.ports import mcp_credential_from_sources

            class _TestCredentialMiddleware:
                def __init__(self, downstream):
                    self._downstream = downstream

                async def __call__(self, scope, receive, send):
                    if scope.get("type") == "http":
                        query = scope.get("query_string", b"").decode("utf-8")
                        query_value = next(
                            (
                                part.split("=", 1)[1]
                                for part in query.split("&")
                                if part.startswith("api_key=") and "=" in part
                            ),
                            None,
                        )
                        headers = {
                            key.decode("latin-1").lower(): value.decode("latin-1")
                            for key, value in scope.get("headers", [])
                        }
                        credential = mcp_credential_from_sources(
                            query_param=query_value,
                            x_api_key_header=headers.get("x-api-key"),
                            authorization_header=headers.get("authorization"),
                        )
                        if credential is not None:
                            scope["okto_pulse.mcp_credential"] = credential
                    await self._downstream(scope, receive, send)

            return _TestCredentialMiddleware(app)

    reset_mcp_host_provider_for_tests()
    register_mcp_host_provider(_CoreTestMcpHost())
    yield
    reset_mcp_host_provider_for_tests()


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


@pytest_asyncio.fixture(autouse=True)
async def _register_test_kg_events_reader_port():
    """Compose the test-only KG event reader and drain pollers between tests."""

    from okto_pulse.core.application.kg_events_hub import (
        configure_kg_events_hub,
        shutdown_kg_events_hub,
    )

    await shutdown_kg_events_hub()
    reset_kg_events_reader_port_for_tests()
    reader = _CoreTestKGEventsReader(get_session_factory())
    register_kg_events_reader_port(reader)
    configure_kg_events_hub(reader)
    yield
    await shutdown_kg_events_hub()
    reset_kg_events_reader_port_for_tests()


@pytest.fixture
def kg_events_reader():
    """Return an isolated test reader for unit-level hub construction."""

    return _CoreTestKGEventsReader(get_session_factory())


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
            logger.info(
                f"KG_BEGIN session={session_id} type={artifact_type} artifact={artifact_id}"
            )

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

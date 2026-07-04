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
from importlib import util as importlib_util
from pathlib import Path

import pytest
import pytest_asyncio

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

from okto_pulse.core.infra.database import (  # noqa: E402
    create_database,
    get_session_factory,
    init_db,
)
from okto_pulse.core.kg.embedding import reset_embedding_provider_cache  # noqa: E402
from okto_pulse.core.kg.schema import bootstrap_board_graph  # noqa: E402
from okto_pulse.core.kg.session_manager import reset_session_manager_for_tests  # noqa: E402
from okto_pulse.core.kg.workers import reset_cleanup_worker_for_tests  # noqa: E402
from okto_pulse.core.models import db as _models  # noqa: E402, F401
# Ensure AppSetting (0.1.4) is registered with Base before init_db() runs;
# otherwise the app_settings table is missing and runtime settings tests fail.
from okto_pulse.core.services import settings_service as _settings_svc  # noqa: E402, F401


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
def _reset_sqlite_pragma_installer_seam():
    """R01B TR5: keep the SQLite PRAGMA installer seam EMPTY for each test so any
    ``create_database()`` call resolves the EXPLICIT core-default fallback (the
    three historical PRAGMAs: WAL + busy_timeout=30000 + synchronous=NORMAL, NO
    foreign_keys) — identical to the prior inline behavior. This is the core-only
    compat path, NOT the Community runtime path (which registers the UNION
    installer). Reset before AND after so a test that registers a Community-style
    installer cannot leak ``foreign_keys=ON`` semantics into the next test."""
    from okto_pulse.core.runtime_registry import reset_sqlite_pragma_installer

    reset_sqlite_pragma_installer()
    yield
    reset_sqlite_pragma_installer()


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

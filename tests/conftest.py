"""Shared fixtures for the KG foundation test suite.

Provides:
- Structured logging with file capture per test
- Test lifecycle hooks (start/teardown/end)
- KG operation tracing
- Timeout handling (default 120s per test)
- Fresh environment per test (complete isolation)
"""

import asyncio
import logging
import os
import sys
import tempfile
import time
from pathlib import Path

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Path setup — must happen before any okto_pulse import
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).parent / ".." / "src"))

# ---------------------------------------------------------------------------
# Test logging infrastructure (must be imported early)
# ---------------------------------------------------------------------------

from test_logging import (  # noqa: E402
    LOG_DIR,
    TestLifecycleLogger,
    TimeoutTracker,
    cleanup_test_logging,
    get_test_logger,
    log_kg_event,
    log_kg_operation,
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
    get_engine,
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

    logger = get_test_logger(request.node.nodeid)
    logger.debug("ISOLATION: singletons reset (session_mgr, cleanup_worker, embedding_cache)")

    yield

    # Post-test: one more reset to ensure clean state for next test
    reset_session_manager_for_tests()
    reset_cleanup_worker_for_tests()
    reset_embedding_provider_cache()
    logger.debug("ISOLATION: singletons reset after teardown")


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

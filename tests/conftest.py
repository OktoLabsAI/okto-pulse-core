"""Shared fixtures for the KG foundation test suite."""

import asyncio
import os
import sys
import tempfile

import pytest
import pytest_asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# Set env before any okto_pulse import
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


@pytest_asyncio.fixture(scope="session", autouse=True, loop_scope="session")
async def _db_init():
    """Create the SQLite schema once per session.

    Pinned to ``loop_scope="session"`` so the connection pool warmed up here
    is reused by every async test (function-scoped loops would re-bind
    aiosqlite handles and race the worker singletons).
    """
    create_database(f"sqlite+aiosqlite:///{_tmpdb}", echo=False)
    await init_db()
    yield


@pytest.fixture(autouse=True)
def _reset_singletons():
    reset_session_manager_for_tests()
    reset_cleanup_worker_for_tests()
    reset_embedding_provider_cache()


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

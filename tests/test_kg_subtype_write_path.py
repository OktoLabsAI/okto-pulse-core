"""MKG-E C3 — subtype opt-in fail-closed on the write path (scenario S3).

Declared kind_of persists on the created node; undeclared kind_of aborts
the commit with kg_subtype_undeclared (declared_subtypes listed, graph
intact); candidates WITHOUT kind_of keep the current flow byte-compatible.
"""

from __future__ import annotations

import asyncio
import gc
import shutil
import tempfile
from pathlib import Path

import pytest
from kg_registry_testing import configure_real_graph_test_kg_registry

from okto_pulse.core.kg.primitives import KGPrimitiveError
from okto_pulse.core.ports.kg_subtype_registry import (
    SubtypeDeclaration,
    require_node_subtype_registry,
)

from test_kg_dedup_nc8 import (  # noqa: F401  (harness reuse)
    _bootstrap_test_board,
)
from test_kg_provenance_commit_fill import (  # noqa: F401  (harness reuse)
    _drive_with_provenance,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _restore_conftest_engine():
    from okto_pulse.core.infra.database import create_database, get_engine

    prior_url = str(get_engine().url)
    yield
    if str(get_engine().url) != prior_url:
        create_database(prior_url, echo=False)


@pytest.fixture
def subtype_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_subtypewp_"))
    db_path = base / "pulse.db"
    kg_path = base / "kg"
    kg_path.mkdir(parents=True, exist_ok=True)

    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("KG_BASE_DIR", str(kg_path))
    monkeypatch.setenv("KG_CLEANUP_ENABLED", "false")
    monkeypatch.setenv("KG_EMBEDDING_MODE", "stub")
    configure_real_graph_test_kg_registry()

    yield base

    try:
        from kg_schema_testing import close_all_connections

        close_all_connections()
    except Exception:
        pass
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


def _kind_of_of(board_id: str, artifact_ref: str):
    from kg_schema_testing import open_board_connection

    with open_board_connection(board_id) as (_kdb, kconn):
        res = kconn.execute(
            "MATCH (n:Entity) WHERE n.source_artifact_ref = $r "
            "RETURN n.kind_of LIMIT 1",
            {"r": artifact_ref},
        )
        try:
            return res.get_next()[0] if res.has_next() else "__absent__"
        finally:
            try:
                res.close()
            except Exception:
                pass


async def test_s3_declared_kind_of_persists(subtype_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    registry = require_node_subtype_registry()
    await registry.declare(
        SubtypeDeclaration(
            node_type="Entity", kind_of="security_control", created_by="t"
        )
    )

    await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-E] Subtipo declarado", content="c",
        provenance={"kind_of": "security_control"},
    )
    assert _kind_of_of(board_id, artifact_ref) == "security_control"


async def test_s3_undeclared_kind_of_rejects_commit_graph_intact(
    subtype_tempdir, monkeypatch
):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    registry = require_node_subtype_registry()
    await registry.declare(
        SubtypeDeclaration(
            node_type="Entity", kind_of="security_control", created_by="t"
        )
    )

    with pytest.raises(KGPrimitiveError) as excinfo:
        await _drive_with_provenance(
            session_factory, board_id, artifact_ref,
            "[MKG-E] Subtipo fantasma", content="c",
            provenance={"kind_of": "compliance_req"},
        )
    assert excinfo.value.code == "kg_subtype_undeclared"
    details = excinfo.value.details or {}
    assert details.get("kind_of") == "compliance_req"
    assert details.get("declared_subtypes") == ["security_control"]
    # Graph intact — the node never landed.
    assert _kind_of_of(board_id, artifact_ref) == "__absent__"


async def test_s3_absent_kind_of_keeps_current_flow(subtype_tempdir, monkeypatch):
    session_factory, board_id, spec_id = await _bootstrap_test_board(monkeypatch)
    artifact_ref = f"spec:{spec_id}"

    commit, _hash = await _drive_with_provenance(
        session_factory, board_id, artifact_ref,
        "[MKG-E] Sem subtipo", content="c",
    )
    assert commit.nodes_added >= 1
    assert _kind_of_of(board_id, artifact_ref) is None

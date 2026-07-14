"""MKG-E C2 — NodeSubtypeRegistry contract (scenario S2).

The SAME contract runs on the community SQLAlchemy adapter (real
kg_node_subtypes table) and the in-memory testing registry: valid declare
persists and lists; duplicates, unknown node_types, physical-name
collisions and empty names are rejected; the resolver fails closed.
"""

from __future__ import annotations

import gc
import shutil
import tempfile
from pathlib import Path

import pytest

from okto_pulse.core.ports.kg_subtype_registry import (
    NodeSubtypeRegistry,
    SubtypeDeclaration,
    SubtypeRegistryError,
    register_node_subtype_registry,
    require_node_subtype_registry,
    reset_node_subtype_registry_for_tests,
    resolve_node_subtype_registry,
    validate_subtype_declaration,
)

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def _clean_port_registry():
    reset_node_subtype_registry_for_tests()
    yield
    reset_node_subtype_registry_for_tests()


def _decl(node_type="Entity", kind_of="security_control", **kw):
    return SubtypeDeclaration(
        node_type=node_type, kind_of=kind_of, created_by="test:mkge", **kw
    )


# ---------------------------------------------------------------------------
# Pure rules (TR3)
# ---------------------------------------------------------------------------


async def test_s2_pure_rules():
    validate_subtype_declaration(_decl())  # valid

    with pytest.raises(SubtypeRegistryError):
        validate_subtype_declaration(_decl(kind_of="   "))
    with pytest.raises(SubtypeRegistryError):
        validate_subtype_declaration(_decl(node_type="NotAType"))
    # Case-insensitive collision with a physical type name.
    with pytest.raises(SubtypeRegistryError):
        validate_subtype_declaration(_decl(kind_of="decision"))
    with pytest.raises(SubtypeRegistryError):
        validate_subtype_declaration(_decl(kind_of="ENTITY"))
    # Uniqueness (normalized) against existing declarations.
    with pytest.raises(SubtypeRegistryError):
        validate_subtype_declaration(
            _decl(kind_of="Security_Control"), existing=(_decl(),)
        )


async def _contract(registry) -> None:
    declared = await registry.declare(_decl(description="controle"))
    assert declared.created_at

    listed = await registry.list_all()
    assert [(d.node_type, d.kind_of) for d in listed] == [
        ("Entity", "security_control")
    ]
    assert (await registry.get("Entity", "SECURITY_CONTROL")) is not None
    assert (await registry.get("Entity", "nao_existe")) is None

    with pytest.raises(SubtypeRegistryError):
        await registry.declare(_decl())  # duplicate
    with pytest.raises(SubtypeRegistryError):
        await registry.declare(_decl(node_type="NotAType", kind_of="x"))
    with pytest.raises(SubtypeRegistryError):
        await registry.declare(_decl(kind_of="Learning"))

    # A second valid declaration keeps the deterministic order.
    await registry.declare(_decl(node_type="Decision", kind_of="adr"))
    listed2 = await registry.list_all()
    assert [(d.node_type, d.kind_of) for d in listed2] == [
        ("Decision", "adr"),
        ("Entity", "security_control"),
    ]


async def test_s2_in_memory_registry_contract():
    from kg_registry_testing import _InMemoryNodeSubtypeRegistry

    await _contract(_InMemoryNodeSubtypeRegistry())


@pytest.fixture
def registry_tempdir(monkeypatch):
    base = Path(tempfile.mkdtemp(prefix="okto_pulse_subtypereg_"))
    db_path = base / "pulse.db"
    monkeypatch.setenv("OKTO_PULSE_DATA_DIR", str(base))
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    yield base
    gc.collect()
    shutil.rmtree(base, ignore_errors=True)


@pytest.fixture(autouse=True)
def _restore_conftest_engine(preserve_relational_runtime):
    yield


async def test_s2_sqlalchemy_registry_contract(registry_tempdir):
    import os

    from okto_pulse.community.adapters.sqlalchemy_base import Base as CommunityBase
    from okto_pulse.community.adapters.sqlalchemy_kg_subtype_registry import (
        CommunitySqlAlchemyNodeSubtypeRegistry,
    )
    from okto_pulse.core.infra.database import (
        create_database,
        get_engine,
        get_session_factory,
        init_db,
    )

    create_database(os.environ["DATABASE_URL"], echo=False)
    await init_db()
    async with get_engine().begin() as conn:
        await conn.run_sync(CommunityBase.metadata.create_all)

    await _contract(CommunitySqlAlchemyNodeSubtypeRegistry(get_session_factory()))


async def test_s2_fail_closed_resolver_and_protocol():
    with pytest.raises(SubtypeRegistryError) as excinfo:
        require_node_subtype_registry()
    assert excinfo.value.failure_reason == "kg_subtype_registry_unavailable"

    from kg_registry_testing import _InMemoryNodeSubtypeRegistry

    registry = _InMemoryNodeSubtypeRegistry()
    register_node_subtype_registry(registry)
    assert resolve_node_subtype_registry() is registry
    assert require_node_subtype_registry() is registry
    assert isinstance(registry, NodeSubtypeRegistry)

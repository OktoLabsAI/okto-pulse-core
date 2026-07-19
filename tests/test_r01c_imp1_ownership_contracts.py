"""R01C IMP1 — ownership contracts + dormant schema-lifecycle seam.

Behavioral proof of the three IMP1 contracts the implementation establishes:
  * AC1 (ac_58577edd): Pydantic schemas import without pulling the SQLAlchemy
    ORM module; enum serialized values are preserved.
  * TR2 (tr_712cdbb3): enum serialized-value parity + Pydantic schema parity
    across the FR1 enum extraction (``core.models.db`` re-exports
    ``core.domain.enums`` by identity, not by copy).
  * FR3 (fr_61a1562b): ``init_db`` delegates the schema lifecycle to a
    registered relational schema-lifecycle orchestrator and fails closed when
    none is registered.
"""

from __future__ import annotations

import ast
import os
import pathlib
import subprocess
import sys

import pytest

# Frozen on-wire contract: the EXACT serialized value of every FR1 enum member.
# Any change here is a breaking change to persisted rows / API payloads, so this
# table is the parity oracle for the extraction (TR2).
_FROZEN_ENUM_VALUES = {
    "IdeationStatus": {
        "DRAFT": "draft", "REVIEW": "review", "APPROVED": "approved",
        "EVALUATING": "evaluating", "DONE": "done", "CANCELLED": "cancelled",
    },
    "IdeationComplexity": {"SMALL": "small", "MEDIUM": "medium", "LARGE": "large"},
    "StoryStatus": {
        "DRAFT": "draft", "TRIAGE": "triage", "READY": "ready", "CONVERTED": "converted",
    },
    "RefinementStatus": {
        "DRAFT": "draft", "REVIEW": "review", "APPROVED": "approved",
        "DONE": "done", "CANCELLED": "cancelled",
    },
    "SprintStatus": {
        "DRAFT": "draft", "ACTIVE": "active", "REVIEW": "review",
        "CLOSED": "closed", "CANCELLED": "cancelled",
    },
    "SprintLaneType": {"NORMAL": "normal", "HOTFIX": "hotfix"},
    "SpecStatus": {
        "DRAFT": "draft", "REVIEW": "review", "APPROVED": "approved",
        "VALIDATED": "validated", "IN_PROGRESS": "in_progress",
        "DONE": "done", "CANCELLED": "cancelled",
    },
    "CardStatus": {
        "NOT_STARTED": "not_started", "STARTED": "started",
        "IN_PROGRESS": "in_progress", "VALIDATION": "validation",
        "ON_HOLD": "on_hold", "DONE": "done", "CANCELLED": "cancelled",
    },
    "CardPriority": {
        "CRITICAL": "critical", "VERY_HIGH": "very_high", "HIGH": "high",
        "MEDIUM": "medium", "LOW": "low", "NONE": "none",
    },
    "CardType": {"NORMAL": "normal", "BUG": "bug", "TEST": "test"},
    "BugSeverity": {"CRITICAL": "critical", "MAJOR": "major", "MINOR": "minor"},
    "CommentType": {"TEXT": "text", "CHOICE": "choice", "MULTI_CHOICE": "multi_choice"},
}


# ---------------------------------------------------------------------------
# AC1 — Pydantic schemas importable without the SQLAlchemy ORM module.
# ---------------------------------------------------------------------------

# Run in a FRESH interpreter: this test process already has sqlalchemy + db
# imported, so only a clean subprocess can prove the import graph (PEP 562 lazy
# core.models.__init__ -> schemas) never reaches the ORM.
_AC1_PROBE = """
import sys
import okto_pulse.core.models.schemas  # noqa: F401
leaked = [m for m in ("sqlalchemy", "okto_pulse.core.models.db") if m in sys.modules]
assert not leaked, "AC1 violated: schemas import pulled ORM modules: %r" % (leaked,)
from okto_pulse.core.domain.enums import CardStatus
assert "okto_pulse.core.models.db" not in sys.modules, "enum import pulled ORM db"
assert CardStatus.DONE.value == "done"
print("AC1_OK")
"""


def test_schema_importable_without_orm():
    src = str(pathlib.Path(__file__).resolve().parents[1] / "src")
    env = {**os.environ, "PYTHONPATH": src + os.pathsep + os.environ.get("PYTHONPATH", "")}
    proc = subprocess.run(
        [sys.executable, "-c", _AC1_PROBE],
        capture_output=True, text=True, env=env,
    )
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    assert "AC1_OK" in proc.stdout


# ---------------------------------------------------------------------------
# TR2 — enum serialized-value parity + Pydantic schema parity.
# ---------------------------------------------------------------------------

def test_enum_serialized_value_parity():
    from okto_pulse.core.domain import enums as de

    for enum_name, members in _FROZEN_ENUM_VALUES.items():
        enum_cls = getattr(de, enum_name)
        got = {m.name: m.value for m in enum_cls}
        assert got == members, (enum_name, got, members)


def test_models_package_exports_domain_enums_without_db_compatibility_module():
    from okto_pulse.core.domain import enums as de
    import okto_pulse.core.models as public_models

    for name in _FROZEN_ENUM_VALUES:
        if hasattr(public_models, name):
            assert getattr(public_models, name) is getattr(de, name)
    assert not hasattr(public_models, "db")


def test_real_schema_round_trips_enum_to_frozen_value():
    # StoryMove has a single required StoryStatus field — a real schema proving
    # validate(wire) -> domain enum and dump -> wire value both hold post-extraction.
    from okto_pulse.core.domain.enums import StoryStatus
    from okto_pulse.core.models.schemas import StoryMove

    parsed = StoryMove.model_validate({"status": "ready"})
    assert parsed.status is StoryStatus.READY
    assert parsed.model_dump(mode="json")["status"] == "ready"


# ---------------------------------------------------------------------------
# FR3 — init_db delegates to a registered schema-lifecycle orchestrator.
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_orchestrator():
    from okto_pulse.core.ports import schema_lifecycle as sl

    previous = sl.resolve_relational_schema_lifecycle_orchestrator()
    sl.reset_relational_schema_lifecycle_orchestrator()
    try:
        yield
    finally:
        sl.reset_relational_schema_lifecycle_orchestrator()
        if previous is not None:
            sl.register_relational_schema_lifecycle_orchestrator(previous)


def test_resolve_is_none_when_unregistered():
    from okto_pulse.core.ports import schema_lifecycle as sl

    assert sl.resolve_relational_schema_lifecycle_orchestrator() is None


async def test_init_db_fails_closed_when_unregistered():
    from okto_pulse.core.infra import database

    with pytest.raises(RuntimeError, match="orchestrator not registered"):
        await database.init_db()


async def test_init_db_delegates_to_registered_orchestrator():
    from okto_pulse.core.infra import database
    from okto_pulse.core.ports import schema_lifecycle as sl

    calls: list[str] = []

    class _FakeOrchestrator:
        async def initialize_schema(self) -> None:
            calls.append("delegated")

    fake = _FakeOrchestrator()
    sl.register_relational_schema_lifecycle_orchestrator(fake)

    # Must delegate and return BEFORE touching get_engine() — so this exercises
    # the seam with no real database/engine in play.
    await database.init_db()

    assert calls == ["delegated"]
    # runtime_checkable Protocol: the fake satisfies the port by structure.
    assert isinstance(fake, sl.RelationalSchemaLifecycleOrchestrator)


def _dotted_name(node: ast.AST) -> str | None:
    parts: list[str] = []
    cur = node
    while isinstance(cur, ast.Attribute):
        parts.append(cur.attr)
        cur = cur.value
    if isinstance(cur, ast.Name):
        parts.append(cur.id)
        return ".".join(reversed(parts))
    return None


def test_core_database_has_no_dormant_schema_lifecycle_sql_residue():
    database_path = (
        pathlib.Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "infra"
        / "database.py"
    )
    tree = ast.parse(database_path.read_text(encoding="utf-8"), filename=str(database_path))

    top_level_migrations = [
        node.name
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("_migrate_")
    ]
    assert top_level_migrations == []

    create_all_refs = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and _dotted_name(node) == "Base.metadata.create_all"
    ]
    assert create_all_refs == []

"""Dedicated executable evidence for Spec B scenario ``ts_1aebc702``.

The scenario has two independent obligations:

* selective Knowledge propagation must consume the canonical
  ``ResourceRevisionStamp`` published by ResourceLineage.v2 instead of
  redefining or structurally inferring the identity tuple; and
* Core must remain free of Community-owned HTTP, relational, and UI
  implementation choices.

Keeping both checks in one stable test gives the Pulse scenario one
re-executable evidence pointer without weakening the broader regression suites.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path

import pytest

from okto_pulse.core.domain.knowledge_selection import (
    KnowledgeAssignment,
    KnowledgePropagationContractError,
)
from okto_pulse.core.domain.resource_revision import (
    ResourceRevisionStamp as CanonicalResourceRevisionStamp,
)
from okto_pulse.core.repositories.core_orm_import_gate import (
    run_core_orm_import_gate,
)
from okto_pulse.core.services import (
    ResourceRevisionStamp as PublicResourceRevisionStamp,
)
from okto_pulse.core.services.resource_lineage import (
    ResourceRevisionStamp as LineageResourceRevisionStamp,
)


CORE_SOURCE = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"
COMMUNITY_OWNED_IMPORTS = {
    "aiosqlite",
    "fastapi",
    "sqlalchemy",
    "starlette",
}
UI_SOURCE_SUFFIXES = {".css", ".jsx", ".scss", ".svelte", ".tsx", ".vue"}


def _assignment(revision_stamp: object) -> KnowledgeAssignment:
    return KnowledgeAssignment(
        assignment_id="assignment-ts-1aebc702",
        board_id="board-ts-1aebc702",
        target_type="card",
        target_id="card-ts-1aebc702",
        source_knowledge_id="kb-physical-v7",
        revision_stamp=revision_stamp,  # type: ignore[arg-type]
        mode="reference",
        state="active",
        origin_class="v2",
        actor_id="agent-ts-1aebc702",
        revision=7,
        justification="Conformance evidence for AC-B18.",
    )


def _parallel_revision_definitions() -> list[str]:
    definitions: list[str] = []
    for path in CORE_SOURCE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        if any(
            isinstance(node, ast.ClassDef) and node.name == "ResourceRevisionStamp"
            for node in ast.walk(tree)
        ):
            definitions.append(path.relative_to(CORE_SOURCE).as_posix())
    return sorted(definitions)


def _community_owned_imports_in_core() -> list[str]:
    violations: list[str] = []
    for path in CORE_SOURCE.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                modules.append(node.module)
            for module in modules:
                root = module.split(".", 1)[0]
                if root in COMMUNITY_OWNED_IMPORTS:
                    violations.append(
                        f"{path.relative_to(CORE_SOURCE).as_posix()}:{node.lineno}:{module}"
                    )
    return sorted(violations)


def test_ts_1aebc702_b8_reuses_resource_lineage_v2_and_keeps_core_pure() -> None:
    """AC-B18/B19: one canonical stamp, fail-closed use, and a clean boundary."""

    assert PublicResourceRevisionStamp is CanonicalResourceRevisionStamp
    assert LineageResourceRevisionStamp is CanonicalResourceRevisionStamp
    assert _parallel_revision_definitions() == ["domain/resource_revision.py"]

    stamp = PublicResourceRevisionStamp(
        root_id="kb-root",
        immediate_parent_id="kb-parent",
        source_revision="7",
        source_content_sha256="a" * 64,
    )
    assignment = _assignment(stamp)
    assert assignment.revision_stamp == stamp
    assert assignment.to_dict()["revision_stamp"] == stamp.to_dict()

    @dataclass(frozen=True)
    class ParallelRevisionDTO:
        root_id: str
        immediate_parent_id: str
        source_revision: str
        source_content_sha256: str

    with pytest.raises(KnowledgePropagationContractError) as parallel:
        _assignment(
            ParallelRevisionDTO(
                root_id="kb-root",
                immediate_parent_id="kb-parent",
                source_revision="7",
                source_content_sha256="a" * 64,
            )
        )
    assert parallel.value.code == "invalid_revision_stamp"

    with pytest.raises(KnowledgePropagationContractError) as inferred:
        _assignment(PublicResourceRevisionStamp(root_id="kb-root"))
    assert inferred.value.code == "v2_assignment_revision_evidence_required"

    orm_report = run_core_orm_import_gate()
    assert orm_report.ok, [violation.as_dict() for violation in orm_report.violations]
    assert orm_report.violations == []
    assert _community_owned_imports_in_core() == []
    assert [
        path.relative_to(CORE_SOURCE).as_posix()
        for path in CORE_SOURCE.rglob("*")
        if path.is_file() and path.suffix.lower() in UI_SOURCE_SUFFIXES
    ] == []

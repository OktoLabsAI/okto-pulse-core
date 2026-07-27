from __future__ import annotations

import ast
import tomllib
from pathlib import Path

from okto_pulse.core.domain.worker_policy import RetryPolicy, WorkState


CORE_ROOT = Path(__file__).resolve().parents[1]
PROCESSOR_ROOT = (
    CORE_ROOT / "src" / "okto_pulse" / "core" / "application" / "processors"
)


def test_core_worker_processors_create_no_asyncio_tasks() -> None:
    offenders: list[str] = []
    forbidden_classes = {"EventDispatcher", "ConsolidationWorker", "OutboxWorker"}
    for path in PROCESSOR_ROOT.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name in forbidden_classes:
                offenders.append(f"{path.name}:{node.lineno}:{node.name}")
            if isinstance(node, ast.Call):
                dotted = _dotted_name(node.func)
                if dotted.endswith("create_task") or dotted.endswith("ensure_future"):
                    offenders.append(f"{path.name}:{node.lineno}:{dotted}")
    assert offenders == []


def test_legacy_worker_implementation_modules_are_absent() -> None:
    legacy = (
        CORE_ROOT / "src/okto_pulse/core/events/dispatcher.py",
        CORE_ROOT / "src/okto_pulse/core/kg/workers/consolidation.py",
        CORE_ROOT / "src/okto_pulse/core/kg/workers/cleanup.py",
        CORE_ROOT / "src/okto_pulse/core/kg/global_discovery/outbox_worker.py",
    )
    assert [str(path) for path in legacy if path.exists()] == []


def test_processor_resource_packaging_paths_exist() -> None:
    manifest = tomllib.loads((CORE_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    force_includes = manifest["tool"]["hatch"]["build"]["targets"]["wheel"][
        "force-include"
    ]

    missing = [
        source for source in force_includes if not (CORE_ROOT / source).is_file()
    ]
    assert missing == []
    assert (
        force_includes["src/okto_pulse/core/application/processors/tech_entities.yml"]
        == "okto_pulse/core/application/processors/tech_entities.yml"
    )


def test_retry_policy_is_deterministic_and_fail_closed() -> None:
    policy = RetryPolicy(max_attempts=5, base=2, cap_seconds=10)

    assert policy.after_failure(1).delay_seconds == 2
    assert policy.after_failure(4).delay_seconds == 10
    terminal = policy.after_failure(5)
    assert terminal.state is WorkState.DEAD_LETTER
    assert terminal.terminal is True


def _dotted_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted_name(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""

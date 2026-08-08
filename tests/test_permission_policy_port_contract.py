"""F08 acceptance tests for the edition-neutral permission boundary."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okto_pulse.core.domain.permissions import (
    DefaultPermissionPolicy,
    InvalidPermissionContext,
    PermissionContext,
    PermissionDecision,
    PermissionSet,
)
from okto_pulse.core.ports.permission_policy import (
    PermissionPolicyPort,
    builtin_permission_presets,
    merge_permission_registry_defaults,
    registered_permission_flags,
)


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.add(node.module)
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "__import__"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            modules.add(node.args[0].value)
    return modules


def test_all_core_ports_are_free_of_infra_imports() -> None:
    violations: list[tuple[str, str]] = []
    for path in sorted((CORE_ROOT / "ports").glob("*.py")):
        for imported in _imported_modules(path):
            if imported.startswith("okto_pulse.core.infra"):
                violations.append((path.name, imported))
    assert violations == []


def test_permission_domain_module_has_only_stdlib_and_canonical_domain_imports() -> None:
    imports = _imported_modules(CORE_ROOT / "domain" / "permissions.py")
    assert imports <= {
        "__future__",
        "copy",
        "dataclasses",
        "json",
        "logging",
        "okto_pulse.core.domain.mcp_permission_registry",
        "okto_pulse.core.domain.sdlc_registry",
        "typing",
    }


def test_default_and_fake_policies_satisfy_the_public_protocol() -> None:
    class FakePolicy:
        def resolve(self, agent_flags, preset_flags, board_overrides):
            return PermissionSet(agent_flags or {})

        def evaluate(self, context: PermissionContext) -> PermissionDecision:
            return PermissionDecision.allow(context.operation)

    assert isinstance(DefaultPermissionPolicy(), PermissionPolicyPort)
    assert isinstance(FakePolicy(), PermissionPolicyPort)
    assert FakePolicy().evaluate(PermissionContext("board.read")).allowed is True


def test_default_policy_preserves_ceiling_and_legacy_decisions() -> None:
    policy = DefaultPermissionPolicy()
    permissions = policy.resolve(
        agent_flags={"board": {"read": True}},
        preset_flags=None,
        board_overrides={"board": {"read": False}},
    )
    denied = policy.evaluate(PermissionContext("board.read", permissions))
    assert denied.allowed is False
    assert denied.required_permission == "board.read"

    legacy = policy.evaluate(
        PermissionContext(
            "board.read",
            permissions=["board:read"],
            legacy_operation="board:read",
        )
    )
    assert legacy == PermissionDecision.allow("board.read")


def test_default_policy_rejects_an_empty_operation() -> None:
    with pytest.raises(InvalidPermissionContext):
        DefaultPermissionPolicy().evaluate(PermissionContext("  "))


def test_infra_compatibility_facade_reexports_domain_identity() -> None:
    from okto_pulse.core.infra.permissions import PermissionSet as LegacyPermissionSet

    assert LegacyPermissionSet is PermissionSet


def test_adapter_bootstrap_views_do_not_expose_mutable_registry_state() -> None:
    registry = registered_permission_flags()
    registry["board"]["read"] = False
    assert registered_permission_flags()["board"]["read"] is True

    presets = builtin_permission_presets()
    assert presets
    presets[0]["flags"]["board"]["read"] = False
    assert builtin_permission_presets()[0]["flags"]["board"]["read"] is True

    merged, added = merge_permission_registry_defaults({"board": {"read": False}})
    assert merged["board"]["read"] is False
    assert added > 0

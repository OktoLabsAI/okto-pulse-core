from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path

import pytest

from okto_pulse.core.kg.schemas import KGEdgeType


REPO_ROOT = Path(__file__).parents[1]
CORE_SRC = REPO_ROOT / "src" / "okto_pulse" / "core"
COMMUNITY_ROOT = REPO_ROOT.parent / "okto_labs_pulse_community"
COMMUNITY_ADAPTERS = COMMUNITY_ROOT / "src" / "okto_pulse" / "community" / "adapters"


CORE_API_EXCLUSIONS = frozenset({"__init__.py", "deps.py", "router.py"})
CORE_README_COUNTERS = (
    "SQLAlchemy models",
    "service classes",
    "API route modules",
    "MCP tools",
    "relationship types",
)

COMMUNITY_SOURCE_MAP_ENTRIES = (
    "community/main.py",
    "community/adapters/composition.py",
    "community/adapters/mcp_auth.py",
    "community/adapters/resources.py",
    "community/adapters/capability_descriptors.py",
    "community/adapters/mcp_trace.py",
    "community/adapters/scheduler.py",
    "community/adapters/workers.py",
    "community/adapters/board_source_reader.py",
    "community/adapters/board_rebuild_ingestion.py",
    "community/adapters/relational_schema_lifecycle.py",
    "community/adapters/sqlalchemy_unit_of_work.py",
    "community/adapters/sqlalchemy_repositories.py",
    "community/adapters/sqlalchemy_database.py",
    "community/adapters/sqlite_outbox_event_bus.py",
    "community/adapters/sqlalchemy_audit_repo.py",
    "install_community_sqlite_pragmas",
)


def _markdown_section(text: str, heading: str) -> str:
    pattern = re.compile(
        rf"^## {re.escape(heading)}\s*$"
        rf"(?P<section>.*?)(?=^## |\Z)",
        flags=re.MULTILINE | re.DOTALL,
    )
    match = pattern.search(text)
    assert match is not None, f"section {heading!r} not found"
    return match.group("section")


def _release_notes(text: str) -> str:
    pattern = re.compile(r"^## Release Notes\s*$(?P<section>.*)", flags=re.MULTILINE | re.DOTALL)
    match = pattern.search(text)
    return match.group("section") if match else ""


def _extract_counter(section: str, label: str) -> int:
    match = re.search(rf"\*\*(\d+)\s+{re.escape(label)}\*\*", section)
    assert match is not None, f"live README section is missing counter {label!r}"
    return int(match.group(1))


def _service_classes() -> list[tuple[str, str]]:
    services: list[tuple[str, str]] = []
    for path in sorted((CORE_SRC / "services").rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name.endswith("Service"):
                services.append((path.relative_to(CORE_SRC).as_posix(), node.name))
    return services


def _sqlalchemy_model_classes() -> list[str]:
    models: list[str] = []
    tree = ast.parse((CORE_SRC / "models" / "db.py").read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.ClassDef):
            continue
        has_tablename = any(
            isinstance(stmt, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id == "__tablename__" for target in stmt.targets)
            for stmt in node.body
        )
        if has_tablename:
            models.append(node.name)
    return models


def _api_route_modules() -> list[Path]:
    return [
        path
        for path in sorted((CORE_SRC / "api").glob("*.py"))
        if path.name not in CORE_API_EXCLUSIONS
    ]


async def _mcp_tool_count() -> int:
    from okto_pulse.core.mcp import server as mcp_server

    tools = await mcp_server.mcp.get_tools()
    return len(tools)


def _assert_core_readme_live_counters(readme: str) -> None:
    live = _markdown_section(readme, "What's inside")
    mcp_tool_count = asyncio.run(_mcp_tool_count())

    assert _extract_counter(live, "SQLAlchemy models") == len(_sqlalchemy_model_classes())
    assert _extract_counter(live, "service classes") == len(_service_classes())
    assert _extract_counter(live, "API route modules") == len(_api_route_modules())
    assert _extract_counter(live, "MCP tools") == mcp_tool_count
    assert _extract_counter(live, "relationship types") == len(KGEdgeType)
    assert f"Community runtime exposure: {mcp_tool_count} core MCP tools" in live

    for source in (
        "Base.registry.mappers",
        "`core/models/db.py`",
        "classes ending in `Service`",
        "`core/api/*.py` excluding `__init__.py`, `deps.py` and `router.py`",
        "FastMCP registry",
        "`len(KGEdgeType)`",
    ):
        assert source in live


def _require_community_repo() -> None:
    if not (COMMUNITY_ROOT / "README.md").exists() or not COMMUNITY_ADAPTERS.exists():
        pytest.skip(
            "Community sibling repo not available; set up okto_labs_pulse_community next to core "
            "to validate the Community README source map."
        )


def _community_adapter_files() -> set[str]:
    _require_community_repo()
    return {
        path.relative_to(COMMUNITY_ROOT / "src" / "okto_pulse").as_posix()
        for path in COMMUNITY_ADAPTERS.rglob("*.py")
        if path.name != "__init__.py" and not path.name.startswith("_") and "__pycache__" not in path.parts
    }


def test_core_readme_live_counters_match_mechanical_sources() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    _assert_core_readme_live_counters(readme)


def test_api_route_module_counter_documents_infrastructure_exclusions() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    live = _markdown_section(readme, "What's inside")
    api_py = sorted((CORE_SRC / "api").glob("*.py"))

    assert len(api_py) == 47
    assert len([path for path in api_py if path.name != "__init__.py"]) == 46
    assert len(_api_route_modules()) == 44
    assert "`deps.py`" in live
    assert "`router.py`" in live

    mutated_readme = readme.replace("`deps.py` and `router.py`", "`deps.py`")
    with pytest.raises(AssertionError):
        _assert_core_readme_live_counters(mutated_readme)


def test_changelog_counters_are_anchor_protected() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    live = _markdown_section(readme, "What's inside")
    history = _release_notes(readme)

    assert "52 models / 28 services / 33 API modules / 215 MCP tools" in history
    assert "26+1 SQLAlchemy models" in history

    mutated_history_readme = readme.replace("52 models / 28 services", "999 models / 28 services")
    _assert_core_readme_live_counters(mutated_history_readme)


def test_guard_requires_live_section_not_historical_counters() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    mutated_readme = readme.replace("**59 SQLAlchemy models**", "**58 SQLAlchemy models**")
    with pytest.raises(AssertionError):
        _assert_core_readme_live_counters(mutated_readme)

    mcp_tool_count = asyncio.run(_mcp_tool_count())
    mutated_mcp_readme = readme.replace(f"**{mcp_tool_count} MCP tools**", f"**{mcp_tool_count - 1} MCP tools**")
    with pytest.raises(AssertionError):
        _assert_core_readme_live_counters(mutated_mcp_readme)


def test_core_architecture_documents_live_counter_maintenance_sources() -> None:
    architecture = (REPO_ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")
    overview = _markdown_section(architecture, "Live Documentation Counters")

    for marker in (
        "Base.registry.mappers",
        "`core/models/db.py`",
        "classes ending in `Service`",
        "`core/api/*.py` excluding `__init__.py`, `deps.py` and `router.py`",
        "FastMCP registry",
        "`len(KGEdgeType)`",
        "Community adapter source map",
    ):
        assert marker in overview


def test_community_readme_live_mcp_count_and_source_map_match_filesystem() -> None:
    _require_community_repo()
    readme = (COMMUNITY_ROOT / "README.md").read_text(encoding="utf-8")
    platform = _markdown_section(readme, "Platform Surface")
    adapters = _markdown_section(readme, "Architecture")
    adapter_files = _community_adapter_files()
    mcp_tool_count = asyncio.run(_mcp_tool_count())

    assert re.search(rf"\| Core MCP tools \| {mcp_tool_count} \|", platform)
    assert re.search(rf"\| MCP tools exposed by `okto-pulse serve` \| {mcp_tool_count} \|", platform)

    for entry in COMMUNITY_SOURCE_MAP_ENTRIES:
        assert entry in adapters, f"{entry} missing from Community live source map"

    for adapter_file in adapter_files:
        documented = adapter_file in adapters or (
            adapter_file.startswith("community/adapters/kuzu_")
            and "community/adapters/kuzu_*" in adapters
        )
        assert documented, f"{adapter_file} is missing from Community live source map"

    for required_adapter in (
        "community/adapters/mcp_auth.py",
        "community/adapters/resources.py",
        "community/adapters/capability_descriptors.py",
        "community/adapters/mcp_trace.py",
        "community/adapters/scheduler.py",
        "community/adapters/workers.py",
        "community/adapters/board_source_reader.py",
        "community/adapters/board_rebuild_ingestion.py",
        "community/adapters/sqlalchemy_unit_of_work.py",
        "community/adapters/sqlalchemy_repositories.py",
        "community/adapters/sqlalchemy_database.py",
    ):
        assert required_adapter in adapter_files


def test_af41_readmes_pin_mcp_runtime_ownership_and_provider_preservation() -> None:
    _require_community_repo()
    core_readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    community_readme = (COMMUNITY_ROOT / "README.md").read_text(encoding="utf-8")

    for marker in (
        "Core exposes `build_mcp_asgi_app()` and `mount_mcp()`",
        "deprecated\n`okto_pulse.core.mcp.server.run_mcp_server` debug shim",
        "`register_instruction_provider`",
        "`register_resource_catalog`",
        "`register_package_version_provider`",
        "`McpAuthenticator`",
        "`McpTraceSink`",
    ):
        assert marker in core_readme

    for marker in (
        "Community declares `uvicorn[standard]` and\n`wsproto` directly",
        "`CommunityMcpAuthenticator`",
        "`build_community_resource_catalog`",
        "`CommunityCapabilityDescriptorSource`",
        "`build_mcp_trace_sink_from_env`",
        "`JsonlMcpTraceSink`",
        "`core.ports.McpTraceSink`",
    ):
        assert marker in community_readme

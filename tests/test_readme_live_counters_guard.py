from __future__ import annotations

import ast
import asyncio
import re
from pathlib import Path

import pytest

from okto_pulse.core.kg.schemas import KGEdgeType
from repository_checkout_testing import community_repo_for


REPO_ROOT = Path(__file__).parents[1]
CORE_SRC = REPO_ROOT / "src" / "okto_pulse" / "core"
COMMUNITY_ROOT = community_repo_for(REPO_ROOT)
COMMUNITY_ADAPTERS = COMMUNITY_ROOT / "src" / "okto_pulse" / "community" / "adapters"


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
    "community/adapters/mcp_host.py",
    "community/adapters/resources.py",
    "community/adapters/capability_descriptors.py",
    "community/adapters/mcp_trace.py",
    "community/adapters/mcp_trace_middleware.py",
    "community/adapters/scheduler.py",
    "community/adapters/workers.py",
    "community/adapters/board_source_reader.py",
    "community/adapters/board_rebuild_ingestion.py",
    "community/adapters/relational_schema_lifecycle.py",
    "community/adapters/sqlalchemy_unit_of_work.py",
    "community/adapters/sqlalchemy_repositories.py",
    "community/adapters/sqlalchemy_database.py",
    "community/adapters/relational_application.py",
    "community/adapters/kg_events.py",
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
    for path in sorted(CORE_SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef):
                continue
            has_tablename = any(
                (
                    isinstance(stmt, ast.Assign)
                    and any(
                        isinstance(target, ast.Name) and target.id == "__tablename__"
                        for target in stmt.targets
                    )
                )
                or (
                    isinstance(stmt, ast.AnnAssign)
                    and isinstance(stmt.target, ast.Name)
                    and stmt.target.id == "__tablename__"
                )
                for stmt in node.body
            )
            if has_tablename:
                models.append(f"{path.relative_to(CORE_SRC).as_posix()}:{node.name}")
    return models


def _api_route_modules() -> list[Path]:
    return sorted((CORE_SRC / "api").glob("*.py"))


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
        "`__tablename__` assignments anywhere under `core/`",
        "classes ending in `Service`",
        "`core/api/*.py`",
        "transport-neutral Core catalog",
        "`len(KGEdgeType)`",
    ):
        assert source in live


def _require_community_repo() -> None:
    if not (COMMUNITY_ROOT / "README.md").exists() or not COMMUNITY_ADAPTERS.exists():
        pytest.skip(
            "Community sibling repo not available; set OKTO_PULSE_COMMUNITY_REPO "
            "or check out okto-pulse next to Core "
            "to validate the Community architecture source map."
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

    assert api_py == []
    assert _api_route_modules() == []
    assert "Community owns the REST adapter and route modules" in live

    mutated_readme = readme.replace("**0 API route modules**", "**1 API route modules**")
    with pytest.raises(AssertionError):
        _assert_core_readme_live_counters(mutated_readme)


def test_historical_counters_do_not_drive_live_counters() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    release_notes = (REPO_ROOT / "docs" / "RELEASE-NOTES.md").read_text(
        encoding="utf-8"
    )

    assert "26+1 SQLAlchemy models" in release_notes
    assert "26+1 SQLAlchemy models" not in readme
    assert "**0 SQLAlchemy models**" in _markdown_section(readme, "What's inside")


def test_guard_requires_live_section_not_historical_counters() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")

    mutated_readme = readme.replace("**0 SQLAlchemy models**", "**1 SQLAlchemy models**")
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
        "`__tablename__` assignments anywhere under `core/`",
        "classes ending in `Service`",
        "Python modules under `core/api`",
        "transport-neutral Core command catalog",
        "`len(KGEdgeType)`",
        "Community adapter source map",
    ):
        assert marker in overview


def test_community_readme_live_mcp_count_and_source_map_match_filesystem() -> None:
    _require_community_repo()
    readme = (COMMUNITY_ROOT / "README.md").read_text(encoding="utf-8")
    platform = _markdown_section(readme, "Platform Surface")
    adapters = (COMMUNITY_ROOT / "docs" / "ARCHITECTURE.md").read_text(
        encoding="utf-8"
    )
    adapter_files = _community_adapter_files()
    mcp_tool_count = asyncio.run(_mcp_tool_count())

    assert re.search(rf"\| Core MCP tools \| {mcp_tool_count} \|", platform)
    assert re.search(rf"\| MCP tools exposed by `okto-pulse serve` \| {mcp_tool_count} \|", platform)

    for entry in COMMUNITY_SOURCE_MAP_ENTRIES:
        assert entry in adapters, f"{entry} missing from Community live source map"

    wildcard_prefixes = {
        entry[:-1]
        for entry in re.findall(r"`(community/adapters/[^`]*\*)`", adapters)
    }
    for adapter_file in adapter_files:
        documented = adapter_file in adapters or any(
            adapter_file.startswith(prefix) for prefix in wildcard_prefixes
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
    core_architecture = (
        REPO_ROOT / "docs" / "ARCHITECTURE-OVERVIEW.md"
    ).read_text(encoding="utf-8")
    community_architecture = (
        COMMUNITY_ROOT / "docs" / "ARCHITECTURE.md"
    ).read_text(encoding="utf-8")

    for marker in (
        "Core exposes `build_mcp_asgi_app()` and `mount_mcp()`",
        "deprecated\n`okto_pulse.core.mcp.server.run_mcp_server` shim always rejects listener startup",
        "`register_instruction_provider`",
        "`register_resource_catalog`",
        "`register_package_version_provider`",
        "`McpAuthenticator`",
        "`McpTraceSink`",
    ):
        assert marker in core_architecture

    for marker in (
        "Community declares `fastmcp`, `uvicorn[standard]` and\n`wsproto` directly",
        "`CommunityMcpAuthenticator`",
        "`build_community_resource_catalog`",
        "`CommunityCapabilityDescriptorSource`",
        "`build_mcp_trace_sink_from_env`",
        "`JsonlMcpTraceSink`",
        "`okto_pulse.core.ports.McpTraceSink`",
    ):
        assert marker in community_architecture

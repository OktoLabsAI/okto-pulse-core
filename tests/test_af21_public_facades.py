from __future__ import annotations

import ast
import importlib
import inspect
from pathlib import Path
from typing import Any


CORE_ROOT = Path(__file__).resolve().parents[1]
COMMUNITY_ROOT = CORE_ROOT.parent / "okto_labs_pulse_community"

PUBLIC_FACADE_SYMBOLS: dict[str, tuple[str, ...]] = {
    "okto_pulse.core.services.application_agents": (
        "authenticate_agent_by_api_key",
        "credential_marker",
        "hash_api_key",
        "list_accessible_board_ids_for_agent",
    ),
    "okto_pulse.core.services.application_startup": (
        "backfill_qa_answered_at",
    ),
    "okto_pulse.core.mcp": (
        "build_mcp_asgi_app",
        "effective_resource_catalog",
        "freeze_resource_catalog",
        "get_authenticated_agent_for_mcp",
        "mount_mcp",
        "register_resource_catalog",
        "register_mcp_authenticator",
        "register_scheduler_control_for_mcp",
    ),
    "okto_pulse.core.ports.runtime_workers": (
        "RuntimeWorkerRegistry",
        "RuntimeWorkerSpec",
    ),
    "okto_pulse.core.ports.relational_runtime": (
        "close_db",
        "configure_database_runtime",
        "get_engine",
        "get_session_factory",
        "init_db",
        "is_database_runtime_configured",
        "resolve_sqlite_database_path",
    ),
    "okto_pulse.core.ports.telemetry": (
        "TelemetryEventStore",
        "TelemetryPort",
        "TelemetrySink",
        "TelemetryStateStore",
    ),
    "okto_pulse.core.services.application_kg": (
        "configure_provider_registry",
        "create_consolidation_processor",
        "create_deterministic_worker",
        "create_provider_registry",
        "drain_kg_health_probes",
        "get_current_provider_registry",
        "signal_consolidation_worker",
        "start_historical_consolidation",
    ),
}

FORBIDDEN_SIGNATURE_TERMS = (
    "AsyncSession",
    "SQLAlchemy",
    "sqlalchemy",
    "okto_pulse.core.infra.database",
    "okto_pulse.core.models.db",
)


def _annotation_text(annotation: Any) -> str:
    if annotation is inspect.Signature.empty:
        return ""
    return str(annotation)


def test_ts_72c34282_public_facade_symbols_are_importable() -> None:
    for module_name, symbols in PUBLIC_FACADE_SYMBOLS.items():
        module = importlib.import_module(module_name)
        for symbol in symbols:
            assert hasattr(module, symbol), f"{module_name}.{symbol} is missing"


def test_af41_provider_seams_stay_public_while_legacy_runner_stays_private() -> None:
    import okto_pulse.core as core
    import okto_pulse.core.mcp as core_mcp
    import okto_pulse.core.ports as core_ports

    public_provider_seams = {
        core_mcp: (
            "build_mcp_asgi_app",
            "mount_mcp",
            "register_instruction_provider",
            "register_resource_catalog",
            "freeze_resource_catalog",
        ),
        core: ("register_package_version_provider",),
        core_ports: ("McpAuthenticator", "McpTraceSink"),
    }

    for module, symbols in public_provider_seams.items():
        for symbol in symbols:
            assert hasattr(module, symbol), f"{module.__name__}.{symbol} is missing"

    assert "run_mcp_server" not in core_mcp.__all__
    assert not hasattr(core_mcp, "run_mcp_server")


def test_ts_72c34282_public_facade_signatures_do_not_expose_orm_internals() -> None:
    for module_name, symbols in PUBLIC_FACADE_SYMBOLS.items():
        module = importlib.import_module(module_name)
        for symbol in symbols:
            obj = getattr(module, symbol)
            try:
                signature = inspect.signature(obj)
            except (TypeError, ValueError):
                continue
            signature_text = " ".join(
                [
                    _annotation_text(signature.return_annotation),
                    *(
                        _annotation_text(param.annotation)
                        for param in signature.parameters.values()
                    ),
                ]
            )
            for forbidden in FORBIDDEN_SIGNATURE_TERMS:
                assert forbidden not in signature_text, (
                    f"{module_name}.{symbol} leaks {forbidden} in {signature}"
                )


def test_ts_72c34282_public_service_facades_import_internals_lazily_only() -> None:
    for rel_path in (
        "src/okto_pulse/core/services/application_kg.py",
        "src/okto_pulse/core/services/application_agents.py",
        "src/okto_pulse/core/services/application_startup.py",
        "src/okto_pulse/core/mcp/__init__.py",
    ):
        tree = ast.parse((CORE_ROOT / rel_path).read_text(encoding="utf-8"))
        for node in tree.body:
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
                assert module not in {
                    "okto_pulse.core.infra.database",
                    "okto_pulse.core.models.db",
                    "okto_pulse.core.kg.interfaces.registry",
                    "okto_pulse.core.kg.governance",
                }, f"{rel_path} imports {module} at module load"


def test_ts_a87cad8b_docs_do_not_bless_private_modules_as_public_api() -> None:
    docs = {
        "core README": CORE_ROOT / "README.md",
        "core CLAUDE": CORE_ROOT / "CLAUDE.md",
        "community README": COMMUNITY_ROOT / "README.md",
        "community CLAUDE": COMMUNITY_ROOT / "CLAUDE.md",
    }
    private_terms = (
        "from okto_pulse.core.infra.database import",
        "from okto_pulse.core.models.db import",
        "from okto_pulse.core.kg.interfaces.registry import",
        "from okto_pulse.core.mcp.server import",
    )

    offenders: list[str] = []
    for label, path in docs.items():
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for term in private_terms:
            if term in text:
                offenders.append(f"{label}: {term}")

    assert offenders == []


def test_ts_a87cad8b_docs_name_the_enforced_af21_boundary() -> None:
    core_readme = (CORE_ROOT / "README.md").read_text(encoding="utf-8")
    community_readme = (COMMUNITY_ROOT / "README.md").read_text(encoding="utf-8")

    assert "okto_pulse.core.services.application_kg" in core_readme
    assert "okto_pulse.core.services.application_agents" in core_readme
    assert "okto_pulse.core.mcp.build_mcp_asgi_app" in core_readme
    assert "okto_pulse.core.ports.runtime_workers" in core_readme
    assert "Community Core Reach-In Ledger" in community_readme
    assert "audit_community_core_import_boundary" in community_readme

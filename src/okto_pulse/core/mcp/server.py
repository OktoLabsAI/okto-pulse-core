"""MCP Server for Okto Pulse Core - enables AI agents to interact with the board."""

import base64
import functools
import inspect
import json
import logging
import os
import re
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import uvicorn
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from okto_pulse.core.infra.config import get_mcp_settings, get_settings
from okto_pulse.core.infra.permissions import Permissions, check_permission
from okto_pulse.core.mcp.helpers import _structured_error, coerce_to_list_str, parse_multi_value, parse_options_json
from okto_pulse.core.mcp.trace_middleware import install_if_enabled as _install_trace
from okto_pulse.core.models.db import Board
from okto_pulse.core.services.main import (
    AgentService,
    AttachmentService,
    BoardService,
    CardOperationError,
    CardService,
    CommentService,
    GuidelineService,
    IdeationKnowledgeService,
    IdeationQAService,
    IdeationService,
    QAService,
    RefinementKnowledgeService,
    RefinementQAService,
    RefinementService,
    SpecKnowledgeService,
    SpecLockedError,
    SpecQAService,
    SpecService,
    StoryService,
    TopicOperationError,
)
from okto_pulse.core.models.schemas import ArchitectureDesignCreate, ArchitectureDesignUpdate
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitectureDiagramAdapterRegistry,
    ArchitectureDiagramStore,
    ArchitecturePropagationService,
    ArchitectureWarningAcknowledgementRequired,
    architecture_design_payload_schema,
)
from okto_pulse.core.services.activity_log import (
    activity_log_summary,
    sanitize_activity_details,
)
from okto_pulse.core.services.reference_resolution import (
    resolve_entity_context_references,
    resolve_spec_references,
    resolve_task_context_references,
    serialize_parent_ideation_context,
)
from okto_pulse.core.services.resource_gate import (
    ENTITY_TYPES,
    ResourceGateError,
    ResourceGateService,
)
from okto_pulse.core.services.spec_structured_entities import (
    StructuredSpecEntityCommand,
    StructuredSpecEntityService,
)
from okto_pulse.core.services.story_permissions import (
    story_move_permission,
    story_state,
    story_update_permissions,
)


import uuid as _uuid


def _trs_to_objects(trs: list[str] | None) -> list | None:
    """Convert TR strings to objects with IDs for task linkage traceability."""
    if not trs:
        return None
    return [
        {"id": f"tr_{_uuid.uuid4().hex[:8]}", "text": tr, "linked_task_ids": []}
        if isinstance(tr, str) else tr
        for tr in trs
    ]


def _load_instructions() -> str:
    """Load agent instructions. Prefers mounted volume (live-editable), falls back to bundled copy."""
    here = Path(__file__).parent
    for candidate in [
        Path("/app/prompts/agent_system_prompt.md"),
        here / "agent_instructions.md",
    ]:
        if candidate.exists():
            return candidate.read_text(encoding="utf-8")
    return ""


# Initialize MCP server
mcp = FastMCP(
    name=get_settings().mcp_server_name,
    version=get_settings().mcp_server_version,
    instructions=_load_instructions(),
)

# Settings
mcp_settings = get_mcp_settings()
settings = get_settings()

# ============================================================================
# MCP Resources — okto-pulse:// URI scheme
# Lazy-loaded at startup; clients that support resources/read see these.
# ============================================================================

_resources_cache: dict[str, str] = {}


def _get_resource_dir() -> Path:
    return Path(__file__).parent / "resources"


def _load_resource_file(relative_path: str) -> str:
    """Load and cache a resource file by its path relative to the resources/ dir."""
    if relative_path not in _resources_cache:
        candidate = _get_resource_dir() / relative_path
        if candidate.exists():
            _resources_cache[relative_path] = candidate.read_text(encoding="utf-8")
        else:
            _resources_cache[relative_path] = ""
    return _resources_cache[relative_path]


_RESOURCE_REGISTRY = [
    ("okto-pulse://workflows/stories", "workflows/stories.md", "Stories & Topics workflow — pre-ideation intake."),
    ("okto-pulse://workflows/ideations", "workflows/ideations.md", "Ideations workflow — scope + ambiguity-killer."),
    ("okto-pulse://workflows/refinements", "workflows/refinements.md", "Refinements workflow — deep investigation."),
    ("okto-pulse://workflows/specs", "workflows/specs.md", "Specs workflow — saturation, gate, evaluation."),
    ("okto-pulse://workflows/cards", "workflows/cards.md", "Cards workflow — impl/bug/test execution."),
    ("okto-pulse://workflows/sprints", "workflows/sprints.md", "Sprints workflow — lifecycle e evaluation."),
    ("okto-pulse://workflows/kg", "workflows/kg.md", "KG workflow — consolidation, query, governance."),
    ("okto-pulse://workflows/preflight", "workflows/preflight.md", "Pre-Flight Checklist — session/entity/card/resource-gate pre-flight sequences (READ FIRST)."),
    ("okto-pulse://reference/errors", "reference/errors.md", "MCP errors matrix com fixes canônicos."),
    ("okto-pulse://reference/multivalue", "reference/multivalue.md", "Multi-value parameter input shapes."),
    ("okto-pulse://reference/destructive_ops", "reference/destructive_ops.md", "Destructive operations governance."),
    ("okto-pulse://reference/card_types", "reference/card_types.md", "Card types — normal/test/bug rules."),
    ("okto-pulse://reference/spec_gates", "reference/spec_gates.md", "Spec validation gate + evaluation gates."),
    ("okto-pulse://reference/transitions", "reference/transitions.md", "Status transitions matrix — cards/sprints/specs."),
    ("okto-pulse://reference/list_tools", "reference/list_tools.md", "Consolidated polymorphic list_* tools."),
    ("okto-pulse://reference/tools_catalog", "reference/tools_catalog.md", "Full MCP tool catalog grouped by domain."),
    ("okto-pulse://reference/projection-profiles", "reference/projection_profiles.md", "Projection profiles (summary/detail/full/legacy) + response envelope (SC1)."),
    ("okto-pulse://reference/kg-health", "reference/kg-health.md", "Full KG health contract: payload fields, when to consult, must-not-do."),
    # R1.1 — lazy long-form tool docs (args/returns/examples) moved off the
    # compact tools/list surface; one resource per tool family (api_fd7c5878).
    ("okto-pulse://reference/tool-docs/activity", "reference/tool-docs/activity.md", "Full long-form docs for activity tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/agent", "reference/tool-docs/agent.md", "Full long-form docs for agent tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/analytics", "reference/tool-docs/analytics.md", "Full long-form docs for analytics tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/api-contract", "reference/tool-docs/api-contract.md", "Full long-form docs for api-contract tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/architecture", "reference/tool-docs/architecture.md", "Full long-form docs for architecture tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/attachment", "reference/tool-docs/attachment.md", "Full long-form docs for attachment tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/board", "reference/tool-docs/board.md", "Full long-form docs for board tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/business-rule", "reference/tool-docs/business-rule.md", "Full long-form docs for business-rule tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/card", "reference/tool-docs/card.md", "Full long-form docs for card tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/comment", "reference/tool-docs/comment.md", "Full long-form docs for comment tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/decision", "reference/tool-docs/decision.md", "Full long-form docs for decision tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/guideline", "reference/tool-docs/guideline.md", "Full long-form docs for guideline tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/ideation", "reference/tool-docs/ideation.md", "Full long-form docs for ideation tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/integration-requirement", "reference/tool-docs/integration-requirement.md", "Full long-form docs for integration-requirement tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/kg", "reference/tool-docs/kg.md", "Full long-form docs for kg tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/knowledge", "reference/tool-docs/knowledge.md", "Full long-form docs for knowledge tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/misc", "reference/tool-docs/misc.md", "Full long-form docs for misc tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/mockup", "reference/tool-docs/mockup.md", "Full long-form docs for mockup tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/observability-requirement", "reference/tool-docs/observability-requirement.md", "Full long-form docs for observability-requirement tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/qa", "reference/tool-docs/qa.md", "Full long-form docs for qa tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/refinement", "reference/tool-docs/refinement.md", "Full long-form docs for refinement tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/snapshot", "reference/tool-docs/snapshot.md", "Full long-form docs for snapshot tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/spec", "reference/tool-docs/spec.md", "Full long-form docs for spec tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/story", "reference/tool-docs/story.md", "Full long-form docs for story tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/sprint", "reference/tool-docs/sprint.md", "Full long-form docs for sprint tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/test-scenario", "reference/tool-docs/test-scenario.md", "Full long-form docs for test-scenario tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/topic", "reference/tool-docs/topic.md", "Full long-form docs for topic tools (args/returns/examples)."),
    ("okto-pulse://reference/tool-docs/traceability", "reference/tool-docs/traceability.md", "Full long-form docs for traceability tools (args/returns/examples)."),
    # R4.2 — lazy tool-family consolidation/migration docs (fr_589a9977 / ir_a1db20f3).
    # Compact tool descriptions point here instead of embedding migration prose.
    ("okto-pulse://reference/tool-families/spec_entity_remove", "reference/tool-families/spec_entity_remove.md", "R4 consolidated spec-entity removal: target_types, aliases, soft-delete asymmetry."),
    ("okto-pulse://reference/tool-families/qa_ask", "reference/tool-families/qa_ask.md", "R4 consolidated Q&A ask: target_types, aliases, sprint asymmetry."),
]


def _make_resource_handler(path: str) -> "Callable[[], str]":
    """Create a closure-safe handler for a specific resource path."""
    def handler() -> str:
        return _load_resource_file(path)
    return handler


# Register every resource in _RESOURCE_REGISTRY dynamically
for _uri, _path, _desc in _RESOURCE_REGISTRY:
    _handler = _make_resource_handler(_path)
    _handler.__name__ = f"resource_{_path.replace('/', '_').replace('.md', '')}"
    _handler.__doc__ = _desc
    mcp.resource(_uri, description=_desc)(_handler)

# Pre-warm the resource cache so first-read latency is minimal
for _, _path, _ in _RESOURCE_REGISTRY:
    _load_resource_file(_path)


# R1.1 — canonical map from a compacted tool to its single lazy long-form doc
# resource URI (api_fd7c5878). Family is derived by priority-ordered keyword so
# the map stays deterministic and matches the generated tool-docs/{family}.md
# resources. Agents/CI resolve a tool's full docs via this one stable URI.
_TOOL_DOCS_FAMILY_RULES = [
    ("architecture", "architecture"),
    ("test_scenario", "test-scenario"),
    ("business_rule", "business-rule"),
    ("api_contract", "api-contract"),
    ("integration_requirement", "integration-requirement"),
    ("observability_requirement", "observability-requirement"),
    ("screen_mockup", "mockup"),
    ("mockup", "mockup"),
    ("knowledge", "knowledge"),
    ("decision", "decision"),
    ("guideline", "guideline"),
    ("sprint", "sprint"),
    ("spec", "spec"),
    ("ideation", "ideation"),
    ("refinement", "refinement"),
    ("story", "story"),
    ("topic", "topic"),
    ("blocker", "card"),
    ("card", "card"),
    ("comment", "comment"),
    ("attachment", "attachment"),
    ("question", "qa"),
    ("choice", "qa"),
    ("traceability", "traceability"),
    ("activity", "activity"),
    ("analytic", "analytics"),
    ("agent", "agent"),
    ("board", "board"),
    ("profile", "agent"),
    ("snapshot", "snapshot"),
]


def tool_docs_family(tool_name: str) -> str:
    """Deterministic tool-docs family for a tool name (R1.1 / api_fd7c5878)."""
    if "kg" in tool_name.split("_"):
        return "kg"
    # R4 consolidated Q&A ask — exact-match before the substring rules because the
    # substring 'ask' collides with 'task' (link_task / get_task_context / …).
    if tool_name == "okto_pulse_ask":
        return "qa"
    for key, family in _TOOL_DOCS_FAMILY_RULES:
        if key in tool_name:
            return family
    return "misc"


def tool_docs_uri(tool_name: str) -> str:
    """Canonical lazy long-form documentation URI for a compacted tool."""
    return f"okto-pulse://reference/tool-docs/{tool_docs_family(tool_name)}"


# ============================================================================
# SESSION-BASED AUTH (API key extracted from request)
# ============================================================================

# Per-request api_key, async-safe via ContextVar. Spec 23350275 (Fix C):
# isolates identity between concurrent MCP requests when the server is mounted
# as a sub-app on the FastAPI principal. The previous module-level global was
# safe only in the single-request-at-a-time MCP standalone.
_active_api_key: ContextVar[str | None] = ContextVar("mcp_active_api_key", default=None)


class ApiKeySessionMiddleware:
    """ASGI middleware that extracts api_key from query param or header."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        token = None
        if scope["type"] == "http":
            request = Request(scope)
            # Extract API key from query param, X-API-Key header, or Authorization Bearer
            api_key = (
                request.query_params.get("api_key")
                or request.headers.get("x-api-key", "")
                or request.headers.get("authorization", "").removeprefix("Bearer ").strip()
            )
            if api_key:
                token = _active_api_key.set(api_key)

        try:
            await self.app(scope, receive, send)
        finally:
            if token is not None:
                _active_api_key.reset(token)


# ============================================================================
# AUTH HELPERS (tools call these instead of passing api_key)
# ============================================================================


# Session factory registration for MCP server
_mcp_session_factory = None


def register_session_factory(factory):
    """Register the database session factory for MCP operations."""
    global _mcp_session_factory
    _mcp_session_factory = factory


def get_db_for_mcp():
    """Get database session for MCP operations."""
    if _mcp_session_factory is None:
        raise RuntimeError("Session factory not registered. Call register_session_factory() first.")
    return _mcp_session_factory()


class AgentContext:
    """Context for authenticated agent."""

    def __init__(
        self,
        agent_id: str,
        agent_name: str,
        board_id: str,
        permissions,  # list[str] | PermissionSet | None
    ):
        self.agent_id = agent_id
        self.agent_name = agent_name
        self.board_id = board_id
        self.permissions = permissions


# ---- Permission cache (TTL 60s) ----
_permission_cache: dict[tuple[str, str], tuple[float, "AgentContext"]] = {}
_PERMISSION_CACHE_TTL = 60.0


def _cache_get(agent_id: str, board_id: str) -> "AgentContext | None":
    """Get cached AgentContext if within TTL."""
    import time
    key = (agent_id, board_id)
    entry = _permission_cache.get(key)
    if entry and (time.time() - entry[0]) < _PERMISSION_CACHE_TTL:
        return entry[1]
    if entry:
        del _permission_cache[key]
    return None


def _cache_set(agent_id: str, board_id: str, ctx: "AgentContext") -> None:
    """Cache AgentContext with current timestamp."""
    import time
    _permission_cache[(agent_id, board_id)] = (time.time(), ctx)


def invalidate_agent_cache(agent_id: str) -> None:
    """Drop all cached AgentContext entries for an agent across all boards.

    Call after any change that affects effective permissions (preset/flags
    update, board grant/revoke, board overrides change). Without this,
    agents see stale permissions for up to _PERMISSION_CACHE_TTL seconds.
    """
    keys_to_drop = [k for k in _permission_cache if k[0] == agent_id]
    for k in keys_to_drop:
        del _permission_cache[k]


async def _get_authenticated_agent():
    """Get the agent authenticated via the active API key from the request."""
    api_key = _active_api_key.get()
    if not api_key:
        return None
    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agent = await service.get_agent_by_key(api_key)
        await db.commit()
        return agent


async def _get_agent_ctx(board_id: str) -> AgentContext | None:
    """Authenticate agent from active API key and verify board access.

    Resolves granular PermissionSet (agent_flags ∩ board_overrides) with 60s cache.
    Falls back to legacy flat permissions if permission_flags is not set.
    """
    api_key = _active_api_key.get()
    if not api_key:
        return None
    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agent = await service.get_agent_by_key(api_key)
        if not agent:
            return None

        # Check board access — also loads AgentBoard record
        from sqlalchemy import select as sa_select
        from okto_pulse.core.models.db import AgentBoard
        ab_query = sa_select(AgentBoard).where(
            AgentBoard.agent_id == agent.id,
            AgentBoard.board_id == board_id,
        )
        ab_result = await db.execute(ab_query)
        agent_board = ab_result.scalar_one_or_none()
        if not agent_board:
            return None

        # Check cache
        cached = _cache_get(agent.id, board_id)
        if cached:
            await db.commit()
            return cached

        # Resolve permissions
        agent_flags = getattr(agent, "permission_flags", None)
        if agent_flags is not None:
            # New granular system
            from okto_pulse.core.infra.permissions import resolve_permissions
            # Load preset flags if agent has a preset
            preset_flags = None
            preset_id = getattr(agent, "preset_id", None)
            if preset_id:
                from okto_pulse.core.models.db import PermissionPreset
                preset = await db.get(PermissionPreset, preset_id)
                if preset:
                    preset_flags = preset.flags
            board_overrides = getattr(agent_board, "permission_overrides", None)
            perm_set = resolve_permissions(agent_flags, preset_flags, board_overrides)
        else:
            # Legacy: use flat permissions list (backward compat)
            perm_set = agent.permissions

        await db.commit()
        ctx = AgentContext(
            agent_id=agent.id,
            agent_name=agent.name,
            board_id=board_id,
            permissions=perm_set,
        )
        _cache_set(agent.id, board_id, ctx)
        return ctx


async def _log_card_activity(
    db, board_id: str, card_id: str, action: str, ctx: AgentContext, details: dict | None = None
) -> None:
    """Log card-level activity from an MCP agent."""
    board_service = BoardService(db)
    await board_service._log_activity(
        board_id=board_id, card_id=card_id,
        action=action, actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
        details=details,
    )


async def _safe_spec_update(service, spec_id: str, agent_id: str, payload):
    """Wrap SpecService.update_spec so the ValueError raised by
    `_validate_spec_linked_refs` (orphan link references) is rendered as
    a structured JSON error instead of propagating to the MCP transport
    as a generic 500.

    Returns a tuple (spec, error_json). On success: (spec, None). On
    validation failure: (None, '{"error": "..."}').
    """
    try:
        spec = await service.update_spec(spec_id, agent_id, payload)
        return spec, None
    except ValueError as exc:
        import json as _json
        return None, _json.dumps({"error": str(exc)})


def _canonical_api_contract_error(exc) -> str:
    """Render a Pydantic ValidationError as a canonical api-contract domain error.

    F10: the agent gets a structured ``invalid_api_contract`` error with a clean
    detail string and NO ``errors.pydantic.dev`` URL / raw exception surface
    (``errors(include_url=False)`` drops the library URL). Mirrors the
    kg_tools.py ValidationError-to-canonical precedent.
    """
    details = "; ".join(
        ((".".join(str(p) for p in e.get("loc", ())) + ": ") if e.get("loc") else "")
        + str(e.get("msg", "invalid value"))
        for e in exc.errors(include_url=False)
    )
    return json.dumps({"error": "invalid_api_contract", "detail": details})


def _validate_api_contract_write(contract: dict) -> str | None:
    """Validate one api-contract dict as a WRITE and surface a canonical error.

    http strictness applies here (``on_write`` validation context, F9 — a
    non-verb method such as ``"CALL"`` is rejected at the boundary); a malformed
    shape becomes the canonical ``invalid_api_contract`` error with no
    ``errors.pydantic.dev`` leak (F10). Returns the error JSON string when
    invalid, else ``None``. Read-back/deserialization elsewhere stays tolerant
    (it never passes ``on_write``), so pre-existing stored contracts still load.
    """
    from pydantic import ValidationError
    from okto_pulse.core.models.schemas import ApiContract

    try:
        ApiContract.model_validate(contract, context={"on_write": True})
        return None
    except ValidationError as exc:
        return _canonical_api_contract_error(exc)


def _auth_error() -> str:
    return json.dumps({"error": "Authentication failed or board access denied"})


def _perm_error(msg: str) -> str:
    return json.dumps({"error": msg})


def _mcp_permission_error_response(msg: str) -> str:
    try:
        payload = json.loads(msg)
    except json.JSONDecodeError:
        payload = {"error": msg}
    return json.dumps(payload)


def _mcp_check_permission(
    permissions: Any,
    granular_permission: str,
    legacy_permission: str | None = None,
) -> str | None:
    """Check granular flags while keeping legacy flat permissions working."""
    if permissions is None:
        return None

    from okto_pulse.core.infra.permissions import PermissionSet

    if isinstance(permissions, PermissionSet):
        return permissions.check(granular_permission)
    if granular_permission in permissions:
        return None
    if legacy_permission and legacy_permission in permissions:
        return None
    return f"Permission denied: requires '{granular_permission}'"


def _mcp_check_story_state_permission(
    permissions: Any,
    granular_permission: str | None,
    story: Any,
    legacy_permission: str | None = None,
) -> str | None:
    if not granular_permission:
        return None

    from okto_pulse.core.infra.permissions import PermissionSet

    if isinstance(permissions, PermissionSet):
        return permissions.check_with_state(
            granular_permission,
            "story",
            story_state(story.status, archived=bool(getattr(story, "archived", False))),
        )
    return _mcp_check_permission(permissions, granular_permission, legacy_permission)


def _mcp_architecture_legacy_permission(parent_type: str, action: str) -> str:
    if action == "read":
        return Permissions.BOARD_READ
    if parent_type == "card":
        return Permissions.CARDS_UPDATE
    return Permissions.SPECS_UPDATE


def _mcp_check_architecture_permission(
    permissions: Any,
    parent_type: str,
    action: str,
) -> str | None:
    return _mcp_check_permission(
        permissions,
        f"{parent_type}.architecture.{action}",
        _mcp_architecture_legacy_permission(parent_type, action),
    )


def _mcp_check_architecture_copy_permission(permissions: Any) -> str | None:
    return _mcp_check_permission(
        permissions,
        "card.copy_from_spec.architecture",
        Permissions.CARDS_UPDATE,
    )


def _mcp_resource_gate_legacy_permission(entity_type: str, action: str) -> str:
    if action == "read":
        return Permissions.BOARD_READ
    if entity_type == "card":
        return Permissions.CARDS_UPDATE
    return Permissions.SPECS_UPDATE


def _mcp_check_resource_gate_permission(
    permissions: Any,
    entity_type: str,
    action: str,
) -> str | None:
    capability = "read" if action == "read" else "edit_fields"
    return _mcp_check_permission(
        permissions,
        f"{entity_type}.entity.{capability}",
        _mcp_resource_gate_legacy_permission(entity_type, action),
    )


def _resource_gate_error_response(exc: ResourceGateError) -> str:
    return json.dumps(
        {
            "success": False,
            "error": str(exc),
            "code": exc.code,
            "details": exc.details,
        },
        default=str,
    )


def _flag_enabled(value: str) -> bool:
    return str(value).lower() in ("true", "1", "yes")


def _dump_model(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


def _serialize_knowledge_base(kb: Any, *, include_content: bool = True) -> dict[str, Any]:
    """Serialize refinement/spec/ideation KB rows without assuming shape drift."""
    if isinstance(kb, dict):
        data = {
            "id": kb.get("id"),
            "title": kb.get("title") or kb.get("name"),
            "description": kb.get("description"),
            "mime_type": kb.get("mime_type") or kb.get("content_type") or "text/markdown",
        }
        for attr in (
            "ideation_id",
            "refinement_id",
            "spec_id",
            "source",
            "source_type",
            "source_id",
            "source_title",
            "source_version",
            "source_kb_id",
        ):
            if kb.get(attr):
                data[attr] = kb[attr]
        if include_content:
            data["content"] = kb.get("content")
        for attr in ("created_by", "created_at", "updated_at"):
            if kb.get(attr):
                data[attr] = kb[attr]
        return data

    data: dict[str, Any] = {
        "id": getattr(kb, "id", None),
        "title": getattr(kb, "title", None),
        "description": getattr(kb, "description", None),
        "mime_type": getattr(kb, "mime_type", "text/markdown"),
    }
    for attr in (
        "ideation_id",
        "refinement_id",
        "spec_id",
        "source_type",
        "source_id",
        "source_title",
        "source_version",
        "source_kb_id",
    ):
        value = getattr(kb, attr, None)
        if value:
            data[attr] = value
    if include_content:
        data["content"] = getattr(kb, "content", None)
    if getattr(kb, "created_by", None):
        data["created_by"] = kb.created_by
    if getattr(kb, "created_at", None):
        data["created_at"] = kb.created_at.isoformat()
    if getattr(kb, "updated_at", None):
        data["updated_at"] = kb.updated_at.isoformat()
    return data


def _mcp_spec_coverage_summary(spec: Any) -> dict[str, Any]:
    """Return canonical coverage plus the legacy get_spec_context aliases."""
    cards = list(getattr(spec, "cards", None) or [])
    coverage = _spec_coverage(spec, cards=cards)
    return {
        **coverage,
        "acceptance_criteria_total": coverage.get("ac_total", 0),
        "acceptance_criteria_covered": coverage.get("ac_covered", 0),
        "uncovered_indices": coverage.get("ac_uncovered_indices", []),
        "test_scenarios_total": coverage.get("scenarios_total", 0),
        "business_rules_total": coverage.get("brs_total", 0),
        "api_contracts_total": coverage.get("contracts_total", 0),
        "integration_requirements_total": coverage.get("irs_total", 0),
        "observability_requirements_total": coverage.get("ors_total", 0),
        "cards_total": len(cards),
        "cards_done": sum(1 for c in cards if getattr(getattr(c, "status", None), "value", None) == "done"),
    }


_LEGACY_COVERAGE_ENV = "OKTO_PULSE_LEGACY_COVERAGE"


def _legacy_coverage_default() -> bool:
    return os.environ.get(_LEGACY_COVERAGE_ENV, "").lower() in ("1", "true", "yes")


def _saturation_or_coverage(coverage_dict: dict[str, Any]) -> dict[str, Any]:
    """Pack the minimal saturation envelope (Ideação token-optimization Story 1).

    Returns {"saturation": {"pct", "blocking"}} by default — ~60 bytes vs ~1.5KB
    of the full coverage block. When OKTO_PULSE_LEGACY_COVERAGE=1, also
    includes the verbose "coverage" block for backwards compatibility.
    """
    from okto_pulse.core.services.analytics_service import spec_saturation_envelope

    out: dict[str, Any] = {"saturation": spec_saturation_envelope(coverage_dict)}
    if _legacy_coverage_default():
        out["coverage"] = coverage_dict
    return out


def _parse_json_arg(value: Any, default: Any) -> tuple[Any, str | None]:
    if value is None or value == "":
        return default, None
    if isinstance(value, (dict, list)):
        return value, None
    try:
        return json.loads(value), None
    except Exception as exc:
        return None, f"Invalid JSON argument: {exc}"


def _mcp_architecture_error(exc: Exception) -> str:
    if isinstance(exc, ArchitectureWarningAcknowledgementRequired):
        return json.dumps({"success": False, **exc.to_payload()}, default=str)
    return json.dumps({"error": str(exc)})


async def _mcp_require_architecture_mutable(db, design_id: str) -> tuple[Any | None, str | None]:
    from okto_pulse.core.models.db import ArchitectureDesign, Spec

    design = await db.get(ArchitectureDesign, design_id)
    if not design:
        return None, "Architecture design not found"
    if design.parent_type == "spec":
        spec = await db.get(Spec, design.spec_id)
        if not spec:
            return None, "Spec not found"
        current_id = getattr(spec, "current_validation_id", None)
        validations = getattr(spec, "validations", None) or []
        current = next((item for item in validations if item.get("id") == current_id), None)
        if current_id and current and current.get("outcome") == "success":
            return None, "Spec is locked because validation passed. Move it back to draft or approved to edit architecture."
    return design, None


async def _mcp_architecture_for_parent(
    db,
    parent_type: str,
    parent_id: str,
    *,
    include_payloads: bool = False,
    permissions: Any = None,
) -> list[dict[str, Any]]:
    if permissions is not None:
        action = "render" if include_payloads else "read"
        if _mcp_check_architecture_permission(permissions, parent_type, action):
            return []

    repo = ArchitectureDesignRepository(db)
    designs = await repo.list(parent_type, parent_id, include_payloads=include_payloads)
    if include_payloads:
        return [_dump_model(repo.to_response(design)) for design in designs]
    return [_dump_model(repo.to_response(design)) for design in designs]


# Maximum bytes loadable via file_path/file_url (16 MB). Prevents runaway memory on large files.
_MAX_CONTENT_BYTES = 16 * 1024 * 1024


async def _resolve_text_content(
    *,
    content: str,
    file_path: str | None,
    file_url: str | None,
) -> tuple[str | None, str | None]:
    """Resolve text content from inline string, local file path, or URL.

    Exactly one source must be provided. When file_path/file_url is used,
    the MCP server reads the content server-side — the bytes never cross
    the LLM context, saving tokens.

    Returns:
        (resolved_content, error) — exactly one is non-None.
    """
    provided = [bool(content), bool(file_path), bool(file_url)]
    if sum(provided) == 0:
        return None, "One of 'content', 'file_path', or 'file_url' must be provided"
    if sum(provided) > 1:
        return None, "Only one of 'content', 'file_path', or 'file_url' may be provided"

    if content:
        return content.replace("\\n", "\n"), None

    if file_path:
        try:
            p = Path(file_path).expanduser().resolve(strict=True)
        except (OSError, RuntimeError) as e:
            return None, f"file_path could not be resolved: {e}"
        if not p.is_file():
            return None, f"file_path is not a regular file: {p}"
        try:
            size = p.stat().st_size
            if size > _MAX_CONTENT_BYTES:
                return None, f"file_path exceeds {_MAX_CONTENT_BYTES} bytes ({size})"
            return p.read_text(encoding="utf-8"), None
        except (OSError, UnicodeDecodeError) as e:
            return None, f"file_path could not be read as UTF-8 text: {e}"

    # file_url
    try:
        import httpx

        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            resp = await client.get(file_url)
            resp.raise_for_status()
            raw = resp.content
            if len(raw) > _MAX_CONTENT_BYTES:
                return None, f"file_url exceeds {_MAX_CONTENT_BYTES} bytes ({len(raw)})"
            try:
                return raw.decode("utf-8"), None
            except UnicodeDecodeError as e:
                return None, f"file_url response is not valid UTF-8 text: {e}"
    except Exception as e:
        return None, f"file_url fetch failed: {e}"


async def _resolve_binary_content(
    *,
    content_base64: str,
    file_path: str | None,
    file_url: str | None,
) -> tuple[bytes | None, str | None]:
    """Resolve binary content from base64 string, local file path, or URL.

    Mirrors _resolve_text_content but returns raw bytes for binary uploads.
    """
    import base64

    provided = [bool(content_base64), bool(file_path), bool(file_url)]
    if sum(provided) == 0:
        return None, "One of 'content_base64', 'file_path', or 'file_url' must be provided"
    if sum(provided) > 1:
        return None, "Only one of 'content_base64', 'file_path', or 'file_url' may be provided"

    if content_base64:
        try:
            return base64.b64decode(content_base64), None
        except Exception as e:
            return None, f"Invalid base64 content: {e}"

    if file_path:
        try:
            p = Path(file_path).expanduser().resolve(strict=True)
        except (OSError, RuntimeError) as e:
            return None, f"file_path could not be resolved: {e}"
        if not p.is_file():
            return None, f"file_path is not a regular file: {p}"
        try:
            size = p.stat().st_size
            if size > _MAX_CONTENT_BYTES:
                return None, f"file_path exceeds {_MAX_CONTENT_BYTES} bytes ({size})"
            return p.read_bytes(), None
        except OSError as e:
            return None, f"file_path could not be read: {e}"

    # file_url
    try:
        import httpx

        async with httpx.AsyncClient(follow_redirects=True, timeout=30.0) as client:
            resp = await client.get(file_url)
            resp.raise_for_status()
            raw = resp.content
            if len(raw) > _MAX_CONTENT_BYTES:
                return None, f"file_url exceeds {_MAX_CONTENT_BYTES} bytes ({len(raw)})"
            return raw, None
    except Exception as e:
        return None, f"file_url fetch failed: {e}"


# D-8: helpers canônicos em services/analytics_service.py — re-exports para
# preservar import paths existentes (tests + callers legados).
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    decisions_stats as _decisions_stats,  # noqa: F401
    filter_decisions_by_status as _filter_decisions_by_status,  # noqa: F401
    render_decisions_markdown as _render_decisions_markdown,  # noqa: F401
)


# D-7: spec_coverage agora canônico em services/analytics_service.py — re-export
# preserva callers existentes em mcp/server.py + tests.
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    resolve_linked_criteria_to_indices as _resolve_linked_criteria_to_indices,  # noqa: F401
    resolve_linked_criteria_to_ids,  # noqa: F401  (write-path strict resolver — spec aafcc73f)
    resolve_linked_requirements_to_ids,  # noqa: F401  (write-path strict FR resolver — spec 9d66847f)
    resolve_linked_fr_indices,  # noqa: F401  (read-path tolerant FR resolver — FR4)
    spec_coverage_summary as _spec_coverage,  # noqa: F401
    _structured_ref_id,  # noqa: F401  (used to enumerate available ac_ids in errors)
)


# ============================================================================
# XML SAFETY MIDDLEWARE - spec 44415298 (centralized detection)
# ============================================================================
# Defensive observer for the client-side tool-use parser bug: nested
# `<parameter>` tags in string content collapse, corrupting the payload
# before it reaches the server. We can't reconstruct lost info, but we
# emit a structured log when literal protocol tags survive into args, so
# operators can pinpoint which tool calls were affected. Applied to every
# `@mcp.tool()` registration via a monkey-patch installed below — single
# point of instrumentation, 100% coverage of the 160 MCP tools.

_XML_SAFETY_LOGGER = logging.getLogger("okto_pulse.mcp.parser_safety")

_SUSPICIOUS_XML_PATTERNS = re.compile(
    r"<\s*/?\s*(?:"
    r"parameter\s*(?:name\s*=)?"
    r"|function_calls"
    r"|invoke\s*(?:name\s*=)?"
    r"|antml:\w+"
    r")",
    re.IGNORECASE,
)


def _detect_nested_parameter_xml(value: Any) -> bool:
    """Return True if `value` contains a literal tool-use protocol tag."""
    if not isinstance(value, str) or not value:
        return False
    return bool(_SUSPICIOUS_XML_PATTERNS.search(value))


def _xml_safety_log_decorator(func):
    """Wrap an MCP tool: log on any string kwarg that holds a literal tool-use tag."""
    @functools.wraps(func)
    async def wrapper(**kwargs):
        for k, v in kwargs.items():
            if isinstance(v, str) and _detect_nested_parameter_xml(v):
                _XML_SAFETY_LOGGER.warning(
                    "mcp.tool.suspicious_xml_field",
                    extra={
                        "event": "mcp.tool.suspicious_xml_field",
                        "tool_name": func.__name__,
                        "field_name": k,
                        "value_preview": v[:200],
                    },
                )
        return await func(**kwargs)

    wrapper._xml_safety_wrapped = True  # type: ignore[attr-defined]
    return wrapper


_XML_SAFETY_DECORATED_COUNT = 0


def _patch_mcp_tool_for_xml_safety() -> None:
    """Patch ``mcp.tool()`` so every registered tool gets the XML safety wrapper.

    Note (FastMCP 2.14+): the original implementation called
    ``_original_mcp_tool(*args, **kwargs)`` first to obtain the registrar
    decorator, then applied ``_wrap`` to the user function. With FastMCP 2.14
    the decorator path returns ``partial(self.tool, ...)`` and ``self.tool``
    is resolved at call time via instance attribute lookup — which finds the
    *patched* ``mcp.tool`` and recurses, so the value that lands in the
    module namespace ends up being our local ``_wrap`` instead of the
    expected ``FunctionTool``. Tests that probe ``inspect.signature(fn.fn)``
    therefore see ``(func)`` and not the real tool signature.

    Fix: bypass the partial entirely by always calling
    ``_original_mcp_tool(wrapped, *args, **kwargs)`` — i.e. pass the wrapped
    function as the first positional argument so FastMCP takes the
    ``isroutine(name_or_fn)`` direct-registration path. This returns the
    ``FunctionTool`` whose ``.fn`` exposes the wrapped function with the
    original signature preserved by ``functools.wraps`` inside
    ``_xml_safety_log_decorator``.
    """
    if getattr(mcp.tool, "_xml_safety_patched", False):
        return

    _original_mcp_tool = mcp.tool

    def _patched_mcp_tool(*args, **kwargs):
        # ``@mcp.tool`` (no parens) — first positional arg is the function.
        if args and inspect.isroutine(args[0]):
            global _XML_SAFETY_DECORATED_COUNT
            func = args[0]
            wrapped = _xml_safety_log_decorator(func)
            _XML_SAFETY_DECORATED_COUNT += 1
            return _original_mcp_tool(wrapped, *args[1:], **kwargs)

        # ``@mcp.tool()`` / ``@mcp.tool("name")`` / ``@mcp.tool(name=...)`` —
        # return a decorator that, when applied, routes through the same
        # direct-registration path (no partial recursion).
        def _wrap(func):
            global _XML_SAFETY_DECORATED_COUNT
            wrapped = _xml_safety_log_decorator(func)
            _XML_SAFETY_DECORATED_COUNT += 1
            return _original_mcp_tool(wrapped, *args, **kwargs)

        return _wrap

    _patched_mcp_tool._xml_safety_patched = True  # type: ignore[attr-defined]
    mcp.tool = _patched_mcp_tool  # type: ignore[assignment]


_patch_mcp_tool_for_xml_safety()


# ============================================================================
# AGENT PROFILE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_my_profile() -> str:
    """
    Get the authenticated agent's own profile including identity, description, objective, and permissions.
    No parameters needed — the agent is identified by the API key in the MCP connection."""
    agent = await _get_authenticated_agent()
    if not agent:
        return json.dumps({"error": "Authentication failed"})

    from okto_pulse.core.infra.permissions import generate_role_summary

    return json.dumps(
        {
            "id": agent.id,
            "name": agent.name,
            "description": agent.description,
            "objective": agent.objective,
            "is_active": agent.is_active,
            "permissions": agent.permissions,
            "role_summary": generate_role_summary(agent.permissions),
            "created_at": agent.created_at.isoformat(),
            "last_used_at": (
                agent.last_used_at.isoformat() if agent.last_used_at else None
            ),
        },
        default=str,
    )


@mcp.tool()
async def okto_pulse_update_my_profile(
    description: str = "",
    objective: str = "",
) -> str:
    """
    Update the authenticated agent's own description and/or objective.
    No board_id needed — this updates the global agent profile."""
    agent = await _get_authenticated_agent()
    if not agent:
        return json.dumps({"error": "Authentication failed"})

    perm_err = check_permission(agent.permissions, Permissions.SELF_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agent = await service.get_agent(agent.id)

        if not agent:
            return json.dumps({"error": "Agent not found"})

        if description:
            agent.description = description
        if objective:
            agent.objective = objective

        await db.commit()

        return json.dumps(
            {
                "success": True,
                "profile": {
                    "id": agent.id,
                    "name": agent.name,
                    "description": agent.description,
                    "objective": agent.objective,
                },
            }
        )


@mcp.tool()
async def okto_pulse_list_my_boards() -> str:
    """
    List all boards the authenticated agent has access to.
    No parameters needed — the agent is identified by the API key in the MCP connection."""
    agent = await _get_authenticated_agent()
    if not agent:
        return json.dumps({"error": "Authentication failed"})

    async with get_db_for_mcp() as db:
        service = AgentService(db)
        boards = await service.list_boards_for_agent(agent.id)
        await db.commit()

        return json.dumps(
            {
                "agent_id": agent.id,
                "agent_name": agent.name,
                "boards": [
                    {
                        "id": b.id,
                        "name": b.name,
                        "description": b.description,
                    }
                    for b in boards
                ],
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_list_my_mentions(board_id: str, include_seen: str = "false") -> str:
    """
    List comments and Q&A items where you are mentioned via @name.
    By default only returns UNSEEN mentions. Use include_seen="true" to get all."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from sqlalchemy import select, or_

    from okto_pulse.core.models.db import AgentSeenItem, Card, Comment, Ideation, IdeationQAItem, QAItem, Refinement, RefinementQAItem, Spec, SpecQAItem

    mention_pattern = f"%@{ctx.agent_name}%"
    show_all = include_seen.lower() == "true"

    async with get_db_for_mcp() as db:
        # Get set of seen item IDs for this agent
        seen_ids: set[str] = set()
        if not show_all:
            seen_query = select(AgentSeenItem.item_id).where(
                AgentSeenItem.agent_id == ctx.agent_id
            )
            seen_ids = {r[0] for r in (await db.execute(seen_query)).all()}

        # Search comments on cards
        comment_query = (
            select(Comment, Card.title)
            .join(Card, Card.id == Comment.card_id)
            .where(Card.board_id == board_id)
            .where(Comment.content.ilike(mention_pattern))
            .order_by(Comment.created_at.desc())
        )
        comment_results = (await db.execute(comment_query)).all()

        # Search QA on cards
        qa_query = (
            select(QAItem, Card.title)
            .join(Card, Card.id == QAItem.card_id)
            .where(Card.board_id == board_id)
            .where(
                or_(
                    QAItem.question.ilike(mention_pattern),
                    QAItem.answer.ilike(mention_pattern),
                )
            )
            .order_by(QAItem.created_at.desc())
        )
        qa_results = (await db.execute(qa_query)).all()

        # Search QA on specs
        spec_qa_query = (
            select(SpecQAItem, Spec.title)
            .join(Spec, Spec.id == SpecQAItem.spec_id)
            .where(Spec.board_id == board_id)
            .where(
                or_(
                    SpecQAItem.question.ilike(mention_pattern),
                    SpecQAItem.answer.ilike(mention_pattern),
                )
            )
            .order_by(SpecQAItem.created_at.desc())
        )
        spec_qa_results = (await db.execute(spec_qa_query)).all()

        # Search QA on ideations
        ideation_qa_query = (
            select(IdeationQAItem, Ideation.title)
            .join(Ideation, Ideation.id == IdeationQAItem.ideation_id)
            .where(Ideation.board_id == board_id)
            .where(
                or_(
                    IdeationQAItem.question.ilike(mention_pattern),
                    IdeationQAItem.answer.ilike(mention_pattern),
                )
            )
            .order_by(IdeationQAItem.created_at.desc())
        )
        ideation_qa_results = (await db.execute(ideation_qa_query)).all()

        # Search QA on refinements
        refinement_qa_query = (
            select(RefinementQAItem, Refinement.title)
            .join(Refinement, Refinement.id == RefinementQAItem.refinement_id)
            .where(Refinement.board_id == board_id)
            .where(
                or_(
                    RefinementQAItem.question.ilike(mention_pattern),
                    RefinementQAItem.answer.ilike(mention_pattern),
                )
            )
            .order_by(RefinementQAItem.created_at.desc())
        )
        refinement_qa_results = (await db.execute(refinement_qa_query)).all()
        await db.commit()

        mentions = []
        for comment, card_title in comment_results:
            if not show_all and comment.id in seen_ids:
                continue
            mentions.append({
                "type": "comment",
                "item_id": comment.id,
                "card_id": comment.card_id,
                "card_title": card_title,
                "content": comment.content,
                "author": comment.author_id,
                "created_at": comment.created_at.isoformat(),
            })
        for qa, card_title in qa_results:
            if not show_all and qa.id in seen_ids:
                continue
            mentions.append({
                "type": "qa",
                "item_id": qa.id,
                "card_id": qa.card_id,
                "card_title": card_title,
                "question": qa.question,
                "answer": qa.answer,
                "asked_by": qa.asked_by,
                "created_at": qa.created_at.isoformat(),
            })
        for spec_qa, spec_title in spec_qa_results:
            if not show_all and spec_qa.id in seen_ids:
                continue
            mentions.append({
                "type": "spec_qa",
                "item_id": spec_qa.id,
                "spec_id": spec_qa.spec_id,
                "spec_title": spec_title,
                "question": spec_qa.question,
                "question_type": spec_qa.question_type,
                "choices": spec_qa.choices,
                "answer": spec_qa.answer,
                "selected": spec_qa.selected,
                "asked_by": spec_qa.asked_by,
                "created_at": spec_qa.created_at.isoformat(),
            })
        for ideation_qa, ideation_title in ideation_qa_results:
            if not show_all and ideation_qa.id in seen_ids:
                continue
            mentions.append({
                "type": "ideation_qa",
                "item_id": ideation_qa.id,
                "ideation_id": ideation_qa.ideation_id,
                "ideation_title": ideation_title,
                "question": ideation_qa.question,
                "question_type": ideation_qa.question_type,
                "choices": ideation_qa.choices,
                "answer": ideation_qa.answer,
                "selected": ideation_qa.selected,
                "asked_by": ideation_qa.asked_by,
                "created_at": ideation_qa.created_at.isoformat(),
            })
        for refinement_qa, refinement_title in refinement_qa_results:
            if not show_all and refinement_qa.id in seen_ids:
                continue
            mentions.append({
                "type": "refinement_qa",
                "item_id": refinement_qa.id,
                "refinement_id": refinement_qa.refinement_id,
                "refinement_title": refinement_title,
                "question": refinement_qa.question,
                "question_type": refinement_qa.question_type,
                "choices": refinement_qa.choices,
                "answer": refinement_qa.answer,
                "selected": refinement_qa.selected,
                "asked_by": refinement_qa.asked_by,
                "created_at": refinement_qa.created_at.isoformat(),
            })

        mentions.sort(key=lambda m: m["created_at"], reverse=True)

        return json.dumps(
            {
                "agent_name": ctx.agent_name,
                "unseen_count": len(mentions),
                "filter": "unseen_only" if not show_all else "all",
                "mentions": mentions,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_mark_as_seen(board_id: str, item_ids: list[str] | str) -> str:
    """
    Mark one or more items as seen so they won't appear in list_my_mentions.
    Use this after processing mentions to avoid seeing them again."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from sqlalchemy import select

    from okto_pulse.core.models.db import AgentSeenItem, Comment, IdeationQAItem, QAItem, RefinementQAItem, SpecQAItem

    try:
        ids = coerce_to_list_str(item_ids)
    except ValueError as e:
        return json.dumps({"error": f"Invalid item_ids: {e}"})
    if not ids:
        return json.dumps({"error": "No item_ids provided"})

    async with get_db_for_mcp() as db:
        marked = 0
        for item_id in ids:
            # Check if already seen
            existing = await db.execute(
                select(AgentSeenItem).where(
                    AgentSeenItem.agent_id == ctx.agent_id,
                    AgentSeenItem.item_id == item_id,
                )
            )
            if existing.scalar_one_or_none():
                continue
            seen = AgentSeenItem(
                agent_id=ctx.agent_id,
                item_type="mention",
                item_id=item_id,
            )
            db.add(seen)
            marked += 1
        await db.commit()

        # Log activity for affected cards and specs
        if marked > 0:
            comment_result = await db.execute(
                select(Comment.card_id).where(Comment.id.in_(ids)).distinct()
            )
            qa_result = await db.execute(
                select(QAItem.card_id).where(QAItem.id.in_(ids)).distinct()
            )
            card_ids = set(
                row[0] for row in comment_result.fetchall()
            ) | set(
                row[0] for row in qa_result.fetchall()
            )
            for card_id in card_ids:
                await _log_card_activity(db, board_id, card_id, "items_seen", ctx, {"item_count": marked})

            # Log spec Q&A seen
            spec_qa_result = await db.execute(
                select(SpecQAItem.spec_id).where(SpecQAItem.id.in_(ids)).distinct()
            )
            spec_ids = {row[0] for row in spec_qa_result.fetchall()}
            if spec_ids:
                board_service = BoardService(db)
                for spec_id in spec_ids:
                    await board_service._log_activity(
                        board_id=board_id, action="spec_qa_seen",
                        actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
                        details={"spec_id": spec_id, "item_count": marked},
                    )

            # Log ideation Q&A seen
            ideation_qa_result = await db.execute(
                select(IdeationQAItem.ideation_id).where(IdeationQAItem.id.in_(ids)).distinct()
            )
            ideation_ids = {row[0] for row in ideation_qa_result.fetchall()}
            if ideation_ids:
                board_service = BoardService(db)
                for ideation_id in ideation_ids:
                    await board_service._log_activity(
                        board_id=board_id, action="ideation_qa_seen",
                        actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
                        details={"ideation_id": ideation_id, "item_count": marked},
                    )

            # Log refinement Q&A seen
            refinement_qa_result = await db.execute(
                select(RefinementQAItem.refinement_id).where(RefinementQAItem.id.in_(ids)).distinct()
            )
            refinement_ids = {row[0] for row in refinement_qa_result.fetchall()}
            if refinement_ids:
                board_service = BoardService(db)
                for refinement_id in refinement_ids:
                    await board_service._log_activity(
                        board_id=board_id, action="refinement_qa_seen",
                        actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
                        details={"refinement_id": refinement_id, "item_count": marked},
                    )
            await db.commit()

        return json.dumps(
            {"success": True, "marked_count": marked, "total_requested": len(ids)}
        )


@mcp.tool()
async def okto_pulse_get_unseen_summary(board_id: str) -> str:
    """
    Quick summary of unseen mentions and activity for the agent on this board.
    Use this to check if there's anything new without fetching full details."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from sqlalchemy import func as sqla_func
    from sqlalchemy import or_, select

    from okto_pulse.core.models.db import (
        ActivityLog,
        AgentSeenItem,
        Card,
        Comment,
        Ideation,
        IdeationQAItem,
        QAItem,
        Refinement,
        RefinementQAItem,
        Spec,
        SpecQAItem,
    )

    mention_pattern = f"%@{ctx.agent_name}%"

    async with get_db_for_mcp() as db:
        # Get seen IDs
        seen_query = select(AgentSeenItem.item_id).where(
            AgentSeenItem.agent_id == ctx.agent_id
        )
        seen_ids = {r[0] for r in (await db.execute(seen_query)).all()}

        # Count comment mentions
        comment_query = (
            select(sqla_func.count())
            .select_from(Comment)
            .join(Card, Card.id == Comment.card_id)
            .where(Card.board_id == board_id)
            .where(Comment.content.ilike(mention_pattern))
        )
        total_comment_mentions = (await db.execute(comment_query)).scalar() or 0

        # Count card QA mentions
        qa_query = (
            select(sqla_func.count())
            .select_from(QAItem)
            .join(Card, Card.id == QAItem.card_id)
            .where(Card.board_id == board_id)
            .where(
                or_(
                    QAItem.question.ilike(mention_pattern),
                    QAItem.answer.ilike(mention_pattern),
                )
            )
        )
        total_qa_mentions = (await db.execute(qa_query)).scalar() or 0

        # Count spec QA mentions
        spec_qa_query = (
            select(sqla_func.count())
            .select_from(SpecQAItem)
            .join(Spec, Spec.id == SpecQAItem.spec_id)
            .where(Spec.board_id == board_id)
            .where(
                or_(
                    SpecQAItem.question.ilike(mention_pattern),
                    SpecQAItem.answer.ilike(mention_pattern),
                )
            )
        )
        total_spec_qa_mentions = (await db.execute(spec_qa_query)).scalar() or 0

        # Count ideation QA mentions
        ideation_qa_query = (
            select(sqla_func.count())
            .select_from(IdeationQAItem)
            .join(Ideation, Ideation.id == IdeationQAItem.ideation_id)
            .where(Ideation.board_id == board_id)
            .where(
                or_(
                    IdeationQAItem.question.ilike(mention_pattern),
                    IdeationQAItem.answer.ilike(mention_pattern),
                )
            )
        )
        total_ideation_qa_mentions = (await db.execute(ideation_qa_query)).scalar() or 0

        # Count refinement QA mentions
        refinement_qa_query = (
            select(sqla_func.count())
            .select_from(RefinementQAItem)
            .join(Refinement, Refinement.id == RefinementQAItem.refinement_id)
            .where(Refinement.board_id == board_id)
            .where(
                or_(
                    RefinementQAItem.question.ilike(mention_pattern),
                    RefinementQAItem.answer.ilike(mention_pattern),
                )
            )
        )
        total_refinement_qa_mentions = (await db.execute(refinement_qa_query)).scalar() or 0

        # Recent activity count (last 24h)
        from datetime import timedelta

        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        activity_query = (
            select(sqla_func.count())
            .select_from(ActivityLog)
            .where(
                ActivityLog.board_id == board_id,
                ActivityLog.created_at >= recent_cutoff,
            )
        )
        recent_activity = (await db.execute(activity_query)).scalar() or 0
        await db.commit()

        total_mentions = total_comment_mentions + total_qa_mentions + total_spec_qa_mentions + total_ideation_qa_mentions + total_refinement_qa_mentions
        unseen_mentions = total_mentions - len(seen_ids)
        if unseen_mentions < 0:
            unseen_mentions = 0

        return json.dumps(
            {
                "board_id": board_id,
                "unseen_mentions": unseen_mentions,
                "total_mentions": total_mentions,
                "seen_count": len(seen_ids),
                "recent_activity_24h": recent_activity,
            }
        )


# ============================================================================
# BOARD TOOLS
# ============================================================================


_BOARD_INCLUDE_KEYS = ("ideations", "specs", "cards", "agents")


def _parse_include(include: str) -> set[str]:
    """Parse the ?include= csv into a set of board collection keys.

    `*` expands to every known collection. Unknown tokens are silently dropped.
    """
    if not include:
        return set()
    if include.strip() == "*":
        return set(_BOARD_INCLUDE_KEYS)
    tokens = {tok.strip() for tok in include.split(",") if tok.strip()}
    return tokens & set(_BOARD_INCLUDE_KEYS)


@mcp.tool()
async def okto_pulse_get_board(board_id: str, include: str = "") -> str:
    """
    Get board details. Defaults to a minimal overview envelope; pass `include` to
    inline collections.

    Ideação MCP-token-optimization Story 2: the default response carries id,
    name, description, owner_id, settings, counts{} and timestamps — ~200B vs
    ~10KB on a typical board."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    wanted = _parse_include(include)

    async with get_db_for_mcp() as db:
        service = BoardService(db)
        board = await service.get_board(board_id)
        await db.commit()

        if not board:
            return json.dumps({"error": "Board not found"})

        agent_service = AgentService(db)
        spec_service = SpecService(db)
        ideation_service = IdeationService(db)

        board_agents = await agent_service.list_agents_for_board(board_id)
        board_specs = await spec_service.list_specs(board_id)
        board_ideations = await ideation_service.list_ideations(board_id)
        board_cards = list(board.cards or [])

        payload: dict[str, Any] = {
            "id": board.id,
            "name": board.name,
            "description": board.description,
            "owner_id": board.owner_id,
            "settings": board.settings or {},
            "created_at": board.created_at.isoformat(),
            "updated_at": board.updated_at.isoformat(),
            "counts": {
                "ideations": len(board_ideations),
                "specs": len(board_specs),
                "cards": len(board_cards),
                "agents": len(board_agents),
            },
        }
        if "ideations" in wanted:
            payload["ideations"] = [
                {
                    "id": i.id,
                    "title": i.title,
                    "status": i.status.value,
                    "complexity": i.complexity.value if i.complexity else None,
                    "version": i.version,
                    "labels": i.labels,
                }
                for i in board_ideations
            ]
        if "specs" in wanted:
            payload["specs"] = [
                {
                    "id": s.id,
                    "title": s.title,
                    "status": s.status.value,
                    "version": s.version,
                    "labels": s.labels,
                }
                for s in board_specs
            ]
        if "cards" in wanted:
            payload["cards"] = [
                {
                    "id": c.id,
                    "title": c.title,
                    "description": c.description,
                    "status": c.status.value,
                    "position": c.position,
                    "assignee_id": c.assignee_id,
                    "spec_id": c.spec_id,
                    "due_date": (c.due_date.isoformat() if c.due_date else None),
                    "labels": c.labels,
                }
                for c in board_cards
            ]
        if "agents" in wanted:
            payload["agents"] = [
                {
                    "id": a.id,
                    "name": a.name,
                    "description": a.description,
                    "is_active": a.is_active,
                }
                for a in board_agents
            ]
        return json.dumps(payload, default=str)


@mcp.tool()
async def okto_pulse_list_agents(board_id: str) -> str:
    """
    List all agents registered on the board."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.infra.permissions import generate_role_summary

    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agents = await service.list_agents(board_id)
        await db.commit()

        return json.dumps(
            [
                {
                    "id": a.id,
                    "name": a.name,
                    "description": a.description,
                    "objective": a.objective,
                    "is_active": a.is_active,
                    "role_summary": generate_role_summary(a.permissions),
                    "created_at": a.created_at.isoformat(),
                    "last_used_at": (
                        a.last_used_at.isoformat() if a.last_used_at else None
                    ),
                }
                for a in agents
            ],
            default=str,
        )


@mcp.tool()
async def okto_pulse_list_board_members(board_id: str) -> str:
    """
    List all members of the board (owner + agents)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        board_service = BoardService(db)
        board = await board_service.get_board(board_id)
        await db.commit()

        if not board:
            return json.dumps({"error": "Board not found"})

        agent_service = AgentService(db)
        board_agents = await agent_service.list_agents_for_board(board_id)

        return json.dumps(
            {
                "owner": {"id": board.owner_id, "type": "user"},
                "agents": [
                    {
                        "id": a.id,
                        "name": a.name,
                        "description": a.description,
                        "objective": a.objective,
                        "is_active": a.is_active,
                        "type": "agent",
                    }
                    for a in board_agents
                ],
            },
            default=str,
        )


def _activity_log_summary(action: str, details: Any) -> str:
    """Backward-compatible wrapper for the shared activity summary service."""
    return activity_log_summary(action, details)


def _encode_activity_cursor(created_at: datetime, row_id: str) -> str:
    """Encode an activity_log row position as an opaque base64 cursor.

    Ideação MCP-token-optimization (cursor-pagination spec). Cursor is the
    URL-safe base64 of a 2-key JSON ``{"ts": <iso>, "id": <row_id>}``. Opaque
    so callers don't depend on the shape — future versions may add fields
    (HMAC, schema version) without breaking existing callers.
    """
    payload = json.dumps({"ts": created_at.isoformat(), "id": row_id})
    return base64.urlsafe_b64encode(payload.encode()).decode()


def _decode_activity_cursor(cursor: str) -> tuple[datetime, str] | None:
    """Decode an opaque cursor back to its ``(created_at, row_id)`` pair.

    Returns ``None`` on malformed input (invalid base64, invalid JSON, or
    missing required keys) — the caller surfaces a structured error
    (``error_code=invalid_cursor``) instead of silently falling back to the
    first page. Never raises.
    """
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor.encode()).decode()
        payload = json.loads(raw)
        ts_str = payload["ts"]
        row_id = payload["id"]
        if not isinstance(ts_str, str) or not isinstance(row_id, str):
            return None
        return (datetime.fromisoformat(ts_str), row_id)
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None


@mcp.tool()
async def okto_pulse_get_activity_log(
    board_id: str,
    limit: int = 50,
    cursor: str = "",
    envelope: bool = False,
    offset: int = 0,
    action: str = "",
    card_id: str = "",
    include_details: bool = False,
) -> str:
    """
    Get the activity log (history) for the board with optional filtering and pagination.

    Ideação MCP-token-optimization Story 3: default response carries id, action,
    trigger, card_id, created_at + a deterministic `summary` string built
    server-side from details — ~120B per row vs ~1.5KB. Pass include_details=true
    to receive the full nested details object (legacy shape).

    Cursor-pagination follow-up: pass ``cursor`` (opaque base64 from a prior
    ``next_cursor``) for O(1) keyset pagination independent of page depth.
    Pass ``envelope=true`` to receive ``{items, next_cursor}`` instead of a
    raw list (default keeps Story 3 list shape — backward compat). Legacy
    ``offset`` is silently ignored unless ``OKTO_PULSE_LEGACY_OFFSET=1``."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    # Cursor takes precedence; offset is a deprecated escape hatch.
    cursor_pair: tuple[datetime, str] | None = None
    if cursor:
        cursor_pair = _decode_activity_cursor(cursor)
        if cursor_pair is None:
            return json.dumps(
                {"error": "Invalid cursor", "error_code": "invalid_cursor"}
            )

    legacy_offset = os.getenv("OKTO_PULSE_LEGACY_OFFSET") == "1"
    effective_offset = offset if (legacy_offset and not cursor_pair) else 0

    from sqlalchemy import and_, func, or_, select

    from okto_pulse.core.models.db import ActivityLog

    async with get_db_for_mcp() as db:
        query = select(ActivityLog).where(ActivityLog.board_id == board_id)
        if action:
            query = query.where(ActivityLog.action == action)
        if card_id:
            query = query.where(ActivityLog.card_id == card_id)
        if cursor_pair is not None:
            ts, rid = cursor_pair
            # Boolean-expanded keyset filter — portable across SQLite,
            # PostgreSQL, MySQL. `tuple_(col1, col2) < (val1, val2)` row
            # comparison is not honored by SQLite's translator and silently
            # degrades to single-column compare, breaking the strict-less-
            # than semantic of the tiebreaker.
            #
            # Microsecond normalization (SQLite quirk): SQLite's func.now()
            # writes `'YYYY-MM-DD HH:MM:SS'` (no microseconds), but SQLAlchemy
            # binds a Python naive datetime as `'... .000000'`. Lexicographic
            # comparison then mis-orders: the row's bare string is "less than"
            # the cursor's `.000000`-suffixed string, so the anchor row would
            # leak into batch 2. Casting both sides via SQLAlchemy's DateTime
            # adapter normalises the format and yields semantic comparison.
            ts_normalized = ts.replace(microsecond=0)
            query = query.where(
                or_(
                    func.datetime(ActivityLog.created_at) < func.datetime(ts_normalized),
                    and_(
                        func.datetime(ActivityLog.created_at) == func.datetime(ts_normalized),
                        ActivityLog.id < rid,
                    ),
                )
            )
        query = (
            query.order_by(ActivityLog.created_at.desc(), ActivityLog.id.desc())
            .offset(effective_offset)
            .limit(limit + 1)
        )
        result = await db.execute(query)
        logs = list(result.scalars().all())
        await db.commit()

        has_more = len(logs) > limit
        if has_more:
            logs = logs[:limit]

        rows: list[dict[str, Any]] = []
        for log in logs:
            row: dict[str, Any] = {
                "id": log.id,
                "action": log.action,
                "card_id": log.card_id,
                "created_at": log.created_at.isoformat(),
                "trigger": (log.details or {}).get("trigger") if isinstance(log.details, dict) else None,
                "summary": _activity_log_summary(log.action, log.details),
            }
            if include_details:
                row["actor_type"] = log.actor_type
                row["actor_id"] = log.actor_id
                row["actor_name"] = log.actor_name
                row["details"] = sanitize_activity_details(log.details)
            rows.append(row)

        next_cursor: str | None = None
        if has_more and logs:
            last = logs[-1]
            next_cursor = _encode_activity_cursor(last.created_at, last.id)

        if envelope:
            return json.dumps(
                {"items": rows, "next_cursor": next_cursor}, default=str
            )
        return json.dumps(rows, default=str)


# ============================================================================
# CARD TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_create_card(
    board_id: str,
    title: str,
    spec_id: str,
    description: str = "",
    details: str = "",
    status: str = "not_started",
    priority: str = "none",
    assignee_id: str = "",
    labels: list[str] | str = "",
    test_scenario_ids: list[str] | str = "",
    card_type: str = "normal",
    origin_task_id: str = "",
    severity: str = "",
    expected_behavior: str = "",
    observed_behavior: str = "",
    steps_to_reproduce: str = "",
    action_plan: str = "",
) -> str:
    """
    Create a new card on the board. Every card MUST be linked to a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    # Check card.create or card.create_test based on card_type
    if card_type == "test":
        perm_err = check_permission(ctx.permissions, Permissions.CARDS_CREATE)
        if not perm_err:
            # Also check the granular create_test flag if using PermissionSet
            from okto_pulse.core.infra.permissions import PermissionSet
            if isinstance(ctx.permissions, PermissionSet):
                perm_err = ctx.permissions.check("card.entity.create_test")
        if perm_err:
            return _perm_error(perm_err)
    else:
        perm_err = check_permission(ctx.permissions, Permissions.CARDS_CREATE)
        if perm_err:
            return _perm_error(perm_err)

    from okto_pulse.core.models.db import Board, BugSeverity, CardPriority, CardStatus, CardType
    from okto_pulse.core.models.schemas import CardCreate

    try:
        card_status = CardStatus(status)
    except ValueError:
        return json.dumps(
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in CardStatus]}"
            }
        )

    try:
        card_priority = CardPriority(priority)
    except ValueError:
        return json.dumps(
            {
                "error": f"Invalid priority. Must be one of: {[p.value for p in CardPriority]}"
            }
        )

    _card_type_value = (card_type or "normal").strip().lower()
    try:
        CardType(_card_type_value)
    except ValueError:
        return json.dumps(
            {
                "error": f"Invalid card_type '{card_type}'. Must be one of: {[t.value for t in CardType]}"
            }
        )

    if severity:
        try:
            BugSeverity(severity.strip().lower())
        except ValueError:
            return json.dumps(
                {
                    "error": f"Invalid severity '{severity}'. Must be one of: {[s.value for s in BugSeverity]}"
                }
            )

    if _card_type_value == "bug":
        missing = [
            name for name, val in (
                ("origin_task_id", origin_task_id),
                ("severity", severity),
                ("expected_behavior", expected_behavior),
                ("observed_behavior", observed_behavior),
            ) if not (val or "").strip()
        ]
        if missing:
            return json.dumps(
                {
                    "error": f"Bug cards require non-empty: {', '.join(missing)}"
                }
            )

    async with get_db_for_mcp() as db:
        service = CardService(db)
        # Normalize escaped newlines (MCP clients may send \\n instead of real newlines)
        _desc = description.replace("\\n", "\n") if description else None
        _details = details.replace("\\n", "\n") if details else None

        try:
            scenario_ids_list = coerce_to_list_str(test_scenario_ids) or None
        except ValueError as e:
            return json.dumps({"error": f"Invalid test_scenario_ids: {e}"})
        try:
            _labels_list = coerce_to_list_str(labels) or None
        except ValueError as e:
            return json.dumps({"error": f"Invalid labels: {e}"})

        # Enforce max scenarios per card from board settings
        if scenario_ids_list:
            board_obj = await db.get(Board, board_id)
            max_per_card = (board_obj.settings or {}).get("max_scenarios_per_card", 3) if board_obj else 3
            if len(scenario_ids_list) > max_per_card:
                return json.dumps({
                    "error": f"Cannot link {len(scenario_ids_list)} scenarios to a single card. "
                    f"Board limit is {max_per_card} scenarios per card. "
                    f"Create separate test cards for better traceability."
                })

        card_create = CardCreate(
            title=title,
            description=_desc,
            details=_details,
            status=card_status,
            priority=card_priority,
            assignee_id=assignee_id or None,
            labels=_labels_list,
            spec_id=spec_id,
            test_scenario_ids=scenario_ids_list,
            card_type=_card_type_value,
            origin_task_id=origin_task_id or None,
            severity=(severity.strip().lower() if severity else None),
            expected_behavior=expected_behavior.replace("\\n", "\n") if expected_behavior else None,
            observed_behavior=observed_behavior.replace("\\n", "\n") if observed_behavior else None,
            steps_to_reproduce=steps_to_reproduce.replace("\\n", "\n") if steps_to_reproduce else None,
            action_plan=action_plan.replace("\\n", "\n") if action_plan else None,
        )

        try:
            card = await service.create_card(
                board_id, ctx.agent_id, card_create, skip_ownership_check=True
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not card:
            return json.dumps({"error": "Failed to create card"})

        # Bidirectional link: update scenarios' linked_task_ids
        if scenario_ids_list:
            spec_service = SpecService(db)
            spec_obj = await spec_service.get_spec(spec_id)
            if spec_obj and spec_obj.test_scenarios:
                from sqlalchemy.orm.attributes import flag_modified

                scenarios = list(spec_obj.test_scenarios)
                changed = False
                for sc in scenarios:
                    if sc.get("id") in scenario_ids_list:
                        task_ids = list(sc.get("linked_task_ids") or [])
                        if card.id not in task_ids:
                            task_ids.append(card.id)
                            sc["linked_task_ids"] = task_ids
                            changed = True
                if changed:
                    spec_obj.test_scenarios = scenarios
                    flag_modified(spec_obj, "test_scenarios")
                    await db.flush()

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id,
            card_id=card.id,
            action="card_created",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={"title": title, "status": status, "priority": priority},
        )
        await db.commit()

        resp_card = {
            "id": card.id,
            "title": card.title,
            "description": card.description,
            "status": card.status.value,
            "priority": card.priority.value,
            "position": card.position,
            "card_type": getattr(card, "card_type", "normal"),
        }
        if getattr(card, "card_type", "normal") == "bug":
            resp_card.update({
                "origin_task_id": card.origin_task_id,
                "severity": getattr(card, "severity", None),
                "expected_behavior": card.expected_behavior,
                "observed_behavior": card.observed_behavior,
                "spec_id": card.spec_id,
            })

        return json.dumps({"success": True, "card": resp_card}, default=str)


@mcp.tool()
async def okto_pulse_get_card(board_id: str, card_id: str) -> str:
    """Get detailed card information including attachments, Q&A, and comments."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        await db.commit()

        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        from okto_pulse.core.mcp.payload_compaction import compact_and_emit
        return json.dumps(
            compact_and_emit({
                "id": card.id,
                "board_id": card.board_id,
                "spec_id": card.spec_id,
                "title": card.title,
                "description": card.description,
                "details": card.details,
                "status": card.status.value,
                "priority": card.priority.value,
                "position": card.position,
                "assignee_id": card.assignee_id,
                "created_by": card.created_by,
                "created_at": card.created_at.isoformat(),
                "updated_at": card.updated_at.isoformat(),
                "due_date": (
                    card.due_date.isoformat() if card.due_date else None
                ),
                "labels": card.labels or [],
                "attachments": [
                    {
                        "id": a.id,
                        "filename": a.original_filename,
                        "mime_type": a.mime_type,
                        "size": a.size,
                        "uploaded_by": a.uploaded_by,
                    }
                    for a in card.attachments
                ],
                "qa_items": [
                    {
                        "id": q.id,
                        "question": q.question,
                        "answer": q.answer,
                        "asked_by": q.asked_by,
                        "answered_by": q.answered_by,
                    }
                    for q in card.qa_items
                ],
                "comments": [
                    {
                        "id": c.id,
                        "content": c.content,
                        "author_id": c.author_id,
                        "created_at": c.created_at.isoformat(),
                    }
                    for c in card.comments
                ],
            }, tool_name="okto_pulse_get_card"),
            default=str,
        )


@mcp.tool()
async def okto_pulse_resolve_bug_regression_scenarios(
    board_id: str,
    bug_id: str,
    affected_task_ids: list[str] | str = "",
    candidate_scenario_ids: list[str] | str = "",
) -> str:
    """
    Preview reusable regression scenarios for a bug without mutating the spec.

    Provide ``affected_task_ids`` when the incident spans additional tasks.
    Provide ``candidate_scenario_ids`` to classify a proposed set, including
    unrelated or cross-spec candidates. Both inputs accept a JSON array, a
    pipe-delimited string, or a native MCP string list.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    try:
        affected_ids = coerce_to_list_str(affected_task_ids)
        candidate_ids = coerce_to_list_str(candidate_scenario_ids)
    except ValueError as exc:
        return json.dumps({
            "error": "invalid_multivalue",
            "message": str(exc),
        })

    from okto_pulse.core.services.bug_regression_preview import (
        BugRegressionScenarioPreviewError,
        BugRegressionScenarioPreviewService,
    )

    async with get_db_for_mcp() as db:
        try:
            payload = await BugRegressionScenarioPreviewService(db).resolve(
                board_id=board_id,
                bug_id=bug_id,
                affected_task_ids=affected_ids,
                candidate_scenario_ids=candidate_ids,
                surface="mcp",
            )
            await db.commit()
            return json.dumps(payload, default=str)
        except BugRegressionScenarioPreviewError as exc:
            await db.rollback()
            return json.dumps(exc.to_dict(), default=str)


@mcp.tool()
async def okto_pulse_get_task_context(
    board_id: str,
    card_id: str,
    include_knowledge: str = "true",
    include_mockups: str = "true",
    include_qa: str = "true",
    include_comments: str = "true",
    include_architecture: str = "true",
    include_superseded: str = "false",
    profile: str = "summary",
) -> str:
    """
    Get the execution context for a task card: the card + its spec's structured
    requirements (FRs/TRs/ACs/scenarios/BRs/contracts/decisions), the card's
    linked test scenarios, validations, knowledge, mockups, architecture, Q&A.

    `profile` (R2): `summary` (default) keeps the unique content an agent needs
    (card body + spec requirement texts + this card's scenarios + validations) and
    deduplicates resolved references; `full`/`legacy` return the complete prior
    payload. **Before doing card work or a status-changing move, call with
    `profile=full`** (see okto-pulse://reference/projection-profiles)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.mcp.projection_envelope import (
        resolve_profile as _resolve_profile,
        unsupported_projection_error as _unsupported_projection_error,
    )
    if _resolve_profile(profile) is None:
        return json.dumps(_unsupported_projection_error(profile))

    _inc_kb = _flag_enabled(include_knowledge)
    _inc_mockups = _flag_enabled(include_mockups)
    _inc_qa = _flag_enabled(include_qa)
    _inc_comments = _flag_enabled(include_comments)
    _inc_architecture = _flag_enabled(include_architecture)
    _inc_superseded = _flag_enabled(include_superseded)

    async with get_db_for_mcp() as db:
        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        await db.commit()

        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        result: dict = {
            "card": {
                "id": card.id,
                "title": card.title,
                "description": card.description,
                "details": card.details,
                "status": card.status.value,
                "priority": card.priority.value,
                "assignee_id": card.assignee_id,
                "labels": card.labels or [],
                "card_type": card.card_type.value if card.card_type else "normal",
                "test_scenario_ids": card.test_scenario_ids or [],
                "due_date": card.due_date.isoformat() if card.due_date else None,
                "created_by": card.created_by,
                "created_at": card.created_at.isoformat(),
            },
        }

        # Bug card fields
        if card.card_type and card.card_type.value == "bug":
            result["card"]["severity"] = card.severity.value if card.severity else None
            result["card"]["origin_task_id"] = card.origin_task_id
            result["card"]["expected_behavior"] = card.expected_behavior
            result["card"]["observed_behavior"] = card.observed_behavior
            result["card"]["steps_to_reproduce"] = card.steps_to_reproduce
            result["card"]["action_plan"] = card.action_plan
            result["card"]["linked_test_task_ids"] = card.linked_test_task_ids or []

        if _inc_mockups and card.screen_mockups:
            result["card"]["screen_mockups"] = card.screen_mockups

        card_architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            card_architecture_designs = await _mcp_architecture_for_parent(
                db, "card", card_id, permissions=ctx.permissions
            )
            result["card"]["architecture_designs"] = card_architecture_designs

        if _inc_qa:
            result["card"]["qa_items"] = [
                {
                    "id": q.id,
                    "question": q.question,
                    "answer": q.answer,
                    "asked_by": q.asked_by,
                    "answered_by": q.answered_by,
                }
                for q in card.qa_items
            ]

        if _inc_comments:
            result["card"]["comments"] = [
                {
                    "id": c.id,
                    "content": c.content,
                    "author_id": c.author_id,
                    "created_at": c.created_at.isoformat(),
                }
                for c in card.comments
            ]

        # Dependencies
        deps = await card_service.get_dependencies(card_id)
        await db.commit()
        if deps:
            result["card"]["depends_on"] = [
                {"id": d.id, "title": d.title, "status": d.status.value}
                for d in deps
            ]

        # Spec context (the core of task context)
        spec = None
        spec_architecture_designs: list[dict[str, Any]] = []
        if card.spec_id:
            spec_service = SpecService(db)
            spec = await spec_service.get_spec(card.spec_id)
            await db.commit()

            if spec:
                spec_data: dict = {
                    "id": spec.id,
                    "title": spec.title,
                    "description": spec.description,
                    "context": spec.context,
                    "status": spec.status.value,
                    "functional_requirements": spec.functional_requirements or [],
                    "technical_requirements": spec.technical_requirements or [],
                    "acceptance_criteria": spec.acceptance_criteria or [],
                    "test_scenarios": spec.test_scenarios or [],
                    "business_rules": spec.business_rules or [],
                    "api_contracts": spec.api_contracts or [],
                    "decisions": _filter_decisions_by_status(
                        getattr(spec, "decisions", None) or [],
                        include_superseded=_inc_superseded,
                    ),
                    "decisions_stats": _decisions_stats(
                        getattr(spec, "decisions", None) or []
                    ),
                    "decisions_markdown": _render_decisions_markdown(
                        getattr(spec, "decisions", None) or [],
                        include_superseded=_inc_superseded,
                    ),
                }
                if _mcp_check_permission(
                    ctx.permissions,
                    "spec.integration_requirements.read",
                    Permissions.BOARD_READ,
                ) is None:
                    spec_data["integration_requirements"] = (
                        getattr(spec, "integration_requirements", None) or []
                    )
                if _mcp_check_permission(
                    ctx.permissions,
                    "spec.observability_requirements.read",
                    Permissions.BOARD_READ,
                ) is None:
                    spec_data["observability_requirements"] = (
                        getattr(spec, "observability_requirements", None) or []
                    )

                if _inc_mockups and spec.screen_mockups:
                    spec_data["screen_mockups"] = spec.screen_mockups

                if _inc_architecture:
                    spec_architecture_designs = await _mcp_architecture_for_parent(
                        db, "spec", spec.id, permissions=ctx.permissions
                    )
                    spec_data["architecture_designs"] = spec_architecture_designs

                if _inc_qa:
                    spec_data["qa_items"] = [
                        {
                            "id": q.id,
                            "question": q.question,
                            "answer": q.answer,
                            "asked_by": q.asked_by,
                            "answered_by": q.answered_by,
                        }
                        for q in (spec.qa_items or [])
                    ]

                if _inc_kb:
                    spec_data["knowledge_bases"] = [
                        _serialize_knowledge_base(kb)
                        for kb in (spec.knowledge_bases or [])
                    ]

                result["spec"] = spec_data

                # Card-own knowledge bases (JSON field)
                if _inc_kb and card.knowledge_bases:
                    result["card_knowledge_bases"] = card.knowledge_bases

                # Filter test scenarios relevant to this card
                if card.test_scenario_ids and spec.test_scenarios:
                    result["my_test_scenarios"] = [
                        ts for ts in spec.test_scenarios
                        if ts.get("id") in card.test_scenario_ids
                    ]

        resolved_references = resolve_task_context_references(
            card,
            spec,
            include_superseded=_inc_superseded,
            include_content=_inc_kb,
            card_architecture_designs=card_architecture_designs if _inc_architecture else [],
            spec_architecture_designs=spec_architecture_designs if _inc_architecture else [],
        )
        if not _inc_kb:
            resolved_references["knowledge_bases"] = []
        if not _inc_mockups:
            resolved_references["screen_mockups"] = []
        if not _inc_architecture:
            resolved_references["architecture_designs"] = []
        result["resolved_references"] = resolved_references
        result["resource_gate_summary"] = await ResourceGateService(db).get_summary(
            board_id,
            "card",
            card_id,
        )
        if spec:
            result["spec"]["resource_gate_summary"] = await ResourceGateService(db).get_summary(
                board_id,
                "spec",
                spec.id,
            )

        # Task validations — critical for agents picking up cards that failed validation
        result["validations"] = list(card.validations or [])

        # Validation gate config (resolved from sprint → spec → board hierarchy)
        from okto_pulse.core.models.db import Board as _Board, Spec as _Spec, Sprint as _Sprint
        board_obj = await db.get(_Board, card.board_id)
        board_settings = board_obj.settings or {} if board_obj else {}
        spec_for_gate = await db.get(_Spec, card.spec_id) if card.spec_id else None
        sprint_for_gate = await db.get(_Sprint, card.sprint_id) if card.sprint_id else None
        result["validation_config"] = card_service._resolve_validation_config(
            card, spec_for_gate, sprint_for_gate, board_settings
        )

        from okto_pulse.core.mcp.context_projection import project_task_context
        return json.dumps(
            project_task_context(result, card_id=card_id, profile=profile),
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_task_conclusions(board_id: str, card_id: str) -> str:
    """
    Get the conclusions of a completed task card. Conclusions describe what was done,
    the root cause (for bugs), decisions made, and any relevant notes.

    Useful for:
    - Understanding what was done in a previous task before starting related work
    - Bug triage — understanding root cause and fix approach
    - Knowledge transfer between agents or team members"""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        await db.commit()

        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        result: dict = {
            "id": card.id,
            "title": card.title,
            "status": card.status.value,
            "card_type": card.card_type.value if card.card_type else "normal",
            "conclusions": card.conclusions or [],
        }

        if card.card_type and card.card_type.value == "bug":
            result["severity"] = card.severity.value if card.severity else None
            result["expected_behavior"] = card.expected_behavior
            result["observed_behavior"] = card.observed_behavior
            result["steps_to_reproduce"] = card.steps_to_reproduce
            result["action_plan"] = card.action_plan

        if not card.conclusions:
            result["note"] = "No conclusions recorded. Conclusions are required when moving a card to 'done'."

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_update_card(
    board_id: str,
    card_id: str,
    title: str = "",
    description: str = "",
    details: str = "",
    priority: str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
    test_scenario_ids: list[str] | str = "",
    severity: str = "",
    expected_behavior: str = "",
    observed_behavior: str = "",
    steps_to_reproduce: str = "",
    action_plan: str = "",
    linked_test_task_ids: list[str] | str = "",
) -> str:
    """Update card details. Pass only the fields you want to change; omit the rest.

    Multi-value fields (labels, test_scenario_ids, linked_test_task_ids): prefer
    native list; legacy pipe-separated string is also accepted. Comma-only strings
    are REJECTED. For bidirectional scenario linking, use okto_pulse_link_task_to_scenario.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import BugSeverity, CardPriority
    from okto_pulse.core.models.schemas import CardUpdate

    async with get_db_for_mcp() as db:
        service = CardService(db)

        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        update_data = {}
        if title:
            update_data["title"] = title
        if description:
            update_data["description"] = description.replace("\\n", "\n")
        if details:
            update_data["details"] = details.replace("\\n", "\n")
        if priority:
            try:
                update_data["priority"] = CardPriority(priority)
            except ValueError:
                return json.dumps(
                    {
                        "error": f"Invalid priority. Must be one of: {[p.value for p in CardPriority]}"
                    }
                )
        if assignee_id:
            update_data["assignee_id"] = assignee_id
        if labels:
            try:
                update_data["labels"] = coerce_to_list_str(labels)
            except ValueError as e:
                return json.dumps({"error": f"Invalid labels: {e}"})
        if test_scenario_ids:
            try:
                update_data["test_scenario_ids"] = coerce_to_list_str(test_scenario_ids)
            except ValueError as e:
                return json.dumps({"error": f"Invalid test_scenario_ids: {e}"})
        if severity:
            _sev = severity.strip().lower()
            try:
                BugSeverity(_sev)
            except ValueError:
                return json.dumps(
                    {
                        "error": f"Invalid severity '{severity}'. Must be one of: {[s.value for s in BugSeverity]}"
                    }
                )
            update_data["severity"] = _sev
        if expected_behavior:
            update_data["expected_behavior"] = expected_behavior.replace("\\n", "\n")
        if observed_behavior:
            update_data["observed_behavior"] = observed_behavior.replace("\\n", "\n")
        if steps_to_reproduce:
            update_data["steps_to_reproduce"] = steps_to_reproduce.replace("\\n", "\n")
        if action_plan:
            update_data["action_plan"] = action_plan.replace("\\n", "\n")
        if linked_test_task_ids:
            try:
                update_data["linked_test_task_ids"] = coerce_to_list_str(linked_test_task_ids)
            except ValueError as e:
                return json.dumps({"error": f"Invalid linked_test_task_ids: {e}"})

        card_update = CardUpdate(**update_data)
        updated = await service.update_card(card_id, ctx.agent_id, card_update)

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id,
            card_id=card_id,
            action="card_updated",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details=update_data,
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "card": {
                    "id": updated.id,
                    "title": updated.title,
                    "status": updated.status.value,
                    "priority": updated.priority.value,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_move_card(
    board_id: str,
    card_id: str,
    status: str,
    position: int = -1,
    conclusion: str = "",
    completeness: int = -1,
    completeness_justification: str = "",
    drift: int = -1,
    drift_justification: str = "",
) -> str:
    """Move a card to a different column/position on the board.

    Moving to 'validation' or 'done' REQUIRES conclusion, completeness (0-100),
    completeness_justification, drift (0-100), and drift_justification so the
    reviewer can validate the claim. Use -1 for completeness/drift when no
    execution report is required (e.g. moving to on_hold or started).
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_MOVE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import CardStatus
    from okto_pulse.core.models.schemas import CardMove

    try:
        card_status = CardStatus(status)
    except ValueError:
        return json.dumps(
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in CardStatus]}"
            }
        )

    async with get_db_for_mcp() as db:
        service = CardService(db)

        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        move_data = CardMove(
            status=card_status,
            position=position if position >= 0 else None,
            conclusion=conclusion.replace("\\n", "\n") if conclusion else None,
            completeness=completeness if completeness >= 0 else None,
            completeness_justification=completeness_justification or None,
            drift=drift if drift >= 0 else None,
            drift_justification=drift_justification or None,
        )

        try:
            updated = await service.move_card(
                card_id, ctx.agent_id, move_data, ctx.agent_name
            )
        except CardOperationError as e:
            return json.dumps({
                "error": e.code,
                **e.to_dict(),
                "blocked_by_dependencies": True,
            })
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        except ValueError as e:
            return json.dumps({"error": str(e), "blocked_by_dependencies": True})

        if not updated:
            return json.dumps({"error": "Failed to move card"})

        await db.commit()

        return json.dumps(
            {
                "success": True,
                "card": {
                    "id": updated.id,
                    "title": updated.title,
                    "status": updated.status.value,
                    "position": updated.position,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_card(board_id: str, card_id: str) -> str:
    """Delete a card from the board. This operation is permanent and cannot be undone."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = CardService(db)

        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id,
            card_id=card_id,
            action="card_deleted",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={"title": card.title},
        )

        deleted = await service.delete_card(card_id, ctx.agent_id)
        await db.commit()

        return json.dumps({"success": deleted})


@mcp.tool()
async def okto_pulse_add_card_dependency(
    board_id: str, card_id: str, depends_on_id: str
) -> str:
    """
    Add a dependency: card_id cannot advance until depends_on_id is done/cancelled.
    Circular dependencies are blocked automatically."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = CardService(db)
        dep = await service.add_dependency(card_id, depends_on_id)
        if not dep:
            return json.dumps(
                {"error": "Dependência circular detectada ou auto-referência"}
            )
        await db.commit()
        return json.dumps(
            {
                "success": True,
                "card_id": card_id,
                "depends_on_id": depends_on_id,
            }
        )


@mcp.tool()
async def okto_pulse_remove_card_dependency(
    board_id: str, card_id: str, depends_on_id: str
) -> str:
    """
    Remove a dependency between two cards."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = CardService(db)
        removed = await service.remove_dependency(card_id, depends_on_id)
        await db.commit()
        return json.dumps({"success": removed})


@mcp.tool()
async def okto_pulse_get_card_dependencies(board_id: str, card_id: str) -> str:
    """
    List cards that this card depends on and cards that depend on it."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = CardService(db)
        deps = await service.get_dependencies(card_id)
        dependents = await service.get_dependents(card_id)
        deps_met, blocking = await service.check_dependencies_met(card_id)
        await db.commit()

        return json.dumps(
            {
                "card_id": card_id,
                "can_advance": deps_met,
                "blocking_titles": blocking,
                "depends_on": [
                    {"id": d.id, "title": d.title, "status": d.status.value}
                    for d in deps
                ],
                "dependents": [
                    {"id": d.id, "title": d.title, "status": d.status.value}
                    for d in dependents
                ],
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_list_cards_by_status(
    board_id: str,
    status: str = "",
    spec_id: str = "",
    priority: str = "",
    assignee_id: str = "",
    offset: int = 0,
    limit: int = 50,
) -> str:
    """List cards on the board with optional filters and pagination.

    status: empty = all, or one of not_started/started/in_progress/validation/on_hold/done/cancelled/open.
    Use 'open' for all cards NOT in done/cancelled. Max limit is 200.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    async with get_db_for_mcp() as db:
        service = BoardService(db)
        board = await service.get_board(board_id)
        await db.commit()

        if not board:
            return json.dumps({"error": "Board not found"})

        cards = board.cards
        total_all = len(cards)

        if status == "open":
            cards = [c for c in cards if c.status.value not in ("done", "cancelled")]
        elif status:
            cards = [c for c in cards if c.status.value == status]
        if spec_id:
            cards = [c for c in cards if c.spec_id == spec_id]
        if priority:
            cards = [c for c in cards if c.priority.value == priority]
        if assignee_id:
            cards = [c for c in cards if c.assignee_id == assignee_id]

        sorted_cards = sorted(cards, key=lambda x: (x.status.value, x.position))
        total_filtered = len(sorted_cards)
        paginated = sorted_cards[offset:offset + limit]

        from okto_pulse.core.mcp.payload_compaction import compact_and_emit
        return json.dumps(
            compact_and_emit({
                "total_all": total_all,
                "filtered_count": total_filtered,
                "offset": offset,
                "limit": limit,
                "cards": [
                    {
                        "id": c.id,
                        "title": c.title,
                        "description": c.description,
                        "status": c.status.value,
                        "priority": c.priority.value,
                        "position": c.position,
                        "assignee_id": c.assignee_id,
                        "spec_id": c.spec_id,
                        "test_scenario_ids": c.test_scenario_ids,
                        "due_date": (
                            c.due_date.isoformat() if c.due_date else None
                        ),
                        "labels": c.labels or [],
                    }
                    for c in paginated
                ],
            }, tool_name="okto_pulse_list_cards_by_status",
               truncated=total_filtered > offset + len(paginated)),
            default=str,
        )


# ============================================================================
# Q&A TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_ask_question(board_id: str, card_id: str, question: str) -> str:
    """
    Add a question to a card's Q&A board."""
    return await _ask_question_impl(
        board_id, "card", card_id, question,
        alias_kind="legacy", tool_name="okto_pulse_ask_question",
    )


# ============================================================================
# R4 — consolidated Q&A ASK (spec 452cb4d5, card R4.1)
#
# qa_ask is the second assertiveness-gate-eligible family: the five legacy ask_*
# tools have IDENTICAL (board_id, parent_id, question) signatures — only the
# parent-id param NAME differed, which the closed target_type enum restores. The
# legacy tools are PRESERVED as additive aliases that delegate here (fr_af4b5c6e /
# tr_b25890c4). DEDICATED ROUTING preserves the SPRINT asymmetry: SprintQAService
# takes a raw string, with NO QA_CREATE permission gate and NO activity-log write.
# ============================================================================


async def _ask_question_impl(
    board_id: str,
    target_type: str,
    parent_id: str,
    question: str,
    *,
    alias_kind: str,
    tool_name: str,
) -> str:
    """Shared implementation behind okto_pulse_ask and the five legacy ask_*
    aliases. Replicates each legacy tool's exact behavior (per-type service/schema,
    activity-log action, error message, and the sprint asymmetry) and emits safe
    alias-usage telemetry (or_4e57890f)."""
    from okto_pulse.core.mcp.tool_family_registry import (
        REGISTRY,
        VIOLATION_UNKNOWN_TARGET_TYPE,
        emit_alias_usage,
        emit_registry_violation,
    )

    def _telemetry(outcome: str) -> None:
        emit_alias_usage(
            family_id="qa_ask",
            alias_kind=alias_kind,
            tool_name=tool_name,
            operation="ask",
            target_type=str(target_type),
            outcome=outcome,
        )

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        _telemetry("error")
        return _auth_error()

    type_err = REGISTRY.validate_target_type("qa_ask", target_type)
    if type_err:
        emit_registry_violation(
            family_id="qa_ask",
            reason=VIOLATION_UNKNOWN_TARGET_TYPE,
            tool_name=tool_name,
            target_type=str(target_type),
        )
        _telemetry("error")
        fam = REGISTRY.get("qa_ask")
        return json.dumps({
            "error": "unsupported_target_type",
            "message": type_err,
            "allowed": list(fam.target_types) if fam else [],
        })

    # Sprint is asymmetric: no QA_CREATE permission gate (preserve legacy behavior).
    if target_type != "sprint":
        perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
        if perm_err:
            _telemetry("error")
            return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        if target_type == "card":
            from okto_pulse.core.models.schemas import QACreate
            service = QAService(db)
            qa = await service.create_question(
                parent_id, ctx.agent_id, QACreate(question=question)
            )
            if not qa:
                _telemetry("error")
                return json.dumps({"error": "Failed to create question (card not found)"})
            await _log_card_activity(
                db, board_id, parent_id, "question_added", ctx,
                {"question": question[:100]},
            )
            await db.commit()
            _telemetry("ok")
            return json.dumps({
                "success": True,
                "qa": {"id": qa.id, "question": qa.question, "asked_by": qa.asked_by},
            })

        if target_type in ("ideation", "refinement", "spec"):
            if target_type == "ideation":
                from okto_pulse.core.models.schemas import IdeationQACreate as _QACreate
                service = IdeationQAService(db)
                action, not_found, key = "ideation_question_added", "Ideation not found", "ideation_id"
            elif target_type == "refinement":
                from okto_pulse.core.models.schemas import RefinementQACreate as _QACreate
                service = RefinementQAService(db)
                action, not_found, key = "refinement_question_added", "Refinement not found", "refinement_id"
            else:
                from okto_pulse.core.models.schemas import SpecQACreate as _QACreate
                service = SpecQAService(db)
                action, not_found, key = "spec_question_added", "Spec not found", "spec_id"
            qa = await service.create_question(parent_id, ctx.agent_id, _QACreate(question=question))
            if not qa:
                _telemetry("error")
                return json.dumps({"error": not_found})
            board_service = BoardService(db)
            await board_service._log_activity(
                board_id=board_id, action=action,
                actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
                details={key: parent_id, "question": question[:100]},
            )
            await db.commit()
            _telemetry("ok")
            return json.dumps({
                "success": True,
                "qa": {"id": qa.id, "question": qa.question, "asked_by": qa.asked_by},
            })

        # sprint — asymmetric: raw string, no QACreate schema, no permission, no log.
        from okto_pulse.core.services.main import SprintQAService
        service = SprintQAService(db)
        qa = await service.create_question(parent_id, ctx.agent_id, question)
        await db.commit()
        if not qa:
            _telemetry("error")
            return json.dumps({"error": "Sprint not found"})
        _telemetry("ok")
        return json.dumps({
            "success": True,
            "qa": {"id": qa.id, "question": qa.question, "asked_by": qa.asked_by},
        })


@mcp.tool()
async def okto_pulse_ask(
    board_id: str,
    target_type: str,
    parent_id: str,
    question: str,
) -> str:
    """
    Consolidated Q&A ask (R4). `target_type` is one of: `card`, `ideation`,
    `refinement`, `spec`, `sprint`; `parent_id` is that work item's id. Equivalent
    to the per-type tools (`okto_pulse_ask_question`/`_ideation_question`/
    `_refinement_question`/`_spec_question`/`_sprint_question`), which remain as
    aliases. Use `@Name` to direct the question. An unsupported `target_type`
    returns a structured error listing the allowed values (no mutation).
    See `okto-pulse://reference/tool-families/qa_ask`."""
    return await _ask_question_impl(
        board_id, target_type, parent_id, question,
        alias_kind="consolidated", tool_name="okto_pulse_ask",
    )


@mcp.tool()
async def okto_pulse_answer_question(
    board_id: str, qa_id: str, answer: str
) -> str:
    """
    Answer a question on a card's Q&A board."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import QAAnswer
    from okto_pulse.core.services import QASelfAnsweringNotAllowedError

    async with get_db_for_mcp() as db:
        service = QAService(db)
        try:
            qa = await service.answer_question(
                qa_id,
                ctx.agent_id,
                QAAnswer(answer=answer),
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as e:
            await db.commit()
            return json.dumps({"error": e.reason, "detail": str(e)})
        if not qa:
            return json.dumps(
                {"error": "Failed to answer question (not found)"}
            )
        await _log_card_activity(
            db, board_id, qa.card_id, "question_answered", ctx,
            {"qa_id": qa_id, "answer": answer[:100]},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "answer": qa.answer,
                    "answered_by": qa.answered_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_delete_question(board_id: str, qa_id: str) -> str:
    """
    Delete a Q&A item from a card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = QAService(db)
        deleted = await service.delete_question(qa_id)
        await db.commit()

        if not deleted:
            return json.dumps({"error": "Q&A item not found"})

        return json.dumps({"success": True})


# ============================================================================
# COMMENT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_add_comment(board_id: str, card_id: str, content: str) -> str:
    """
    Add a comment to a card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.COMMENTS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import CommentCreate

    async with get_db_for_mcp() as db:
        service = CommentService(db)
        comment = await service.create_comment(
            card_id, ctx.agent_id, CommentCreate(content=content)
        )
        if not comment:
            return json.dumps(
                {"error": "Failed to create comment (card not found)"}
            )
        await _log_card_activity(
            db, board_id, card_id, "comment_added", ctx,
            {"content": content[:100]},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "content": comment.content,
                    "author_id": comment.author_id,
                    "created_at": comment.created_at.isoformat(),
                },
            }
        )


@mcp.tool()
async def okto_pulse_add_choice_comment(
    board_id: str,
    card_id: str,
    question: str,
    options: list[str] | str,
    comment_type: str = "choice",
    allow_free_text: str = "false",
    options_json: str = "",
) -> str:
    """
    Add a choice board (poll) to a card. Responders can select from the options.

options_json (optional, takes precedence): JSON array of option objects, e.g. '[{"label":"A","recommended":true,"tradeoff":"costs more"}]'. When present and non-empty, options is ignored. Each object requires a non-empty label; recommended defaults to false; tradeoff defaults to null.
Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.COMMENTS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import ChoiceOption, CommentCreate

    try:
        parsed_objects = parse_options_json(options_json or None)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options_json: {e}"})

    if parsed_objects is not None:
        choice_list = [
            ChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
            for i, obj in enumerate(parsed_objects)
        ]
    else:
        try:
            option_labels = coerce_to_list_str(options)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
        if not option_labels:
            return json.dumps({"error": "At least one option is required"})
        choice_list = [
            ChoiceOption(id=f"opt_{i}", label=label)
            for i, label in enumerate(option_labels)
        ]

    async with get_db_for_mcp() as db:
        service = CommentService(db)
        data = CommentCreate(
            content=question,
            comment_type=comment_type if comment_type in ("choice", "multi_choice") else "choice",
            choices=choice_list,
            allow_free_text=allow_free_text.lower() == "true",
        )
        comment = await service.create_comment(card_id, ctx.agent_id, data)
        if not comment:
            return json.dumps({"error": "Failed to create choice comment (card not found)"})

        await _log_card_activity(
            db, board_id, card_id, "choice_comment_added", ctx,
            {"question": question[:100], "option_count": len(choice_list), "type": comment_type},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "comment_type": comment.comment_type,
                    "content": comment.content,
                    "choices": comment.choices,
                    "allow_free_text": comment.allow_free_text,
                    "responses": [],
                },
            }
        )


@mcp.tool()
async def okto_pulse_respond_to_choice(
    board_id: str,
    comment_id: str,
    selected: list[str] | str,
    free_text: str = "",
) -> str:
    """
    Respond to a choice board comment by selecting one or more options.

Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    try:
        selected_ids = coerce_to_list_str(selected)
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if not selected_ids:
        return json.dumps({"error": "At least one selection is required"})

    async with get_db_for_mcp() as db:
        service = CommentService(db)
        comment = await service.respond_to_choice(
            comment_id=comment_id,
            responder_id=ctx.agent_id,
            responder_name=ctx.agent_name,
            selected=selected_ids,
            free_text=free_text or None,
        )
        if not comment:
            return json.dumps({"error": "Choice comment not found or invalid selection"})

        await db.commit()

        return json.dumps(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "comment_type": comment.comment_type,
                    "content": comment.content,
                    "choices": comment.choices,
                    "responses": comment.responses,
                },
            }
        )


@mcp.tool()
async def okto_pulse_get_choice_responses(board_id: str, comment_id: str) -> str:
    """
    Get all responses for a choice board comment."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.models.db import Comment as CommentModel

    async with get_db_for_mcp() as db:
        comment = await db.get(CommentModel, comment_id)
        await db.commit()

        if not comment or comment.comment_type == "text":
            return json.dumps({"error": "Choice comment not found"})

        return json.dumps(
            {
                "id": comment.id,
                "comment_type": comment.comment_type,
                "question": comment.content,
                "choices": comment.choices,
                "allow_free_text": comment.allow_free_text,
                "responses": comment.responses or [],
                "response_count": len(comment.responses or []),
            }
        )


@mcp.tool()
async def okto_pulse_list_comments(board_id: str, card_id: str) -> str:
    """
    List all comments on a card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        await db.commit()

        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        result = []
        for c in card.comments:
            item: dict = {
                "id": c.id,
                "content": c.content,
                "author_id": c.author_id,
                "comment_type": getattr(c, "comment_type", "text") or "text",
                "created_at": c.created_at.isoformat(),
                "updated_at": c.updated_at.isoformat(),
            }
            if item["comment_type"] != "text":
                item["choices"] = getattr(c, "choices", None)
                item["responses"] = getattr(c, "responses", None) or []
                item["allow_free_text"] = getattr(c, "allow_free_text", False)
            result.append(item)
        from okto_pulse.core.mcp.payload_compaction import compact_and_emit
        return json.dumps(
            compact_and_emit(result, tool_name="okto_pulse_list_comments"),
            default=str,
        )


@mcp.tool()
async def okto_pulse_update_comment(
    board_id: str, comment_id: str, content: str
) -> str:
    """
    Update the agent's own comment."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.COMMENTS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import CommentUpdate

    async with get_db_for_mcp() as db:
        service = CommentService(db)
        comment = await service.update_comment(
            comment_id, ctx.agent_id, CommentUpdate(content=content)
        )

        if not comment:
            return json.dumps(
                {"error": "Comment not found or not owned by this agent"}
            )

        await _log_card_activity(
            db, board_id, comment.card_id, "comment_updated", ctx,
            {"content": content[:100]},
        )
        await db.commit()
        await db.refresh(comment)

        return json.dumps(
            {
                "success": True,
                "comment": {
                    "id": comment.id,
                    "content": comment.content,
                    "updated_at": (
                        comment.updated_at.isoformat()
                        if comment.updated_at
                        else None
                    ),
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_comment(board_id: str, comment_id: str) -> str:
    """
    Delete the agent's own comment."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.COMMENTS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = CommentService(db)
        # Get card_id before deleting
        from okto_pulse.core.models.db import Comment as CommentModel
        comment_obj = await db.get(CommentModel, comment_id)
        card_id = comment_obj.card_id if comment_obj else None

        deleted = await service.delete_comment(comment_id, ctx.agent_id)
        if not deleted:
            return json.dumps(
                {"error": "Comment not found or not owned by this agent"}
            )

        if card_id:
            await _log_card_activity(
                db, board_id, card_id, "comment_deleted", ctx,
            )
        await db.commit()

        return json.dumps({"success": True})


# ============================================================================
# ATTACHMENT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_upload_attachment(
    board_id: str,
    card_id: str,
    filename: str,
    content_base64: str = "",
    mime_type: str = "application/octet-stream",
    file_path: str | None = None,
    file_url: str | None = None,
) -> str:
    """
    Upload a file attachment to a card.

    Provide exactly ONE of: content_base64, file_path, or file_url. Prefer
    file_path or file_url for binary files — the bytes are loaded server-side
    and never pass through the LLM context, saving tokens."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.ATTACHMENTS_UPLOAD)
    if perm_err:
        return _perm_error(perm_err)

    content, err = await _resolve_binary_content(
        content_base64=content_base64, file_path=file_path, file_url=file_url
    )
    if err:
        return json.dumps({"error": err})

    async with get_db_for_mcp() as db:
        service = AttachmentService(db)

        attachment = await service.upload_attachment(
            card_id=card_id,
            user_id=ctx.agent_id,
            filename=filename,
            content=content,
            mime_type=mime_type,
        )
        await db.commit()

        if not attachment:
            return json.dumps(
                {"error": "Failed to upload attachment (card not found)"}
            )

        return json.dumps(
            {
                "success": True,
                "attachment": {
                    "id": attachment.id,
                    "filename": attachment.original_filename,
                    "mime_type": attachment.mime_type,
                    "size": attachment.size,
                },
            }
        )


@mcp.tool()
async def okto_pulse_list_attachments(board_id: str, card_id: str) -> str:
    """
    List all attachments on a card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        await db.commit()

        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        return json.dumps(
            [
                {
                    "id": a.id,
                    "filename": a.original_filename,
                    "mime_type": a.mime_type,
                    "size": a.size,
                    "uploaded_by": a.uploaded_by,
                    "created_at": a.created_at.isoformat(),
                }
                for a in card.attachments
            ],
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_attachment(board_id: str, attachment_id: str) -> str:
    """
    Delete an attachment."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.ATTACHMENTS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = AttachmentService(db)
        deleted = await service.delete_attachment(attachment_id)
        await db.commit()

        if not deleted:
            return json.dumps({"error": "Attachment not found"})

        return json.dumps({"success": True})


# ============================================================================
# STORY TOOLS
# ============================================================================


def _story_payload(story) -> dict:
    return {
        "id": story.id,
        "board_id": story.board_id,
        "topic_id": story.topic_id,
        "title": story.title,
        "description": story.description,
        "actor": story.actor,
        "goal": story.goal,
        "benefit": story.benefit,
        "labels": story.labels,
        "status": story.status.value,
        "assignee_id": story.assignee_id,
        "screen_mockups": story.screen_mockups,
        "archived": story.archived,
        "ideation_links": [
            {"id": link.id, "ideation_id": link.ideation_id, "created_at": link.created_at.isoformat()}
            for link in (story.ideation_links or [])
        ],
        "created_at": story.created_at.isoformat(),
        "updated_at": story.updated_at.isoformat(),
    }


def _topic_payload(topic) -> dict:
    return {
        "id": topic.id,
        "board_id": topic.board_id,
        "name": topic.name,
        "description": topic.description,
        "archived": bool(topic.archived),
        "story_count": getattr(topic, "story_count", 0),
        "active_count": getattr(topic, "active_count", getattr(topic, "story_count", 0)),
        "archived_count": getattr(topic, "archived_count", 0),
        "total_associated_count": getattr(topic, "total_associated_count", getattr(topic, "story_count", 0)),
        "created_by": topic.created_by,
        "created_at": topic.created_at.isoformat(),
        "updated_at": topic.updated_at.isoformat(),
    }


def _topic_impact(topic) -> dict:
    return {
        "topic_id": topic.id,
        "story_count": getattr(topic, "story_count", 0),
        "active_count": getattr(topic, "active_count", getattr(topic, "story_count", 0)),
        "archived_count": getattr(topic, "archived_count", 0),
        "total_associated_count": getattr(topic, "total_associated_count", getattr(topic, "story_count", 0)),
    }


def _topic_operation_error_response(exc: TopicOperationError) -> str:
    return json.dumps({"success": False, "error": str(exc), "code": exc.code, **exc.details}, default=str)


@mcp.tool()
async def okto_pulse_create_topic(board_id: str, name: str, description: str = "") -> str:
    """Create a board-scoped Topic for grouping Stories."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "topic.entity.create", Permissions.SPECS_CREATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.schemas import TopicCreate

    async with get_db_for_mcp() as db:
        try:
            topic = await StoryService(db).create_topic(
                board_id,
                ctx.agent_id,
                TopicCreate(name=name, description=description or None),
                skip_ownership_check=True,
            )
        except TopicOperationError as e:
            return _topic_operation_error_response(e)
        await db.commit()
        if not topic:
            return json.dumps({"error": "Board not found"})
        return json.dumps(
            {
                "success": True,
                "message": f"Topic '{topic.name}' created.",
                "topic": _topic_payload(topic),
                "impact": _topic_impact(topic),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_update_topic(
    board_id: str,
    topic_id: str,
    name: str = "",
    description: str = "",
) -> str:
    """Update a Topic's editable fields. Use archive/restore tools for lifecycle."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "topic.entity.edit_fields", Permissions.SPECS_UPDATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.db import Topic
    from okto_pulse.core.models.schemas import TopicUpdate

    update_data = {}
    if name:
        update_data["name"] = name
    if description:
        update_data["description"] = description
    if not update_data:
        return json.dumps({"success": False, "error": "Provide at least one field to update"})

    async with get_db_for_mcp() as db:
        topic = await db.get(Topic, topic_id)
        if not topic or topic.board_id != board_id:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        try:
            updated = await StoryService(db).update_topic(topic_id, ctx.agent_id, TopicUpdate(**update_data))
        except TopicOperationError as e:
            return _topic_operation_error_response(e)
        await db.commit()
        if not updated:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        return json.dumps(
            {
                "success": True,
                "message": f"Topic '{updated.name}' updated.",
                "topic": _topic_payload(updated),
                "impact": _topic_impact(updated),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_archive_topic(board_id: str, topic_id: str) -> str:
    """Archive a Topic without archiving its Stories."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "topic.entity.archive", Permissions.SPECS_UPDATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.db import Topic
    from okto_pulse.core.models.schemas import TopicUpdate

    async with get_db_for_mcp() as db:
        topic = await db.get(Topic, topic_id)
        if not topic or topic.board_id != board_id:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        try:
            archived = await StoryService(db).update_topic(topic_id, ctx.agent_id, TopicUpdate(archived=True))
        except TopicOperationError as e:
            return _topic_operation_error_response(e)
        await db.commit()
        if not archived:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        return json.dumps(
            {
                "success": True,
                "message": "Topic archived. Stories remain unchanged and visible through All topics/search unless the Story itself is archived.",
                "topic": _topic_payload(archived),
                "impact": _topic_impact(archived),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_restore_topic(board_id: str, topic_id: str) -> str:
    """Restore an archived Topic."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "topic.entity.restore", Permissions.SPECS_UPDATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.db import Topic
    from okto_pulse.core.models.schemas import TopicUpdate

    async with get_db_for_mcp() as db:
        topic = await db.get(Topic, topic_id)
        if not topic or topic.board_id != board_id:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        try:
            restored = await StoryService(db).update_topic(topic_id, ctx.agent_id, TopicUpdate(archived=False))
        except TopicOperationError as e:
            return _topic_operation_error_response(e)
        await db.commit()
        if not restored:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        return json.dumps(
            {
                "success": True,
                "message": f"Topic '{restored.name}' restored.",
                "topic": _topic_payload(restored),
                "impact": _topic_impact(restored),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_topic(board_id: str, topic_id: str) -> str:
    """Delete a Topic only when it has no associated Stories, including archived Stories."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "topic.entity.delete", Permissions.SPECS_DELETE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.db import Topic

    async with get_db_for_mcp() as db:
        topic = await db.get(Topic, topic_id)
        if not topic or topic.board_id != board_id:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        try:
            deleted = await StoryService(db).delete_topic(topic_id, ctx.agent_id)
        except TopicOperationError as e:
            return _topic_operation_error_response(e)
        await db.commit()
        if not deleted:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        return json.dumps(
            {
                "success": True,
                "message": f"Topic '{deleted.name}' deleted.",
                "deleted_topic_id": topic_id,
                "impact": {"topic_id": topic_id, "active_count": 0, "archived_count": 0, "total_associated_count": 0},
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_merge_topics(board_id: str, source_topic_id: str, target_topic_id: str) -> str:
    """Merge a source Topic into an active target Topic while preserving Story-Ideation links."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "topic.entity.merge", Permissions.SPECS_UPDATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.db import Topic

    async with get_db_for_mcp() as db:
        source = await db.get(Topic, source_topic_id)
        if not source or source.board_id != board_id:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        try:
            result = await StoryService(db).merge_topics(source_topic_id, target_topic_id, ctx.agent_id)
        except TopicOperationError as e:
            return _topic_operation_error_response(e)
        await db.commit()
        if not result:
            return json.dumps({"success": False, "error": "Topic not found", "code": "topic_not_found"})
        return json.dumps(
            {
                "success": True,
                "message": (
                    f"Merged Topic '{result['source'].name}' into '{result['target'].name}'. "
                    "Story-Ideation links were preserved and the source Topic was archived."
                ),
                "source": _topic_payload(result["source"]),
                "target": _topic_payload(result["target"]),
                "impact": {
                    "source_topic_id": source_topic_id,
                    "target_topic_id": target_topic_id,
                    "moved_count": result["moved_count"],
                    "active_count": result["active_count"],
                    "archived_count": result["archived_count"],
                    "target_total_before": result["target_total_before"],
                    "target_total_after": result["target_total_after"],
                },
            },
            default=str,
        )


# ============================================================================
# RESOURCE GATE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_resource_gate_summary(
    board_id: str,
    entity_type: str,
    entity_id: str,
) -> str:
    """
    Get the Resource Gate state for an SDLC entity."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    if entity_type not in ENTITY_TYPES:
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid entity_type. Expected one of: {', '.join(ENTITY_TYPES)}",
                "code": "invalid_entity_type",
            }
        )
    perm_err = _mcp_check_resource_gate_permission(ctx.permissions, entity_type, "read")
    if perm_err:
        return _mcp_permission_error_response(perm_err)

    async with get_db_for_mcp() as db:
        try:
            summary = await ResourceGateService(db).get_summary(
                board_id,
                entity_type,
                entity_id,
            )
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        await db.commit()
        return json.dumps({"success": True, **summary}, default=str)


@mcp.tool()
async def okto_pulse_mark_resource_not_applicable(
    board_id: str,
    entity_type: str,
    entity_id: str,
    resource_type: str,
    justification: str = "",
) -> str:
    """
    Mark a mandatory resource as not applicable through the MCP channel."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    if entity_type not in ENTITY_TYPES:
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid entity_type. Expected one of: {', '.join(ENTITY_TYPES)}",
                "code": "invalid_entity_type",
            }
        )
    perm_err = _mcp_check_resource_gate_permission(ctx.permissions, entity_type, "write")
    if perm_err:
        return _mcp_permission_error_response(perm_err)

    async with get_db_for_mcp() as db:
        try:
            result = await ResourceGateService(db).mark_not_applicable(
                board_id,
                entity_type,
                entity_id,
                resource_type,
                ctx.agent_id,
                justification=justification,
                source_channel="mcp",
            )
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        await db.commit()
        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_clear_resource_not_applicable(
    board_id: str,
    entity_type: str,
    entity_id: str,
    resource_type: str,
    reason: str = "",
) -> str:
    """
    Clear an active Resource Gate N/A mark.

    Use this when the resource becomes applicable after all, or when the real
    Architecture, Mockup, or Knowledge Base has been attached."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    if entity_type not in ENTITY_TYPES:
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid entity_type. Expected one of: {', '.join(ENTITY_TYPES)}",
                "code": "invalid_entity_type",
            }
        )
    perm_err = _mcp_check_resource_gate_permission(ctx.permissions, entity_type, "write")
    if perm_err:
        return _mcp_permission_error_response(perm_err)

    async with get_db_for_mcp() as db:
        try:
            result = await ResourceGateService(db).clear_not_applicable(
                board_id,
                entity_type,
                entity_id,
                resource_type,
                ctx.agent_id,
                reason=reason or None,
            )
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        await db.commit()
        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_create_story(
    board_id: str,
    topic_id: str,
    title: str,
    description: str,
    actor: str = "",
    goal: str = "",
    benefit: str = "",
    labels: list[str] | str = "",
    status: str = "draft",
) -> str:
    """Create a lightweight Story before Ideation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "story.entity.create", Permissions.SPECS_CREATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.db import StoryStatus
    from okto_pulse.core.models.schemas import StoryCreate

    try:
        story_status = StoryStatus(status)
        label_list = coerce_to_list_str(labels) or None
    except ValueError as e:
        return json.dumps({"error": str(e)})

    async with get_db_for_mcp() as db:
        try:
            story = await StoryService(db).create_story(
                board_id,
                ctx.agent_id,
                StoryCreate(
                    topic_id=topic_id,
                    title=title,
                    description=description.replace("\\n", "\n"),
                    actor=actor or None,
                    goal=goal or None,
                    benefit=benefit or None,
                    labels=label_list,
                    status=story_status,
                ),
                skip_ownership_check=True,
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()
        if not story:
            return json.dumps({"error": "Board not found"})
        return json.dumps({"success": True, "story": _story_payload(story)}, default=str)


@mcp.tool()
async def okto_pulse_update_story(
    board_id: str,
    story_id: str,
    topic_id: str = "",
    title: str = "",
    description: str = "",
    actor: str = "",
    goal: str = "",
    benefit: str = "",
    labels: list[str] | str = "",
) -> str:
    """Update editable Story fields through MCP."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    from okto_pulse.core.models.schemas import StoryUpdate

    update_data: dict[str, Any] = {}
    if topic_id:
        update_data["topic_id"] = topic_id
    if title:
        update_data["title"] = title
    if description:
        update_data["description"] = description.replace("\\n", "\n")
    if actor:
        update_data["actor"] = actor
    if goal:
        update_data["goal"] = goal
    if benefit:
        update_data["benefit"] = benefit
    if labels != "":
        try:
            update_data["labels"] = coerce_to_list_str(labels) or []
        except ValueError as e:
            return json.dumps({"error": str(e)})
    if not update_data:
        return json.dumps({"success": False, "error": "Provide at least one field to update"})

    async with get_db_for_mcp() as db:
        service = StoryService(db)
        existing = await service.get_story(story_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        for required_permission in story_update_permissions(update_data):
            perm_err = _mcp_check_story_state_permission(
                ctx.permissions,
                required_permission,
                existing,
                Permissions.SPECS_UPDATE,
            )
            if perm_err:
                return _mcp_permission_error_response(perm_err)
        try:
            story = await service.update_story(story_id, ctx.agent_id, StoryUpdate(**update_data))
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()
        if not story or story.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        return json.dumps({"success": True, "story": _story_payload(story)}, default=str)


@mcp.tool()
async def okto_pulse_move_story(board_id: str, story_id: str, status: str) -> str:
    """Move a Story through draft, triage, and ready. Converted is set by link/conversion."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    from okto_pulse.core.models.db import StoryStatus
    from okto_pulse.core.models.schemas import StoryMove

    try:
        story_status = StoryStatus(status)
    except ValueError:
        return json.dumps({"error": f"Invalid status. Must be one of: {[s.value for s in StoryStatus]}"})

    async with get_db_for_mcp() as db:
        service = StoryService(db)
        existing = await service.get_story(story_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        perm_err = _mcp_check_story_state_permission(
            ctx.permissions,
            story_move_permission(existing.status, story_status),
            existing,
            Permissions.SPECS_CREATE,
        )
        if perm_err:
            return _mcp_permission_error_response(perm_err)
        try:
            story = await service.move_story(story_id, ctx.agent_id, StoryMove(status=story_status))
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()
        if not story or story.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        return json.dumps({"success": True, "story": _story_payload(story)}, default=str)


@mcp.tool()
async def okto_pulse_archive_story(board_id: str, story_id: str) -> str:
    """Archive a Story without deleting lineage or linked Ideations."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    async with get_db_for_mcp() as db:
        service = StoryService(db)
        existing = await service.get_story(story_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        perm_err = _mcp_check_story_state_permission(
            ctx.permissions,
            "story.entity.archive",
            existing,
            Permissions.SPECS_UPDATE,
        )
        if perm_err:
            return _mcp_permission_error_response(perm_err)
        story = await service.archive_story(story_id, ctx.agent_id, archived=True)
        await db.commit()
        if not story or story.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        return json.dumps({"success": True, "story": _story_payload(story)}, default=str)


@mcp.tool()
async def okto_pulse_restore_story(board_id: str, story_id: str) -> str:
    """Restore an archived Story."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    async with get_db_for_mcp() as db:
        service = StoryService(db)
        existing = await service.get_story(story_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        perm_err = _mcp_check_story_state_permission(
            ctx.permissions,
            "story.entity.restore",
            existing,
            Permissions.SPECS_UPDATE,
        )
        if perm_err:
            return _mcp_permission_error_response(perm_err)
        story = await service.archive_story(story_id, ctx.agent_id, archived=False)
        await db.commit()
        if not story or story.board_id != board_id:
            return json.dumps({"error": "Story not found"})
        return json.dumps({"success": True, "story": _story_payload(story)}, default=str)


@mcp.tool()
async def okto_pulse_link_story_to_ideation(
    board_id: str,
    story_id: str,
    ideation_id: str,
    mark_converted: str = "true",
) -> str:
    """Link a Story to one Ideation; multiple Stories may feed the same Ideation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    async with get_db_for_mcp() as db:
        service = StoryService(db)
        story = await service.get_story(story_id)
        if not story or story.board_id != board_id:
            return json.dumps({"error": "Story or Ideation not found"})
        perm_err = _mcp_check_story_state_permission(
            ctx.permissions,
            "story.links.ideation",
            story,
            Permissions.SPECS_CREATE,
        )
        if perm_err:
            return _mcp_permission_error_response(perm_err)
        try:
            link = await service.link_story_to_ideation(
                story_id,
                ideation_id,
                ctx.agent_id,
                mark_converted=True,
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        story = await service.get_story(story_id)
        await db.commit()
        if not link or link.board_id != board_id:
            return json.dumps({"error": "Story or Ideation not found"})
        return json.dumps(
            {
                "success": True,
                "link": {"id": link.id, "story_id": link.story_id, "ideation_id": link.ideation_id},
                "story": _story_payload(story) if story else None,
                "mark_converted_input_ignored": not _flag_enabled(mark_converted),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_convert_stories_to_ideation(
    board_id: str,
    story_ids: list[str] | str,
    ideation_id: str = "",
    title: str = "",
    description: str = "",
    problem_statement: str = "",
    proposed_approach: str = "",
    mockup_ids: list[str] | str = "",
) -> str:
    """Create a new Ideation or link an existing Ideation from selected Stories."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(ctx.permissions, "story.conversion.to_ideation", Permissions.SPECS_CREATE)
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.models.schemas import StoryConversionRequest

    try:
        story_id_list = coerce_to_list_str(story_ids)
        mockup_id_list = coerce_to_list_str(mockup_ids) if mockup_ids else None
    except ValueError as e:
        return json.dumps({"error": str(e)})
    if not story_id_list:
        return json.dumps({"error": "At least one story_id is required"})

    async with get_db_for_mcp() as db:
        service = StoryService(db)
        for story_id in story_id_list:
            story = await service.get_story(story_id)
            if not story or story.board_id != board_id:
                return json.dumps({"error": "One or more Stories were not found in this board"})
            perm_err = _mcp_check_story_state_permission(
                ctx.permissions,
                "story.conversion.to_ideation",
                story,
                Permissions.SPECS_CREATE,
            )
            if perm_err:
                return _mcp_permission_error_response(perm_err)
        try:
            result = await service.convert_stories(
                board_id,
                ctx.agent_id,
                StoryConversionRequest(
                    story_ids=story_id_list,
                    ideation_id=ideation_id or None,
                    title=title or None,
                    description=description.replace("\\n", "\n") if description else None,
                    problem_statement=problem_statement.replace("\\n", "\n") if problem_statement else None,
                    proposed_approach=proposed_approach.replace("\\n", "\n") if proposed_approach else None,
                    mockup_ids=mockup_id_list,
                ),
                skip_ownership_check=True,
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()
        if not result:
            return json.dumps({"error": "Board not found"})
        ideation, links, propagated = result
        return json.dumps(
            {
                "success": True,
                "ideation": {"id": ideation.id, "title": ideation.title, "status": ideation.status.value},
                "links": [{"id": link.id, "story_id": link.story_id, "ideation_id": link.ideation_id} for link in links],
                "propagated_mockups": propagated,
            },
            default=str,
        )


# ============================================================================
# IDEATION TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_create_ideation(
    board_id: str,
    title: str,
    description: str = "",
    problem_statement: str = "",
    proposed_approach: str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Create a new ideation on the board. Ideations are the starting point — raw ideas that may be
    evaluated, refined into refinements, and eventually derived into specs."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationCreate

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        ideation_data = IdeationCreate(
            title=title,
            description=description.replace("\\n", "\n") if description else None,
            problem_statement=problem_statement.replace("\\n", "\n") if problem_statement else None,
            proposed_approach=proposed_approach.replace("\\n", "\n") if proposed_approach else None,
            assignee_id=assignee_id or None,
            labels=coerce_to_list_str(labels) or None,
        )

        ideation = await service.create_ideation(
            board_id, ctx.agent_id, ideation_data, skip_ownership_check=True
        )
        await db.commit()

        if not ideation:
            return json.dumps({"error": "Failed to create ideation"})

        return json.dumps(
            {
                "success": True,
                "ideation": {
                    "id": ideation.id,
                    "title": ideation.title,
                    "status": ideation.status.value,
                    "version": ideation.version,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_ideation(board_id: str, ideation_id: str) -> str:
    """
    Get full details of an ideation including its refinements, specs, and Q&A items."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        ideation = await service.get_ideation(ideation_id)
        await db.commit()

        if not ideation or ideation.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps(
            {
                "id": ideation.id,
                "board_id": ideation.board_id,
                "title": ideation.title,
                "description": ideation.description,
                "problem_statement": ideation.problem_statement,
                "proposed_approach": ideation.proposed_approach,
                "scope_assessment": ideation.scope_assessment,
                "complexity": ideation.complexity.value if ideation.complexity else None,
                "status": ideation.status.value,
                "version": ideation.version,
                "assignee_id": ideation.assignee_id,
                "created_by": ideation.created_by,
                "created_at": ideation.created_at.isoformat(),
                "updated_at": ideation.updated_at.isoformat(),
                "labels": ideation.labels,
                "refinements": [
                    {
                        "id": r.id,
                        "title": r.title,
                        "status": r.status.value,
                        "version": r.version,
                    }
                    for r in ideation.refinements
                ],
                "specs": [
                    {
                        "id": s.id,
                        "title": s.title,
                        "status": s.status.value,
                    }
                    for s in ideation.specs
                ],
                "qa_items": [
                    {
                        "id": q.id,
                        "question": q.question,
                        "question_type": q.question_type,
                        "choices": q.choices,
                        "answer": q.answer,
                        "selected": q.selected,
                        "asked_by": q.asked_by,
                        "answered_by": q.answered_by,
                    }
                    for q in ideation.qa_items
                ],
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_ideation_context(
    board_id: str,
    ideation_id: str,
    include_knowledge: str = "true",
    include_mockups: str = "true",
    include_qa: str = "true",
    include_architecture: str = "true",
) -> str:
    """
    Get the FULL consolidated context of an ideation. Returns all data needed
    to evaluate, review, or derive refinements/specs from this ideation.

    **Always call this before evaluating, moving, or deriving from an ideation.**"""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_kb = _flag_enabled(include_knowledge)
    _inc_mockups = _flag_enabled(include_mockups)
    _inc_qa = _flag_enabled(include_qa)
    _inc_architecture = _flag_enabled(include_architecture)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        ideation = await service.get_ideation(ideation_id)
        await db.commit()

        if not ideation or ideation.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})

        result: dict = {
            "id": ideation.id,
            "board_id": ideation.board_id,
            "title": ideation.title,
            "description": ideation.description,
            "problem_statement": ideation.problem_statement,
            "proposed_approach": ideation.proposed_approach,
            "scope_assessment": ideation.scope_assessment,
            "complexity": ideation.complexity.value if ideation.complexity else None,
            "status": ideation.status.value,
            "version": ideation.version,
            "assignee_id": ideation.assignee_id,
            "created_by": ideation.created_by,
            "created_at": ideation.created_at.isoformat() if ideation.created_at else None,
            "updated_at": ideation.updated_at.isoformat() if ideation.updated_at else None,
            "labels": ideation.labels or [],
            "refinements": [
                {"id": r.id, "title": r.title, "status": r.status.value, "version": r.version}
                for r in ideation.refinements
            ],
            "specs": [
                {"id": s.id, "title": s.title, "status": s.status.value}
                for s in (ideation.specs if hasattr(ideation, "specs") else [])
            ],
        }

        if _inc_qa:
            result["qa_items"] = [
                {
                    "id": q.id,
                    "question": q.question,
                    "question_type": q.question_type,
                    "choices": q.choices,
                    "answer": q.answer,
                    "selected": q.selected,
                    "asked_by": q.asked_by,
                    "answered_by": q.answered_by,
                }
                for q in ideation.qa_items
            ]

        if _inc_mockups and hasattr(ideation, "screen_mockups") and ideation.screen_mockups:
            result["screen_mockups"] = ideation.screen_mockups

        architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            architecture_designs = await _mcp_architecture_for_parent(
                db, "ideation", ideation_id, permissions=ctx.permissions
            )
            result["architecture_designs"] = architecture_designs

        if _inc_kb and hasattr(ideation, "knowledge_bases"):
            result["knowledge_bases"] = [
                _serialize_knowledge_base(kb)
                for kb in (ideation.knowledge_bases or [])
            ]

        resolved_references = resolve_entity_context_references(
            ideation,
            source_type="ideation",
            include_content=_inc_kb,
            architecture_designs=architecture_designs if _inc_architecture else [],
        )
        if not _inc_kb:
            resolved_references["knowledge_bases"] = []
        if not _inc_mockups:
            resolved_references["screen_mockups"] = []
        if not _inc_architecture:
            resolved_references["architecture_designs"] = []
        result["resolved_references"] = resolved_references

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_update_ideation(
    board_id: str,
    ideation_id: str,
    title: str = "",
    description: str = "",
    problem_statement: str = "",
    proposed_approach: str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Update an ideation's fields. Content changes bump the version. Only non-empty fields are updated."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationUpdate

    update_kwargs: dict[str, Any] = {}
    if title:
        update_kwargs["title"] = title
    if description:
        update_kwargs["description"] = description.replace("\\n", "\n")
    if problem_statement:
        update_kwargs["problem_statement"] = problem_statement.replace("\\n", "\n")
    if proposed_approach:
        update_kwargs["proposed_approach"] = proposed_approach.replace("\\n", "\n")
    if assignee_id:
        update_kwargs["assignee_id"] = assignee_id
    if labels:
        try:
            update_kwargs["labels"] = coerce_to_list_str(labels)
        except ValueError as e:
            return json.dumps({"error": f"Invalid labels: {e}"})

    if not update_kwargs:
        return json.dumps({"error": "No fields to update"})

    ideation_update = IdeationUpdate(**update_kwargs)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        ideation = await service.update_ideation(ideation_id, ctx.agent_id, ideation_update)
        await db.commit()

        if not ideation:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps(
            {
                "success": True,
                "ideation": {
                    "id": ideation.id,
                    "title": ideation.title,
                    "status": ideation.status.value,
                    "version": ideation.version,
                    "complexity": ideation.complexity.value if ideation.complexity else None,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_move_ideation(board_id: str, ideation_id: str, status: str) -> str:
    """
    Change an ideation's status (draft -> review -> approved -> evaluating -> done).

    Allowed transitions:
    - draft → review, cancelled
    - review → draft, approved, cancelled
    - approved → review, evaluating, cancelled
    - evaluating → approved, done, cancelled
    - done → draft (new version)"""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_MOVE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import IdeationStatus
    from okto_pulse.core.models.schemas import IdeationMove

    try:
        ideation_status = IdeationStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in IdeationStatus]}"}
        )

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        existing = await service.get_ideation(ideation_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})
        old_status = existing.status.value
        try:
            ideation = await service.move_ideation(
                ideation_id, ctx.agent_id, IdeationMove(status=ideation_status), actor_name=ctx.agent_name
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not ideation:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps(
            {
                "success": True,
                "ideation_id": ideation.id,
                "from_status": old_status,
                "to_status": status,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_ideation(board_id: str, ideation_id: str) -> str:
    """
    Delete an ideation. Linked refinements and Q&A are also deleted (cascade)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        deleted = await service.delete_ideation(ideation_id, ctx.agent_id)
        await db.commit()

        if not deleted:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_evaluate_ideation(
    board_id: str,
    ideation_id: str,
    domains: str = "",
    domains_justification: str = "",
    ambiguity: str = "",
    ambiguity_justification: str = "",
    dependencies: str = "",
    dependencies_justification: str = "",
) -> str:
    """Evaluate an ideation's scope and compute complexity (small/medium/large). Set each
dimension's score 1-5 WITH a justification, then the system computes: any >=3 ->
large (needs refinements first); any >=2 -> medium; all 1 -> small (derive spec
directly). PRE-REQUISITE: ideation status MUST be 'evaluating' (flow:
draft->review->approved->evaluating->this tool->done); other statuses fail.
Transitions are explicit gate decisions and are not auto-promoted. Full details:
okto-pulse://reference/tool-docs/ideation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)

        # First, update scope_assessment if any scores provided
        scope = {}
        if domains:
            scope["domains"] = int(domains)
        if domains_justification:
            scope["domains_justification"] = domains_justification.replace("\\n", "\n")
        if ambiguity:
            scope["ambiguity"] = int(ambiguity)
        if ambiguity_justification:
            scope["ambiguity_justification"] = ambiguity_justification.replace("\\n", "\n")
        if dependencies:
            scope["dependencies"] = int(dependencies)
        if dependencies_justification:
            scope["dependencies_justification"] = dependencies_justification.replace("\\n", "\n")

        if scope:
            # Merge with existing scope_assessment
            ideation = await service.get_ideation(ideation_id)
            if not ideation or ideation.board_id != board_id:
                return json.dumps({"error": "Ideation not found"})

            existing_scope = ideation.scope_assessment or {}
            existing_scope.update(scope)

            # Write scope_assessment directly (bypasses draft-only edit guard
            # since evaluation requires writing scores in 'evaluating' status)
            from sqlalchemy.orm.attributes import flag_modified
            ideation.scope_assessment = existing_scope
            flag_modified(ideation, "scope_assessment")

        # Then evaluate complexity
        ideation = await service.evaluate_complexity(ideation_id, ctx.agent_id)
        await db.commit()

        if not ideation:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps(
            {
                "success": True,
                "ideation_id": ideation.id,
                "scope_assessment": ideation.scope_assessment,
                "complexity": ideation.complexity.value if ideation.complexity else None,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_derive_spec_from_ideation(
    board_id: str,
    ideation_id: str,
    mockup_ids: str = "",
    kb_ids: str = "",
    architecture_design_ids: list[str] | str = "",
    architecture_propagation_mode: str = "copy",
) -> str:
    """
    Create a spec draft from a DONE ideation. The ideation must be in 'done' status
    (meaning it has been fully reviewed and snapshotted). The spec will have rich context
    compiled from the ideation but structured fields (requirements, criteria) left empty
    for deliberate analysis.

    Artifacts (mockups, KBs, Architecture Designs) from the ideation are
    automatically propagated to the spec. Use mockup_ids/kb_ids/
    architecture_design_ids to select specific ones (default: all)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    _mockup_ids = parse_multi_value(mockup_ids) or None
    _kb_ids = parse_multi_value(kb_ids) or None
    try:
        _architecture_ids = coerce_to_list_str(architecture_design_ids) or None
    except ValueError as e:
        return json.dumps({"error": f"Invalid architecture_design_ids: {e}"})

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        try:
            spec = await service.derive_spec(
                ideation_id, ctx.agent_id, skip_ownership_check=True,
                mockup_ids=_mockup_ids, kb_ids=_kb_ids,
                architecture_design_ids=_architecture_ids,
                architecture_propagation_mode=architecture_propagation_mode,
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not spec:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps(
            {
                "success": True,
                "ideation_id": ideation_id,
                "spec": {
                    "id": spec.id,
                    "title": spec.title,
                    "status": spec.status.value,
                    "version": spec.version,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_ideation_snapshot(board_id: str, ideation_id: str, version: str) -> str:
    """
    Get the full immutable snapshot of an ideation at a specific version.
    Includes all fields as they were when the ideation was marked 'done',
    plus a snapshot of all Q&A at that point."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        snapshot = await service.get_snapshot(ideation_id, int(version))
        await db.commit()

        if not snapshot:
            return json.dumps({"error": f"Snapshot v{version} not found"})

        return json.dumps(
            {
                "ideation_id": ideation_id,
                "version": snapshot.version,
                "title": snapshot.title,
                "description": snapshot.description,
                "problem_statement": snapshot.problem_statement,
                "proposed_approach": snapshot.proposed_approach,
                "scope_assessment": snapshot.scope_assessment,
                "complexity": snapshot.complexity,
                "labels": snapshot.labels,
                "qa_snapshot": snapshot.qa_snapshot,
                "created_by": snapshot.created_by,
                "created_at": snapshot.created_at.isoformat(),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_ideation_history(board_id: str, ideation_id: str, limit: str = "30") -> str:
    """
    Get the detailed change history of an ideation. Shows every modification with field-level diffs,
    who made the change, and when."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        entries = await service.list_history(ideation_id, int(limit))
        await db.commit()

        return json.dumps(
            {
                "ideation_id": ideation_id,
                "count": len(entries),
                "history": [
                    {
                        "id": e.id,
                        "action": e.action,
                        "actor_type": e.actor_type,
                        "actor_id": e.actor_id,
                        "actor_name": e.actor_name,
                        "changes": e.changes,
                        "summary": e.summary,
                        "version": e.version,
                        "created_at": e.created_at.isoformat(),
                    }
                    for e in entries
                ],
            },
            default=str,
        )


# ============================================================================
# IDEATION KNOWLEDGE BASE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_ideation_knowledge(
    board_id: str,
    ideation_id: str,
    knowledge_id: str,
) -> str:
    """Get the full content of an ideation knowledge base item."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        ideation = await IdeationService(db).get_ideation(ideation_id)
        if not ideation or ideation.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})
        kb = await IdeationKnowledgeService(db).get_knowledge(knowledge_id)
        await db.commit()
        if not kb or kb.ideation_id != ideation_id:
            return json.dumps({"error": "Knowledge base item not found"})
        return json.dumps(_serialize_knowledge_base(kb), default=str)


@mcp.tool()
async def okto_pulse_add_ideation_knowledge(
    board_id: str,
    ideation_id: str,
    title: str,
    content: str = "",
    description: str = "",
    mime_type: str = "text/markdown",
    file_path: str | None = None,
    file_url: str | None = None,
) -> str:
    """
    Add a knowledge base item to an ideation.

    Provide exactly ONE of content, file_path, or file_url. Ideation KBs are
    propagated to refinements/specs by default when those artifacts are derived
    or created from the ideation.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    resolved_content, err = await _resolve_text_content(
        content=content, file_path=file_path, file_url=file_url
    )
    if err:
        return json.dumps({"error": err})

    from okto_pulse.core.models.schemas import IdeationKnowledgeCreate

    async with get_db_for_mcp() as db:
        ideation = await IdeationService(db).get_ideation(ideation_id)
        if not ideation or ideation.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})
        kb_data = IdeationKnowledgeCreate(
            title=title,
            description=description or None,
            content=resolved_content,
            mime_type=mime_type,
        )
        kb = await IdeationKnowledgeService(db).create_knowledge(
            ideation_id, ctx.agent_id, kb_data
        )
        await db.commit()
        if not kb:
            return json.dumps({"error": "Failed to create knowledge base item"})
        return json.dumps(
            {"success": True, "knowledge": _serialize_knowledge_base(kb)},
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_ideation_knowledge(
    board_id: str,
    ideation_id: str,
    knowledge_id: str,
) -> str:
    """Delete a knowledge base item from an ideation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        ideation = await IdeationService(db).get_ideation(ideation_id)
        if not ideation or ideation.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})
        service = IdeationKnowledgeService(db)
        kb = await service.get_knowledge(knowledge_id)
        if not kb or kb.ideation_id != ideation_id:
            return json.dumps({"error": "Knowledge base item not found"})
        await service.delete_knowledge(knowledge_id)
        await db.commit()
        return json.dumps({"success": True})


# ============================================================================
# IDEATION Q&A TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_ask_ideation_question(board_id: str, ideation_id: str, question: str) -> str:
    """
    Ask a question on an ideation's Q&A board. Use @Name to direct the question."""
    return await _ask_question_impl(
        board_id, "ideation", ideation_id, question,
        alias_kind="legacy", tool_name="okto_pulse_ask_ideation_question",
    )


@mcp.tool()
async def okto_pulse_ask_ideation_choice_question(
    board_id: str,
    ideation_id: str,
    question: str,
    options: list[str] | str,
    question_type: str = "choice",
    allow_free_text: str = "false",
    options_json: str = "",
) -> str:
    """
    Ask a choice question (poll/form) on an ideation's Q&A board.

options_json (optional, takes precedence): JSON array of option objects, e.g. '[{"label":"A","recommended":true,"tradeoff":"costs more"}]'. When present and non-empty, options is ignored. Each object requires a non-empty label; recommended defaults to false; tradeoff defaults to null.
Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationQAChoiceOption, IdeationQACreate

    try:
        parsed_objects = parse_options_json(options_json or None)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options_json: {e}"})

    if parsed_objects is not None:
        choice_list = [
            IdeationQAChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
            for i, obj in enumerate(parsed_objects)
        ]
    else:
        try:
            option_labels = coerce_to_list_str(options)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
        if not option_labels:
            return json.dumps({"error": "At least one option is required"})
        choice_list = [
            IdeationQAChoiceOption(id=f"opt_{i}", label=label)
            for i, label in enumerate(option_labels)
        ]

    async with get_db_for_mcp() as db:
        service = IdeationQAService(db)
        data = IdeationQACreate(
            question=question,
            question_type=question_type if question_type in ("choice", "multi_choice") else "choice",
            choices=choice_list,
            allow_free_text=allow_free_text.lower() == "true",
        )
        qa = await service.create_question(ideation_id, ctx.agent_id, data)
        if not qa:
            return json.dumps({"error": "Ideation not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="ideation_choice_question_added",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"ideation_id": ideation_id, "question": question[:100], "option_count": len(choice_list)},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "choices": qa.choices,
                    "allow_free_text": qa.allow_free_text,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_answer_ideation_question(board_id: str, ideation_id: str, qa_id: str, answer: str = "", selected: list[str] | str = "") -> str:
    """
    Answer a question on an ideation's Q&A board.
    For text questions, provide answer. For choice questions, provide selected option IDs.

Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationQAAnswer
    from okto_pulse.core.services import QASelfAnsweringNotAllowedError

    try:
        selected_list = coerce_to_list_str(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    async with get_db_for_mcp() as db:
        service = IdeationQAService(db)
        try:
            qa = await service.answer_question(
                qa_id,
                ctx.agent_id,
                IdeationQAAnswer(answer=answer or None, selected=selected_list),
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as e:
            await db.commit()
            return json.dumps({"error": e.reason, "detail": str(e)})
        if not qa:
            return json.dumps({"error": "Q&A item not found or invalid selection"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="ideation_question_answered",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"ideation_id": ideation_id, "qa_id": qa_id, "answer": (answer or "")[:100], "selected": selected_list},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "answer": qa.answer,
                    "selected": qa.selected,
                    "asked_by": qa.asked_by,
                    "answered_by": qa.answered_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_create_refinement(
    board_id: str,
    ideation_id: str,
    title: str,
    description: str = "",
    in_scope: list[str] | str = "",
    out_of_scope: list[str] | str = "",
    analysis: str = "",
    decisions: list[str] | str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
    mockup_ids: str = "",
    kb_ids: str = "",
    architecture_design_ids: list[str] | str = "",
    architecture_propagation_mode: str = "copy",
) -> str:
    """
    Create a new refinement for a DONE ideation. The ideation must be in 'done' status
    (snapshotted) before refinements can be created. The parent ideation context
    is always preserved; when description is provided, inherited context is appended.

    Artifacts (mockups, KBs, Architecture Designs) from the ideation are
    automatically propagated. Use mockup_ids/kb_ids/architecture_design_ids
    to select specific ones (default: all)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementCreate

    try:
        in_scope_list = coerce_to_list_str(in_scope) or None
        out_of_scope_list = coerce_to_list_str(out_of_scope) or None
        decisions_list = coerce_to_list_str(decisions) or None
        label_list = coerce_to_list_str(labels) or None
        architecture_ids = coerce_to_list_str(architecture_design_ids) or None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        refinement_data = RefinementCreate(
            ideation_id=ideation_id,
            title=title,
            description=description.replace("\\n", "\n") if description else None,
            in_scope=in_scope_list,
            out_of_scope=out_of_scope_list,
            analysis=analysis.replace("\\n", "\n") if analysis else None,
            decisions=decisions_list,
            assignee_id=assignee_id or None,
            labels=label_list,
            mockup_ids=parse_multi_value(mockup_ids) or None,
            kb_ids=parse_multi_value(kb_ids) or None,
            architecture_design_ids=architecture_ids,
            architecture_propagation_mode=architecture_propagation_mode,
        )

        try:
            refinement = await service.create_refinement(
                ideation_id, ctx.agent_id, refinement_data, skip_ownership_check=True
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not refinement:
            return json.dumps({"error": "Failed to create refinement (ideation not found)"})

        return json.dumps(
            {
                "success": True,
                "refinement": {
                    "id": refinement.id,
                    "title": refinement.title,
                    "status": refinement.status.value,
                    "version": refinement.version,
                    "ideation_id": refinement.ideation_id,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_refinement(board_id: str, refinement_id: str) -> str:
    """
    Get full details of a refinement including its specs and Q&A items."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        refinement = await service.get_refinement(refinement_id)
        await db.commit()

        if not refinement or refinement.board_id != board_id:
            return json.dumps({"error": "Refinement not found"})

        return json.dumps(
            {
                "id": refinement.id,
                "ideation_id": refinement.ideation_id,
                "board_id": refinement.board_id,
                "title": refinement.title,
                "description": refinement.description,
                "in_scope": refinement.in_scope,
                "out_of_scope": refinement.out_of_scope,
                "analysis": refinement.analysis,
                "decisions": refinement.decisions,
                "status": refinement.status.value,
                "version": refinement.version,
                "assignee_id": refinement.assignee_id,
                "created_by": refinement.created_by,
                "created_at": refinement.created_at.isoformat(),
                "updated_at": refinement.updated_at.isoformat(),
                "labels": refinement.labels,
                "specs": [
                    {
                        "id": s.id,
                        "title": s.title,
                        "status": s.status.value,
                    }
                    for s in refinement.specs
                ],
                "qa_items": [
                    {
                        "id": q.id,
                        "question": q.question,
                        "question_type": q.question_type,
                        "choices": q.choices,
                        "answer": q.answer,
                        "selected": q.selected,
                        "asked_by": q.asked_by,
                        "answered_by": q.answered_by,
                    }
                    for q in refinement.qa_items
                ],
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_refinement_context(
    board_id: str,
    refinement_id: str,
    include_knowledge: str = "true",
    include_mockups: str = "true",
    include_qa: str = "true",
    include_architecture: str = "true",
) -> str:
    """
    Get the FULL consolidated context of a refinement. Returns all data needed
    to review, derive specs, or evaluate this refinement.

    **Always call this before moving, evaluating, or deriving a spec from a refinement.**"""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_kb = _flag_enabled(include_knowledge)
    _inc_mockups = _flag_enabled(include_mockups)
    _inc_qa = _flag_enabled(include_qa)
    _inc_architecture = _flag_enabled(include_architecture)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        refinement = await service.get_refinement(refinement_id)
        await db.commit()

        if not refinement or refinement.board_id != board_id:
            return json.dumps({"error": "Refinement not found"})

        result: dict = {
            "id": refinement.id,
            "ideation_id": refinement.ideation_id,
            "board_id": refinement.board_id,
            "title": refinement.title,
            "description": refinement.description,
            "in_scope": refinement.in_scope,
            "out_of_scope": refinement.out_of_scope,
            "analysis": refinement.analysis,
            "decisions": refinement.decisions,
            "status": refinement.status.value,
            "version": refinement.version,
            "assignee_id": refinement.assignee_id,
            "created_by": refinement.created_by,
            "created_at": refinement.created_at.isoformat() if refinement.created_at else None,
            "updated_at": refinement.updated_at.isoformat() if refinement.updated_at else None,
            "labels": refinement.labels or [],
            "specs": [
                {"id": s.id, "title": s.title, "status": s.status.value}
                for s in (refinement.specs if hasattr(refinement, "specs") else [])
            ],
        }
        parent_ideation = serialize_parent_ideation_context(
            getattr(refinement, "ideation", None),
            include_qa=_inc_qa,
        )
        if parent_ideation:
            result["parent_ideation"] = parent_ideation

        if _inc_qa:
            result["qa_items"] = [
                {
                    "id": q.id,
                    "question": q.question,
                    "question_type": q.question_type,
                    "choices": q.choices,
                    "answer": q.answer,
                    "selected": q.selected,
                    "asked_by": q.asked_by,
                    "answered_by": q.answered_by,
                }
                for q in refinement.qa_items
            ]

        if _inc_mockups and hasattr(refinement, "screen_mockups") and refinement.screen_mockups:
            result["screen_mockups"] = refinement.screen_mockups

        architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            architecture_designs = await _mcp_architecture_for_parent(
                db, "refinement", refinement_id, permissions=ctx.permissions
            )
            result["architecture_designs"] = architecture_designs

        if _inc_kb and hasattr(refinement, "knowledge_bases"):
            result["knowledge_bases"] = [
                _serialize_knowledge_base(kb)
                for kb in (refinement.knowledge_bases or [])
            ]

        resolved_references = resolve_entity_context_references(
            refinement,
            source_type="refinement",
            include_content=_inc_kb,
            architecture_designs=architecture_designs if _inc_architecture else [],
            include_parent_qa=_inc_qa,
        )
        if not _inc_kb:
            resolved_references["knowledge_bases"] = []
        if not _inc_mockups:
            resolved_references["screen_mockups"] = []
        if not _inc_architecture:
            resolved_references["architecture_designs"] = []
        result["resolved_references"] = resolved_references

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_update_refinement(
    board_id: str,
    refinement_id: str,
    title: str = "",
    description: str = "",
    in_scope: list[str] | str = "",
    out_of_scope: list[str] | str = "",
    analysis: str = "",
    decisions: list[str] | str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Update a refinement's fields. Content changes bump the version. Only non-empty fields are updated."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementUpdate

    update_kwargs: dict[str, Any] = {}
    if title:
        update_kwargs["title"] = title
    if description:
        update_kwargs["description"] = description.replace("\\n", "\n")
    if in_scope:
        try:
            update_kwargs["in_scope"] = coerce_to_list_str(in_scope)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if out_of_scope:
        try:
            update_kwargs["out_of_scope"] = coerce_to_list_str(out_of_scope)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if analysis:
        update_kwargs["analysis"] = analysis.replace("\\n", "\n")
    if decisions:
        try:
            update_kwargs["decisions"] = coerce_to_list_str(decisions)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if assignee_id:
        update_kwargs["assignee_id"] = assignee_id
    if labels:
        try:
            update_kwargs["labels"] = coerce_to_list_str(labels)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    if not update_kwargs:
        return json.dumps({"error": "No fields to update"})

    refinement_update = RefinementUpdate(**update_kwargs)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        refinement = await service.update_refinement(refinement_id, ctx.agent_id, refinement_update)
        await db.commit()

        if not refinement:
            return json.dumps({"error": "Refinement not found"})

        return json.dumps(
            {
                "success": True,
                "refinement": {
                    "id": refinement.id,
                    "title": refinement.title,
                    "status": refinement.status.value,
                    "version": refinement.version,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_move_refinement(board_id: str, refinement_id: str, status: str) -> str:
    """
    Change a refinement's status (draft -> review -> approved -> done).

    Allowed transitions:
    - draft → review, cancelled
    - review → draft, approved, cancelled
    - approved → review, done, cancelled
    - done → draft (new version)"""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_MOVE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import RefinementStatus
    from okto_pulse.core.models.schemas import RefinementMove

    try:
        refinement_status = RefinementStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in RefinementStatus]}"}
        )

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        existing = await service.get_refinement(refinement_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Refinement not found"})
        old_status = existing.status.value
        try:
            refinement = await service.move_refinement(
                refinement_id, ctx.agent_id, RefinementMove(status=refinement_status), actor_name=ctx.agent_name
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not refinement:
            return json.dumps({"error": "Refinement not found"})

        return json.dumps(
            {
                "success": True,
                "refinement_id": refinement.id,
                "from_status": old_status,
                "to_status": status,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_refinement(board_id: str, refinement_id: str) -> str:
    """
    Delete a refinement. Linked Q&A items are also deleted (cascade)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        deleted = await service.delete_refinement(refinement_id, ctx.agent_id)
        await db.commit()

        if not deleted:
            return json.dumps({"error": "Refinement not found"})

        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_derive_spec_from_refinement(
    board_id: str,
    refinement_id: str,
    mockup_ids: str = "",
    kb_ids: str = "",
    architecture_design_ids: list[str] | str = "",
    architecture_propagation_mode: str = "copy",
) -> str:
    """
    Create a spec draft from a DONE refinement. The refinement must be in 'done' status.
    Context is compiled from the refinement's scope, analysis, decisions, and Q&A.

    Artifacts (mockups, KBs, Architecture Designs) from the refinement are
    automatically propagated to the spec. Use mockup_ids/kb_ids/
    architecture_design_ids to select specific ones (default: all)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    _mockup_ids = parse_multi_value(mockup_ids) or None
    _kb_ids = parse_multi_value(kb_ids) or None
    try:
        _architecture_ids = coerce_to_list_str(architecture_design_ids) or None
    except ValueError as e:
        return json.dumps({"error": f"Invalid architecture_design_ids: {e}"})

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        try:
            spec = await service.derive_spec(
                refinement_id, ctx.agent_id, skip_ownership_check=True,
                mockup_ids=_mockup_ids, kb_ids=_kb_ids,
                architecture_design_ids=_architecture_ids,
                architecture_propagation_mode=architecture_propagation_mode,
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not spec:
            return json.dumps({"error": "Refinement not found"})

        return json.dumps(
            {
                "success": True,
                "refinement_id": refinement_id,
                "spec": {
                    "id": spec.id,
                    "title": spec.title,
                    "status": spec.status.value,
                    "version": spec.version,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_refinement_history(board_id: str, refinement_id: str, limit: str = "30") -> str:
    """
    Get the detailed change history of a refinement. Shows every modification with field-level diffs,
    who made the change, and when."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        entries = await service.list_history(refinement_id, int(limit))
        await db.commit()

        return json.dumps(
            {
                "refinement_id": refinement_id,
                "count": len(entries),
                "history": [
                    {
                        "id": e.id,
                        "action": e.action,
                        "actor_type": e.actor_type,
                        "actor_id": e.actor_id,
                        "actor_name": e.actor_name,
                        "changes": e.changes,
                        "summary": e.summary,
                        "version": e.version,
                        "created_at": e.created_at.isoformat(),
                    }
                    for e in entries
                ],
            },
            default=str,
        )


# ============================================================================
# REFINEMENT Q&A TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_ask_refinement_question(board_id: str, refinement_id: str, question: str) -> str:
    """
    Ask a question on a refinement's Q&A board. Use @Name to direct the question."""
    return await _ask_question_impl(
        board_id, "refinement", refinement_id, question,
        alias_kind="legacy", tool_name="okto_pulse_ask_refinement_question",
    )


@mcp.tool()
async def okto_pulse_ask_refinement_choice_question(
    board_id: str,
    refinement_id: str,
    question: str,
    options: list[str] | str,
    question_type: str = "choice",
    allow_free_text: str = "false",
    options_json: str = "",
) -> str:
    """
    Ask a choice question (poll/form) on a refinement's Q&A board.

options_json (optional, takes precedence): JSON array of option objects, e.g. '[{"label":"A","recommended":true,"tradeoff":"costs more"}]'. When present and non-empty, options is ignored. Each object requires a non-empty label; recommended defaults to false; tradeoff defaults to null.
Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementQAChoiceOption, RefinementQACreate

    try:
        parsed_objects = parse_options_json(options_json or None)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options_json: {e}"})

    if parsed_objects is not None:
        choice_list = [
            RefinementQAChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
            for i, obj in enumerate(parsed_objects)
        ]
    else:
        try:
            option_labels = coerce_to_list_str(options)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
        if not option_labels:
            return json.dumps({"error": "At least one option is required"})
        choice_list = [
            RefinementQAChoiceOption(id=f"opt_{i}", label=label)
            for i, label in enumerate(option_labels)
        ]

    async with get_db_for_mcp() as db:
        service = RefinementQAService(db)
        data = RefinementQACreate(
            question=question,
            question_type=question_type if question_type in ("choice", "multi_choice") else "choice",
            choices=choice_list,
            allow_free_text=allow_free_text.lower() == "true",
        )
        qa = await service.create_question(refinement_id, ctx.agent_id, data)
        if not qa:
            return json.dumps({"error": "Refinement not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="refinement_choice_question_added",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"refinement_id": refinement_id, "question": question[:100], "option_count": len(choice_list)},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "choices": qa.choices,
                    "allow_free_text": qa.allow_free_text,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_answer_refinement_question(board_id: str, refinement_id: str, qa_id: str, answer: str = "", selected: list[str] | str = "") -> str:
    """
    Answer a question on a refinement's Q&A board.
    For text questions, provide answer. For choice questions, provide selected option IDs.

Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementQAAnswer
    from okto_pulse.core.services import QASelfAnsweringNotAllowedError

    try:
        selected_list = coerce_to_list_str(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    async with get_db_for_mcp() as db:
        service = RefinementQAService(db)
        try:
            qa = await service.answer_question(
                qa_id,
                ctx.agent_id,
                RefinementQAAnswer(answer=answer or None, selected=selected_list),
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as e:
            await db.commit()
            return json.dumps({"error": e.reason, "detail": str(e)})
        if not qa:
            return json.dumps({"error": "Q&A item not found or invalid selection"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="refinement_question_answered",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"refinement_id": refinement_id, "qa_id": qa_id, "answer": (answer or "")[:100], "selected": selected_list},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "answer": qa.answer,
                    "selected": qa.selected,
                    "asked_by": qa.asked_by,
                    "answered_by": qa.answered_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_create_spec(
    board_id: str,
    title: str,
    description: str = "",
    context: str = "",
    functional_requirements: list[str] | str = "",
    technical_requirements: list[str] | str = "",
    acceptance_criteria: list[str] | str = "",
    status: str = "draft",
    assignee_id: str = "",
    labels: list[str] | str = "",
    ideation_id: str = "",
    refinement_id: str = "",
) -> str:
    """
    Create a new spec (specification) on the board. Specs define requirements that drive card/task creation.
    AI agents can create specs to propose work, which can then be reviewed, approved, and derived into cards."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import SpecStatus
    from okto_pulse.core.models.schemas import SpecCreate

    try:
        spec_status = SpecStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in SpecStatus]}"}
        )

    try:
        frs_list = coerce_to_list_str(functional_requirements) or None
        trs_list = coerce_to_list_str(technical_requirements) or None
        acs_list = coerce_to_list_str(acceptance_criteria) or None
        label_list = coerce_to_list_str(labels) or None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec_data = SpecCreate(
            title=title,
            description=description.replace("\\n", "\n") if description else None,
            context=context.replace("\\n", "\n") if context else None,
            functional_requirements=frs_list,
            technical_requirements=_trs_to_objects(trs_list),
            acceptance_criteria=acs_list,
            status=spec_status,
            assignee_id=assignee_id or None,
            labels=label_list,
            ideation_id=ideation_id or None,
            refinement_id=refinement_id or None,
        )

        spec = await service.create_spec(
            board_id, ctx.agent_id, spec_data, skip_ownership_check=True
        )
        await db.commit()

        if not spec:
            return json.dumps({"error": "Failed to create spec"})

        return json.dumps(
            {
                "success": True,
                "spec": {
                    "id": spec.id,
                    "title": spec.title,
                    "status": spec.status.value,
                    "version": spec.version,
                    "functional_requirements": spec.functional_requirements,
                    "technical_requirements": spec.technical_requirements,
                    "acceptance_criteria": spec.acceptance_criteria,
                    "integration_requirements": getattr(spec, "integration_requirements", None) or [],
                    "observability_requirements": getattr(spec, "observability_requirements", None) or [],
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_spec(board_id: str, spec_id: str) -> str:
    """
    Get full details of a spec including its derived cards."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        await db.commit()

        if not spec:
            return json.dumps({"error": "Spec not found"})

        payload = {
            "id": spec.id,
            "board_id": spec.board_id,
            "title": spec.title,
            "description": spec.description,
            "context": spec.context,
            "functional_requirements": spec.functional_requirements,
            "technical_requirements": spec.technical_requirements,
            "acceptance_criteria": spec.acceptance_criteria,
            "status": spec.status.value,
            "version": spec.version,
            "assignee_id": spec.assignee_id,
            "created_by": spec.created_by,
            "created_at": spec.created_at.isoformat(),
            "updated_at": spec.updated_at.isoformat(),
            "labels": spec.labels,
            "cards": [
                {
                    "id": c.id,
                    "title": c.title,
                    "status": c.status.value,
                    "priority": c.priority.value,
                    "assignee_id": c.assignee_id,
                }
                for c in spec.cards
            ],
        }
        if _mcp_check_permission(
            ctx.permissions,
            "spec.integration_requirements.read",
            Permissions.BOARD_READ,
        ) is None:
            payload["integration_requirements"] = (
                getattr(spec, "integration_requirements", None) or []
            )
        if _mcp_check_permission(
            ctx.permissions,
            "spec.observability_requirements.read",
            Permissions.BOARD_READ,
        ) is None:
            payload["observability_requirements"] = (
                getattr(spec, "observability_requirements", None) or []
            )
        return json.dumps(payload, default=str)


@mcp.tool()
async def okto_pulse_get_spec_context(
    board_id: str,
    spec_id: str,
    include_knowledge: str = "true",
    include_mockups: str = "true",
    include_qa: str = "true",
    include_architecture: str = "true",
    include_superseded: str = "false",
    profile: str = "summary",
) -> str:
    """
    Get the consolidated context of a spec: requirements, test scenarios, business
    rules, API contracts, IRs, ORs, decisions, mockups, knowledge, Q&A,
    evaluations, cards, and sprints.

    `profile` (R2): `summary` (default) keeps the structured requirement content
    and omits semantically-empty fields; `full`/`legacy` return the complete prior
    payload. **Before evaluating, moving, or deriving cards from a spec, call with
    `profile=full`** (see okto-pulse://reference/projection-profiles)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.mcp.projection_envelope import (
        resolve_profile as _resolve_profile,
        unsupported_projection_error as _unsupported_projection_error,
    )
    if _resolve_profile(profile) is None:
        return json.dumps(_unsupported_projection_error(profile))

    _inc_kb = _flag_enabled(include_knowledge)
    _inc_mockups = _flag_enabled(include_mockups)
    _inc_qa = _flag_enabled(include_qa)
    _inc_architecture = _flag_enabled(include_architecture)
    _inc_superseded = _flag_enabled(include_superseded)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        await db.commit()

        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})

        result: dict = {
            "id": spec.id,
            "board_id": spec.board_id,
            "title": spec.title,
            "description": spec.description,
            "context": spec.context,
            "status": spec.status.value,
            "version": spec.version,
            "assignee_id": spec.assignee_id,
            "created_by": spec.created_by,
            "created_at": spec.created_at.isoformat() if spec.created_at else None,
            "updated_at": spec.updated_at.isoformat() if spec.updated_at else None,
            "labels": spec.labels or [],
            "ideation_id": spec.ideation_id,
            "refinement_id": spec.refinement_id,
            # Requirements
            "functional_requirements": spec.functional_requirements or [],
            "technical_requirements": spec.technical_requirements or [],
            "acceptance_criteria": spec.acceptance_criteria or [],
            # Structured sections — gated by their own granular read flags when available.
            "test_scenarios": spec.test_scenarios or [],
            "business_rules": spec.business_rules or [],
            "api_contracts": spec.api_contracts or [],
            "decisions": _filter_decisions_by_status(
                getattr(spec, "decisions", None) or [],
                include_superseded=_inc_superseded,
            ),
            "decisions_stats": _decisions_stats(
                getattr(spec, "decisions", None) or []
            ),
            # Evaluations
            "evaluations": spec.evaluations or [],
            # Skip flags
            "skip_test_coverage": spec.skip_test_coverage,
            "skip_rules_coverage": getattr(spec, "skip_rules_coverage", False),
            "skip_ir_coverage": getattr(spec, "skip_ir_coverage", False),
            "skip_or_coverage": getattr(spec, "skip_or_coverage", False),
            "skip_decisions_coverage": getattr(spec, "skip_decisions_coverage", True),
            "skip_qualitative_validation": getattr(spec, "skip_qualitative_validation", False),
            "validation_threshold": getattr(spec, "validation_threshold", None),
            # Cards
            "cards": [
                {
                    "id": c.id,
                    "title": c.title,
                    "status": c.status.value,
                    "priority": c.priority.value,
                    "assignee_id": c.assignee_id,
                    "card_type": c.card_type.value if c.card_type else "normal",
                    "sprint_id": c.sprint_id,
                    "test_scenario_ids": c.test_scenario_ids or [],
                }
                for c in spec.cards
            ],
            # Sprints — loaded separately to avoid lazy-load issues
            "sprints": [],
        }
        if _mcp_check_permission(
            ctx.permissions,
            "spec.integration_requirements.read",
            Permissions.BOARD_READ,
        ) is None:
            result["integration_requirements"] = (
                getattr(spec, "integration_requirements", None) or []
            )
        if _mcp_check_permission(
            ctx.permissions,
            "spec.observability_requirements.read",
            Permissions.BOARD_READ,
        ) is None:
            result["observability_requirements"] = (
                getattr(spec, "observability_requirements", None) or []
            )

        if _inc_mockups and spec.screen_mockups:
            result["screen_mockups"] = spec.screen_mockups

        architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            architecture_designs = await _mcp_architecture_for_parent(
                db, "spec", spec_id, permissions=ctx.permissions
            )
            result["architecture_designs"] = architecture_designs

        if _inc_qa:
            result["qa_items"] = [
                {
                    "id": q.id,
                    "question": q.question,
                    "question_type": getattr(q, "question_type", "text"),
                    "choices": getattr(q, "choices", None),
                    "answer": q.answer,
                    "selected": getattr(q, "selected", None),
                    "asked_by": q.asked_by,
                    "answered_by": q.answered_by,
                }
                for q in (spec.qa_items or [])
            ]

        if _inc_kb:
            result["knowledge_bases"] = [
                _serialize_knowledge_base(kb)
                for kb in (spec.knowledge_bases or [])
            ]

        resolved_references = resolve_spec_references(
            spec,
            include_superseded=_inc_superseded,
            include_content=_inc_kb,
            architecture_designs=architecture_designs if _inc_architecture else [],
        )
        if not _inc_kb:
            resolved_references["knowledge_bases"] = []
        if not _inc_mockups:
            resolved_references["screen_mockups"] = []
        if not _inc_architecture:
            resolved_references["architecture_designs"] = []
        result["resolved_references"] = resolved_references
        result["resource_gate_summary"] = await ResourceGateService(db).get_summary(
            board_id,
            "spec",
            spec_id,
        )

        result["coverage_summary"] = _mcp_spec_coverage_summary(spec)

        # Load sprints separately to avoid lazy-load error
        try:
            from okto_pulse.core.services.main import SprintService
            sprint_service = SprintService(db)
            sprints = await sprint_service.list_board_sprints(board_id, spec_id=spec_id)
            await db.commit()
            result["sprints"] = [
                {
                    "id": s.id,
                    "title": s.title,
                    "status": s.status.value,
                    "description": s.description,
                    "objective": getattr(s, "objective", None),
                    "expected_outcome": getattr(s, "expected_outcome", None),
                    "lane_type": s.lane_type.value if getattr(s, "lane_type", None) else "normal",
                    "origin_sprint_id": getattr(s, "origin_sprint_id", None),
                    "origin_bug_id": getattr(s, "origin_bug_id", None),
                    "normal_sprint_created": getattr(s, "normal_sprint_created", True),
                }
                for s in sprints
            ]
        except Exception:
            pass

        from okto_pulse.core.mcp.context_projection import project_spec_context
        return json.dumps(
            project_spec_context(result, profile=profile),
            default=str,
        )


@mcp.tool()
async def okto_pulse_update_spec(
    board_id: str,
    spec_id: str,
    title: str = "",
    description: str = "",
    context: str = "",
    functional_requirements: list[str] | str = "",
    technical_requirements: list[str] | str = "",
    acceptance_criteria: list[str] | str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Update a spec's fields. Content changes (description, context, requirements, criteria) bump the version.
    Only non-empty fields are updated."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import SpecUpdate

    # Build update data with only non-empty fields
    update_kwargs: dict[str, Any] = {}
    if title:
        update_kwargs["title"] = title
    if description:
        update_kwargs["description"] = description.replace("\\n", "\n")
    if context:
        update_kwargs["context"] = context.replace("\\n", "\n")
    if functional_requirements:
        try:
            update_kwargs["functional_requirements"] = coerce_to_list_str(functional_requirements)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if technical_requirements:
        try:
            update_kwargs["technical_requirements"] = _trs_to_objects(coerce_to_list_str(technical_requirements))
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if acceptance_criteria:
        try:
            update_kwargs["acceptance_criteria"] = coerce_to_list_str(acceptance_criteria)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if assignee_id:
        update_kwargs["assignee_id"] = assignee_id
    if labels:
        try:
            update_kwargs["labels"] = coerce_to_list_str(labels)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    if not update_kwargs:
        return json.dumps({"error": "No fields to update"})

    spec_update = SpecUpdate(**update_kwargs)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, spec_update)
        if _err:
            return _err
        await db.commit()

        if not spec:
            return json.dumps({"error": "Spec not found"})

        return json.dumps(
            {
                "success": True,
                "spec": {
                    "id": spec.id,
                    "title": spec.title,
                    "status": spec.status.value,
                    "version": spec.version,
                    "functional_requirements": spec.functional_requirements,
                    "technical_requirements": spec.technical_requirements,
                    "acceptance_criteria": spec.acceptance_criteria,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_move_spec(board_id: str, spec_id: str, status: str) -> str:
    """
    Change a spec's status (e.g. draft → review → approved → validated → in_progress → done)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_MOVE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import SpecStatus
    from okto_pulse.core.models.schemas import SpecMove

    try:
        spec_status = SpecStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in SpecStatus]}"}
        )

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        existing = await service.get_spec(spec_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        old_status = existing.status.value
        try:
            spec = await service.move_spec(
                spec_id, ctx.agent_id, SpecMove(status=spec_status), actor_name=ctx.agent_name
            )
        except ValueError as e:
            return json.dumps({"error": str(e)})
        await db.commit()

        if not spec:
            return json.dumps({"error": "Spec not found"})

        return json.dumps(
            {
                "success": True,
                "spec_id": spec.id,
                "from_status": old_status,
                "to_status": status,
            },
            default=str,
        )


# ============================================================================
# TEST SCENARIO TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_add_test_scenario(
    board_id: str,
    spec_id: str,
    title: str,
    given: str,
    when: str,
    then: str,
    scenario_type: str = "integration",
    linked_criteria: str = "",
    notes: str = "",
) -> str:
    """
    Add a test scenario to a spec. Test scenarios translate acceptance criteria into
    concrete Given/When/Then test plans."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        scenario_id = f"ts_{_uuid.uuid4().hex[:8]}"
        criteria = spec.acceptance_criteria or []

        # Resolve linked_criteria tokens to canonical ac_id strings. Write-path is
        # STRICT (exact index/ac_id/text), FAIL-CLOSED and ATOMIC: any unresolved
        # token aborts before appending and persists nothing partial. The tolerant
        # read resolver stays separate. See spec aafcc73f / KB 26b0e005.
        criteria_list = None
        if linked_criteria:
            resolved_ids, unresolved = resolve_linked_criteria_to_ids(
                parse_multi_value(linked_criteria), criteria
            )
            if unresolved:
                available_ids = [
                    aid for aid in (_structured_ref_id(c) for c in criteria) if aid
                ]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_criteria token(s): {unresolved}. "
                        f"Valid indices: 0..{len(criteria) - 1}. "
                        f"Available ac_ids: {available_ids}. "
                        f"No scenario was appended."
                    )
                })
            criteria_list = resolved_ids

        scenario = {
            "id": scenario_id,
            "title": title,
            "linked_criteria": criteria_list,
            "scenario_type": scenario_type if scenario_type in ("unit", "integration", "e2e", "manual") else "integration",
            "given": given.replace("\\n", "\n"),
            "when": when.replace("\\n", "\n"),
            "then": then.replace("\\n", "\n"),
            "notes": notes.replace("\\n", "\n") if notes else None,
            "status": "draft",
            "linked_task_ids": None,
        }

        scenarios = list(spec.test_scenarios or [])
        scenarios.append(scenario)

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(test_scenarios=scenarios))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, scenarios=scenarios)
        return json.dumps({"success": True, "scenario": scenario, **_saturation_or_coverage(cov)}, default=str)


@mcp.tool()
async def okto_pulse_list_test_scenarios(
    board_id: str,
    spec_id: str,
    status: str = "",
    scenario_type: str = "",
    linked: str = "",
    offset: int = 0,
    limit: int = 50,
) -> str:
    """
    List test scenarios for a spec with coverage information. Supports filtering and pagination."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        await db.commit()

        if not spec:
            return json.dumps({"error": "Spec not found"})

        all_scenarios = spec.test_scenarios or []
        criteria = spec.acceptance_criteria or []

        # Apply filters
        filtered = all_scenarios
        if status:
            filtered = [s for s in filtered if s.get("status") == status]
        if scenario_type:
            filtered = [s for s in filtered if s.get("scenario_type") == scenario_type]
        if linked == "linked":
            filtered = [s for s in filtered if s.get("linked_task_ids")]
        elif linked == "unlinked":
            filtered = [s for s in filtered if not s.get("linked_task_ids")]

        total_filtered = len(filtered)
        paginated = filtered[offset:offset + limit]

        # Build coverage map (always from full set)
        coverage: dict[int, list[str]] = {i: [] for i, _ in enumerate(criteria)}
        for scenario in all_scenarios:
            for index in _resolve_linked_criteria_to_indices(
                scenario.get("linked_criteria"),
                criteria,
            ):
                coverage.setdefault(index, []).append(scenario["id"])

        indexed_criteria = [
            {"index": i, "text": c} for i, c in enumerate(criteria)
        ]

        return json.dumps(
            {
                "spec_id": spec_id,
                "total_scenarios": len(all_scenarios),
                "filtered_count": total_filtered,
                "offset": offset,
                "limit": limit,
                "scenarios": paginated,
                "acceptance_criteria": indexed_criteria,
                "coverage": {
                    "total_criteria": len(criteria),
                    "covered": sum(1 for v in coverage.values() if v),
                    "uncovered_indices": [i for i, _ in enumerate(criteria) if not coverage.get(i)],
                    "uncovered": [c for i, c in enumerate(criteria) if not coverage.get(i)],
                    "details": {str(i): coverage.get(i, []) for i, _ in enumerate(criteria)},
                },
                "summary": {
                    "by_status": {st: sum(1 for s in all_scenarios if s.get("status") == st) for st in ("draft", "ready", "automated", "passed", "failed") if any(s.get("status") == st for s in all_scenarios)},
                    "by_type": {t: sum(1 for s in all_scenarios if s.get("scenario_type") == t) for t in ("unit", "integration", "e2e", "manual") if any(s.get("scenario_type") == t for s in all_scenarios)},
                    "linked": sum(1 for s in all_scenarios if s.get("linked_task_ids")),
                    "unlinked": sum(1 for s in all_scenarios if not s.get("linked_task_ids")),
                },
            },
            default=str,
        )


# ============================================================================
# TEST THEATER PREVENTION GATE (spec 873e98cc — Wave 2 NC-9)
# ============================================================================

# Validação por status alvo. Cada status alvo tem requirements diferentes
# para evidence dict. draft/ready não exigem nada (intent declarado).
#
# Cada rule é uma tupla de keys:
#   - len(group) == 1 → AND-required (single key, must be present)
#   - len(group)  > 1 → OR-required (one-of: pelo menos uma key)
# NC-9 evidence rule + scenario lifecycle guards live in a single leaf module
# (services/test_scenario_lifecycle.py). This file imports from it — never the
# reverse — so the rule is defined exactly once.
from okto_pulse.core.services.test_scenario_lifecycle import (  # noqa: E402
    StatusNotMutableError,
    VALID_SCENARIO_STATUSES,
    validate_test_scenario_evidence,
)



@mcp.tool()
async def okto_pulse_update_test_scenario_status(
    board_id: str,
    spec_id: str,
    scenario_id: str,
    status: str,
    evidence: str = "",
) -> str:
    """Update a test scenario's status, optionally attaching structured evidence that the
test exists/ran. Test-theater prevention gate (NC-9): when skip_test_evidence_global
is False (default), status=automated requires evidence.test_file_path+test_function;
passed/failed require evidence.last_run_at AND (output_snippet OR test_run_id);
draft/ready optional. When skip is True the gate is bypassed but a
test_scenario.evidence_gate_skipped audit log is emitted. Evidence persists inline;
test_scenario.status_changed audit emitted on success. Full details:
okto-pulse://reference/tool-docs/test-scenario."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    valid = VALID_SCENARIO_STATUSES
    if status not in valid:
        return json.dumps({"error": f"Invalid status. Must be one of: {valid}"})

    # Parse evidence param if provided.
    evidence_dict: dict | None = None
    if evidence:
        try:
            parsed = json.loads(evidence)
            if not isinstance(parsed, dict):
                return json.dumps({
                    "error": "invalid_evidence_json",
                    "message": "evidence must be a JSON object",
                })
            evidence_dict = parsed
        except json.JSONDecodeError as exc:
            return json.dumps({
                "error": "invalid_evidence_json",
                "message": f"evidence is not valid JSON: {exc}",
            })

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        try:
            result = await service.set_test_scenario_status(
                spec_id, ctx.agent_id, scenario_id, status, evidence_dict
            )
        except StatusNotMutableError as exc:
            return json.dumps({
                "error": "status_not_mutable",
                "spec_status": exc.spec_status,
                "message": str(exc),
            })
        except ValueError as exc:
            msg = str(exc)
            if msg.startswith("evidence_required"):
                _ok, missing = validate_test_scenario_evidence(status, evidence_dict)
                return json.dumps({
                    "error": "evidence_required",
                    "required": missing,
                    "message": (
                        f"Cannot mark scenario as {status} without structured "
                        f"evidence ({', '.join(missing)}). This prevents the test "
                        "theater anti-pattern. To bypass, enable "
                        "skip_test_evidence_global on the board."
                    ),
                })
            if msg.startswith("scenario_not_found"):
                if "spec not found" in msg:
                    return json.dumps({"error": "Spec not found"})
                return json.dumps({"error": f"Scenario '{scenario_id}' not found"})
            return json.dumps({"error": msg})

    return json.dumps({
        "success": True,
        "scenario_id": result["scenario_id"],
        "old_status": result["old_status"],
        "new_status": result["new_status"],
        "evidence_provided": result["evidence_provided"],
        "evidence_gate_skipped": result["evidence_gate_skipped"],
    })


@mcp.tool()
async def okto_pulse_update_test_scenario(
    board_id: str,
    spec_id: str,
    scenario_id: str,
    title: str = "",
    given: str = "",
    when: str = "",
    then: str = "",
    scenario_type: str = "",
    linked_criteria: str = "",
    notes: str = "",
    clear: str = "",
) -> str:
    """Edit the BODY of a test scenario (title/given/when/then/scenario_type/
    linked_criteria/notes). Does NOT accept status — status stays exclusive to
    okto_pulse_update_test_scenario_status so no second NC-9 bypass is created.

    Empty-string params mean "leave unchanged". To intentionally CLEAR a field,
    list it in `clear` (pipe-separated); only `notes` and `linked_criteria` are
    clearable. `linked_criteria` is a pipe-separated list of AC index/id/text,
    resolved to AC ids (fail-closed on unresolved tokens).

    Editing a SEMANTIC field (given/when/then/scenario_type/linked_criteria) of a
    scenario that holds evidence invalidates it: status resets to `ready` and the
    evidence is dropped. Cosmetic edits (title/notes) preserve status + evidence.
    Respects the spec content-lock."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    clear_fields = parse_multi_value(clear) if clear else None
    lc = parse_multi_value(linked_criteria) if linked_criteria else None

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        try:
            result = await service.update_test_scenario(
                spec_id,
                ctx.agent_id,
                scenario_id,
                title=title or None,
                given=given or None,
                when=when or None,
                then=then or None,
                scenario_type=scenario_type or None,
                linked_criteria=lc,
                notes=notes or None,
                clear=clear_fields,
            )
        except SpecLockedError:
            return json.dumps({
                "error": "spec_locked",
                "message": (
                    "Spec is locked by a passed validation; the scenario body "
                    "cannot be edited. Move the spec back to draft/approved first."
                ),
            })
        except ValueError as exc:
            msg = str(exc)
            code = "invalid_update"
            if msg.startswith("scenario_not_found"):
                code = "scenario_not_found"
            elif msg.startswith("unresolved_criteria"):
                code = "unresolved_criteria"
            return json.dumps({"error": code, "message": msg})

    return json.dumps({
        "success": True,
        "scenario_id": result["scenario_id"],
        "updated_fields": result["updated_fields"],
        "evidence_invalidated": result["evidence_invalidated"],
    })


@mcp.tool()
async def okto_pulse_delete_test_scenario(
    board_id: str,
    spec_id: str,
    scenario_id: str,
) -> str:
    """Delete a test scenario and clean Card.test_scenario_ids in CASCADE.

    Removes the scenario from the spec AND drops its id from every card that
    references it, atomically (all-or-nothing). Does not block on existing links.
    Respects the spec content-lock."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        try:
            result = await service.delete_test_scenario(
                spec_id, ctx.agent_id, scenario_id
            )
        except SpecLockedError:
            return json.dumps({
                "error": "spec_locked",
                "message": (
                    "Spec is locked by a passed validation; scenarios cannot be "
                    "deleted. Move the spec back to draft/approved first."
                ),
            })
        except ValueError as exc:
            return json.dumps({"error": "scenario_not_found", "message": str(exc)})

    return json.dumps({
        "success": True,
        "scenario_id": result["scenario_id"],
        "cards_unlinked": result["cards_unlinked"],
    })


_LINK_TASK_TARGET_TYPES = ("scenario", "rule", "decision", "tr", "contract", "ir", "or", "spec")


@mcp.tool()
async def okto_pulse_link_task(
    board_id: str,
    target_type: str,
    target_id: str,
    card_id: str,
    spec_id: str = "",
) -> str:
    """
    Generic task-linking tool — dispatches on `target_type`. Equivalent to the
    per-type tools (`okto_pulse_link_task_to_rule`, `…_to_decision`, `…_to_tr`,
    `…_to_integration_requirement`, `…_to_observability_requirement`,
    `…_to_scenario`, `…_to_contract`, `okto_pulse_link_card_to_spec`) but
    exposes a single entry point so agents don't have to pre-load eight near-
    identical tool schemas.

    Ideação MCP-token-optimization Story 5."""
    target_type = (target_type or "").strip().lower()
    if target_type not in _LINK_TASK_TARGET_TYPES:
        return json.dumps({
            "error": f"Unknown target_type '{target_type}'. Must be one of: {', '.join(_LINK_TASK_TARGET_TYPES)}"
        })
    # Dispatch to internal helpers (no @mcp.tool() decoration — see commit
    # removing 8 link_task_to_* shims in favor of this unified entry point).
    if target_type == "spec":
        return await _link_card_to_spec_internal(board_id, target_id, card_id)
    if not spec_id:
        return json.dumps({"error": f"spec_id is required when target_type='{target_type}'"})
    if target_type == "scenario":
        return await _link_task_to_scenario_internal(board_id, spec_id, target_id, card_id)
    if target_type == "rule":
        return await _link_task_to_rule_internal(board_id, spec_id, target_id, card_id)
    if target_type == "decision":
        return await _link_task_to_decision_internal(board_id, spec_id, target_id, card_id)
    if target_type == "tr":
        return await _link_task_to_tr_internal(board_id, spec_id, target_id, card_id)
    if target_type == "contract":
        return await _link_task_to_contract_internal(board_id, spec_id, target_id, card_id)
    if target_type == "ir":
        return await _link_task_to_integration_requirement_internal(board_id, spec_id, target_id, card_id)
    if target_type == "or":
        return await _link_task_to_observability_requirement_internal(board_id, spec_id, target_id, card_id)
    return json.dumps({"error": f"Internal dispatch error for target_type '{target_type}'"})


async def _link_task_to_scenario_internal(
    board_id: str, spec_id: str, scenario_id: str, card_id: str
) -> str:
    """Internal helper for link_task target_type='scenario'. Invoked exclusively
    by okto_pulse_link_task — no @mcp.tool() registration to keep the inventory
    focused on the unified dispatcher (Story 5).
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        # Update scenario's linked_task_ids
        scenarios = list(spec.test_scenarios or [])
        found = False
        for s in scenarios:
            if s.get("id") == scenario_id:
                task_ids = list(s.get("linked_task_ids") or [])
                if card_id not in task_ids:
                    task_ids.append(card_id)
                s["linked_task_ids"] = task_ids
                found = True
                break

        if not found:
            return json.dumps({"error": f"Scenario '{scenario_id}' not found"})

        # Verify card exists BEFORE writing — prevents orphan task references.
        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": f"Card '{card_id}' not found — cannot link a non-existent card."})

        from sqlalchemy.orm.attributes import flag_modified
        from okto_pulse.core.models.schemas import CardUpdate

        spec.test_scenarios = scenarios
        flag_modified(spec, "test_scenarios")
        await db.flush()

        # Update card's test_scenario_ids (with max limit check)
        if card:
            existing_ids = list(card.test_scenario_ids or [])
            if scenario_id not in existing_ids:
                from okto_pulse.core.models.db import Board as BoardModel
                board_obj = await db.get(BoardModel, board_id)
                max_per_card = (board_obj.settings or {}).get("max_scenarios_per_card", 3) if board_obj else 3
                if len(existing_ids) >= max_per_card:
                    return json.dumps({
                        "error": f"Card already has {len(existing_ids)} linked scenarios (board limit: {max_per_card}). "
                        f"Create a separate test card for better traceability."
                    })
                existing_ids.append(scenario_id)
            await card_service.update_card(card_id, ctx.agent_id, CardUpdate(test_scenario_ids=existing_ids))

        await db.commit()

        cov = _spec_coverage(spec, scenarios=scenarios)
        return json.dumps({"success": True, "scenario_id": scenario_id, "card_id": card_id, **_saturation_or_coverage(cov)})


async def _link_task_to_rule_internal(
    board_id: str, spec_id: str, rule_id: str, card_id: str
) -> str:
    """Internal helper for link_task target_type='rule'. Invoked exclusively
    by okto_pulse_link_task.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        # Verify card exists
        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        # Update rule's linked_task_ids
        rules = list(spec.business_rules or [])
        found = False
        for r in rules:
            if r.get("id") == rule_id:
                task_ids = list(r.get("linked_task_ids") or [])
                if card_id not in task_ids:
                    task_ids.append(card_id)
                r["linked_task_ids"] = task_ids
                found = True
                break

        if not found:
            return json.dumps({"error": f"Business rule '{rule_id}' not found in spec"})

        from okto_pulse.core.models.schemas import SpecUpdate
        _, err = await _safe_spec_update(spec_service, spec_id, ctx.agent_id, SpecUpdate(business_rules=rules))
        if err:
            return err
        await db.commit()

        cov = _spec_coverage(spec, rules=rules)
        return json.dumps({"success": True, "rule_id": rule_id, "card_id": card_id, **_saturation_or_coverage(cov)})


async def _link_task_to_contract_internal(
    board_id: str, spec_id: str, contract_id: str, card_id: str
) -> str:
    """Internal helper for link_task target_type='contract'."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        contracts = list(spec.api_contracts or [])
        found = False
        for ct in contracts:
            if ct.get("id") == contract_id:
                task_ids = list(ct.get("linked_task_ids") or [])
                if card_id not in task_ids:
                    task_ids.append(card_id)
                ct["linked_task_ids"] = task_ids
                found = True
                break

        if not found:
            return json.dumps({"error": f"API contract '{contract_id}' not found in spec"})

        from okto_pulse.core.models.schemas import SpecUpdate
        _, err = await _safe_spec_update(spec_service, spec_id, ctx.agent_id, SpecUpdate(api_contracts=contracts))
        if err:
            return err
        await db.commit()

        cov = _spec_coverage(spec, contracts=contracts)
        return json.dumps({"success": True, "contract_id": contract_id, "card_id": card_id, **_saturation_or_coverage(cov)})


async def _link_task_to_tr_internal(
    board_id: str, spec_id: str, tr_id: str, card_id: str
) -> str:
    """Internal helper for link_task target_type='tr'."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        trs = list(spec.technical_requirements or [])
        found = False
        for tr in trs:
            if isinstance(tr, dict) and tr.get("id") == tr_id:
                task_ids = list(tr.get("linked_task_ids") or [])
                if card_id not in task_ids:
                    task_ids.append(card_id)
                tr["linked_task_ids"] = task_ids
                found = True
                break

        if not found:
            return json.dumps({
                "error": f"Technical requirement '{tr_id}' not found in spec. "
                f"TRs may be in legacy string format — update the spec via "
                f"okto_pulse_update_spec to convert them to objects with IDs."
            })

        from okto_pulse.core.models.schemas import SpecUpdate
        _, err = await _safe_spec_update(spec_service, spec_id, ctx.agent_id, SpecUpdate(technical_requirements=trs))
        if err:
            return err
        await db.commit()

        cov = _spec_coverage(spec, trs=trs)
        return json.dumps({"success": True, "tr_id": tr_id, "card_id": card_id, **_saturation_or_coverage(cov)})


# ==================== ARCHIVE & RESTORE ====================


@mcp.tool()
async def okto_pulse_archive_tree(
    board_id: str, entity_type: str, entity_id: str
) -> str:
    """
    Archive an entity and all its descendants in cascade.
    Saves pre_archive_status before setting archived=true."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.services.main import ArchiveService

    async with get_db_for_mcp() as db:
        service = ArchiveService(db)
        try:
            counts = await service.archive_tree(entity_type, entity_id)
        except ValueError as e:
            return json.dumps({"error": str(e)})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id,
            card_id=None,
            action="tree_archived",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={"entity_type": entity_type, "entity_id": entity_id, "counts": counts},
        )
        await db.commit()

        return json.dumps({"success": True, "archived_count": counts}, default=str)


@mcp.tool()
async def okto_pulse_restore_tree(
    board_id: str, entity_type: str, entity_id: str
) -> str:
    """
    Restore an archived entity and all its descendants.
    Returns each entity to its pre_archive_status."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.services.main import ArchiveService

    async with get_db_for_mcp() as db:
        service = ArchiveService(db)
        try:
            counts = await service.restore_tree(entity_type, entity_id)
        except ValueError as e:
            return json.dumps({"error": str(e)})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id,
            card_id=None,
            action="tree_restored",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={"entity_type": entity_type, "entity_id": entity_id, "counts": counts},
        )
        await db.commit()

        return json.dumps({"success": True, "restored_count": counts}, default=str)


# ==================== SPEC-TO-CARD COPY TOOLS ====================


@mcp.tool()
async def okto_pulse_list_architecture_designs(
    board_id: str,
    parent_type: str,
    parent_id: str,
    include_payloads: str = "false",
) -> str:
    """
    List Architecture Designs for an ideation, refinement, spec, or card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    action = "render" if _flag_enabled(include_payloads) else "read"
    perm_err = _mcp_check_architecture_permission(ctx.permissions, parent_type, action)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        repo = ArchitectureDesignRepository(db)
        try:
            parent_model, _ = repo._parent_config(parent_type)
        except ValueError as exc:
            return json.dumps({"error": str(exc)})
        parent = await db.get(parent_model, parent_id)
        if not parent or getattr(parent, "board_id") != board_id:
            return json.dumps({"error": f"{parent_type} not found"})
        designs = await repo.list(parent_type, parent_id, include_payloads=_flag_enabled(include_payloads))
        payload = [
            _dump_model(repo.to_response(design) if _flag_enabled(include_payloads) else repo.to_summary(design))
            for design in designs
        ]
        await db.commit()
        return json.dumps({"success": True, "architecture_designs": payload}, default=str)


@mcp.tool()
async def okto_pulse_get_architecture_design(
    board_id: str,
    design_id: str,
    include_payloads: str = "false",
) -> str:
    """
    Get one Architecture Design by ID."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.get(design_id, include_payloads=_flag_enabled(include_payloads))
        if not design or design.board_id != board_id:
            return json.dumps({"error": "Architecture design not found"})
        action = "render" if _flag_enabled(include_payloads) else "read"
        perm_err = _mcp_check_architecture_permission(ctx.permissions, design.parent_type, action)
        if perm_err:
            return _perm_error(perm_err)
        payload = _dump_model(repo.to_response(design))
        await db.commit()
        return json.dumps({"success": True, "architecture_design": payload}, default=str)


@mcp.tool()
async def okto_pulse_get_architecture_design_schema(board_id: str) -> str:
    """Return the machine-readable Architecture Design payload schema: allowed enums,
entity/interface contracts, Excalidraw adapter rules, good/bad/minimal examples,
and the semantic_node_registry mapping entity_type ->
{displayType, architectureKind, iconName} plus the icon allowlist. MANDATORY flow:
read schema -> build payload (prefer entity_type + linkedEntityId; metadata
auto-filled) -> okto_pulse_validate_architecture_design_payload -> add/update. Do
NOT invent iconName/displayType/architectureKind outside the registry. Full guide:
okto-pulse://reference/tool-docs/architecture."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = _mcp_check_permission(ctx.permissions, Permissions.BOARD_READ, None)
    if perm_err:
        return _perm_error(perm_err)

    return json.dumps({"success": True, "schema": architecture_design_payload_schema()}, default=str)


@mcp.tool()
async def okto_pulse_validate_architecture_design_payload(
    board_id: str,
    parent_type: str = "",
    parent_id: str = "",
    design_id: str = "",
    title: str = "",
    global_description: str = "",
    entities: list[dict] | str = "",
    interfaces: list[dict] | str = "",
    diagrams: list[dict] | str = "",
    architecture_warning_acknowledgement: dict | str = "",
    commit: bool = False,
    include_design: bool = False,
) -> str:
    """Dry-run critique for an Architecture Design payload without persisting. Use before
okto_pulse_add_architecture_design / update — for creates pass parent_type+parent_id;
for updates pass design_id + only changed fields (omitted merged from existing).
Response: valid, issues (blocking, with JSON paths), warnings, suggested_fixes, and
summary counts. Catches duplicate name/entity_type, invalid participants/direction,
non-excalidraw diagram formats, and bad linkedEntityId/linkedInterfaceIds/
connectionType. Full catalog: okto-pulse://reference/tool-docs/architecture."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    parsed_fields: dict[str, Any] = {}
    for field_name, raw in (("entities", entities), ("interfaces", interfaces), ("diagrams", diagrams)):
        parsed, err = _parse_json_arg(raw, None)
        if err:
            return json.dumps({"error": f"Invalid {field_name}: {err}"})
        if parsed is not None:
            parsed_fields[field_name] = parsed
    acknowledgement, err = _parse_json_arg(architecture_warning_acknowledgement, None)
    if err:
        return json.dumps({"error": f"Invalid architecture_warning_acknowledgement: {err}"})

    async with get_db_for_mcp() as db:
        repo = ArchitectureDesignRepository(db)
        mode = "update" if design_id else "create"

        if design_id:
            design, err = await _mcp_require_architecture_mutable(db, design_id)
            if err:
                return json.dumps({"error": err})
            if design.board_id != board_id:
                return json.dumps({"error": "Architecture design not found"})
            perm_err = _mcp_check_architecture_permission(ctx.permissions, design.parent_type, "edit")
            if perm_err:
                return _perm_error(perm_err)
            loaded = await repo.get(design_id, include_payloads=True)
            if not loaded:
                return json.dumps({"error": "Architecture design not found"})
            candidate = {
                "title": title or loaded.title,
                "global_description": global_description or loaded.global_description,
                "entities": loaded.entities or [],
                "interfaces": loaded.interfaces or [],
                "diagrams": loaded.diagrams or [],
            }
            candidate.update(parsed_fields)
        else:
            if not parent_type or not parent_id:
                return json.dumps({"error": "parent_type and parent_id are required when design_id is omitted"})
            perm_err = _mcp_check_architecture_permission(ctx.permissions, parent_type, "create")
            if perm_err:
                return _perm_error(perm_err)
            try:
                parent_model, _ = repo._parent_config(parent_type)
            except ValueError as exc:
                return json.dumps({"error": str(exc)})
            parent = await db.get(parent_model, parent_id)
            if not parent or getattr(parent, "board_id") != board_id:
                return json.dumps({"error": f"{parent_type} not found"})
            if parent_type == "spec":
                current_id = getattr(parent, "current_validation_id", None)
                current = next((item for item in (getattr(parent, "validations", None) or []) if item.get("id") == current_id), None)
                if current_id and current and current.get("outcome") == "success":
                    return json.dumps({"error": "Spec is locked because validation passed. Move it back to draft or approved to edit architecture."})
            candidate = {
                "title": title,
                "global_description": global_description,
                "entities": parsed_fields.get("entities", []),
                "interfaces": parsed_fields.get("interfaces", []),
                "diagrams": parsed_fields.get("diagrams", []),
            }

        critique = repo.critique_payload(candidate)
        if not commit or not critique.get("valid"):
            await db.commit()
            return json.dumps({"success": True, "mode": mode, **critique}, default=str)

        try:
            if mode == "create":
                payload = ArchitectureDesignCreate(
                    title=candidate["title"],
                    global_description=candidate["global_description"],
                    entities=candidate.get("entities") or [],
                    interfaces=candidate.get("interfaces") or [],
                    diagrams=candidate.get("diagrams") or [],
                    architecture_warning_acknowledgement=acknowledgement,
                )
                design = await repo.create(parent_type, parent_id, payload, ctx.agent_id)
            else:
                patch_payload = ArchitectureDesignUpdate(**{
                    k: candidate[k]
                    for k in ("title", "global_description", "entities", "interfaces", "diagrams")
                    if candidate.get(k) is not None
                })
                patch_payload.architecture_warning_acknowledgement = acknowledgement
                design = await repo.update(design_id, patch_payload, ctx.agent_id)
            await db.commit()
        except ValueError as exc:
            return _mcp_architecture_error(exc)

        warnings = list(critique.get("warnings") or [])
        structured_warnings = list(critique.get("structured_warnings") or [])
        suppressed_warnings = list(critique.get("suppressed_warnings") or [])
        envelope: dict[str, Any] = {
            "success": True,
            "mode": mode,
            "committed": True,
            "id": design.id,
            "version": design.version,
            "warnings_count": len(warnings),
            "structured_warnings_count": len(structured_warnings),
            "suppressed_warnings_count": len(suppressed_warnings),
            "normalized": bool(warnings),
        }
        if include_design:
            envelope["architecture_design"] = _dump_model(repo.to_response(design))
        return json.dumps(envelope, default=str)


@mcp.tool()
async def okto_pulse_add_architecture_design(
    board_id: str,
    parent_type: str,
    parent_id: str,
    title: str,
    global_description: str,
    entities: list[dict] | str = "",
    interfaces: list[dict] | str = "",
    diagrams: list[dict] | str = "",
    architecture_warning_acknowledgement: dict | str = "",
) -> str:
    """Create an Architecture Design on an ideation, refinement, spec, or card. Use whenever
the artifact benefits from explicit architecture (services, modules, databases,
queues, events, integrations, runtime boundaries, API contracts, data flows,
ownership). For non-trivial payloads call okto_pulse_get_architecture_design_schema
then okto_pulse_validate_architecture_design_payload, and persist only after
valid=true + reviewed warnings. The server critiques the full payload before
accepting; fix cited fields and retry rather than hiding architecture in prose. Full
guide: okto-pulse://reference/tool-docs/architecture."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = _mcp_check_architecture_permission(ctx.permissions, parent_type, "create")
    if perm_err:
        return _perm_error(perm_err)

    ents, err = _parse_json_arg(entities, [])
    if err:
        return json.dumps({"error": f"Invalid entities: {err}"})
    ifaces, err = _parse_json_arg(interfaces, [])
    if err:
        return json.dumps({"error": f"Invalid interfaces: {err}"})
    diags, err = _parse_json_arg(diagrams, [])
    if err:
        return json.dumps({"error": f"Invalid diagrams: {err}"})
    acknowledgement, err = _parse_json_arg(architecture_warning_acknowledgement, None)
    if err:
        return json.dumps({"error": f"Invalid architecture_warning_acknowledgement: {err}"})

    async with get_db_for_mcp() as db:
        repo = ArchitectureDesignRepository(db)
        try:
            parent_model, _ = repo._parent_config(parent_type)
        except ValueError as exc:
            return json.dumps({"error": str(exc)})
        parent = await db.get(parent_model, parent_id)
        if not parent or getattr(parent, "board_id") != board_id:
            return json.dumps({"error": f"{parent_type} not found"})
        if parent_type == "spec":
            current_id = getattr(parent, "current_validation_id", None)
            current = next((item for item in (getattr(parent, "validations", None) or []) if item.get("id") == current_id), None)
            if current_id and current and current.get("outcome") == "success":
                return json.dumps({"error": "Spec is locked because validation passed. Move it back to draft or approved to edit architecture."})
        try:
            design = await repo.create(
                parent_type,
                parent_id,
                ArchitectureDesignCreate(
                    title=title,
                    global_description=global_description,
                    entities=ents,
                    interfaces=ifaces,
                    diagrams=diags,
                    architecture_warning_acknowledgement=acknowledgement,
                ),
                ctx.agent_id,
            )
            payload = _dump_model(repo.to_response(design))
            await db.commit()
            return json.dumps({"success": True, "architecture_design": payload}, default=str)
        except Exception as exc:
            return _mcp_architecture_error(exc)


@mcp.tool()
async def okto_pulse_update_architecture_design(
    board_id: str,
    design_id: str,
    title: str = "",
    global_description: str = "",
    entities: list[dict] | str = "",
    interfaces: list[dict] | str = "",
    diagrams: list[dict] | str = "",
    change_summary: str = "",
    architecture_warning_acknowledgement: dict | str = "",
) -> str:
    """Update an Architecture Design; omitted fields are left unchanged. For large or
generated updates, call okto_pulse_get_architecture_design_schema once per session
then okto_pulse_validate_architecture_design_payload first. The update is critiqued
against the full resulting design before saving — prefer replacing
entities/interfaces/diagrams with the complete intended arrays so links stay
deterministic. Full args + contextual validation examples:
okto-pulse://reference/tool-docs/architecture."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    patch: dict[str, Any] = {}
    if title:
        patch["title"] = title
    if global_description:
        patch["global_description"] = global_description
    for field_name, raw in (("entities", entities), ("interfaces", interfaces), ("diagrams", diagrams)):
        parsed, err = _parse_json_arg(raw, None)
        if err:
            return json.dumps({"error": f"Invalid {field_name}: {err}"})
        if parsed is not None:
            patch[field_name] = parsed
    if change_summary:
        patch["change_summary"] = change_summary
    acknowledgement, err = _parse_json_arg(architecture_warning_acknowledgement, None)
    if err:
        return json.dumps({"error": f"Invalid architecture_warning_acknowledgement: {err}"})
    if acknowledgement is not None:
        patch["architecture_warning_acknowledgement"] = acknowledgement
    if not patch:
        return json.dumps({"error": "No fields provided for update"})

    async with get_db_for_mcp() as db:
        design, err = await _mcp_require_architecture_mutable(db, design_id)
        if err:
            return json.dumps({"error": err})
        if design.board_id != board_id:
            return json.dumps({"error": "Architecture design not found"})
        perm_err = _mcp_check_architecture_permission(ctx.permissions, design.parent_type, "edit")
        if perm_err:
            return _perm_error(perm_err)
        repo = ArchitectureDesignRepository(db)
        try:
            updated = await repo.update(design_id, ArchitectureDesignUpdate(**patch), ctx.agent_id)
            payload = _dump_model(repo.to_response(updated))
            await db.commit()
            return json.dumps({"success": True, "architecture_design": payload}, default=str)
        except Exception as exc:
            return _mcp_architecture_error(exc)


@mcp.tool()
async def okto_pulse_delete_architecture_design(board_id: str, design_id: str) -> str:
    """
    Delete an Architecture Design."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        design, err = await _mcp_require_architecture_mutable(db, design_id)
        if err:
            return json.dumps({"error": err})
        if design.board_id != board_id:
            return json.dumps({"error": "Architecture design not found"})
        perm_err = _mcp_check_architecture_permission(ctx.permissions, design.parent_type, "delete")
        if perm_err:
            return _perm_error(perm_err)
        repo = ArchitectureDesignRepository(db)
        deleted = await repo.delete(design_id, ctx.agent_id)
        await db.commit()
        return json.dumps({"success": bool(deleted)})


@mcp.tool()
async def okto_pulse_import_excalidraw_architecture_diagram(
    board_id: str,
    design_id: str,
    title: str,
    payload_json: dict | str,
    diagram_type: str = "other",
    replace_diagram_id: str = "",
    description: str = "",
    order_index: int = 0,
    change_summary: str = "",
) -> str:
    """
    Import an Excalidraw JSON scene into an Architecture Design."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    payload, err = _parse_json_arg(payload_json, None)
    if err or payload is None:
        return json.dumps({"error": f"Invalid payload_json: {err or 'payload is required'}"})

    async with get_db_for_mcp() as db:
        design, err = await _mcp_require_architecture_mutable(db, design_id)
        if err:
            return json.dumps({"error": err})
        if design.board_id != board_id:
            return json.dumps({"error": "Architecture design not found"})
        perm_err = _mcp_check_architecture_permission(ctx.permissions, design.parent_type, "import")
        if perm_err:
            return _perm_error(perm_err)
        diagrams = [dict(item) for item in design.diagrams or []]
        imported = {
            "title": title,
            "diagram_type": diagram_type,
            "format": "excalidraw_json",
            "description": description or None,
            "order_index": order_index,
            "adapter_payload": payload,
        }
        if replace_diagram_id:
            index = next((idx for idx, item in enumerate(diagrams) if item.get("id") == replace_diagram_id), -1)
            if index < 0:
                return json.dumps({"error": "Diagram not found"})
            diagrams[index] = {**diagrams[index], **imported, "id": replace_diagram_id}
        else:
            diagrams.append(imported)
        repo = ArchitectureDesignRepository(db)
        try:
            updated = await repo.update(
                design_id,
                ArchitectureDesignUpdate(
                    diagrams=diagrams,
                    change_summary=change_summary or "Imported Excalidraw diagram",
                ),
                ctx.agent_id,
            )
            payload_out = _dump_model(repo.to_response(updated))
            await db.commit()
            return json.dumps({"success": True, "architecture_design": payload_out}, default=str)
        except Exception as exc:
            return json.dumps({"error": str(exc)})


@mcp.tool()
async def okto_pulse_dump_architecture_diagram(
    board_id: str,
    design_id: str,
    diagram_id: str,
) -> str:
    """
    Load and dump a diagram payload through its ArchitectureDiagramAdapter."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        repo = ArchitectureDesignRepository(db)
        design = await repo.get(design_id)
        if not design or design.board_id != board_id:
            return json.dumps({"error": "Architecture design not found"})
        perm_err = _mcp_check_architecture_permission(ctx.permissions, design.parent_type, "render")
        if perm_err:
            return _perm_error(perm_err)
        diagram = next((item for item in design.diagrams or [] if item.get("id") == diagram_id), None)
        if not diagram or not diagram.get("adapter_payload_ref"):
            return json.dumps({"error": "Diagram payload not found"})
        store = ArchitectureDiagramStore(db)
        try:
            payload = await store.load_payload(diagram["adapter_payload_ref"])
            adapter = ArchitectureDiagramAdapterRegistry().get(diagram.get("format") or "raw")
            await db.commit()
            return json.dumps(
                {
                    "success": True,
                    "design_id": design_id,
                    "diagram_id": diagram_id,
                    "format": diagram.get("format"),
                    "payload": payload,
                    "dump": adapter.dump(payload),
                },
                default=str,
            )
        except Exception as exc:
            return json.dumps({"error": str(exc)})


@mcp.tool()
async def okto_pulse_copy_architecture_to_card(
    board_id: str,
    spec_id: str,
    card_id: str,
    design_ids: list[str] | str = "",
    architecture_warning_acknowledgement: dict | str = "",
    profile: str = "summary",
) -> str:
    """
    Copy Architecture Designs from a spec to a card/task as deep-copy snapshots.

    `profile` (R2): `summary` (default) returns the copy metadata only —
    `copied`, `design_ids`, `total_on_card` and the R5 `projection` envelope, NOT
    the full architecture bodies. `full`/`legacy` return the prior payload with the
    complete copied `architecture_designs`. The bodies are persisted on the card
    regardless of profile; read them with `okto_pulse_get_task_context(profile=full)`
    or re-call here with `profile=full`."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = _mcp_check_architecture_copy_permission(ctx.permissions)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.mcp.copy_projection import (
        project_copy_architecture_response,
        resolve_copy_profile,
        unsupported_copy_profile_error,
    )
    # Validate the profile BEFORE the copy — a bad profile must not mutate state.
    if resolve_copy_profile(profile) is None:
        return json.dumps(unsupported_copy_profile_error(profile))

    try:
        ids = coerce_to_list_str(design_ids) if design_ids else None
    except ValueError as exc:
        return json.dumps({"error": f"Invalid design_ids: {exc}"})
    acknowledgement, err = _parse_json_arg(architecture_warning_acknowledgement, None)
    if err:
        return json.dumps({"error": f"Invalid architecture_warning_acknowledgement: {err}"})

    async with get_db_for_mcp() as db:
        service = ArchitecturePropagationService(db)
        try:
            designs = await service.copy_spec_to_card(
                spec_id,
                card_id,
                ctx.agent_id,
                design_ids=ids,
                architecture_warning_acknowledgement=acknowledgement,
            )
            repo = ArchitectureDesignRepository(db)
            payload = [_dump_model(repo.to_response(design)) for design in designs]
            # Total Architecture Designs on the card after the copy (no payloads —
            # we only need the count for the summary metadata).
            total_on_card = len(await repo.list("card", card_id, include_payloads=False))
            await db.commit()
            return json.dumps(
                project_copy_architecture_response(
                    payload, total_on_card=total_on_card, profile=profile
                ),
                default=str,
            )
        except Exception as exc:
            return _mcp_architecture_error(exc)


@mcp.tool()
async def okto_pulse_copy_mockups_to_card(
    board_id: str, spec_id: str, card_id: str, screen_ids: list[str] | str = ""
) -> str:
    """
    Copy screen mockups from a spec to a card. Use this when creating implementation
    cards to carry the relevant mockups into the card for the implementer's context."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        source_mockups = list(spec.screen_mockups or [])
        if screen_ids:
            try:
                ids = set(coerce_to_list_str(screen_ids))
            except ValueError as e:
                return json.dumps({"error": f"Invalid screen_ids: {e}"})
            source_mockups = [m for m in source_mockups if m.get("id") in ids]

        if not source_mockups:
            return json.dumps({"error": "No mockups to copy"})

        existing = list(card.screen_mockups or [])
        existing_ids = {m.get("id") for m in existing}
        copied = 0
        for m in source_mockups:
            if m.get("id") not in existing_ids:
                existing.append(m)
                copied += 1

        from okto_pulse.core.models.schemas import CardUpdate
        await card_service.update_card(card_id, ctx.agent_id, CardUpdate(screen_mockups=existing))
        await db.commit()

    return json.dumps({"success": True, "copied": copied, "total_on_card": len(existing)})


@mcp.tool()
async def okto_pulse_copy_knowledge_to_card(
    board_id: str, spec_id: str, card_id: str, knowledge_ids: list[str] | str = ""
) -> str:
    """
    Copy knowledge base entries from a spec to a card as inline card KEs.
    Each copied entry is stored in Card.knowledge_bases with stable provenance."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        kb_service = SpecKnowledgeService(db)
        kbs = await kb_service.list_knowledge(spec_id)
        if knowledge_ids:
            try:
                ids = set(coerce_to_list_str(knowledge_ids))
            except ValueError as e:
                return json.dumps({"error": f"Invalid knowledge_ids: {e}"})
            kbs = [kb for kb in kbs if kb.id in ids]

        if not kbs:
            return json.dumps({"error": "No knowledge bases to copy"})

        from okto_pulse.core.models.schemas import CardUpdate

        existing = list(card.knowledge_bases or [])
        existing_sources = {str(kb.get("source") or "") for kb in existing if isinstance(kb, dict)}
        existing_ids = {str(kb.get("id") or "") for kb in existing if isinstance(kb, dict)}
        copied = 0
        copied_ids: list[str] = []
        for kb in kbs:
            source = f"copied_from_spec:{spec_id}:{kb.id}"
            card_kb_id = f"cardkb_{kb.id}"
            if source in existing_sources or card_kb_id in existing_ids:
                continue
            existing.append(
                {
                    "id": card_kb_id,
                    "title": kb.title,
                    "description": getattr(kb, "description", None),
                    "content": kb.content,
                    "mime_type": getattr(kb, "mime_type", None) or "text/markdown",
                    "source": source,
                    "author_id": ctx.agent_id,
                }
            )
            existing_sources.add(source)
            existing_ids.add(card_kb_id)
            copied_ids.append(card_kb_id)
            copied += 1

        await card_service.update_card(card_id, ctx.agent_id, CardUpdate(knowledge_bases=existing))
        await db.commit()

    return json.dumps({"success": True, "copied": copied, "knowledge_ids": copied_ids, "total_on_card": len(existing)})


# ============================================================================
# Card.knowledge_bases — inline JSONB lifecycle (symmetric to spec_knowledge)
# ============================================================================


def _new_card_kb_id() -> str:
    import hashlib
    import time

    return "kb_" + hashlib.md5(f"{time.time_ns()}".encode()).hexdigest()[:10]


@mcp.tool()
async def okto_pulse_add_card_knowledge(
    board_id: str,
    card_id: str,
    title: str,
    content: str,
    description: str = "",
    mime_type: str = "text/markdown",
    source: str = "manual",
) -> str:
    """
    Attach a knowledge base entry directly to a card. Stored inline on
    `Card.knowledge_bases` (JSONB). Symmetric to spec_knowledge but scoped
    to a single task."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    if not (title or "").strip() or not (content or "").strip():
        return json.dumps({"error": "title and content are required"})

    from okto_pulse.core.models.schemas import CardUpdate

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        kbs = list(card.knowledge_bases or [])
        kb = {
            "id": _new_card_kb_id(),
            "title": title.strip(),
            "description": (description or "").strip() or None,
            "content": content.replace("\\n", "\n"),
            "mime_type": mime_type or "text/markdown",
            "source": source or "manual",
            "author_id": ctx.agent_id,
        }
        kbs.append(kb)

        await service.update_card(card_id, ctx.agent_id, CardUpdate(knowledge_bases=kbs))
        await db.commit()

    return json.dumps({"success": True, "knowledge": kb}, default=str)


@mcp.tool()
async def okto_pulse_get_card_knowledge(board_id: str, card_id: str, knowledge_id: str) -> str:
    """Get a single KE by id from a card's inline knowledge_bases array."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

    for kb in (card.knowledge_bases or []):
        if kb.get("id") == knowledge_id:
            return json.dumps({"success": True, "knowledge": kb}, default=str)
    return json.dumps({"error": "Knowledge entry not found"})


@mcp.tool()
async def okto_pulse_update_card_knowledge(
    board_id: str,
    card_id: str,
    knowledge_id: str,
    title: str = "",
    description: str = "",
    content: str = "",
    mime_type: str = "",
) -> str:
    """Update fields of an existing KE on a card. Only provided fields change."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import CardUpdate

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        kbs = list(card.knowledge_bases or [])
        idx = next((i for i, kb in enumerate(kbs) if kb.get("id") == knowledge_id), -1)
        if idx == -1:
            return json.dumps({"error": "Knowledge entry not found"})

        kb = dict(kbs[idx])
        if title:
            kb["title"] = title.strip()
        if description:
            kb["description"] = description.strip()
        if content:
            kb["content"] = content.replace("\\n", "\n")
        if mime_type:
            kb["mime_type"] = mime_type
        kbs[idx] = kb

        await service.update_card(card_id, ctx.agent_id, CardUpdate(knowledge_bases=kbs))
        await db.commit()

    return json.dumps({"success": True, "knowledge": kb}, default=str)


@mcp.tool()
async def okto_pulse_delete_card_knowledge(board_id: str, card_id: str, knowledge_id: str) -> str:
    """Delete a KE from a card's inline knowledge_bases array."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import CardUpdate

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

        kbs = list(card.knowledge_bases or [])
        before = len(kbs)
        kbs = [kb for kb in kbs if kb.get("id") != knowledge_id]
        if len(kbs) == before:
            return json.dumps({"error": "Knowledge entry not found"})

        await service.update_card(card_id, ctx.agent_id, CardUpdate(knowledge_bases=kbs))
        await db.commit()

    return json.dumps({"success": True, "deleted": knowledge_id, "remaining": len(kbs)})


@mcp.tool()
async def okto_pulse_copy_qa_to_card(
    board_id: str, spec_id: str, card_id: str
) -> str:
    """
    Copy answered Q&A items from a spec to a card as a consolidated comment.
    Only copies Q&As that have been answered — unanswered questions are skipped."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        # Get answered Q&A
        qa_items = [qa for qa in (spec.qa_items or []) if qa.answer]
        if not qa_items:
            return json.dumps({"error": "No answered Q&A to copy"})

        lines = ["## Spec Q&A Context\n"]
        for qa in qa_items:
            lines.append(f"**Q:** {qa.question}\n**A:** {qa.answer}\n")

        from okto_pulse.core.models.db import Comment
        comment = Comment(
            card_id=card_id,
            author_id=ctx.agent_id,
            content="\n".join(lines),
        )
        db.add(comment)
        await db.commit()

    return json.dumps({"success": True, "copied": len(qa_items)})


# ==================== ANALYTICS TOOLS ====================


@mcp.tool()
async def okto_pulse_get_analytics(
    board_id: str,
    metric_type: str = "overview",
    from_date: str = "",
    to_date: str = "",
) -> str:
    """
    Get analytics data for a board. Supports multiple metric types."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import (
        Board, Card, CardStatus, Ideation, Refinement, Spec,
    )
    from sqlalchemy import select

    def _parse_dt(value: str) -> datetime | None:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None

    dt_from = _parse_dt(from_date)
    dt_to = _parse_dt(to_date)

    def _is_test(card) -> bool:
        ids = card.test_scenario_ids
        return bool(ids and isinstance(ids, list) and len(ids) > 0)

    def _last_conclusion(card) -> dict | None:
        conclusions = card.conclusions
        if not conclusions or not isinstance(conclusions, list):
            return None
        last = conclusions[-1]
        return last if isinstance(last, dict) else None

    async with get_db_for_mcp() as db:
        # Verify board exists
        board = (await db.execute(select(Board).where(Board.id == board_id))).scalars().first()
        if not board:
            return json.dumps({"error": "Board not found"})

        if metric_type == "overview":
            from okto_pulse.core.models.db import Sprint

            # Ideations
            ideation_q = select(Ideation).where(Ideation.board_id == board_id)
            if dt_from:
                ideation_q = ideation_q.where(Ideation.created_at >= dt_from)
            if dt_to:
                ideation_q = ideation_q.where(Ideation.created_at <= dt_to)
            ideations = list((await db.execute(ideation_q)).scalars().all())

            # Refinements
            refinement_q = select(Refinement).where(Refinement.board_id == board_id)
            if dt_from:
                refinement_q = refinement_q.where(Refinement.created_at >= dt_from)
            if dt_to:
                refinement_q = refinement_q.where(Refinement.created_at <= dt_to)
            refinements = list((await db.execute(refinement_q)).scalars().all())

            # Specs
            spec_q = select(Spec).where(Spec.board_id == board_id)
            if dt_from:
                spec_q = spec_q.where(Spec.created_at >= dt_from)
            if dt_to:
                spec_q = spec_q.where(Spec.created_at <= dt_to)
            specs = list((await db.execute(spec_q)).scalars().all())

            # Sprints
            sprint_q = select(Sprint).where(Sprint.board_id == board_id)
            if dt_from:
                sprint_q = sprint_q.where(Sprint.created_at >= dt_from)
            if dt_to:
                sprint_q = sprint_q.where(Sprint.created_at <= dt_to)
            sprints = list((await db.execute(sprint_q)).scalars().all())

            # Cards
            card_q = select(Card).where(Card.board_id == board_id)
            if dt_from:
                card_q = card_q.where(Card.created_at >= dt_from)
            if dt_to:
                card_q = card_q.where(Card.created_at <= dt_to)
            cards = list((await db.execute(card_q)).scalars().all())

            impl_cards = [c for c in cards if not _is_test(c)]
            test_cards = [c for c in cards if _is_test(c)]
            done_cards = [c for c in cards if c.status == CardStatus.DONE]
            bug_cards = [c for c in cards if getattr(c, "card_type", "normal") == "bug"]

            # --- Self-reported quality (from card.conclusions) ---
            comp_vals = []
            drift_vals = []
            for c in cards:
                concl = _last_conclusion(c)
                if concl and "completeness" in concl:
                    comp_vals.append(concl["completeness"])
                if concl and "drift" in concl:
                    drift_vals.append(concl["drift"])

            avg_completeness = round(sum(comp_vals) / len(comp_vals), 1) if comp_vals else None
            avg_drift = round(sum(drift_vals) / len(drift_vals), 1) if drift_vals else None

            # --- Task Validation Gate (D-2 migrado em ideação #9) ---
            # Delega ao service; MCP converge para o shape completo do REST
            # (+ avg_attempts_per_card, first_pass_rate, rejection_reasons).
            from okto_pulse.core.services.analytics_service import (
                aggregate_spec_validation_gate as _agg_sv,
                aggregate_task_validation_gate as _agg_tv,
            )
            task_validation_gate = _agg_tv(cards)
            spec_validation_gate = _agg_sv(specs)

            # Fallback: use validation scores if conclusion-based averages are empty
            if avg_completeness is None and task_validation_gate["avg_scores"]["completeness"] is not None:
                avg_completeness = task_validation_gate["avg_scores"]["completeness"]
            if avg_drift is None and task_validation_gate["avg_scores"]["drift"] is not None:
                avg_drift = task_validation_gate["avg_scores"]["drift"]

            # --- Cycle time (from done cards) ---
            cycle_times = []
            for c in done_cards:
                if c.created_at and c.updated_at:
                    ct = round((c.updated_at - c.created_at).total_seconds() / 3600.0, 1)
                    cycle_times.append(ct)
            avg_cycle_hours = round(sum(cycle_times) / len(cycle_times), 1) if cycle_times else None

            # --- Lifecycle cycle times (created_at → updated_at for done items) ---
            def _lifecycle_cycle_time(items, done_status) -> float | None:
                times = []
                for item in items:
                    if str(getattr(item, "status", "")) == str(done_status) and item.created_at and item.updated_at:
                        times.append(round((item.updated_at - item.created_at).total_seconds() / 3600.0, 1))
                return round(sum(times) / len(times), 1) if times else None

            # --- Sprint evaluations ---
            sprint_evals_total = 0
            sprint_eval_scores = []
            for sp in sprints:
                evals = getattr(sp, "evaluations", None) or []
                if isinstance(evals, list):
                    sprint_evals_total += len(evals)
                    for e in evals:
                        if isinstance(e, dict) and e.get("overall_score") is not None:
                            sprint_eval_scores.append(int(e["overall_score"]))

            funnel = {
                "ideations": len(ideations),
                "refinements": len(refinements),
                "specs": len(specs),
                "sprints": len(sprints),
                "cards": len(cards),
                "done": len(done_cards),
            }

            bugs_open = sum(1 for c in bug_cards if c.status not in (CardStatus.DONE, CardStatus.CANCELLED))

            return json.dumps({
                "board_id": board_id,
                "ideation_count": len(ideations),
                "refinement_count": len(refinements),
                "spec_count": len(specs),
                "sprint_count": len(sprints),
                "task_count": {
                    "total": len(cards),
                    "impl": len(impl_cards),
                    "tests": len(test_cards),
                    "bugs": len(bug_cards),
                },
                "avg_completeness": avg_completeness,
                "avg_drift": avg_drift,
                "avg_cycle_hours": avg_cycle_hours,
                "cycle_time": {
                    "ideation": _lifecycle_cycle_time(ideations, "done"),
                    "refinement": _lifecycle_cycle_time(refinements, "done"),
                    "spec": _lifecycle_cycle_time(specs, "done"),
                    "sprint": _lifecycle_cycle_time(sprints, "closed"),
                    "card": avg_cycle_hours,
                },
                "task_validation_gate": task_validation_gate,
                "spec_validation_gate": spec_validation_gate,
                "sprint_evaluation": {
                    "total_submitted": sprint_evals_total,
                    "avg_overall_score": round(sum(sprint_eval_scores) / len(sprint_eval_scores), 1) if sprint_eval_scores else None,
                },
                "funnel": funnel,
                "bugs": {
                    "total": len(bug_cards),
                    "open": bugs_open,
                    "done": sum(1 for c in bug_cards if c.status == CardStatus.DONE),
                    "by_severity": {
                        "critical": sum(1 for c in bug_cards if getattr(c, "severity", None) == "critical"),
                        "major": sum(1 for c in bug_cards if getattr(c, "severity", None) == "major"),
                        "minor": sum(1 for c in bug_cards if getattr(c, "severity", None) == "minor"),
                    },
                },
            }, default=str)

        elif metric_type == "funnel":
            # Delegado para service (D-4). MCP agora recebe o shape completo
            # do REST: status_breakdowns, cards_by_type, BR/contract counts,
            # cycle_time_by_phase, bug metrics.
            from okto_pulse.core.services.analytics_service import compute_funnel
            counts = await compute_funnel(
                db, board_id, dt_from=dt_from, dt_to=dt_to,
                include_archived=True,  # MCP histórico
            )
            return json.dumps(counts, default=str)

        elif metric_type == "quality":
            q = select(Card).where(Card.board_id == board_id, Card.status == CardStatus.DONE)
            if dt_from:
                q = q.where(Card.created_at >= dt_from)
            if dt_to:
                q = q.where(Card.created_at <= dt_to)
            cards = list((await db.execute(q)).scalars().all())

            result = []
            for c in cards:
                concl = _last_conclusion(c)
                if concl and "completeness" in concl and "drift" in concl:
                    result.append({
                        "card_id": c.id,
                        "title": c.title,
                        "completeness": concl["completeness"],
                        "drift": concl["drift"],
                    })
            return json.dumps(result, default=str)

        elif metric_type == "velocity":
            # Delegado para service (D-5). MCP agora suporta granularity=day|week,
            # buckets configuráveis (weeks=12, days=30) e séries extras
            # (bug, validation_bounce, spec_done, sprint_done) além de impl/test.
            from okto_pulse.core.services.analytics_service import compute_velocity
            velocity = await compute_velocity(
                db, board_id,
                granularity="week", weeks=12,
                dt_from=dt_from, dt_to=dt_to,
                include_archived=True,  # MCP histórico
            )
            return json.dumps(velocity, default=str)

        elif metric_type == "coverage":
            # Delegado para o service layer (ideação #9 / D-1). MCP agora recebe
            # os 4 campos extras que o REST já expunha: business_rules_count,
            # api_contracts_count, fr_with_rules_pct, fr_with_contracts_pct.
            from okto_pulse.core.services.analytics_service import compute_coverage
            result = await compute_coverage(
                db, board_id, dt_from=dt_from, dt_to=dt_to,
                include_archived=True,  # preserva comportamento histórico MCP
            )
            return json.dumps(result, default=str)

        elif metric_type == "agents":
            q = select(Card).where(Card.board_id == board_id)
            if dt_from:
                q = q.where(Card.created_at >= dt_from)
            if dt_to:
                q = q.where(Card.created_at <= dt_to)
            cards = list((await db.execute(q)).scalars().all())

            groups: dict[str, list] = {}
            for c in cards:
                groups.setdefault(c.created_by, []).append(c)

            result = []
            for actor_id, actor_cards in groups.items():
                done = [c for c in actor_cards if c.status == CardStatus.DONE]
                cv = [_last_conclusion(c) for c in done]
                comp = [x["completeness"] for x in cv if x and "completeness" in x]
                dr = [x["drift"] for x in cv if x and "drift" in x]
                result.append({
                    "actor_id": actor_id,
                    "total_cards": len(actor_cards),
                    "done_cards": len(done),
                    "avg_completeness": round(sum(comp) / len(comp), 1) if comp else None,
                    "avg_drift": round(sum(dr) / len(dr), 1) if dr else None,
                })
            result.sort(key=lambda x: x["done_cards"], reverse=True)
            return json.dumps(result, default=str)

        else:
            return json.dumps({"error": f"Unknown metric_type: {metric_type}. Use one of: overview, funnel, quality, velocity, coverage, agents"})


@mcp.tool()
async def okto_pulse_list_blockers(
    board_id: str,
    stale_hours: int = 72,
    filter_type: str = "",
) -> str:
    """
    Triage view of everything stalling the funnel, with root-cause classification.

    Every returned entry carries a `type` so the agent can act directly:

    - `dependency_blocked` — card is active while at least one `depends_on`
      target is not DONE.
    - `on_hold` — card is explicitly paused (status=on_hold).
    - `stale` — card is started/in_progress/validation and hasn't been
      touched for more than `stale_hours`.
    - `spec_pending_validation` — spec is approved but has no 'approve'
      evaluation yet, blocking promotion to in_progress.
    - `spec_no_cards` — spec is validated/in_progress but has zero linked
      cards (implementation hasn't started).
    - `uncovered_scenario` — test scenario has no linked test card, so the
      test-coverage gate will fail."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    if stale_hours < 1:
        return json.dumps({"error": "stale_hours must be >= 1"})

    # Delegado ao service (D-6 ideação #9). REST board_blockers agora também
    # aceita filter_type, garantindo paridade 1:1 com este tool.
    from okto_pulse.core.services.analytics_service import compute_blockers

    async with get_db_for_mcp() as db:
        result = await compute_blockers(
            db, board_id,
            stale_hours=stale_hours,
            filter_type=filter_type or None,
        )
        return json.dumps(result, default=str)


# ============================================================================
# BUSINESS RULE TOOLS
# ============================================================================


_STRUCTURED_SPEC_ENTITY_MCP_TYPES = {
    "functional_requirement",
    "business_rule",
    "technical_requirement",
    "decision",
    "acceptance_criterion",
    "integration_requirement",
    "observability_requirement",
}

_STRUCTURED_SPEC_ENTITY_LEGACY_WARNING = (
    "This legacy per-type mutation surface is compatibility-only. Prefer "
    "okto_pulse_update_spec_entity or okto_pulse_update_spec_api_contract for "
    "atomic structured spec child edits."
)


def _parse_expected_spec_version(raw: str) -> int | None:
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(str(raw).strip())
    except ValueError as exc:
        raise ValueError("expected_spec_version must be an integer when provided.") from exc


async def _mcp_apply_structured_spec_entity(
    *,
    board_id: str,
    spec_id: str,
    entity_type: str,
    operation: str,
    entity_id: str = "",
    payload_json: dict | str = "",
    expected_spec_version: str = "",
    task_id: str = "",
    ack_token: str = "",
    allow_api_contract: bool = False,
) -> str:
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    if entity_type == "api_contract" and not allow_api_contract:
        return json.dumps({
            "error": "api_contract uses the dedicated okto_pulse_update_spec_api_contract wrapper.",
        })
    if entity_type != "api_contract" and entity_type not in _STRUCTURED_SPEC_ENTITY_MCP_TYPES:
        return json.dumps({"error": f"Unsupported structured spec entity type: {entity_type}"})

    if isinstance(payload_json, dict):
        payload = payload_json
    elif payload_json:
        try:
            payload = json.loads(payload_json)
        except json.JSONDecodeError as exc:
            return json.dumps({"error": f"Invalid payload_json: {exc}"})
    else:
        payload = {}
    if not isinstance(payload, dict):
        return json.dumps({"error": "payload_json must decode to an object."})
    try:
        expected = _parse_expected_spec_version(expected_spec_version)
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    async with get_db_for_mcp() as db:
        service = StructuredSpecEntityService(db)
        result = await service.apply(
            StructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                actor_id=ctx.agent_id,
                entity_type=entity_type,
                entity_id=entity_id or None,
                operation=operation,
                payload=payload,
                expected_spec_version=expected,
                task_id=task_id or None,
                ack_token=ack_token or None,
                permission_set=ctx.permissions,
            )
        )
        if result.success and result.changed_fields:
            await db.commit()
        else:
            await db.rollback()
        return json.dumps(result.as_dict(), default=str)


@mcp.tool()
async def okto_pulse_update_spec_entity(
    board_id: str,
    spec_id: str,
    entity_type: str,
    operation: str,
    entity_id: str = "",
    payload_json: dict | str = "",
    expected_spec_version: str = "",
    task_id: str = "",
    ack_token: str = "",
) -> str:
    """
    Polymorphic structured spec entity mutation tool for FR, BR, TR, Decision, AC, IR and OR.

    API Contracts intentionally use okto_pulse_update_spec_api_contract so the richer
    payload shape remains explicit while still delegating to StructuredSpecEntityService.
    """
    return await _mcp_apply_structured_spec_entity(
        board_id=board_id,
        spec_id=spec_id,
        entity_type=entity_type,
        operation=operation,
        entity_id=entity_id,
        payload_json=payload_json,
        expected_spec_version=expected_spec_version,
        task_id=task_id,
        ack_token=ack_token,
        allow_api_contract=False,
    )


@mcp.tool()
async def okto_pulse_update_spec_api_contract(
    board_id: str,
    spec_id: str,
    contract_id: str,
    operation: str = "update",
    payload_json: dict | str = "",
    expected_spec_version: str = "",
    task_id: str = "",
    ack_token: str = "",
) -> str:
    """
    Thin API Contract structured mutation wrapper.

    This wrapper owns no authorization, persistence, impact or event logic; it only fixes
    entity_type=api_contract and delegates to StructuredSpecEntityService.
    """
    return await _mcp_apply_structured_spec_entity(
        board_id=board_id,
        spec_id=spec_id,
        entity_type="api_contract",
        operation=operation,
        entity_id=contract_id,
        payload_json=payload_json,
        expected_spec_version=expected_spec_version,
        task_id=task_id,
        ack_token=ack_token,
        allow_api_contract=True,
    )


@mcp.tool()
async def okto_pulse_add_business_rule(
    board_id: str,
    spec_id: str,
    title: str,
    rule: str,
    when: str,
    then: str,
    linked_requirements: str = "",
    notes: str = "",
) -> str:
    """
    Add a business rule to a spec. Business rules define system behavior constraints
    using When/Then format."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        rule_id = f"br_{_uuid.uuid4().hex[:8]}"
        frs = spec.functional_requirements or []

        # Resolve linked_requirements to canonical fr_ids (write-path, STRICT,
        # FAIL-CLOSED). Mirrors add_test_scenario's AC resolver pattern: any
        # unresolved token aborts before persistence (no partial write).
        # Accepts index, fr_id, or exact text — all stored as fr_id. spec 9d66847f.
        req_list = None
        if linked_requirements:
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), frs
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No business rule was appended."
                    )
                })
            req_list = _resolved_fr_ids or None

        br = {
            "id": rule_id,
            "title": title,
            "rule": rule.replace("\\n", "\n"),
            "when": when.replace("\\n", "\n"),
            "then": then.replace("\\n", "\n"),
            "linked_requirements": req_list,
            "notes": notes.replace("\\n", "\n") if notes else None,
        }

        rules = list(spec.business_rules or [])
        rules.append(br)

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(business_rules=rules))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, rules=rules)
        return json.dumps({"success": True, "business_rule": br, **_saturation_or_coverage(cov)}, default=str)


@mcp.tool()
async def okto_pulse_update_business_rule(
    board_id: str,
    spec_id: str,
    rule_id: str,
    title: str = "",
    rule: str = "",
    when: str = "",
    then: str = "",
    linked_requirements: str = "",
    notes: str = "",
) -> str:
    """
    Update an existing business rule on a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        rules = list(spec.business_rules or [])
        target = None
        for r in rules:
            if r.get("id") == rule_id:
                target = r
                break
        if not target:
            return json.dumps({"error": f"Business rule '{rule_id}' not found"})

        if title:
            target["title"] = title
        if rule:
            target["rule"] = rule.replace("\\n", "\n")
        if when:
            target["when"] = when.replace("\\n", "\n")
        if then:
            target["then"] = then.replace("\\n", "\n")
        if notes == "CLEAR":
            target["notes"] = None
        elif notes:
            target["notes"] = notes.replace("\\n", "\n")

        frs = spec.functional_requirements or []
        if linked_requirements == "CLEAR":
            target["linked_requirements"] = None
        elif linked_requirements:
            # Write-path: resolve to canonical fr_ids, fail-closed. spec 9d66847f.
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), frs
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No business rule was updated."
                    )
                })
            target["linked_requirements"] = _resolved_fr_ids or None

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(business_rules=rules))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, rules=rules)
        return json.dumps({
            "success": True,
            "business_rule": target,
            "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
            **_saturation_or_coverage(cov),
        }, default=str)


@mcp.tool()
async def okto_pulse_remove_business_rule(
    board_id: str,
    spec_id: str,
    rule_id: str,
) -> str:
    """
    Remove a business rule from a spec."""
    return await _remove_spec_entity_impl(
        board_id, spec_id, "business_rule", rule_id,
        alias_kind="legacy", tool_name="okto_pulse_remove_business_rule",
    )


# ============================================================================
# Decisions — formalized design choices on a spec (spec b66d2562)
#
# Decision vs BusinessRule: a Decision records *why* a choice was made
# ("We chose LadybugDB over Neo4j because..."); a BusinessRule is a prescriptive
# norm ("The system MUST clamp scores at 1.5"). They're distinct entities
# with distinct semantics — don't mix them.
# ============================================================================


@mcp.tool()
async def okto_pulse_list_integration_requirements(
    board_id: str,
    spec_id: str,
    include_inactive: str = "false",
) -> str:
    """List Integration Requirements (IR) for a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "spec.integration_requirements.read",
        Permissions.BOARD_READ,
    )
    if perm_err:
        return _perm_error(perm_err)

    include_all = _flag_enabled(include_inactive)
    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        requirements = list(getattr(spec, "integration_requirements", None) or [])
        if not include_all:
            requirements = [
                item for item in requirements
                if not isinstance(item, dict) or item.get("status", "active") == "active"
            ]
        return json.dumps(
            {"spec_id": spec_id, "integration_requirements": requirements},
            default=str,
        )


@mcp.tool()
async def okto_pulse_add_integration_requirement(
    board_id: str,
    spec_id: str,
    title: str,
    integration_type: str = "api",
    description: str = "",
    provider: str = "",
    consumer: str = "",
    contract_ref: str = "",
    endpoint: str = "",
    method: str = "",
    data_contract_json: dict | str = "",
    linked_requirements: str = "",
    linked_api_contracts: list[str] | str = "",
    notes: str = "",
) -> str:
    """
    Add an Integration Requirement (IR) to a spec.

    Use IR for APIs, queues, stored procedures, events, files, and data
    contracts that need traceability beyond a single endpoint.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "spec.integration_requirements.create",
        Permissions.SPECS_UPDATE,
    )
    if perm_err:
        return _perm_error(perm_err)

    allowed_types = {"api", "queue", "stored_procedure", "data_contract", "event", "file", "other"}
    if integration_type not in allowed_types:
        return json.dumps({"error": f"Invalid integration_type. Use one of: {sorted(allowed_types)}"})

    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})

        # Write-path: resolve to canonical fr_ids, fail-closed. spec 9d66847f.
        _frs_ir = spec.functional_requirements or []
        req_list = None
        if linked_requirements:
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), _frs_ir
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in _frs_ir) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(_frs_ir) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No integration requirement was appended."
                    )
                })
            req_list = _resolved_fr_ids or None

        data_contract = None
        if data_contract_json:
            data_contract, json_err = _parse_json_arg(data_contract_json, None)
            if json_err:
                return json.dumps({"error": json_err})

        linked_api_contracts_list = None
        if linked_api_contracts:
            try:
                linked_api_contracts_list = coerce_to_list_str(linked_api_contracts) or None
            except ValueError as e:
                return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

        requirement = {
            "id": f"ir_{_uuid.uuid4().hex[:8]}",
            "title": title,
            "integration_type": integration_type,
            "description": description.replace("\\n", "\n") if description else "",
            "provider": provider or None,
            "consumer": consumer or None,
            "contract_ref": contract_ref or None,
            "endpoint": endpoint or None,
            "method": method or None,
            "data_contract": data_contract,
            "linked_requirements": req_list,
            "linked_api_contracts": linked_api_contracts_list,
            "linked_task_ids": None,
            "status": "active",
            "notes": notes.replace("\\n", "\n") if notes else None,
        }

        requirements = list(getattr(spec, "integration_requirements", None) or [])
        requirements.append(requirement)

        from okto_pulse.core.models.schemas import SpecUpdate
        _, update_err = await _safe_spec_update(
            service,
            spec_id,
            ctx.agent_id,
            SpecUpdate(integration_requirements=requirements),
        )
        if update_err:
            return update_err
        await db.commit()

        coverage = _spec_coverage(spec, integration_requirements=requirements)
        return json.dumps(
            {"success": True, "integration_requirement": requirement, **_saturation_or_coverage(coverage)},
            default=str,
        )


async def _link_task_to_integration_requirement_internal(
    board_id: str,
    spec_id: str,
    requirement_id: str,
    card_id: str,
) -> str:
    """Internal helper for link_task target_type='ir'."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "spec.integration_requirements.link_task",
        Permissions.SPECS_UPDATE,
    )
    if perm_err:
        return _perm_error(perm_err)
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "card.link_to.ir",
        Permissions.CARDS_UPDATE,
    )
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        requirements = list(getattr(spec, "integration_requirements", None) or [])
        target = next((item for item in requirements if item.get("id") == requirement_id), None)
        if target is None:
            return json.dumps({"error": f"Integration requirement '{requirement_id}' not found"})

        task_ids = list(target.get("linked_task_ids") or [])
        if card_id not in task_ids:
            task_ids.append(card_id)
        target["linked_task_ids"] = task_ids

        from okto_pulse.core.models.schemas import SpecUpdate
        _, update_err = await _safe_spec_update(
            spec_service,
            spec_id,
            ctx.agent_id,
            SpecUpdate(integration_requirements=requirements),
        )
        if update_err:
            return update_err
        await db.commit()

        coverage = _spec_coverage(spec, integration_requirements=requirements)
        return json.dumps(
            {
                "success": True,
                "requirement_id": requirement_id,
                "card_id": card_id,
                "linked_tasks": task_ids,
                **_saturation_or_coverage(coverage),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_list_observability_requirements(
    board_id: str,
    spec_id: str,
    include_inactive: str = "false",
) -> str:
    """List Observability Requirements (OR) for a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "spec.observability_requirements.read",
        Permissions.BOARD_READ,
    )
    if perm_err:
        return _perm_error(perm_err)

    include_all = _flag_enabled(include_inactive)
    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        requirements = list(getattr(spec, "observability_requirements", None) or [])
        if not include_all:
            requirements = [
                item for item in requirements
                if not isinstance(item, dict) or item.get("status", "active") == "active"
            ]
        return json.dumps(
            {"spec_id": spec_id, "observability_requirements": requirements},
            default=str,
        )


@mcp.tool()
async def okto_pulse_add_observability_requirement(
    board_id: str,
    spec_id: str,
    title: str,
    signal_type: str = "metric",
    description: str = "",
    target: str = "",
    metric_name: str = "",
    threshold: str = "",
    severity: str = "",
    owner: str = "",
    linked_requirements: str = "",
    linked_integration_requirements: list[str] | str = "",
    notes: str = "",
) -> str:
    """Add an Observability Requirement (OR) to a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "spec.observability_requirements.create",
        Permissions.SPECS_UPDATE,
    )
    if perm_err:
        return _perm_error(perm_err)

    allowed_types = {"metric", "log", "trace", "dashboard", "alert", "slo", "other"}
    if signal_type not in allowed_types:
        return json.dumps({"error": f"Invalid signal_type. Use one of: {sorted(allowed_types)}"})

    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})

        # Write-path: resolve to canonical fr_ids, fail-closed. spec 9d66847f.
        _frs_or = spec.functional_requirements or []
        req_list = None
        if linked_requirements:
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), _frs_or
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in _frs_or) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(_frs_or) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No observability requirement was appended."
                    )
                })
            req_list = _resolved_fr_ids or None

        linked_irs_list = None
        if linked_integration_requirements:
            try:
                linked_irs_list = coerce_to_list_str(linked_integration_requirements) or None
            except ValueError as e:
                return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

        requirement = {
            "id": f"or_{_uuid.uuid4().hex[:8]}",
            "title": title,
            "signal_type": signal_type,
            "description": description.replace("\\n", "\n") if description else "",
            "target": target or None,
            "metric_name": metric_name or None,
            "threshold": threshold or None,
            "severity": severity or None,
            "owner": owner or None,
            "linked_requirements": req_list,
            "linked_integration_requirements": linked_irs_list,
            "linked_task_ids": None,
            "status": "active",
            "notes": notes.replace("\\n", "\n") if notes else None,
        }

        requirements = list(getattr(spec, "observability_requirements", None) or [])
        requirements.append(requirement)

        from okto_pulse.core.models.schemas import SpecUpdate
        _, update_err = await _safe_spec_update(
            service,
            spec_id,
            ctx.agent_id,
            SpecUpdate(observability_requirements=requirements),
        )
        if update_err:
            return update_err
        await db.commit()

        coverage = _spec_coverage(spec, observability_requirements=requirements)
        return json.dumps(
            {"success": True, "observability_requirement": requirement, **_saturation_or_coverage(coverage)},
            default=str,
        )


async def _link_task_to_observability_requirement_internal(
    board_id: str,
    spec_id: str,
    requirement_id: str,
    card_id: str,
) -> str:
    """Internal helper for link_task target_type='or'."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "spec.observability_requirements.link_task",
        Permissions.SPECS_UPDATE,
    )
    if perm_err:
        return _perm_error(perm_err)
    perm_err = _mcp_check_permission(
        ctx.permissions,
        "card.link_to.or",
        Permissions.CARDS_UPDATE,
    )
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        requirements = list(getattr(spec, "observability_requirements", None) or [])
        target = next((item for item in requirements if item.get("id") == requirement_id), None)
        if target is None:
            return json.dumps({"error": f"Observability requirement '{requirement_id}' not found"})

        task_ids = list(target.get("linked_task_ids") or [])
        if card_id not in task_ids:
            task_ids.append(card_id)
        target["linked_task_ids"] = task_ids

        from okto_pulse.core.models.schemas import SpecUpdate
        _, update_err = await _safe_spec_update(
            spec_service,
            spec_id,
            ctx.agent_id,
            SpecUpdate(observability_requirements=requirements),
        )
        if update_err:
            return update_err
        await db.commit()

        coverage = _spec_coverage(spec, observability_requirements=requirements)
        return json.dumps(
            {
                "success": True,
                "requirement_id": requirement_id,
                "card_id": card_id,
                "linked_tasks": task_ids,
                **_saturation_or_coverage(coverage),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_add_decision(
    board_id: str,
    spec_id: str,
    title: str,
    rationale: str,
    context: str = "",
    alternatives_considered: list[str] | str = "",
    supersedes_decision_id: str = "",
    linked_requirements: str = "",
    notes: str = "",
) -> str:
    """
    Add a formalized Decision to a spec.

    A Decision records a contextual CHOICE — the reasoning behind picking one
    path over alternatives. Different from BusinessRule (which is a NORM, a
    prescriptive "DEVE" statement): use a Decision to capture design
    intent, tradeoffs, or team consensus. The KG extracts Decisions into
    queryable nodes, and the optional coverage gate (opt-in) can require each
    Decision to have ≥1 linked task."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    alts = None
    if alternatives_considered:
        try:
            alts = coerce_to_list_str(alternatives_considered) or None
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        frs = spec.functional_requirements or []

        # Write-path: resolve to canonical fr_ids, fail-closed. spec 9d66847f.
        req_list = None
        if linked_requirements:
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), frs
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No decision was appended."
                    )
                })
            req_list = _resolved_fr_ids or None

        decisions = list(spec.decisions or [])

        # Auto-supersede: if the new decision supersedes an existing one,
        # flip the target's status to "superseded" in the same update.
        if supersedes_decision_id:
            found_target = False
            for d in decisions:
                if d.get("id") == supersedes_decision_id:
                    d["status"] = "superseded"
                    found_target = True
                    break
            if not found_target:
                return json.dumps({
                    "error": f"supersedes_decision_id '{supersedes_decision_id}' "
                             f"not found in spec.decisions"
                })

        dec_id = f"dec_{_uuid.uuid4().hex[:8]}"
        decision = {
            "id": dec_id,
            "title": title,
            "rationale": rationale.replace("\\n", "\n"),
            "context": context.replace("\\n", "\n") if context else None,
            "alternatives_considered": alts,
            "supersedes_decision_id": supersedes_decision_id or None,
            "linked_requirements": req_list,
            "linked_task_ids": None,
            "status": "active",
            "notes": notes.replace("\\n", "\n") if notes else None,
        }
        decisions.append(decision)

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(
            service, spec_id, ctx.agent_id,
            SpecUpdate(decisions=decisions),
        )
        if _err:
            return _err
        await db.commit()

        return json.dumps(
            {"success": True, "decision": decision, "decisions_total": len(decisions)},
            default=str,
        )


@mcp.tool()
async def okto_pulse_update_decision(
    board_id: str,
    spec_id: str,
    decision_id: str,
    title: str = "",
    rationale: str = "",
    context: str = "",
    alternatives_considered: list[str] | str = "",
    supersedes_decision_id: str = "",
    linked_requirements: str = "",
    notes: str = "",
    status: str = "",
) -> str:
    """
    Update an existing Decision. Only non-empty fields are changed; pass "CLEAR"
    to wipe optional string/list fields."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        decisions = list(spec.decisions or [])
        target = next((d for d in decisions if d.get("id") == decision_id), None)
        if target is None:
            return json.dumps({"error": f"Decision '{decision_id}' not found"})

        if title:
            target["title"] = title
        if rationale:
            target["rationale"] = rationale.replace("\\n", "\n")
        if context:
            target["context"] = None if context.strip().upper() == "CLEAR" else context.replace("\\n", "\n")
        if alternatives_considered:
            # When the Union delivers a list, .strip() is not applicable; check
            # for the CLEAR sentinel only when it is a plain string.
            if isinstance(alternatives_considered, str) and alternatives_considered.strip().upper() == "CLEAR":
                target["alternatives_considered"] = None
            else:
                try:
                    target["alternatives_considered"] = coerce_to_list_str(alternatives_considered) or None
                except ValueError as e:
                    return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
        if supersedes_decision_id:
            if supersedes_decision_id.strip().upper() == "CLEAR":
                target["supersedes_decision_id"] = None
            else:
                target["supersedes_decision_id"] = supersedes_decision_id
                # Also flip the referenced decision's status
                for d in decisions:
                    if d.get("id") == supersedes_decision_id:
                        d["status"] = "superseded"
                        break
        if linked_requirements:
            frs = spec.functional_requirements or []
            # Write-path: resolve to canonical fr_ids, fail-closed. spec 9d66847f.
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), frs
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No decision was updated."
                    )
                })
            target["linked_requirements"] = _resolved_fr_ids or None
        if notes:
            target["notes"] = None if notes.strip().upper() == "CLEAR" else notes.replace("\\n", "\n")
        if status:
            if status not in ("active", "superseded", "revoked"):
                return json.dumps({"error": f"Invalid status '{status}'. Use active/superseded/revoked."})
            target["status"] = status

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(
            service, spec_id, ctx.agent_id,
            SpecUpdate(decisions=decisions),
        )
        if _err:
            return _err
        await db.commit()

        return json.dumps({
            "success": True,
            "decision": target,
            "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
        }, default=str)


@mcp.tool()
async def okto_pulse_remove_decision(
    board_id: str,
    spec_id: str,
    decision_id: str,
) -> str:
    """
    Remove a Decision (soft-delete: status becomes "revoked").

    Preserves history so the KG still surfaces the decision with its
    revocation reason. Use okto_pulse_update_decision with status=active to
    restore."""
    return await _remove_spec_entity_impl(
        board_id, spec_id, "decision", decision_id,
        alias_kind="legacy", tool_name="okto_pulse_remove_decision",
    )


async def _link_task_to_decision_internal(
    board_id: str,
    spec_id: str,
    decision_id: str,
    card_id: str,
) -> str:
    """Internal helper for link_task target_type='decision'. Idempotent —
    re-linking the same card is a no-op. Populates decision.linked_task_ids so
    the opt-in coverage gate (skip_decisions_coverage=False) can verify each
    active Decision has at least one linked task.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = CardService(db)
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        decisions = list(spec.decisions or [])
        target = next((d for d in decisions if d.get("id") == decision_id), None)
        if target is None:
            return json.dumps({"error": f"Decision '{decision_id}' not found"})

        task_ids = list(target.get("linked_task_ids") or [])
        if card_id not in task_ids:
            task_ids.append(card_id)
        target["linked_task_ids"] = task_ids

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(
            spec_service, spec_id, ctx.agent_id,
            SpecUpdate(decisions=decisions),
        )
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, decisions=decisions)
        return json.dumps({
            "success": True,
            "decision_id": decision_id,
            "card_id": card_id,
            "linked_tasks": task_ids,
            **_saturation_or_coverage(cov),
        })


@mcp.tool()
async def okto_pulse_migrate_spec_decisions(
    board_id: str,
    spec_id: str,
) -> str:
    """
    One-shot migrator: extract "## Decisions" markdown bullets from spec.context
    into structured spec.decisions[] entries, then remove the block from context.

    Idempotent — running twice on a migrated spec is a no-op. Existing
    decisions are preserved; only the markdown-sourced ones are added, and
    duplicates (same title) are skipped."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import re
    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        context_text = spec.context or ""
        # Match the ## Decisions block up to the next heading or EOF. Bullets
        # are "- " prefixed. Mirrors the worker's existing extractor.
        pattern = re.compile(
            r"(?m)^##\s+Decisions\s*\n((?:(?:[-*]\s+.*\n?)|\s*\n)+?)(?=^##\s+|\Z)"
        )
        match = pattern.search(context_text)
        if not match:
            return json.dumps({
                "success": True,
                "decisions_added": 0,
                "context_modified": False,
                "note": "No '## Decisions' block found — nothing to migrate.",
            })

        bullets_block = match.group(1)
        bullet_pat = re.compile(r"^[-*]\s+(.+?)\s*$", re.MULTILINE)
        raw_bullets = [b.strip() for b in bullet_pat.findall(bullets_block) if b.strip()]

        existing = list(spec.decisions or [])
        existing_titles = {d.get("title", "").strip() for d in existing}

        added: list[dict] = []
        for raw in raw_bullets:
            if raw in existing_titles:
                continue  # idempotent dedupe
            dec = {
                "id": f"dec_{_uuid.uuid4().hex[:8]}",
                "title": raw[:200],
                "rationale": raw,  # no richer context available from bullets
                "context": None,
                "alternatives_considered": None,
                "supersedes_decision_id": None,
                "linked_requirements": None,
                "linked_task_ids": None,
                "status": "active",
                "notes": "Migrated from spec.context '## Decisions' markdown",
            }
            existing.append(dec)
            existing_titles.add(dec["title"])
            added.append(dec)

        # Remove the block from context (only if we consumed bullets — or always,
        # so the markdown source disappears and the extractor's backward-compat
        # path doesn't re-emit the same decisions on next consolidation).
        new_context = pattern.sub("", context_text).rstrip() + "\n"
        context_modified = new_context.strip() != (context_text or "").strip()

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(
            service, spec_id, ctx.agent_id,
            SpecUpdate(decisions=existing, context=new_context),
        )
        if _err:
            return _err
        await db.commit()

        return json.dumps({
            "success": True,
            "decisions_added": len(added),
            "context_modified": context_modified,
            "added": [{"id": d["id"], "title": d["title"]} for d in added],
        })


@mcp.tool()
async def okto_pulse_list_business_rules(
    board_id: str,
    spec_id: str,
    include_inactive: str = "false",
) -> str:
    """
    List all business rules for a spec with linked functional requirements resolved as text."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        include_all = _flag_enabled(include_inactive)
        rules = list(spec.business_rules or [])
        if not include_all:
            rules = [
                item for item in rules
                if not isinstance(item, dict) or item.get("status", "active") == "active"
            ]
        frs = spec.functional_requirements or []

        from okto_pulse.core.mcp.payload_compaction import emit_compaction_metric
        from okto_pulse.core.services.analytics_service import (
            _structured_ref_text,
            resolve_linked_fr_indices,
        )

        result = []
        deduped_count = 0
        for r in rules:
            entry = dict(r)
            linked = r.get("linked_requirements") or []
            # FR7 dedup: previously the full FR text was emitted twice — under
            # the raw ``linked_requirements`` AND under ``resolved_requirements``.
            # Resolve once (structured-FR aware): emit canonical fr_id under
            # ``linked_requirements`` (IMPL-2: projection now emits fr_id, not
            # re-derived index); carry the human ``[FR-n] <text>`` only under
            # ``resolved_requirements``. Legacy FRs without an id fall back to
            # str(index) so output is never empty.
            idxs = sorted(resolve_linked_fr_indices(linked, frs))
            entry["linked_requirements"] = [
                _structured_ref_id(frs[i]) or str(i)
                for i in idxs
                if 0 <= i < len(frs)
            ]
            entry["resolved_requirements"] = [
                f"[FR-{i}] {_structured_ref_text(frs[i])}"
                for i in idxs
                if 0 <= i < len(frs)
            ]
            # Robustness: preserve legacy refs that don't resolve to any FR so
            # old data does not silently lose context.
            unresolved = [
                ref for ref in linked if not resolve_linked_fr_indices([ref], frs)
            ]
            if unresolved:
                entry["unresolved_requirements"] = unresolved
            deduped_count += len(entry["resolved_requirements"])
            result.append(entry)

        # FR8 / or_f4159e58: this list dedups full FR text — emit the safe
        # metric (counts only, no FR text / body).
        emit_compaction_metric(
            tool_name="okto_pulse_list_business_rules",
            deduped_count=deduped_count,
        )
        return json.dumps({
            "spec_id": spec_id,
            "total": len(result),
            "business_rules": result,
        }, default=str)


# ============================================================================
# API CONTRACT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_add_api_contract(
    board_id: str,
    spec_id: str,
    method: str,
    path: str,
    description: str = "",
    request_body_json: dict | str = "",
    response_success_json: dict | str = "",
    response_errors_json: list[dict] | str = "",
    linked_requirements: str = "",
    linked_rules: list[str] | str = "",
    notes: str = "",
) -> str:
    """
    Add an API contract to a spec. API contracts define endpoints, request/response
    shapes, and link to requirements and business rules."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        contract_id = f"api_{_uuid.uuid4().hex[:8]}"
        frs = spec.functional_requirements or []
        existing_rules = spec.business_rules or []

        # Parse JSON strings (or accept native dict/list directly)
        request_body = None
        if request_body_json:
            if isinstance(request_body_json, dict):
                request_body = request_body_json
            else:
                try:
                    request_body = json.loads(request_body_json)
                except json.JSONDecodeError as e:
                    return json.dumps({"error": f"Invalid request_body_json: {e}"})

        response_success = None
        if response_success_json:
            if isinstance(response_success_json, dict):
                response_success = response_success_json
            else:
                try:
                    response_success = json.loads(response_success_json)
                except json.JSONDecodeError as e:
                    return json.dumps({"error": f"Invalid response_success_json: {e}"})

        response_errors = None
        if response_errors_json:
            if isinstance(response_errors_json, list):
                response_errors = response_errors_json
            else:
                try:
                    response_errors = json.loads(response_errors_json)
                except json.JSONDecodeError as e:
                    return json.dumps({"error": f"Invalid response_errors_json: {e}"})

        # Resolve linked_requirements to canonical fr_ids (write-path, STRICT,
        # FAIL-CLOSED — mirrors add_test_scenario AC pattern). spec 9d66847f.
        req_list = None
        if linked_requirements:
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), frs
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No API contract was appended."
                    )
                })
            req_list = _resolved_fr_ids or None

        # Resolve linked rules
        rules_list = None
        if linked_rules:
            try:
                linked_rules_tokens = coerce_to_list_str(linked_rules)
            except ValueError as e:
                return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
            rule_ids = {r.get("id") for r in existing_rules}
            rules_list = []
            for token in linked_rules_tokens:
                if token in rule_ids:
                    rules_list.append(token)
                else:
                    return json.dumps({"error": f"Business rule '{token}' not found in spec"})

        contract = {
            "id": contract_id,
            "method": method.upper(),
            "path": path,
            "description": description.replace("\\n", "\n") if description else "",
            "request_body": request_body,
            "response_success": response_success,
            "response_errors": response_errors,
            "linked_requirements": req_list,
            "linked_rules": rules_list,
            "notes": notes.replace("\\n", "\n") if notes else None,
        }

        # Validate the NEW contract as a write (http strictness via on_write,
        # F9) and surface a canonical error with no errors.pydantic.dev URL (F10).
        _cerr = _validate_api_contract_write(contract)
        if _cerr:
            return _cerr

        contracts = list(spec.api_contracts or [])
        contracts.append(contract)

        from pydantic import ValidationError
        from okto_pulse.core.models.schemas import SpecUpdate
        # Build the bulk SpecUpdate INSIDE a try (it was the inline argument
        # outside _safe_spec_update's try, so a ValidationError leaked raw). The
        # existing contracts re-validate tolerantly (read-back, no on_write); the
        # new one was already checked on_write above.
        try:
            _contract_update = SpecUpdate(api_contracts=contracts)
        except ValidationError as exc:
            return _canonical_api_contract_error(exc)
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, _contract_update)
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, contracts=contracts)
        return json.dumps({"success": True, "api_contract": contract, **_saturation_or_coverage(cov)}, default=str)


@mcp.tool()
async def okto_pulse_update_api_contract(
    board_id: str,
    spec_id: str,
    contract_id: str,
    method: str = "",
    path: str = "",
    description: str = "",
    request_body_json: dict | str = "",
    response_success_json: dict | str = "",
    response_errors_json: list[dict] | str = "",
    linked_requirements: str = "",
    linked_rules: list[str] | str = "",
    notes: str = "",
) -> str:
    """
    Update an existing API contract on a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        contracts = list(spec.api_contracts or [])
        target = None
        for c in contracts:
            if c.get("id") == contract_id:
                target = c
                break
        if not target:
            return json.dumps({"error": f"API contract '{contract_id}' not found"})

        if method:
            target["method"] = method.upper()
        if path:
            target["path"] = path

        if description == "CLEAR":
            target["description"] = ""
        elif description:
            target["description"] = description.replace("\\n", "\n")

        if isinstance(request_body_json, dict):
            target["request_body"] = request_body_json
        elif request_body_json == "CLEAR":
            target["request_body"] = None
        elif request_body_json:
            try:
                target["request_body"] = json.loads(request_body_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid request_body_json: {e}"})

        if isinstance(response_success_json, dict):
            target["response_success"] = response_success_json
        elif response_success_json == "CLEAR":
            target["response_success"] = None
        elif response_success_json:
            try:
                target["response_success"] = json.loads(response_success_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid response_success_json: {e}"})

        if isinstance(response_errors_json, list):
            target["response_errors"] = response_errors_json
        elif response_errors_json == "CLEAR":
            target["response_errors"] = None
        elif response_errors_json:
            try:
                target["response_errors"] = json.loads(response_errors_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid response_errors_json: {e}"})

        if notes == "CLEAR":
            target["notes"] = None
        elif notes:
            target["notes"] = notes.replace("\\n", "\n")

        frs = spec.functional_requirements or []
        if linked_requirements == "CLEAR":
            target["linked_requirements"] = None
        elif linked_requirements:
            # Write-path: resolve to canonical fr_ids, fail-closed. spec 9d66847f.
            _resolved_fr_ids, _unresolved_frs = resolve_linked_requirements_to_ids(
                parse_multi_value(linked_requirements), frs
            )
            if _unresolved_frs:
                _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
                return json.dumps({
                    "error": (
                        f"Unresolved linked_requirements token(s): {_unresolved_frs}. "
                        f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                        f"Available fr_ids: {_available_fr_ids}. "
                        f"No API contract was updated."
                    )
                })
            target["linked_requirements"] = _resolved_fr_ids or None

        existing_rules = spec.business_rules or []
        if isinstance(linked_rules, str) and linked_rules == "CLEAR":
            target["linked_rules"] = None
        elif linked_rules:
            try:
                linked_rules_tokens = coerce_to_list_str(linked_rules)
            except ValueError as e:
                return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
            rule_ids = {r.get("id") for r in existing_rules}
            rules_list = []
            for token in linked_rules_tokens:
                if token in rule_ids:
                    rules_list.append(token)
                else:
                    return json.dumps({"error": f"Business rule '{token}' not found in spec"})
            target["linked_rules"] = rules_list

        # Validate the MODIFIED contract as a write (http strictness via
        # on_write, F9) and surface a canonical error with no URL leak (F10).
        _cerr = _validate_api_contract_write(target)
        if _cerr:
            return _cerr

        from pydantic import ValidationError
        from okto_pulse.core.models.schemas import SpecUpdate
        # Build the bulk SpecUpdate inside a try (was the inline arg outside the
        # _safe_spec_update try); existing contracts re-validate tolerantly.
        try:
            _contract_update = SpecUpdate(api_contracts=contracts)
        except ValidationError as exc:
            return _canonical_api_contract_error(exc)
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, _contract_update)
        if _err:
            return _err
        await db.commit()

        return json.dumps({
            "success": True,
            "api_contract": target,
            "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
        }, default=str)


@mcp.tool()
async def okto_pulse_remove_api_contract(
    board_id: str,
    spec_id: str,
    contract_id: str,
) -> str:
    """
    Remove an API contract from a spec."""
    return await _remove_spec_entity_impl(
        board_id, spec_id, "api_contract", contract_id,
        alias_kind="legacy", tool_name="okto_pulse_remove_api_contract",
    )


# ============================================================================
# R4 — consolidated spec-entity REMOVE (spec 452cb4d5, card R4.1)
#
# spec_entity_remove is one of the two assertiveness-gate-eligible families
# (owner decision after the R4 audit): the three legacy remove_* tools have
# IDENTICAL (board_id, spec_id, <id>) signatures, so consolidation loses ZERO
# per-type field schema. The legacy tools are PRESERVED as additive aliases that
# delegate here (fr_af4b5c6e / tr_b25890c4 — one shared impl, no duplicated
# logic). DEDICATED ROUTING preserves the per-type behavioral asymmetry: decision
# is a SOFT-delete (status=revoked, restorable) while business_rule/api_contract
# are hard removals.
# ============================================================================


async def _remove_spec_entity_impl(
    board_id: str,
    spec_id: str,
    target_type: str,
    entity_id: str,
    *,
    alias_kind: str,
    tool_name: str,
) -> str:
    """Shared implementation behind okto_pulse_remove_spec_entity and the three
    legacy remove_* aliases. Replicates each legacy tool's exact behavior and
    response shape (parity), and emits safe alias-usage telemetry (or_4e57890f)."""
    from okto_pulse.core.mcp.tool_family_registry import (
        REGISTRY,
        VIOLATION_UNKNOWN_TARGET_TYPE,
        emit_alias_usage,
        emit_registry_violation,
    )

    def _telemetry(outcome: str) -> None:
        emit_alias_usage(
            family_id="spec_entity_remove",
            alias_kind=alias_kind,
            tool_name=tool_name,
            operation="remove",
            target_type=str(target_type),
            outcome=outcome,
        )

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        _telemetry("error")
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        _telemetry("error")
        return _perm_error(perm_err)

    # fr_452f7d7f / ac_c0c8f0f3: unsupported target_type -> structured error, no mutation.
    type_err = REGISTRY.validate_target_type("spec_entity_remove", target_type)
    if type_err:
        emit_registry_violation(
            family_id="spec_entity_remove",
            reason=VIOLATION_UNKNOWN_TARGET_TYPE,
            tool_name=tool_name,
            target_type=str(target_type),
        )
        _telemetry("error")
        fam = REGISTRY.get("spec_entity_remove")
        return json.dumps({
            "error": "unsupported_target_type",
            "message": type_err,
            "allowed": list(fam.target_types) if fam else [],
        })

    from okto_pulse.core.models.schemas import SpecUpdate

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            _telemetry("error")
            return json.dumps({"error": "Spec not found"})

        if target_type == "business_rule":
            rules = list(spec.business_rules or [])
            new_rules = [r for r in rules if r.get("id") != entity_id]
            if len(new_rules) == len(rules):
                _telemetry("error")
                return json.dumps({"error": f"Business rule '{entity_id}' not found"})
            _, _err = await _safe_spec_update(
                service, spec_id, ctx.agent_id, SpecUpdate(business_rules=new_rules)
            )
            if _err:
                _telemetry("error")
                return _err
            await db.commit()
            cov = _spec_coverage(spec, rules=new_rules)
            _telemetry("ok")
            return json.dumps({
                "success": True, "removed": entity_id, "remaining": len(new_rules),
                **_saturation_or_coverage(cov),
            })

        if target_type == "api_contract":
            contracts = list(spec.api_contracts or [])
            new_contracts = [c for c in contracts if c.get("id") != entity_id]
            if len(new_contracts) == len(contracts):
                _telemetry("error")
                return json.dumps({"error": f"API contract '{entity_id}' not found"})
            _, _err = await _safe_spec_update(
                service, spec_id, ctx.agent_id, SpecUpdate(api_contracts=new_contracts)
            )
            if _err:
                _telemetry("error")
                return _err
            await db.commit()
            cov = _spec_coverage(spec, contracts=new_contracts)
            _telemetry("ok")
            return json.dumps({
                "success": True, "removed": entity_id, "remaining": len(new_contracts),
                **_saturation_or_coverage(cov),
            })

        # decision — SOFT-delete (status=revoked, restorable via update_decision).
        decisions = list(spec.decisions or [])
        target = next((d for d in decisions if d.get("id") == entity_id), None)
        if target is None:
            _telemetry("error")
            return json.dumps({"error": f"Decision '{entity_id}' not found"})
        target["status"] = "revoked"
        _, _err = await _safe_spec_update(
            service, spec_id, ctx.agent_id, SpecUpdate(decisions=decisions)
        )
        if _err:
            _telemetry("error")
            return _err
        await db.commit()
        _telemetry("ok")
        return json.dumps({"success": True, "revoked": entity_id, "decision": target})


@mcp.tool()
async def okto_pulse_remove_spec_entity(
    board_id: str,
    spec_id: str,
    target_type: str,
    entity_id: str,
) -> str:
    """
    Consolidated spec-entity removal (R4). `target_type` is one of: `business_rule`,
    `api_contract`, `decision`. Equivalent to the per-type tools
    (`okto_pulse_remove_business_rule`/`_api_contract`/`_decision`), which remain as
    aliases. Note the asymmetry: `decision` is a SOFT-delete (status becomes
    `revoked`, restorable via `okto_pulse_update_decision`), while `business_rule`
    and `api_contract` are hard removals. An unsupported `target_type` returns a
    structured error listing the allowed values (no mutation).
    See `okto-pulse://reference/tool-families/spec_entity_remove`."""
    return await _remove_spec_entity_impl(
        board_id, spec_id, target_type, entity_id,
        alias_kind="consolidated", tool_name="okto_pulse_remove_spec_entity",
    )


@mcp.tool()
async def okto_pulse_list_api_contracts(
    board_id: str,
    spec_id: str,
    include_inactive: str = "false",
) -> str:
    """
    List all API contracts for a spec with linked business rules resolved."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        include_all = _flag_enabled(include_inactive)
        contracts = list(spec.api_contracts or [])
        if not include_all:
            contracts = [
                item for item in contracts
                if not isinstance(item, dict) or item.get("status", "active") == "active"
            ]
        existing_rules = {r.get("id"): r for r in (spec.business_rules or [])}
        frs = spec.functional_requirements or []

        from okto_pulse.core.mcp.payload_compaction import emit_compaction_metric
        from okto_pulse.core.services.analytics_service import (
            _structured_ref_text,
            resolve_linked_fr_indices,
        )

        result = []
        deduped_count = 0
        for c in contracts:
            entry = dict(c)

            # Resolve linked rules
            linked_rule_ids = c.get("linked_rules") or []
            resolved_rules = []
            for rid in linked_rule_ids:
                br = existing_rules.get(rid)
                if br:
                    resolved_rules.append(f"[{rid}] {br.get('title', '')}")
                else:
                    resolved_rules.append(rid)
            entry["resolved_rules"] = resolved_rules

            # FR7 dedup: same as list_business_rules — emit canonical fr_id
            # under ``linked_requirements`` (IMPL-2: projection now emits fr_id,
            # not re-derived index; legacy FRs without id fall back to str(idx))
            # and carry the human ``[FR-n] <text>`` only under
            # ``resolved_requirements`` so the full FR text is not serialized twice.
            linked_reqs = c.get("linked_requirements") or []
            idxs = sorted(resolve_linked_fr_indices(linked_reqs, frs))
            entry["linked_requirements"] = [
                _structured_ref_id(frs[i]) or str(i)
                for i in idxs
                if 0 <= i < len(frs)
            ]
            entry["resolved_requirements"] = [
                f"[FR-{i}] {_structured_ref_text(frs[i])}"
                for i in idxs
                if 0 <= i < len(frs)
            ]
            # Robustness: preserve legacy refs that don't resolve to any FR.
            unresolved = [
                ref for ref in linked_reqs if not resolve_linked_fr_indices([ref], frs)
            ]
            if unresolved:
                entry["unresolved_requirements"] = unresolved
            deduped_count += len(entry["resolved_requirements"])

            result.append(entry)

        # FR8 / or_f4159e58: this list dedups full FR text — emit the safe
        # metric (counts only, no FR text / body).
        emit_compaction_metric(
            tool_name="okto_pulse_list_api_contracts",
            deduped_count=deduped_count,
        )
        return json.dumps({
            "spec_id": spec_id,
            "total": len(result),
            "api_contracts": result,
        }, default=str)


# ==================== SCREEN MOCKUP TOOLS ====================


SCREEN_MOCKUP_ENTITY_TYPES = ("spec", "ideation", "refinement", "card", "story")


def _validate_screen_mockup_entity_type(entity_type: str) -> str | None:
    if entity_type in SCREEN_MOCKUP_ENTITY_TYPES:
        return None
    allowed = ", ".join(SCREEN_MOCKUP_ENTITY_TYPES)
    return f"Invalid entity_type '{entity_type}'. Must be one of: {allowed}"


async def _load_entity_mockups(db, entity_type: str, entity_id: str):
    """Load an entity and return (entity, screen_mockups, service, update_schema_class) or error string."""
    if entity_type == "spec":
        service = SpecService(db)
        entity = await service.get_spec(entity_id)
        from okto_pulse.core.models.schemas import SpecUpdate
        return entity, service, SpecUpdate
    elif entity_type == "ideation":
        service = IdeationService(db)
        entity = await service.get_ideation(entity_id)
        from okto_pulse.core.models.schemas import IdeationUpdate
        return entity, service, IdeationUpdate
    elif entity_type == "refinement":
        service = RefinementService(db)
        entity = await service.get_refinement(entity_id)
        from okto_pulse.core.models.schemas import RefinementUpdate
        return entity, service, RefinementUpdate
    elif entity_type == "card":
        service = CardService(db)
        entity = await service.get_card(entity_id)
        from okto_pulse.core.models.schemas import CardUpdate
        return entity, service, CardUpdate
    elif entity_type == "story":
        service = StoryService(db)
        entity = await service.get_story(entity_id)
        from okto_pulse.core.models.schemas import StoryUpdate
        return entity, service, StoryUpdate
    return None, None, None


async def _save_entity_mockups(service, entity_type, entity_id, agent_id, screens, UpdateClass):
    """Save screen_mockups back to the entity."""
    if entity_type == "spec":
        _, _err = await _safe_spec_update(service, entity_id, agent_id, UpdateClass(screen_mockups=screens))
        if _err:
            return _err
    elif entity_type == "ideation":
        await service.update_ideation(entity_id, agent_id, UpdateClass(screen_mockups=screens))
    elif entity_type == "refinement":
        await service.update_refinement(entity_id, agent_id, UpdateClass(screen_mockups=screens))
    elif entity_type == "card":
        await service.update_card(entity_id, agent_id, UpdateClass(screen_mockups=screens))
    elif entity_type == "story":
        await service.update_story(entity_id, agent_id, UpdateClass(screen_mockups=screens))


def _sanitize_html(html: str) -> str:
    import re
    sanitized = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    sanitized = re.sub(r"\s+on\w+\s*=\s*[\"'][^\"']*[\"']", "", sanitized, flags=re.IGNORECASE)
    sanitized = re.sub(r"\s+on\w+\s*=\s*\S+", "", sanitized, flags=re.IGNORECASE)
    return sanitized


@mcp.tool()
async def okto_pulse_add_screen_mockup(
    board_id: str,
    entity_id: str,
    title: str,
    entity_type: str = "spec",
    description: str = "",
    screen_type: str = "page",
    html_content: str = "",
) -> str:
    """
    Add a screen mockup to any entity (spec, ideation, refinement, card, or story).
    Screens contain HTML+Tailwind content that renders as visual mockups in the dashboard."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})

    import hashlib
    import time

    screen_id = "sm_" + hashlib.md5(f"{entity_id}{title}{time.time()}".encode()).hexdigest()[:8]

    screen = {
        "id": screen_id,
        "title": title,
        "description": description or None,
        "screen_type": screen_type,
        "html_content": _sanitize_html(html_content),
        "annotations": [],
        "order": 0,
    }

    async with get_db_for_mcp() as db:
        entity, service, UpdateClass = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        screens = list(entity.screen_mockups or [])
        screen["order"] = len(screens)
        screens.append(screen)

        await _save_entity_mockups(service, entity_type, entity_id, ctx.agent_id, screens, UpdateClass)
        await db.commit()

    return json.dumps({"success": True, "entity_type": entity_type, "screen": screen}, default=str)


@mcp.tool()
async def okto_pulse_update_screen_mockup(
    board_id: str,
    entity_id: str,
    screen_id: str,
    entity_type: str = "spec",
    title: str = "",
    description: str = "",
    html_content: str = "",
    screen_type: str = "",
) -> str:
    """
    Update an existing screen mockup's fields on any entity."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})

    async with get_db_for_mcp() as db:
        entity, service, UpdateClass = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        screens = list(entity.screen_mockups or [])
        screen = next((s for s in screens if s.get("id") == screen_id), None)
        if not screen:
            return json.dumps({"error": f"Screen '{screen_id}' not found"})

        if title:
            screen["title"] = title
        if description:
            screen["description"] = description
        if screen_type:
            screen["screen_type"] = screen_type
        if html_content:
            screen["html_content"] = _sanitize_html(html_content)

        await _save_entity_mockups(service, entity_type, entity_id, ctx.agent_id, screens, UpdateClass)
        await db.commit()

    return json.dumps({"success": True, "screen": screen}, default=str)


@mcp.tool()
async def okto_pulse_annotate_mockup(
    board_id: str,
    entity_id: str,
    screen_id: str,
    text: str,
    entity_type: str = "spec",
) -> str:
    """
    Add a design annotation/note to a screen mockup on any entity."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})

    import hashlib
    import time

    ann_id = "an_" + hashlib.md5(f"{screen_id}{text}{time.time()}".encode()).hexdigest()[:8]

    annotation = {
        "id": ann_id,
        "text": text,
        "author_id": ctx.agent_id,
    }

    async with get_db_for_mcp() as db:
        entity, service, UpdateClass = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        screens = list(entity.screen_mockups or [])
        screen = next((s for s in screens if s.get("id") == screen_id), None)
        if not screen:
            return json.dumps({"error": f"Screen '{screen_id}' not found"})

        anns = screen.get("annotations") or []
        anns.append(annotation)
        screen["annotations"] = anns

        await _save_entity_mockups(service, entity_type, entity_id, ctx.agent_id, screens, UpdateClass)
        await db.commit()

    return json.dumps({"success": True, "annotation": annotation})


@mcp.tool()
async def okto_pulse_list_screen_mockups(
    board_id: str, entity_id: str, entity_type: str = "spec",
    screen_type: str = "", offset: int = 0, limit: int = 50
) -> str:
    """
    List screen mockups for any entity with optional filtering and pagination."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})

    limit = min(limit, 200)

    async with get_db_for_mcp() as db:
        entity, service, _ = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        screens = list(entity.screen_mockups or [])
        if screen_type:
            screens = [s for s in screens if s.get("screen_type") == screen_type]

        total = len(screens)
        paginated = screens[offset:offset + limit]

        return json.dumps({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "total": total,
            "offset": offset,
            "limit": limit,
            "screens": paginated,
        }, default=str)


@mcp.tool()
async def okto_pulse_delete_screen_mockup(
    board_id: str, entity_id: str, screen_id: str, entity_type: str = "spec"
) -> str:
    """
    Delete a screen mockup from any entity."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})

    async with get_db_for_mcp() as db:
        entity, service, UpdateClass = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        screens = list(entity.screen_mockups or [])
        original_len = len(screens)
        screens = [s for s in screens if s.get("id") != screen_id]
        if len(screens) == original_len:
            return json.dumps({"error": f"Screen '{screen_id}' not found"})

        await _save_entity_mockups(service, entity_type, entity_id, ctx.agent_id, screens, UpdateClass)
        await db.commit()

    return json.dumps({"success": True, "screen_id": screen_id})


# ============================================================================
# GUIDELINE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_board_guidelines(board_id: str) -> str:
    """
    Get all guidelines for a board, ordered by priority. This is the PRIMARY tool
    for reading board guidelines — call it BEFORE doing any work on a board.

    Returns linked global guidelines and inline board guidelines merged and sorted."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = GuidelineService(db)
        items = await service.get_board_guidelines(board_id, surface="mcp")
        await db.commit()

        return json.dumps({"board_id": board_id, "count": len(items), "guidelines": items}, default=str)


@mcp.tool()
async def okto_pulse_list_guidelines(
    board_id: str, offset: str = "0", limit: str = "50", tag: str = "",
) -> str:
    """
    List global guidelines from the catalog. Use this to browse available guidelines
    that can be linked to boards."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        # Use the board owner as the owner_id for listing
        board = await db.get(Board, board_id)
        if not board:
            return json.dumps({"error": "Board not found"})

        service = GuidelineService(db)
        guidelines = await service.list_guidelines(
            owner_id=board.owner_id,
            offset=int(offset),
            limit=int(limit),
            tag=tag or None,
        )
        await db.commit()

        return json.dumps(
            {
                "count": len(guidelines),
                "guidelines": [
                    {
                        "id": g.id,
                        "title": g.title,
                        "content": g.content,
                        "tags": g.tags,
                        "scope": g.scope,
                        "created_at": g.created_at.isoformat() if g.created_at else None,
                    }
                    for g in guidelines
                ],
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_create_guideline(
    board_id: str, title: str, content: str, tags: list[str] | str = "", scope: str = "global",
) -> str:
    """
    Create a new guideline. If scope is "global", it goes into the catalog and can be
    linked to any board. If scope is "inline", set a board_id to make it board-specific."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE if hasattr(Permissions, 'BOARD_UPDATE') else Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    try:
        tag_list = coerce_to_list_str(tags) or None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    async with get_db_for_mcp() as db:
        board = await db.get(Board, board_id)
        if not board:
            return json.dumps({"error": "Board not found"})

        from okto_pulse.core.models.schemas import GuidelineCreate
        data = GuidelineCreate(
            title=title,
            content=content,
            tags=tag_list,
            scope=scope,
            board_id=board_id if scope == "inline" else None,
        )

        service = GuidelineService(db)
        guideline = await service.create_guideline(owner_id=board.owner_id, data=data)
        await db.commit()

        return json.dumps(
            {
                "id": guideline.id,
                "title": guideline.title,
                "content": guideline.content,
                "tags": guideline.tags,
                "scope": guideline.scope,
                "board_id": guideline.board_id,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_update_guideline(
    board_id: str, guideline_id: str, title: str = "", content: str = "", tags: list[str] | str = "",
) -> str:
    """
    Update a guideline's title, content, or tags."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE if hasattr(Permissions, 'BOARD_UPDATE') else Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    if tags:
        try:
            tags_list = coerce_to_list_str(tags) or None
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    else:
        tags_list = None

    async with get_db_for_mcp() as db:
        board = await db.get(Board, board_id)
        if not board:
            return json.dumps({"error": "Board not found"})

        from okto_pulse.core.models.schemas import GuidelineUpdate
        data = GuidelineUpdate(
            title=title or None,
            content=content or None,
            tags=tags_list,
        )

        service = GuidelineService(db)
        guideline = await service.update_guideline(guideline_id, board.owner_id, data)
        if not guideline:
            return json.dumps({"error": "Guideline not found or not owned by board owner"})
        await db.commit()

        return json.dumps(
            {
                "id": guideline.id,
                "title": guideline.title,
                "content": guideline.content,
                "tags": guideline.tags,
                "scope": guideline.scope,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_guideline(board_id: str, guideline_id: str) -> str:
    """
    Delete a guideline. Also removes all board links."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE if hasattr(Permissions, 'BOARD_UPDATE') else Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        board = await db.get(Board, board_id)
        if not board:
            return json.dumps({"error": "Board not found"})

        service = GuidelineService(db)
        deleted = await service.delete_guideline(guideline_id, board.owner_id)
        if not deleted:
            return json.dumps({"error": "Guideline not found or not owned by board owner"})
        await db.commit()

        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_link_guideline_to_board(
    board_id: str, guideline_id: str, priority: str = "0",
) -> str:
    """
    Link a global guideline to a board so agents see it when loading board guidelines."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE if hasattr(Permissions, 'BOARD_UPDATE') else Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = GuidelineService(db)
        guideline = await service.get_guideline(guideline_id)
        if not guideline:
            return json.dumps({"error": "Guideline not found"})

        link = await service.link_guideline_to_board(board_id, guideline_id, int(priority))
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "board_id": board_id,
                "guideline_id": guideline_id,
                "priority": link.priority,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_unlink_guideline_from_board(board_id: str, guideline_id: str) -> str:
    """
    Unlink a guideline from a board. The guideline itself is not deleted."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE if hasattr(Permissions, 'BOARD_UPDATE') else Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = GuidelineService(db)
        unlinked = await service.unlink_guideline_from_board(board_id, guideline_id)
        if not unlinked:
            return json.dumps({"error": "Link not found"})
        await db.commit()

        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_delete_spec(board_id: str, spec_id: str) -> str:
    """
    Delete a spec. Derived cards are unlinked but not deleted."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        deleted = await service.delete_spec(spec_id, ctx.agent_id)
        await db.commit()

        if not deleted:
            return json.dumps({"error": "Spec not found"})

        return json.dumps({"success": True})


async def _link_card_to_spec_internal(board_id: str, spec_id: str, card_id: str) -> str:
    """Internal helper for link_task target_type='spec'. The card and spec
    must belong to the same board.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        linked = await service.link_card(spec_id, card_id, user_id=ctx.agent_id)
        await db.commit()

        if not linked:
            return json.dumps({"error": "Spec or card not found, or they belong to different boards"})

        return json.dumps({"success": True, "spec_id": spec_id, "card_id": card_id})


# ============================================================================
# SPEC EVALUATION TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_submit_spec_evaluation(
    board_id: str,
    spec_id: str,
    breakdown_completeness: int,
    breakdown_justification: str,
    granularity: int,
    granularity_justification: str,
    dependency_coherence: int,
    dependency_justification: str,
    test_coverage_quality: int,
    test_coverage_justification: str,
    overall_score: int,
    overall_justification: str,
    recommendation: str,
) -> str:
    """
    Submit a qualitative evaluation for a spec in 'validated' status.
    Multiple evaluators can submit independent evaluations."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_EVALUATE)
    if perm_err:
        return _perm_error(perm_err)

    # Caminho de escrita único: SpecService.submit_spec_evaluation — o mesmo
    # consumido pelo gêmeo REST POST /specs/{id}/evaluations (paridade de
    # superfícies; evita drift de validação/shape entre MCP e REST).
    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.critical_context_guard import FullContextGuardError
        from okto_pulse.core.services.main import SpecService

        service = SpecService(db)
        try:
            evaluation = await service.submit_spec_evaluation(
                spec_id,
                ctx.agent_id,
                ctx.agent_name,
                {
                    "breakdown_completeness": breakdown_completeness,
                    "breakdown_justification": breakdown_justification,
                    "granularity": granularity,
                    "granularity_justification": granularity_justification,
                    "dependency_coherence": dependency_coherence,
                    "dependency_justification": dependency_justification,
                    "test_coverage_quality": test_coverage_quality,
                    "test_coverage_justification": test_coverage_justification,
                    "overall_score": overall_score,
                    "overall_justification": overall_justification,
                    "recommendation": recommendation,
                },
                actor_type="agent",
                surface="mcp",
            )
        except FullContextGuardError as exc:
            await db.commit()
            return json.dumps({
                "error": str(exc),
                "reason": exc.reason,
                "decision": exc.decision.audit_details(),
            })
        except ValueError as exc:
            return json.dumps({"error": str(exc)})
        await db.commit()

    return json.dumps({"success": True, "evaluation": evaluation}, default=str)


@mcp.tool()
async def okto_pulse_list_spec_evaluations(board_id: str, spec_id: str) -> str:
    """
    List all qualitative evaluations for a spec, with stale indication."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SpecService
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        evaluations = spec.evaluations or []
        non_stale = [e for e in evaluations if not e.get("stale")]
        approvals = [e for e in non_stale if e.get("recommendation") == "approve"]

        summary = {
            "total": len(evaluations),
            "non_stale": len(non_stale),
            "approvals": len(approvals),
            "rejections": len([e for e in non_stale if e.get("recommendation") == "reject"]),
            "request_changes": len([e for e in non_stale if e.get("recommendation") == "request_changes"]),
            "avg_score_approvals": (
                sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
                if approvals else 0
            ),
            "stale_count": len(evaluations) - len(non_stale),
        }

        # Return summary view (without full dimensions)
        eval_list = [
            {
                "id": e.get("id"),
                "evaluator_id": e.get("evaluator_id"),
                "evaluator_name": e.get("evaluator_name"),
                "evaluator_type": e.get("evaluator_type"),
                "overall_score": e.get("overall_score"),
                "recommendation": e.get("recommendation"),
                "stale": e.get("stale", False),
                "created_at": e.get("created_at"),
            }
            for e in evaluations
        ]

    return json.dumps({"evaluations": eval_list, "summary": summary}, default=str)


@mcp.tool()
async def okto_pulse_get_spec_evaluation(
    board_id: str, spec_id: str, evaluation_id: str
) -> str:
    """
    Get full details of a specific evaluation including all dimensions and justifications."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SpecService
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        for e in (spec.evaluations or []):
            if e.get("id") == evaluation_id:
                return json.dumps({"evaluation": e}, default=str)

    return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})


@mcp.tool()
async def okto_pulse_delete_spec_evaluation(
    board_id: str, spec_id: str, evaluation_id: str
) -> str:
    """
    Delete your own evaluation. Only the author can delete their evaluation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_EVALUATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SpecService
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        evaluations = list(spec.evaluations or [])
        target = None
        for e in evaluations:
            if e.get("id") == evaluation_id:
                target = e
                break

        if not target:
            return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})

        if target.get("evaluator_id") != ctx.agent_id:
            return json.dumps({
                "error": "Cannot delete evaluation: you can only delete your own evaluations"
            })

        evaluations.remove(target)
        spec.evaluations = evaluations
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(spec, "evaluations")
        await db.commit()

    return json.dumps({"success": True, "deleted_evaluation_id": evaluation_id})


# SPEC HISTORY TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_spec_history(board_id: str, spec_id: str, limit: str = "30") -> str:
    """
    Get the detailed change history of a spec. Shows every modification with field-level diffs
    (old value vs new value), who made the change, and when. Use this to understand how a spec
    evolved over time and what exactly was modified at each step."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        entries = await service.list_history(spec_id, int(limit))
        await db.commit()

        return json.dumps(
            {
                "spec_id": spec_id,
                "count": len(entries),
                "history": [
                    {
                        "id": e.id,
                        "action": e.action,
                        "actor_type": e.actor_type,
                        "actor_id": e.actor_id,
                        "actor_name": e.actor_name,
                        "changes": e.changes,
                        "summary": e.summary,
                        "version": e.version,
                        "created_at": e.created_at.isoformat(),
                    }
                    for e in entries
                ],
            },
            default=str,
        )


# ============================================================================
# SPEC Q&A TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_ask_spec_question(board_id: str, spec_id: str, question: str) -> str:
    """
    Ask a question on a spec's Q&A board. Use @Name to direct the question.
    Both humans and agents can ask questions — this is for clarifying spec requirements
    BEFORE work begins on tasks."""
    return await _ask_question_impl(
        board_id, "spec", spec_id, question,
        alias_kind="legacy", tool_name="okto_pulse_ask_spec_question",
    )


@mcp.tool()
async def okto_pulse_ask_spec_choice_question(
    board_id: str,
    spec_id: str,
    question: str,
    options: list[str] | str,
    question_type: str = "choice",
    allow_free_text: str = "false",
    options_json: str = "",
) -> str:
    """
    Ask a choice question (poll/form) on a spec's Q&A board. The respondent picks from predefined options.
    Use this when you need a structured answer — e.g. "Which auth approach?" with options.

options_json (optional, takes precedence): JSON array of option objects, e.g. '[{"label":"A","recommended":true,"tradeoff":"costs more"}]'. When present and non-empty, options is ignored. Each object requires a non-empty label; recommended defaults to false; tradeoff defaults to null.
Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import SpecQAChoiceOption, SpecQACreate

    try:
        parsed_objects = parse_options_json(options_json or None)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options_json: {e}"})

    if parsed_objects is not None:
        choice_list = [
            SpecQAChoiceOption(id=f"opt_{i}", label=obj["label"], recommended=obj["recommended"], tradeoff=obj["tradeoff"])
            for i, obj in enumerate(parsed_objects)
        ]
    else:
        try:
            option_labels = coerce_to_list_str(options)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
        if not option_labels:
            return json.dumps({"error": "At least one option is required"})
        choice_list = [
            SpecQAChoiceOption(id=f"opt_{i}", label=label)
            for i, label in enumerate(option_labels)
        ]

    async with get_db_for_mcp() as db:
        service = SpecQAService(db)
        data = SpecQACreate(
            question=question,
            question_type=question_type if question_type in ("choice", "multi_choice") else "choice",
            choices=choice_list,
            allow_free_text=allow_free_text.lower() == "true",
        )
        qa = await service.create_question(spec_id, ctx.agent_id, data)
        if not qa:
            return json.dumps({"error": "Spec not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="spec_choice_question_added",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"spec_id": spec_id, "question": question[:100], "option_count": len(choice_list)},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "choices": qa.choices,
                    "allow_free_text": qa.allow_free_text,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_answer_spec_question(board_id: str, spec_id: str, qa_id: str, answer: str = "", selected: list[str] | str = "") -> str:
    """
    Answer a question on a spec's Q&A board.
    For text questions, provide answer. For choice questions, provide selected option IDs.

Multi-value params (options/selected): pass a JSON array (preferred — safe for labels containing commas) or a pipe-separated string. Full format rules: okto-pulse://reference/multivalue."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import SpecQAAnswer
    from okto_pulse.core.services import QASelfAnsweringNotAllowedError

    try:
        selected_list = coerce_to_list_str(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    async with get_db_for_mcp() as db:
        service = SpecQAService(db)
        try:
            qa = await service.answer_question(
                qa_id,
                ctx.agent_id,
                SpecQAAnswer(answer=answer or None, selected=selected_list),
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as e:
            await db.commit()
            return json.dumps({"error": e.reason, "detail": str(e)})
        if not qa:
            return json.dumps({"error": "Q&A item not found or invalid selection"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="spec_question_answered",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"spec_id": spec_id, "qa_id": qa_id, "answer": (answer or "")[:100], "selected": selected_list},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "question_type": qa.question_type,
                    "answer": qa.answer,
                    "selected": qa.selected,
                    "asked_by": qa.asked_by,
                    "answered_by": qa.answered_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_get_traceability_report(
    board_id: str,
    ideation_id: str = "",
    spec_id: str = "",
    include_artifacts: str = "false",
) -> str:
    """
    okto_pulse_get_traceability_report — return a consolidated SDLC traceability report:
    ideation → refinement → spec → sprint → card/test/bug → artifacts.

    Use this at the end of an E2E flow to verify whether the agent can answer
    what was implemented in each flow and whether KBs, mockups, architecture,
    tests, bugs, cards, and parent references stayed queryable."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _include_artifacts = _flag_enabled(include_artifacts)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.traceability import (
            TraceabilityReadError,
            build_traceability_report,
        )

        try:
            report = await build_traceability_report(
                db,
                board_id,
                ideation_id=ideation_id,
                spec_id=spec_id,
                include_artifacts=_include_artifacts,
            )
        except TraceabilityReadError as exc:
            return json.dumps({"error": exc.message, "code": exc.code})
        await db.commit()

        # FR8 / or_f4159e58: this response is deduped (bug cards → slim index)
        # and, by default, compacted (artifact bodies → counts/ids/drilldown).
        # Emit the safe metric with counts only — no bodies.
        def _count_traceability_compaction(node: object) -> tuple[int, int]:
            omitted = 0
            deduped = 0
            if isinstance(node, dict):
                if "artifact_summary" in node:
                    omitted += 1  # one entity whose artifact bodies were omitted
                bugs = node.get("bugs")
                if isinstance(bugs, list):
                    deduped += len(bugs)  # each bug deduped from full → slim
                for value in node.values():
                    sub_o, sub_d = _count_traceability_compaction(value)
                    omitted += sub_o
                    deduped += sub_d
            elif isinstance(node, list):
                for item in node:
                    sub_o, sub_d = _count_traceability_compaction(item)
                    omitted += sub_o
                    deduped += sub_d
            return omitted, deduped

        from okto_pulse.core.mcp.payload_compaction import emit_compaction_metric

        omitted_count, deduped_count = _count_traceability_compaction(report)
        emit_compaction_metric(
            tool_name="okto_pulse_get_traceability_report",
            profile="full" if _include_artifacts else "compact",
            omitted_count=omitted_count,
            deduped_count=deduped_count,
        )
        return json.dumps(report, default=str)


# ============================================================================
# SPEC KNOWLEDGE BASE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_spec_knowledge(board_id: str, spec_id: str, knowledge_id: str) -> str:
    """
    Get the full content of a knowledge base item."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecKnowledgeService(db)
        kb = await service.get_knowledge(knowledge_id)
        await db.commit()

        if not kb or kb.spec_id != spec_id:
            return json.dumps({"error": "Knowledge base item not found"})

        return json.dumps(
            {
                "id": kb.id,
                "title": kb.title,
                "description": kb.description,
                "content": kb.content,
                "mime_type": kb.mime_type,
                "created_at": kb.created_at.isoformat(),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_add_spec_knowledge(
    board_id: str,
    spec_id: str,
    title: str,
    content: str = "",
    description: str = "",
    mime_type: str = "text/markdown",
    file_path: str | None = None,
    file_url: str | None = None,
) -> str:
    """
    Add a knowledge base item to a spec. Use this to attach reference documents,
    design docs, API specs, or any context that helps agents understand the spec.

    Provide exactly ONE of: content, file_path, or file_url. Prefer file_path or
    file_url for large documents — the content is loaded server-side and never
    passes through the LLM context, saving tokens."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    resolved_content, err = await _resolve_text_content(
        content=content, file_path=file_path, file_url=file_url
    )
    if err:
        return json.dumps({"error": err})

    from okto_pulse.core.models.schemas import SpecKnowledgeCreate

    async with get_db_for_mcp() as db:
        service = SpecKnowledgeService(db)
        kb_data = SpecKnowledgeCreate(
            title=title,
            description=description or None,
            content=resolved_content,
            mime_type=mime_type,
        )
        kb = await service.create_knowledge(spec_id, ctx.agent_id, kb_data)
        await db.commit()

        if not kb:
            return json.dumps({"error": "Failed to create knowledge base item — spec not found"})

        return json.dumps(
            {
                "success": True,
                "knowledge": {
                    "id": kb.id,
                    "title": kb.title,
                    "mime_type": kb.mime_type,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_spec_knowledge(board_id: str, spec_id: str, knowledge_id: str) -> str:
    """
    Delete a knowledge base item from a spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecKnowledgeService(db)
        kb = await service.get_knowledge(knowledge_id)
        if not kb or kb.spec_id != spec_id:
            return json.dumps({"error": "Knowledge base item not found"})
        await service.delete_knowledge(knowledge_id)
        await db.commit()

        return json.dumps({"success": True})


# ============================================================================
# REFINEMENT SNAPSHOT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_refinement_snapshot(board_id: str, refinement_id: str, version: str) -> str:
    """
    Get the full immutable snapshot of a refinement at a specific version.
    Includes all fields as they were when the refinement was marked 'done',
    plus a snapshot of all Q&A at that point."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        snapshot = await service.get_snapshot(refinement_id, int(version))
        await db.commit()

        if not snapshot:
            return json.dumps({"error": f"Snapshot v{version} not found"})

        return json.dumps(
            {
                "refinement_id": refinement_id,
                "version": snapshot.version,
                "title": snapshot.title,
                "description": snapshot.description,
                "in_scope": snapshot.in_scope,
                "out_of_scope": snapshot.out_of_scope,
                "analysis": snapshot.analysis,
                "decisions": snapshot.decisions,
                "labels": snapshot.labels,
                "qa_snapshot": snapshot.qa_snapshot,
                "created_by": snapshot.created_by,
                "created_at": snapshot.created_at.isoformat(),
            },
            default=str,
        )


# ============================================================================
# REFINEMENT KNOWLEDGE BASE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_refinement_knowledge(board_id: str, refinement_id: str, knowledge_id: str) -> str:
    """
    Get the full content of a refinement knowledge base item."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementKnowledgeService(db)
        kb = await service.get_knowledge(knowledge_id)
        await db.commit()

        if not kb or kb.refinement_id != refinement_id:
            return json.dumps({"error": "Knowledge base item not found"})

        return json.dumps(
            {
                "id": kb.id,
                "title": kb.title,
                "description": kb.description,
                "content": kb.content,
                "mime_type": kb.mime_type,
                "created_at": kb.created_at.isoformat(),
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_add_refinement_knowledge(
    board_id: str,
    refinement_id: str,
    title: str,
    content: str = "",
    description: str = "",
    mime_type: str = "text/markdown",
    file_path: str | None = None,
    file_url: str | None = None,
) -> str:
    """
    Add a knowledge base item to a refinement. Use this to attach reference documents,
    design docs, analysis notes, or any context that helps agents understand the refinement.

    Provide exactly ONE of: content, file_path, or file_url. Prefer file_path or
    file_url for large documents — the content is loaded server-side and never
    passes through the LLM context, saving tokens."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    resolved_content, err = await _resolve_text_content(
        content=content, file_path=file_path, file_url=file_url
    )
    if err:
        return json.dumps({"error": err})

    from okto_pulse.core.models.schemas import RefinementKnowledgeCreate

    async with get_db_for_mcp() as db:
        service = RefinementKnowledgeService(db)
        kb_data = RefinementKnowledgeCreate(
            title=title,
            description=description or None,
            content=resolved_content,
            mime_type=mime_type,
        )
        kb = await service.create_knowledge(refinement_id, ctx.agent_id, kb_data)
        await db.commit()

        if not kb:
            return json.dumps({"error": "Failed to create knowledge base item — refinement not found"})

        return json.dumps(
            {
                "success": True,
                "knowledge": {
                    "id": kb.id,
                    "title": kb.title,
                    "mime_type": kb.mime_type,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_refinement_knowledge(board_id: str, refinement_id: str, knowledge_id: str) -> str:
    """
    Delete a knowledge base item from a refinement."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementKnowledgeService(db)
        kb = await service.get_knowledge(knowledge_id)
        if not kb or kb.refinement_id != refinement_id:
            return json.dumps({"error": "Knowledge base item not found"})
        await service.delete_knowledge(knowledge_id)
        await db.commit()

        return json.dumps({"success": True})


# ============================================================================
# SPRINT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_create_sprint(
    board_id: str,
    spec_id: str,
    title: str,
    description: str = "",
    objective: str = "",
    expected_outcome: str = "",
    lane_type: str = "normal",
    origin_sprint_id: str = "",
    origin_bug_id: str = "",
    test_scenario_ids: list[str] | str = "",
    business_rule_ids: list[str] | str = "",
    start_date: str = "",
    end_date: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Create a new sprint for a spec. Sprints break specs into incremental deliverables."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.infra.permissions import PermissionSet
    perm_err = check_permission(ctx.permissions, "sprint.entity.create")
    if isinstance(ctx.permissions, PermissionSet):
        perm_err = ctx.permissions.check("sprint.entity.create")
    else:
        perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import SprintCreate

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintOperationError, SprintService
        service = SprintService(db)
        try:
            data = SprintCreate(
                title=title, description=description or None, spec_id=spec_id,
                objective=objective or None,
                expected_outcome=expected_outcome or None,
                lane_type=lane_type or "normal",
                origin_sprint_id=origin_sprint_id or None,
                origin_bug_id=origin_bug_id or None,
                test_scenario_ids=coerce_to_list_str(test_scenario_ids) or None,
                business_rule_ids=coerce_to_list_str(business_rule_ids) or None,
                start_date=start_date or None, end_date=end_date or None,
                labels=coerce_to_list_str(labels) or None,
            )
            sprint = await service.create_sprint(board_id, ctx.agent_id, data, skip_ownership_check=True)
            await db.commit()
            if not sprint:
                return json.dumps({"error": "Failed to create sprint (spec not found or wrong board)"})
            return json.dumps({
                "success": True,
                "sprint": {
                    "id": sprint.id,
                    "title": sprint.title,
                    "status": sprint.status.value,
                    "spec_id": sprint.spec_id,
                    "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
                    "origin_sprint_id": sprint.origin_sprint_id,
                    "origin_bug_id": sprint.origin_bug_id,
                    "normal_sprint_created": sprint.normal_sprint_created,
                },
            })
        except SprintOperationError as e:
            return json.dumps({"error": e.code, **e.to_dict()})
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_update_sprint(
    board_id: str,
    sprint_id: str,
    title: str = "",
    description: str = "",
    test_scenario_ids: list[str] | str = "",
    business_rule_ids: list[str] | str = "",
    labels: list[str] | str = "",
    lane_type: str = "",
    origin_sprint_id: str = "",
    origin_bug_id: str = "",
    skip_test_coverage: str = "",
    skip_rules_coverage: str = "",
    skip_qualitative_validation: str = "",
) -> str:
    """
    Update sprint fields."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.models.schemas import SprintUpdate

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        kwargs = {}
        if title:
            kwargs["title"] = title
        if description:
            kwargs["description"] = description
        if test_scenario_ids:
            try:
                kwargs["test_scenario_ids"] = coerce_to_list_str(test_scenario_ids)
            except ValueError as e:
                return json.dumps({"error": f"Invalid test_scenario_ids: {e}"})
        if business_rule_ids:
            try:
                kwargs["business_rule_ids"] = coerce_to_list_str(business_rule_ids)
            except ValueError as e:
                return json.dumps({"error": f"Invalid business_rule_ids: {e}"})
        if labels:
            try:
                kwargs["labels"] = coerce_to_list_str(labels)
            except ValueError as e:
                return json.dumps({"error": f"Invalid labels: {e}"})
        if lane_type:
            kwargs["lane_type"] = lane_type
        if origin_sprint_id:
            kwargs["origin_sprint_id"] = origin_sprint_id
        if origin_bug_id:
            kwargs["origin_bug_id"] = origin_bug_id
        if skip_test_coverage:
            kwargs["skip_test_coverage"] = skip_test_coverage.lower() == "true"
        if skip_rules_coverage:
            kwargs["skip_rules_coverage"] = skip_rules_coverage.lower() == "true"
        if skip_qualitative_validation:
            kwargs["skip_qualitative_validation"] = skip_qualitative_validation.lower() == "true"

        try:
            data = SprintUpdate(**kwargs)
            sprint = await service.update_sprint(sprint_id, ctx.agent_id, data)
            await db.commit()
            if not sprint:
                return json.dumps({"error": "Sprint not found"})
            return json.dumps({
                "success": True,
                "sprint": {
                    "id": sprint.id,
                    "title": sprint.title,
                    "version": sprint.version,
                    "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
                    "origin_sprint_id": sprint.origin_sprint_id,
                    "origin_bug_id": sprint.origin_bug_id,
                    "normal_sprint_created": sprint.normal_sprint_created,
                },
            })
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_move_sprint(
    board_id: str,
    sprint_id: str,
    status: str,
) -> str:
    """
    Move a sprint to a new status. State machine: draft→active→review→closed.
    Gates: draft→active requires cards, active→review requires scoped test coverage, review→closed requires evaluation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.models.db import SprintStatus
    from okto_pulse.core.models.schemas import SprintMove

    try:
        sprint_status = SprintStatus(status)
    except ValueError:
        return json.dumps({"error": f"Invalid status. Must be one of: {[s.value for s in SprintStatus]}"})

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        try:
            sprint = await service.move_sprint(sprint_id, ctx.agent_id, SprintMove(status=sprint_status))
            await db.commit()
            if not sprint:
                return json.dumps({"error": "Sprint not found"})
            return json.dumps({
                "success": True,
                "sprint": {"id": sprint.id, "title": sprint.title, "status": sprint.status.value},
            })
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_get_sprint(board_id: str, sprint_id: str) -> str:
    """
    Get full sprint details including cards, evaluations, and Q&A."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        sprint = await service.get_sprint(sprint_id)
        if not sprint:
            return json.dumps({"error": "Sprint not found"})
        return json.dumps({
            "id": sprint.id, "spec_id": sprint.spec_id, "board_id": sprint.board_id,
            "title": sprint.title, "description": sprint.description,
            "status": sprint.status.value, "spec_version": sprint.spec_version,
            "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
            "origin_sprint_id": sprint.origin_sprint_id,
            "origin_bug_id": sprint.origin_bug_id,
            "normal_sprint_created": sprint.normal_sprint_created,
            "start_date": sprint.start_date.isoformat() if sprint.start_date else None,
            "end_date": sprint.end_date.isoformat() if sprint.end_date else None,
            "test_scenario_ids": sprint.test_scenario_ids,
            "business_rule_ids": sprint.business_rule_ids,
            "evaluations": sprint.evaluations,
            "skip_test_coverage": sprint.skip_test_coverage,
            "skip_rules_coverage": sprint.skip_rules_coverage,
            "skip_qualitative_validation": sprint.skip_qualitative_validation,
            "version": sprint.version, "labels": sprint.labels,
            "cards": [
                {"id": c.id, "title": c.title, "status": c.status.value, "priority": c.priority.value}
                for c in sprint.cards
            ],
            "qa_items": [
                {"id": q.id, "question": q.question, "answer": q.answer, "asked_by": q.asked_by}
                for q in sprint.qa_items
            ],
            "created_by": sprint.created_by,
            "created_at": sprint.created_at.isoformat() if sprint.created_at else None,
        })


@mcp.tool()
async def okto_pulse_get_sprint_context(
    board_id: str,
    sprint_id: str,
    include_spec: str = "true",
) -> str:
    """
    Get the FULL consolidated context of a sprint. Returns sprint data plus
    the parent spec's structured sections (requirements, test scenarios, BRs,
    contracts) for scope resolution and evaluation.

    **Always call this before evaluating, moving, or reviewing a sprint.**"""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_spec = include_spec.lower() in ("true", "1", "yes")

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        sprint = await service.get_sprint(sprint_id)
        await db.commit()

        if not sprint or sprint.board_id != board_id:
            return json.dumps({"error": "Sprint not found"})

        result: dict = {
            "id": sprint.id,
            "spec_id": sprint.spec_id,
            "board_id": sprint.board_id,
            "title": sprint.title,
            "description": sprint.description,
            "objective": getattr(sprint, "objective", None),
            "expected_outcome": getattr(sprint, "expected_outcome", None),
            "status": sprint.status.value,
            "lane_type": sprint.lane_type.value if sprint.lane_type else "normal",
            "origin_sprint_id": sprint.origin_sprint_id,
            "origin_bug_id": sprint.origin_bug_id,
            "normal_sprint_created": sprint.normal_sprint_created,
            "spec_version": sprint.spec_version,
            "version": sprint.version,
            "start_date": sprint.start_date.isoformat() if sprint.start_date else None,
            "end_date": sprint.end_date.isoformat() if sprint.end_date else None,
            "test_scenario_ids": sprint.test_scenario_ids or [],
            "business_rule_ids": sprint.business_rule_ids or [],
            "evaluations": sprint.evaluations or [],
            "skip_test_coverage": sprint.skip_test_coverage,
            "skip_rules_coverage": sprint.skip_rules_coverage,
            "skip_qualitative_validation": sprint.skip_qualitative_validation,
            "labels": sprint.labels or [],
            "cards": [
                {
                    "id": c.id,
                    "title": c.title,
                    "status": c.status.value,
                    "priority": c.priority.value,
                    "card_type": c.card_type.value if c.card_type else "normal",
                    "test_scenario_ids": c.test_scenario_ids or [],
                }
                for c in sprint.cards
            ],
            "qa_items": [
                {"id": q.id, "question": q.question, "answer": q.answer, "asked_by": q.asked_by}
                for q in sprint.qa_items
            ],
            "created_by": sprint.created_by,
            "created_at": sprint.created_at.isoformat() if sprint.created_at else None,
        }

        # Parent spec context for scope resolution
        if _inc_spec and sprint.spec_id:
            spec_service = SpecService(db)
            spec = await spec_service.get_spec(sprint.spec_id)
            await db.commit()

            if spec:
                sprint_card_ids = {c.id for c in sprint.cards}
                spec_ts = spec.test_scenarios or []
                spec_brs = spec.business_rules or []
                spec_trs = spec.technical_requirements or []
                spec_contracts = spec.api_contracts or []
                spec_irs = getattr(spec, "integration_requirements", None) or []
                spec_ors = getattr(spec, "observability_requirements", None) or []

                # Resolve scoped items
                scoped_ts_ids = set(sprint.test_scenario_ids or [])
                scoped_ts = [ts for ts in spec_ts if ts.get("id") in scoped_ts_ids or
                             any(tid in sprint_card_ids for tid in (ts.get("linked_task_ids") or []))]
                scoped_brs_ids = set(sprint.business_rule_ids or [])
                scoped_brs = [br for br in spec_brs if br.get("id") in scoped_brs_ids or
                              any(tid in sprint_card_ids for tid in (br.get("linked_task_ids") or []))]
                scoped_trs = [tr for tr in spec_trs if isinstance(tr, dict) and
                              any(tid in sprint_card_ids for tid in (tr.get("linked_task_ids") or []))]
                scoped_contracts = [c for c in spec_contracts if
                                    any(tid in sprint_card_ids for tid in (c.get("linked_task_ids") or []))]
                scoped_irs = [ir for ir in spec_irs if
                              any(tid in sprint_card_ids for tid in (ir.get("linked_task_ids") or []))]
                scoped_ors = [req for req in spec_ors if
                              any(tid in sprint_card_ids for tid in (req.get("linked_task_ids") or []))]

                result["spec"] = {
                    "id": spec.id,
                    "title": spec.title,
                    "status": spec.status.value,
                    "functional_requirements": spec.functional_requirements or [],
                    "technical_requirements": spec_trs,
                    "acceptance_criteria": spec.acceptance_criteria or [],
                    "test_scenarios": spec_ts,
                    "business_rules": spec_brs,
                    "api_contracts": spec_contracts,
                    "integration_requirements": spec_irs,
                    "observability_requirements": spec_ors,
                }

                result["scoped"] = {
                    "test_scenarios": scoped_ts,
                    "business_rules": scoped_brs,
                    "technical_requirements": scoped_trs,
                    "api_contracts": scoped_contracts,
                    "integration_requirements": scoped_irs,
                    "observability_requirements": scoped_ors,
                }

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_assign_tasks_to_sprint(
    board_id: str,
    sprint_id: str,
    card_ids: list[str] | str,
) -> str:
    """
    Assign cards to a sprint. Cards must belong to the same spec as the sprint."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    try:
        ids = coerce_to_list_str(card_ids)
    except ValueError as e:
        return json.dumps({"error": f"Invalid card_ids: {e}"})
    if not ids:
        return json.dumps({"error": "No card IDs provided"})

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintOperationError, SprintService
        service = SprintService(db)
        try:
            count = await service.assign_tasks(sprint_id, ids, ctx.agent_id)
            await db.commit()
            sprint = await service.get_sprint(sprint_id)
            lane_type = sprint.lane_type.value if sprint else None
            accepted_card_types = (
                ["bug", "test"]
                if lane_type == "hotfix"
                else ["normal", "test", "bug"]
            )
            return json.dumps({
                "success": True,
                "assigned": count,
                "assigned_count": count,
                "lane_type": lane_type,
                "accepted_card_types": accepted_card_types,
            })
        except SprintOperationError as e:
            return json.dumps({"error": e.code, **e.to_dict()})
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_submit_sprint_evaluation(
    board_id: str,
    sprint_id: str,
    breakdown_completeness: int,
    breakdown_justification: str,
    granularity: int,
    granularity_justification: str,
    dependency_coherence: int,
    dependency_justification: str,
    test_coverage_quality: int,
    test_coverage_justification: str,
    overall_score: int,
    overall_justification: str,
    recommendation: str,
) -> str:
    """
    Submit a qualitative evaluation for a sprint in 'review' status."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    if recommendation not in ("approve", "request_changes", "reject"):
        return json.dumps({"error": "recommendation must be: approve, request_changes, or reject"})

    evaluation = {
        "dimensions": {
            "breakdown_completeness": {"score": breakdown_completeness, "justification": breakdown_justification},
            "granularity": {"score": granularity, "justification": granularity_justification},
            "dependency_coherence": {"score": dependency_coherence, "justification": dependency_justification},
            "test_coverage_quality": {"score": test_coverage_quality, "justification": test_coverage_justification},
        },
        "overall_score": overall_score,
        "overall_justification": overall_justification,
        "recommendation": recommendation,
    }

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        try:
            sprint = await service.submit_evaluation(sprint_id, ctx.agent_id, evaluation)
            await db.commit()
            if not sprint:
                return json.dumps({"error": "Sprint not found"})
            last_eval = sprint.evaluations[-1] if sprint.evaluations else {}
            return json.dumps({
                "success": True,
                "evaluation_id": last_eval.get("id"),
                "overall_score": overall_score,
                "recommendation": recommendation,
            })
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_list_sprint_evaluations(board_id: str, sprint_id: str) -> str:
    """
    List all evaluations for a sprint."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.models.db import Sprint
        sprint = await db.get(Sprint, sprint_id)
        if not sprint:
            return json.dumps({"error": "Sprint not found"})
        evaluations = sprint.evaluations or []
        non_stale = [e for e in evaluations if not e.get("stale")]
        approvals = [e for e in non_stale if e.get("recommendation") == "approve"]
        return json.dumps({
            "sprint_id": sprint_id, "total": len(evaluations),
            "non_stale": len(non_stale), "approvals": len(approvals),
            "avg_score": (sum(e.get("overall_score", 0) for e in approvals) / len(approvals)) if approvals else 0,
            "evaluations": evaluations,
        })


@mcp.tool()
async def okto_pulse_get_sprint_evaluation(
    board_id: str, sprint_id: str, evaluation_id: str,
) -> str:
    """
    Get full details of a specific sprint evaluation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.models.db import Sprint
        sprint = await db.get(Sprint, sprint_id)
        if not sprint:
            return json.dumps({"error": "Sprint not found"})
        for e in (sprint.evaluations or []):
            if e.get("id") == evaluation_id:
                return json.dumps(e)
        return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})


@mcp.tool()
async def okto_pulse_delete_sprint_evaluation(
    board_id: str, sprint_id: str, evaluation_id: str,
) -> str:
    """
    Delete your own sprint evaluation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.models.db import Sprint
        sprint = await db.get(Sprint, sprint_id)
        if not sprint:
            return json.dumps({"error": "Sprint not found"})
        evaluations = list(sprint.evaluations or [])
        target = None
        for e in evaluations:
            if e.get("id") == evaluation_id:
                target = e
                break
        if not target:
            return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})
        if target.get("evaluator_id") != ctx.agent_id:
            return json.dumps({"error": "You can only delete your own evaluations"})
        evaluations.remove(target)
        sprint.evaluations = evaluations
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(sprint, "evaluations")
        await db.commit()
        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_ask_sprint_question(
    board_id: str,
    sprint_id: str,
    question: str,
) -> str:
    """
    Ask a question on a sprint."""
    return await _ask_question_impl(
        board_id, "sprint", sprint_id, question,
        alias_kind="legacy", tool_name="okto_pulse_ask_sprint_question",
    )


@mcp.tool()
async def okto_pulse_answer_sprint_question(
    board_id: str,
    sprint_id: str,
    qa_id: str,
    answer: str,
) -> str:
    """
    Answer a question on a sprint."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintQAService
        from okto_pulse.core.services import QASelfAnsweringNotAllowedError
        service = SprintQAService(db)
        try:
            qa = await service.answer_question(
                qa_id, ctx.agent_id, answer, actor_type="agent", surface="mcp"
            )
        except QASelfAnsweringNotAllowedError as e:
            await db.commit()
            return json.dumps({"error": e.reason, "detail": str(e)})
        await db.commit()
        if not qa:
            return json.dumps({"error": "Q&A item not found"})
        return json.dumps({
            "success": True,
            "qa": {"id": qa.id, "question": qa.question, "answer": qa.answer, "answered_by": qa.answered_by},
        })


@mcp.tool()
async def okto_pulse_delete_spec_question(board_id: str, spec_id: str, qa_id: str) -> str:
    """
    Delete a Q&A item from a spec. Use this to invalidate outdated questions
    or remove resolved clarifications that no longer apply."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecQAService(db)
        deleted = await service.delete_question(qa_id)
        if not deleted:
            return json.dumps({"error": "Q&A item not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="spec_question_deleted",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"spec_id": spec_id, "qa_id": qa_id},
        )
        await db.commit()
        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_delete_ideation_question(board_id: str, ideation_id: str, qa_id: str) -> str:
    """
    Delete a Q&A item from an ideation. Use this to invalidate outdated questions
    or remove resolved clarifications that no longer apply."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import IdeationQAService
        service = IdeationQAService(db)
        deleted = await service.delete_question(qa_id)
        if not deleted:
            return json.dumps({"error": "Q&A item not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="ideation_question_deleted",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"ideation_id": ideation_id, "qa_id": qa_id},
        )
        await db.commit()
        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_delete_refinement_question(board_id: str, refinement_id: str, qa_id: str) -> str:
    """
    Delete a Q&A item from a refinement. Use this to invalidate outdated questions
    or remove resolved clarifications that no longer apply."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import RefinementQAService
        service = RefinementQAService(db)
        deleted = await service.delete_question(qa_id)
        if not deleted:
            return json.dumps({"error": "Q&A item not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="refinement_question_deleted",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"refinement_id": refinement_id, "qa_id": qa_id},
        )
        await db.commit()
        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_delete_sprint_question(board_id: str, sprint_id: str, qa_id: str) -> str:
    """
    Delete a Q&A item from a sprint. Use this to invalidate outdated questions
    or remove resolved clarifications that no longer apply."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintQAService
        service = SprintQAService(db)
        deleted = await service.delete_question(qa_id)
        if not deleted:
            return json.dumps({"error": "Q&A item not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="sprint_question_deleted",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"sprint_id": sprint_id, "qa_id": qa_id},
        )
        await db.commit()
        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_suggest_sprints(
    board_id: str,
    spec_id: str,
    threshold: int = 8,
) -> str:
    """
    Suggest a sprint breakdown for a spec based on tasks, FRs, and dependencies.
    Does NOT create sprints — returns suggestions for review."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        try:
            suggestions = await service.suggest_sprints(spec_id, threshold)
            return json.dumps({"suggestions": suggestions, "count": len(suggestions)})
        except ValueError as e:
            return json.dumps({"error": str(e)})


# ============================================================================
# TASK VALIDATION TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_submit_task_validation(
    board_id: str,
    card_id: str,
    confidence: int,
    confidence_justification: str,
    estimated_completeness: int,
    completeness_justification: str,
    estimated_drift: int,
    drift_justification: str,
    general_justification: str,
    recommendation: str,
) -> str:
    """
    Submit a task validation for a card in 'validation' status.

    Evaluates the implementation quality of a completed task against three
    dimensions: confidence, completeness, and drift. The system applies
    threshold checks (resolved from sprint → spec → board hierarchy) and
    automatically routes the card: success → done; failed remains in
    validation so the validator feedback stays visible and the executor can
    decide whether to move the card back for rework."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "card.validation.submit")
    if perm_err:
        return _perm_error(perm_err)

    if recommendation not in ("approve", "reject"):
        return json.dumps({"error": "recommendation must be: approve or reject"})

    # Validate scores
    for name, score in [
        ("confidence", confidence),
        ("estimated_completeness", estimated_completeness),
        ("estimated_drift", estimated_drift),
    ]:
        if not (0 <= score <= 100):
            return json.dumps({"error": f"{name} must be between 0 and 100"})

    data = {
        "confidence": confidence,
        "confidence_justification": confidence_justification,
        "estimated_completeness": estimated_completeness,
        "completeness_justification": completeness_justification,
        "estimated_drift": estimated_drift,
        "drift_justification": drift_justification,
        "general_justification": general_justification,
        "recommendation": recommendation,
    }

    async with get_db_for_mcp() as db:
        card_service = CardService(db)
        try:
            result = await card_service.submit_task_validation(
                card_id, ctx.agent_id, ctx.agent_name, data
            )
            await db.commit()
            return json.dumps(result, default=str)
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_list_task_validations(board_id: str, card_id: str) -> str:
    """
    List all validations for a task card in reverse chronological order.

    Useful for understanding the validation history of a card, especially
    cards that have been through multiple validation cycles (failed → reworked → resubmitted)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "card.validation.read")
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        card_service = CardService(db)
        try:
            validations = await card_service.list_task_validations(card_id)
            await db.commit()
            return json.dumps({
                "card_id": card_id,
                "total": len(validations),
                "validations": validations,
            }, default=str)
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_get_task_validation(
    board_id: str, card_id: str, validation_id: str,
) -> str:
    """
    Get full details of a specific task validation entry."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "card.validation.read")
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        card_service = CardService(db)
        try:
            validation = await card_service.get_task_validation(card_id, validation_id)
            await db.commit()
            if not validation:
                return json.dumps({"error": f"Validation '{validation_id}' not found"})
            return json.dumps(validation, default=str)
        except ValueError as e:
            return json.dumps({"error": str(e)})


# ============================================================================
# SPEC VALIDATION GATE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_submit_spec_validation(
    board_id: str,
    spec_id: str,
    completeness: int,
    completeness_justification: str,
    assertiveness: int,
    assertiveness_justification: str,
    ambiguity: int,
    ambiguity_justification: str,
    general_justification: str,
    recommendation: str,
) -> str:
    """Submit a Spec Validation Gate record for a spec in 'approved' status — a semantic
quality gate that runs AFTER the deterministic coverage gates (AC/FR/TR/Contract).
Coverage runs first; if any fails the submit is rejected with the violation. Outcome
is FAILED if any threshold is violated or recommendation=reject, SUCCESS only if all
thresholds pass AND recommendation=approve. On SUCCESS the spec is atomically
promoted approved->validated and content-locked. ANTI-PATTERN WARNING: inflating
scores to pass the gate is a grave violation — if outcome=failed, iterate on content
(scenarios, BRs, TRs) rather than just raising numbers. Full details:
okto-pulse://reference/tool-docs/spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "spec.validation.submit")
    if perm_err:
        return _perm_error(perm_err)

    if recommendation not in ("approve", "reject"):
        return json.dumps({"error": "recommendation must be: approve or reject"})

    for name, score in [
        ("completeness", completeness),
        ("assertiveness", assertiveness),
        ("ambiguity", ambiguity),
    ]:
        if not (0 <= score <= 100):
            return json.dumps({"error": f"{name} must be between 0 and 100"})

    # Length checks (Pydantic will re-validate but fail fast here)
    if len(completeness_justification.strip()) < 10:
        return json.dumps({"error": "completeness_justification must be at least 10 characters"})
    if len(assertiveness_justification.strip()) < 10:
        return json.dumps({"error": "assertiveness_justification must be at least 10 characters"})
    if len(ambiguity_justification.strip()) < 10:
        return json.dumps({"error": "ambiguity_justification must be at least 10 characters"})
    if len(general_justification.strip()) < 20:
        return json.dumps({"error": "general_justification must be at least 20 characters"})

    data = {
        "completeness": completeness,
        "completeness_justification": completeness_justification,
        "assertiveness": assertiveness,
        "assertiveness_justification": assertiveness_justification,
        "ambiguity": ambiguity,
        "ambiguity_justification": ambiguity_justification,
        "general_justification": general_justification,
        "recommendation": recommendation,
    }

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        try:
            result = await spec_service.submit_spec_validation(
                spec_id, ctx.agent_id, ctx.agent_name, data
            )
            await db.commit()
            return json.dumps(result, default=str)
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_list_spec_validations(board_id: str, spec_id: str) -> str:
    """
    List all Spec Validation Gate records in reverse chronological order.

    Useful for understanding why a spec was validated (or failed). Each record
    includes the 3 scores, justifications, outcome, threshold violations, and
    a resolved_thresholds snapshot of what was in effect when the submit happened.
    The record currently pointed to by current_validation_id has active=true."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "spec.validation.read")
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        spec_service = SpecService(db)
        try:
            result = await spec_service.list_spec_validations(spec_id)
            await db.commit()
            return json.dumps({
                "spec_id": spec_id,
                **result,
            }, default=str)
        except ValueError as e:
            return json.dumps({"error": str(e)})


# ============================================================================
# KG CONSOLIDATION PRIMITIVES (MVP Fase 0)
# ============================================================================

from okto_pulse.core.mcp.kg_tools import register_kg_tools as _register_kg_tools  # noqa: E402
from okto_pulse.core.mcp.kg_query_tools import register_kg_query_tools as _register_kg_query_tools  # noqa: E402

_register_kg_tools(mcp, get_agent=_get_authenticated_agent, get_db=get_db_for_mcp)
_register_kg_query_tools(mcp, get_agent=_get_authenticated_agent, get_db=get_db_for_mcp)

from okto_pulse.core.mcp.kg_power_tools import register_kg_power_tools as _register_kg_power_tools  # noqa: E402
_register_kg_power_tools(mcp, get_agent=_get_authenticated_agent, get_db=get_db_for_mcp)


# ============================================================================
# KG HEALTH (spec 20f67c2a — Ideação #5, FR2)
# ============================================================================


@mcp.tool()
async def okto_pulse_kg_health(board_id: str, profile: str = "summary") -> str:
    """Snapshot of a board's KG health (gemelar do REST GET /api/v1/kg/health). Default
profile=summary returns the slim stop-rule fields an agent needs before a KG
mutation — graph_state, discovery_state, overall_state, metric_status,
classification_reason, correlation_id, memory_pressure_status, recent_events — plus
operational scalars (queue_depth, dead_letter_count, total_nodes, default_score_ratio,
avg_relevance, contradict_warn_count, last_tick_status), decay_scheduler_diagnostics,
and storage_footprint_proxy. Scheduler debt is operational debt and does not by
itself require graph recovery. Verbose diagnostics omitted; pass profile=full
(or legacy) for the complete dashboard payload. Full guide:
okto-pulse://reference/tool-docs/kg."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.services.kg_health_service import (
        BoardNotFoundError,
        get_kg_health,
    )
    from okto_pulse.core.mcp.kg_query_safety import KGHealthMCPProjection

    try:
        async with get_db_for_mcp() as db:
            data = await get_kg_health(board_id, db)
    except BoardNotFoundError as exc:
        return json.dumps({"error": str(exc)})
    # FR4: slim default projection — keep the stop-rule fields, omit verbose
    # diagnostics until profile=full/legacy is requested.
    data = KGHealthMCPProjection().project(data, profile=profile)
    return json.dumps(data, default=str)


# ============================================================================
# KG ORPHAN INTEGRITY (spec KG-ZO-02 — FR6/TR4)
# ============================================================================


def _kg_orphan_graph_unavailable_payload(board_id: str, exc: Exception) -> dict[str, Any]:
    return {
        "error": "kg_orphan_graph_unavailable",
        "board_id": board_id,
        "error_type": type(exc).__name__,
        "operator_action": "inspect_kg_health",
    }


async def _kg_orphan_backfill_health_refusal(board_id: str) -> dict[str, Any] | None:
    from okto_pulse.core.services.kg_health_service import get_kg_health

    async with get_db_for_mcp() as db:
        health = await get_kg_health(board_id, db)
    state = str(health.get("overall_state") or health.get("graph_state") or "")
    if state in {"recovery_needed", "quarantined"}:
        return {
            "error": "kg_orphan_backfill_refused_by_health",
            "board_id": board_id,
            "overall_state": health.get("overall_state"),
            "graph_state": health.get("graph_state"),
            "operator_action": "inspect_kg_health_recovery_flow",
        }
    return None


@mcp.tool()
async def okto_pulse_kg_orphan_report(
    board_id: str,
    generation_id: str | None = None,
    limit: int = 25,
) -> str:
    """
    Return a bounded safe orphan-node report for a board KG.

    The payload intentionally contains only safe identifiers and aggregate
    counts: board_id, generation_id, orphan_count_by_type, safe samples,
    unresolved_reasons, backfill_summary and correlation_id. Raw node text,
    embeddings, prompts and payload bodies are never returned.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.kg.orphan_integrity import (
        DEFAULT_ORPHAN_SAMPLE_LIMIT,
        MAX_ORPHAN_SAMPLE_LIMIT,
        OrphanNodeScanner,
    )

    bounded_limit = max(
        0,
        min(
            int(limit or DEFAULT_ORPHAN_SAMPLE_LIMIT),
            MAX_ORPHAN_SAMPLE_LIMIT,
        ),
    )
    try:
        report = OrphanNodeScanner().scan(
            board_id=board_id,
            generation_id=generation_id,
            limit=bounded_limit,
        )
    except Exception as exc:
        return json.dumps(_kg_orphan_graph_unavailable_payload(board_id, exc))

    payload = report.to_safe_dict()
    payload["backfill_summary"] = {
        "status": "not_run",
        "dry_run": None,
        "detected": None,
        "connected": None,
        "noop": None,
        "unresolved": None,
        "ambiguous": None,
        "semantic_pending": None,
    }
    return json.dumps(payload, default=str)


@mcp.tool()
async def okto_pulse_kg_orphan_backfill(
    board_id: str,
    generation_id: str | None = None,
    dry_run: bool = True,
    node_ids: list[str] | str = "",
    limit: int = 25,
) -> str:
    """
    Run explicit orphan backfill for structurally resolvable nodes.

    Defaults to dry_run=True. node_ids accepts the standard MCP multi-value
    format: JSON array, native list, or pipe-separated string. Backfill is
    refused when KG Health is recovery_needed/quarantined so operators use the
    recovery flow instead of mutating a degraded graph.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        parsed_node_ids = coerce_to_list_str(node_ids) or None
    except ValueError as exc:
        return json.dumps({
            "error": "invalid_node_ids",
            "reason": str(exc),
            "expected_format": "JSON array, native list, or pipe-separated string",
        })

    try:
        refusal = await _kg_orphan_backfill_health_refusal(board_id)
    except Exception as exc:
        return json.dumps(_kg_orphan_graph_unavailable_payload(board_id, exc))
    if refusal is not None:
        return json.dumps(refusal, default=str)

    from okto_pulse.core.kg.orphan_integrity import (
        DEFAULT_ORPHAN_SAMPLE_LIMIT,
        MAX_ORPHAN_SAMPLE_LIMIT,
        OrphanBackfillReconciler,
    )

    bounded_limit = max(
        0,
        min(
            int(limit or DEFAULT_ORPHAN_SAMPLE_LIMIT),
            MAX_ORPHAN_SAMPLE_LIMIT,
        ),
    )
    try:
        result = OrphanBackfillReconciler().run(
            board_id=board_id,
            generation_id=generation_id,
            dry_run=dry_run,
            node_ids=parsed_node_ids,
            limit=bounded_limit,
        )
    except Exception as exc:
        return json.dumps(_kg_orphan_graph_unavailable_payload(board_id, exc))

    return json.dumps({
        "board_id": board_id,
        "generation_id": generation_id,
        "dry_run": dry_run,
        "backfill_summary": result.to_safe_dict(),
        "correlation_id": result.correlation_id,
    }, default=str)


# ============================================================================
# DEAD LETTER INSPECTOR (spec ed17b1fe — Wave 2 NC 1ede3471)
# ============================================================================


@mcp.tool()
async def okto_pulse_kg_dead_letter_list(
    board_id: str,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List dead-lettered consolidation rows.

    Use this when `okto_pulse_kg_health` reports `dead_letter_count > 0`
    and you need to inspect which artifacts failed, what error repeated, and
    how many attempts were made. Each row includes the full `errors` array:
    one entry per attempt with error_type, message, occurred_at, and optional
    traceback.

    After fixing the root cause (schema migration, WAL recovery, code fix, or
    transient lock contention), call `okto_pulse_kg_dead_letter_reprocess` to
    move selected rows back to the consolidation queue."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.services.dead_letter_inspector_service import (
        list_dead_letter_rows,
    )

    async with get_db_for_mcp() as db:
        data = await list_dead_letter_rows(
            db, board_id, limit=limit, offset=offset,
        )
    return json.dumps(data, default=str)


@mcp.tool()
async def okto_pulse_kg_dead_letter_reprocess(
    board_id: str,
    dead_letter_ids: list[str] | str = "",
    limit: int = 50,
    process_now: str = "true",
) -> str:
    """
    okto_pulse_kg_dead_letter_reprocess — requeue dead-lettered KG
    consolidation rows after the root cause is fixed.

    Use this after `okto_pulse_kg_migrate_schema`, WAL recovery, or a code fix
    when DLQ rows should be retried. The tool is idempotent: if a matching
    pending queue row already exists for the same board/artifact, it resets that
    row and removes the DLQ entry instead of creating duplicates."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "kg.admin.historical_consolidation")
    if perm_err:
        return _perm_error(perm_err)

    try:
        ids = coerce_to_list_str(dead_letter_ids) if dead_letter_ids else []
    except ValueError as exc:
        return json.dumps({"error": f"Invalid dead_letter_ids: {exc}"})

    from okto_pulse.core.services.dead_letter_inspector_service import (
        reprocess_dead_letter_rows,
    )

    async with get_db_for_mcp() as db:
        data = await reprocess_dead_letter_rows(
            db,
            board_id,
            dead_letter_ids=ids or None,
            limit=limit,
        )
        await db.commit()

    if _flag_enabled(process_now):
        from okto_pulse.core.kg.workers.consolidation import (
            get_consolidation_worker,
            signal_consolidation_worker,
        )

        worker = get_consolidation_worker(_mcp_session_factory)
        signal_consolidation_worker()
        data["worker_running"] = worker.is_running
        if worker.is_running:
            data["processed_now_count"] = 0
            data["process_now_mode"] = "signalled_singleton"
        else:
            # MCP can be hosted without the background startup hook in
            # tests or ad-hoc tooling. In that case run one batch through
            # the singleton object instead of creating a competing worker.
            data["processed_now_count"] = await worker.process_batch()
            data["process_now_mode"] = "singleton_direct_batch"

    return json.dumps(data, default=str)


# ============================================================================
# SCHEMA MIGRATION SELF-HEAL (spec 818748f2 — FR5)
# ============================================================================


@mcp.tool()
async def okto_pulse_kg_migrate_schema(
    board_id: str = "",
    all_boards: bool = False,
) -> str:
    """
    Force-apply schema migrations to fix legacy boards (board pre v0.3.2)
    — gemelar do REST POST /api/v1/kg/{board_id}/migrate-schema.

    Use quando consolidation falha com `Binder exception: Cannot find
    property X for n` — geralmente significa que ALTER ADD para schema
    column foi missed em board bootstrapped antes daquela versão.

    Idempotente: re-rodar em board já migrado retorna `migrated=true`
    com `columns_added` vazio (no-op).

    NUNCA delete `graph.lbug` para "consertar" — destruiria todo o KG
    do board. Use esta tool em vez disso."""
    if not board_id and not all_boards:
        return json.dumps({"error": "missing_board_or_all_boards"})

    from okto_pulse.core.kg.schema import migrate_schema_for_board

    if all_boards:
        # Iterar todos os boards conhecidos via SQLite.
        from sqlalchemy import select as _select
        from okto_pulse.core.models.db import Board as _Board

        results: list[dict[str, Any]] = []
        async with get_db_for_mcp() as db:
            rows = await db.execute(_select(_Board.id, _Board.name))
            board_pairs = list(rows.all())
        for bid, _bname in board_pairs:
            try:
                summary = migrate_schema_for_board(bid)
                results.append(summary)
            except Exception as exc:
                results.append({
                    "board_id": bid,
                    "migrated": False,
                    "columns_added": {},
                    "errors": [f"unhandled: {exc}"],
                    "duration_ms": 0,
                })
        return json.dumps({"results": results}, default=str)

    # Single board path
    summary = migrate_schema_for_board(board_id)
    return json.dumps(summary, default=str)


# ============================================================================
# KG TICK CONTROLLABILITY (spec 54399628 — Wave 2 NC f9732afc)
# ============================================================================

# E2E spec c2115d15 — TS-E descobriu NameError "name 'logger' is not defined"
# em okto_pulse_kg_tick_run_now: a função usa logger.info mas o módulo só
# definia loggers nomeados específicos (_XML_SAFETY_LOGGER, _evidence_logger).
# Logger dedicado para audit do tick.
_tick_logger = logging.getLogger("okto_pulse.mcp.tick")


@mcp.tool()
async def okto_pulse_kg_tick_run_now(
    board_id: str = "",
    force_full_rebuild: bool = False,
) -> str:
    """
    Trigger the KG decay tick manually — gemelar do REST POST /api/v1/kg/tick/run-now.

    Dispara um tick imediato sem esperar o cron periódico. Operador agente
    chama esta ferramenta quando: (a) acabou de reescalar nodes em massa
    e quer scoring fresh imediato, (b) detectou que `default_score_ratio`
    está acima de 0.7 e suspeita de stale ranking, (c) está debugando
    scoring de um board específico (passe `board_id`).

    Use `force_full_rebuild=true` para zerar `last_recomputed_at` antes
    do tick (ignora staleness threshold) — útil para boards 0.3.x cujos
    nodes herdaram defaults sem benefício do tick. SOMENTE per-trigger;
    NUNCA é setting persistido para evitar full-rebuild noturno acidental.

    Concurrent calls (cron + manual OU duas chamadas manuais) recebem
    erro `tick_already_running` — primeiro a chegar ganha o advisory lock."""
    # Per-board scope auth: when board_id provided, validate access.
    if board_id:
        ctx = await _get_agent_ctx(board_id)
        if ctx is None:
            return _auth_error()
        triggered_by = ctx.agent.id if hasattr(ctx, "agent") else "agent-mcp"
    else:
        # Global scope — allow any authenticated agent (no per-board check).
        triggered_by = "agent-mcp-global"

    from okto_pulse.core.kg.workers.advisory_lock import get_async_lock

    lock = get_async_lock("kg_daily_tick", "global")
    if lock.locked():
        return json.dumps({
            "error": "tick_already_running",
            "message": "Tick already running, retry shortly",
        })

    # F17 admission gate (gemelar): refuse a degraded concrete board with the
    # SAME structured graph_recovery_needed refusal as the REST endpoint, via the
    # SAME shared _refuse_tick_if_degraded gate (one predicate, no MCP-side
    # duplication). The MCP path owns no request session, so probe under a
    # short-lived one. Runs after the lock check, before tick_id allocation.
    if board_id:
        from okto_pulse.core.api.kg_tick import _refuse_tick_if_degraded
        from okto_pulse.core.infra.database import get_session_factory

        async with get_session_factory()() as _health_session:
            refusal = await _refuse_tick_if_degraded(board_id, _health_session)
        if refusal is not None:
            return json.dumps(refusal)

    import uuid as _uuid
    from datetime import datetime, timezone

    from okto_pulse.core.api.kg_tick import _dispatch_manual_tick

    tick_id = str(_uuid.uuid4())
    scheduled_at = datetime.now(timezone.utc).isoformat()

    _tick_logger.info(
        "kg.tick.manual_triggered tick_id=%s user=%s board=%s force=%s source=mcp",
        tick_id, triggered_by, board_id or None, force_full_rebuild,
        extra={
            "event": "kg.tick.manual_triggered",
            "tick_id": tick_id,
            "triggered_by_user_id": triggered_by,
            "board_id": board_id or None,
            "force_full_rebuild": force_full_rebuild,
            "source": "mcp",
        },
    )

    try:
        await _dispatch_manual_tick(
            tick_id=tick_id,
            board_id=board_id or None,
            force_full_rebuild=force_full_rebuild,
        )
    except Exception as exc:
        _tick_logger.error(
            "kg.tick.manual_schedule_failed tick_id=%s err=%s source=mcp",
            tick_id, exc,
            extra={
                "event": "kg.tick.manual_schedule_failed",
                "tick_id": tick_id,
                "board_id": board_id or None,
                "force_full_rebuild": force_full_rebuild,
                "source": "mcp",
                "error": str(exc),
            },
        )
        return json.dumps({
            "error": "tick_schedule_failed",
            "message": (
                "Failed to persist the KG tick event. "
                "No background tick was scheduled."
            ),
            "detail": str(exc),
        })

    return json.dumps({
        "tick_id": tick_id,
        "status": "running",
        "scheduled_at": scheduled_at,
    })


# ============================================================================
# KG REBUILD FAMILY (spec R2a 959115c0 — IMPL-3)
#
# Three MCP twins for the REST /kg/rebuild/{preflight,confirm,run} lane.
# Each tool:
#   * authenticates + scopes to the board via _get_agent_ctx (same as tick)
#   * calls the rebuild-scoped admission predicate from kg_rebuild.py
#     (_refuse_rebuild_if_quarantined) — quarantined → refuse, recovery_needed → pass
#   * delegates 100 % of business logic to the shared REST service objects
#     (RebuildPreflightService / RebuildConfirmationStore / KGRebuildService)
#   * resolves base_dir via default_rebuild_base_dir() (same constant as REST)
#   * serialises all results to JSON strings (MCP transport contract)
#
# Pattern: okto_pulse_kg_tick_run_now (lines 12457-12564) — twin structure,
# _get_agent_ctx auth, run_in_threadpool for sync service calls.
# ============================================================================

_rebuild_logger = logging.getLogger("okto_pulse.mcp.rebuild")


@mcp.tool()
async def okto_pulse_kg_rebuild_preflight(
    board_id: str,
) -> str:
    """
    Run the KG rebuild preflight for a board — gemelar do REST POST /api/v1/kg/rebuild/preflight.

    Executa a checagem pré-rebuild (read-only, TR13): enumera sources reais via
    BoardSourceStore (SQLite), classifica o estado de saúde do KG e persiste
    o manifesto imutável necessário para /confirm.

    Admission gate (FR8): recusa com rebuild_refused_quarantined quando
    graph_state == 'quarantined'. recovery_needed É ADMITIDO — rebuild é a
    saída prescrita desse estado.

    Retorna o mesmo payload do REST: outcome, action_required, base_state,
    eligible_source_count, preflight_hash, manifest_ref, source_set_hash.
    Passe manifest_ref + preflight_hash para okto_pulse_kg_rebuild_confirm.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from starlette.concurrency import run_in_threadpool

    from okto_pulse.core.api.kg_rebuild import (
        _REBUILD_BASE_DIR,
        _build_source_store,
        _refuse_rebuild_if_quarantined,
    )
    from okto_pulse.core.infra.database import get_session_factory
    from okto_pulse.core.kg.rebuild_preflight import (
        RebuildHealthSummary,
        RebuildPreflightService,
        RebuildSourceSummary,
    )
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        RebuildSourceEnumerator,
    )
    from okto_pulse.core.services.kg_health_service import get_kg_health

    # Admission gate — async probe under a short-lived session.
    async with get_session_factory()() as _health_session:
        refusal = await _refuse_rebuild_if_quarantined(board_id, _health_session)
        if refusal is not None:
            return json.dumps(refusal)

        # FR9 — real health probe (same session, no extra round-trip).
        _raw_health = await get_kg_health(board_id, _health_session)

    def health_probe(_bid: str) -> RebuildHealthSummary:
        return RebuildHealthSummary(
            base_state=_raw_health.get("graph_state", "healthy"),
            metric_status=_raw_health.get("metric_status", "unavailable"),
            current_kg_generation_id=_raw_health.get("current_kg_generation_id"),
        )

    try:
        enumerator = RebuildSourceEnumerator(source_store=_build_source_store())
        source_set = await run_in_threadpool(enumerator.enumerate, board_id=board_id)
    except Exception as exc:
        _rebuild_logger.error("kg.rebuild.preflight.enumerate_failed board=%s err=%s", board_id, exc)
        return json.dumps({"error": "preflight_enumerate_failed", "detail": str(exc)})

    def source_probe(_bid: str) -> RebuildSourceSummary:
        return RebuildSourceSummary(
            eligible_count=source_set.eligible_count,
            skipped_cancelled_count=source_set.skipped_cancelled_count,
            has_non_deterministic_inputs=source_set.has_non_deterministic_inputs,
        )

    service = RebuildPreflightService(
        source_probe=source_probe,
        health_probe=health_probe,
    )
    try:
        result = await run_in_threadpool(service.run, board_id=board_id)
    except Exception as exc:
        _rebuild_logger.error("kg.rebuild.preflight.service_failed board=%s err=%s", board_id, exc)
        return json.dumps({"error": "preflight_service_failed", "detail": str(exc)})

    try:
        manifest_store = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)
        manifest = await run_in_threadpool(
            manifest_store.build,
            source_set=source_set,
            preflight_hash=result.preflight_hash,
        )
    except Exception as exc:
        _rebuild_logger.error("kg.rebuild.preflight.manifest_failed board=%s err=%s", board_id, exc)
        return json.dumps({"error": "preflight_manifest_failed", "detail": str(exc)})

    payload = result.to_dict()
    payload["manifest_ref"] = manifest.manifest_ref
    payload["source_set_hash"] = manifest.source_set_hash

    _rebuild_logger.info(
        "kg.rebuild.preflight.done board=%s outcome=%s manifest_ref=%s",
        board_id, result.outcome, manifest.manifest_ref,
    )
    return json.dumps(payload, default=str)


@mcp.tool()
async def okto_pulse_kg_rebuild_confirm(
    board_id: str,
    operation: str,
    preflight_hash: str,
    manifest_ref: str,
) -> str:
    """
    Emite o token de confirmação single-use para um rebuild — gemelar do REST POST /api/v1/kg/rebuild/confirm.

    Carrega o manifesto persistido em /preflight via manifest_ref (NUNCA
    re-enumera), verifica que preflight_hash bate, e emite o token de
    confirmação. Passe o token para okto_pulse_kg_rebuild_run.

    Parâmetros:
        board_id       — UUID do board (mesmo usado em /preflight)
        operation      — operação canônica (ex: 'rebuild_full')
        preflight_hash — SHA-256 hex recebido de /preflight (64 chars)
        manifest_ref   — identificador do manifesto recebido de /preflight
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    actor_id = ctx.agent.id if hasattr(ctx, "agent") else "agent-mcp"

    from starlette.concurrency import run_in_threadpool

    from okto_pulse.core.api.kg_rebuild import _REBUILD_BASE_DIR
    from okto_pulse.core.kg.rebuild_confirmation import (
        CANONICAL_OPERATIONS,
        RebuildConfirmationStore,
    )
    from okto_pulse.core.kg.rebuild_service import SUPPORTED_REBUILD_OPERATIONS
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        validate_preflight_hash,
    )

    if operation not in CANONICAL_OPERATIONS:
        return json.dumps({
            "error": "unsupported_operation",
            "reason": "operation not in canonical set",
        })

    if operation not in SUPPORTED_REBUILD_OPERATIONS:
        return json.dumps({
            "error": "operation_pending_implementation",
            "reason": (
                f"operation={operation!r} not implemented yet; "
                f"only {sorted(SUPPORTED_REBUILD_OPERATIONS)} supported"
            ),
        })

    try:
        validate_preflight_hash(preflight_hash)
    except ValueError as exc:
        return json.dumps({"error": "invalid_preflight_hash", "reason": str(exc)})

    def _load_and_issue():
        manifest_store = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)
        manifest = manifest_store.load(manifest_ref)
        if manifest is None:
            return {"error": "manifest_not_found", "reason": "manifest_ref does not exist"}
        if manifest.board_id != board_id:
            return {"error": "manifest_board_mismatch", "reason": "manifest_ref belongs to a different board"}
        if manifest.preflight_hash != preflight_hash:
            return {"error": "preflight_hash_mismatch", "reason": "preflight_hash does not match manifest binding"}

        store = RebuildConfirmationStore(base_dir=_REBUILD_BASE_DIR)
        token = store.issue(
            board_id=board_id,
            actor_id=actor_id,
            operation=operation,
            preflight_hash=preflight_hash,
            manifest_ref=manifest.manifest_ref,
        )
        return {
            "confirmation_id": token.confirmation_id,
            "manifest_ref": manifest.manifest_ref,
            "source_set_hash": manifest.source_set_hash,
            "expires_at": token.expires_at,
        }

    try:
        result = await run_in_threadpool(_load_and_issue)
    except Exception as exc:
        _rebuild_logger.error("kg.rebuild.confirm.failed board=%s err=%s", board_id, exc)
        return json.dumps({"error": "confirm_failed", "detail": str(exc)})

    _rebuild_logger.info(
        "kg.rebuild.confirm.done board=%s confirmation_id=%s",
        board_id, result.get("confirmation_id"),
    )
    return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_kg_rebuild_run(
    board_id: str,
    confirmation_id: str,
    operation: str,
    preflight_hash: str,
    manifest_ref: str,
    reason: str,
) -> str:
    """
    Executa o rebuild do KG — gemelar do REST POST /api/v1/kg/rebuild/run.

    Consome o token single-use emitido por okto_pulse_kg_rebuild_confirm e
    executa o rebuild completo sob o admin lane KG-01. NUNCA muta o grafo se
    o token for inválido, o manifesto tiver mudado ou o lock exclusivo não
    puder ser adquirido.

    Admission gate (FR8): recusa com rebuild_refused_quarantined quando
    graph_state == 'quarantined' mesmo antes de consumir o token.
    recovery_needed É ADMITIDO (rebuild é a saída prescrita desse estado).

    Parâmetros:
        board_id        — UUID do board
        confirmation_id — token emitido por /confirm
        operation       — operação canônica (deve bater com /confirm)
        preflight_hash  — SHA-256 hex (deve bater com /confirm)
        manifest_ref    — identificador do manifesto (deve bater com /confirm)
        reason          — descrição textual (auditoria), máx 512 chars
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    actor_id = ctx.agent.id if hasattr(ctx, "agent") else "agent-mcp"

    from starlette.concurrency import run_in_threadpool

    from okto_pulse.core.api.kg_rebuild import (
        _REBUILD_BASE_DIR,
        _build_source_store,
        _refuse_rebuild_if_quarantined,
        _resolve_pulse_db_path,
    )
    from okto_pulse.core.infra.database import get_session_factory

    # FR8 — admission gate before consuming the token.
    async with get_session_factory()() as _gate_session:
        refusal = await _refuse_rebuild_if_quarantined(board_id, _gate_session)
    if refusal is not None:
        return json.dumps(refusal)

    from okto_pulse.core.kg.board_rebuild_adapter import BoardRebuildIngestionAdapter
    from okto_pulse.core.kg.rebuild_audit import (
        CognitivePendingMarker,
        ConfirmationConsumptionAuditRecorder,
        KGRebuiltEventPublisher,
        build_kg_rebuilt_event_handler,
    )
    from okto_pulse.core.kg.rebuild_confirmation import RebuildConfirmationStore
    from okto_pulse.core.kg.rebuild_generation import (
        KGGenerationPromotionGuard,
        KGGenerationRepository,
    )
    from okto_pulse.core.kg.rebuild_report import (
        RebuildReportStore,
        RebuildReportTerminalStateGuard,
    )
    from okto_pulse.core.kg.rebuild_service import KGRebuildService
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        RebuildSourceEnumerator,
    )
    from okto_pulse.core.kg.safe_write_lifecycle import (
        HealthProbe,
        KGSafeWriteLifecycle,
        LockOwnerProbe,
    )
    from okto_pulse.core.kg.schema import apply_ladybug_lifecycle_step
    from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock

    lock = KGSingleWriterLock(base_dir=_REBUILD_BASE_DIR / "locks")

    def _always_owner(bid: str, owner_token: str) -> bool:
        m = lock.inspect(board_id=bid)
        return m is not None and m.owner_token == owner_token

    safe_lifecycle = KGSafeWriteLifecycle(
        step_adapter=apply_ladybug_lifecycle_step,
        owner_probe=LockOwnerProbe(is_active_owner=_always_owner),
        health_probe=HealthProbe(classify=lambda b, g, status, step: "at_risk"),
    )

    source_store_fetch = _build_source_store()
    enumerator = RebuildSourceEnumerator(source_store=source_store_fetch)
    manifest_store_obj = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)
    ingestion = BoardRebuildIngestionAdapter(db_path=_resolve_pulse_db_path())

    def _step_source_resolver(req):
        m = manifest_store_obj.load(req.manifest_ref)
        if m is None:
            return ()
        return tuple(row.to_dict() for row in m.sources)

    _step_adapter_with_sources = ingestion.build_step_adapter(
        source_resolver=_step_source_resolver,
    )

    audit_recorder = ConfirmationConsumptionAuditRecorder(base_dir=_REBUILD_BASE_DIR)
    event_publisher = KGRebuiltEventPublisher(base_dir=_REBUILD_BASE_DIR)
    cognitive_marker = CognitivePendingMarker(base_dir=_REBUILD_BASE_DIR)

    def _source_resolver(event_payload):
        m = manifest_store_obj.load(event_payload.get("manifest_ref", ""))
        if m is None:
            return ()
        return tuple(row.to_dict() for row in m.sources)

    event_handler = build_kg_rebuilt_event_handler(
        publisher=event_publisher,
        cognitive_marker=cognitive_marker,
        source_resolver=_source_resolver,
    )
    from okto_pulse.core.kg.orphan_integrity import OrphanNodeScanner
    orphan_scanner = OrphanNodeScanner()

    service = KGRebuildService(
        base_dir=_REBUILD_BASE_DIR,
        single_writer_lock=lock,
        safe_write_lifecycle=safe_lifecycle,
        quarantine_service=None,
        confirmation_store=RebuildConfirmationStore(
            base_dir=_REBUILD_BASE_DIR, audit_recorder=audit_recorder,
        ),
        manifest_store=manifest_store_obj,
        source_enumerator=enumerator,
        rebuild_step_adapter=_step_adapter_with_sources,
        generation_repository=KGGenerationRepository(base_dir=_REBUILD_BASE_DIR),
        promotion_guard=KGGenerationPromotionGuard,
        report_store=RebuildReportStore(base_dir=_REBUILD_BASE_DIR),
        terminal_state_guard=RebuildReportTerminalStateGuard,
        event_emitter=event_handler,
        orphan_scan_provider=lambda board_id, generation_id: orphan_scanner.scan(
            board_id=board_id,
            generation_id=generation_id,
        ),
    )

    try:
        result = await run_in_threadpool(
            service.run,
            confirmation_id=confirmation_id,
            board_id=board_id,
            actor_id=actor_id,
            operation=operation,
            preflight_hash=preflight_hash,
            manifest_ref=manifest_ref,
            reason=reason,
        )
    except Exception as exc:
        _rebuild_logger.error("kg.rebuild.run.failed board=%s err=%s", board_id, exc)
        return json.dumps({"error": "rebuild_run_failed", "detail": str(exc)})

    _rebuild_logger.info(
        "kg.rebuild.run.done board=%s outcome=%s run_id=%s",
        board_id, result.outcome, result.run_id,
    )
    return json.dumps({
        "run_id": result.run_id,
        "outcome": result.outcome,
        "reason": result.reason,
        "audit_ref": result.audit_ref,
        "previous_kg_generation_id": result.previous_kg_generation_id,
        "current_kg_generation_id": result.current_kg_generation_id,
        "started_at": result.started_at,
        "finished_at": result.finished_at,
        "affected_files": list(result.affected_files),
        "report_ref": result.report_ref,
        "report_id": result.report_id,
        "publishable_status": result.publishable_status,
        "promotion_outcome": result.promotion_outcome,
        "operator_action": result.operator_action,
        "event_emitted": result.event_emitted,
    }, default=str)


# ============================================================================
# CONSOLIDATED POLYMORPHIC LIST HANDLERS (spec P0.B — TR-B1)
#
# These 4 tools are the supported list surface. The 15 entity-specific
# okto_pulse_list_* tools are intentionally not registered.
# ============================================================================


@mcp.tool()
async def okto_pulse_list_by_board(
    board_id: str,
    entity_type: str,
    filters: dict[str, Any] | str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> str:
    """List top-level entities of a board by type.

    Consolidates: list_specs, list_ideations, list_refinements,
    list_sprints, list_stories, list_topics.

    Use this single tool instead of the individual list_* tools."""
    from okto_pulse.core.mcp.filters import validate_filters

    # Auto-deserialize string JSON (MCP transport convention — other tools use coerce_to_list_str)
    if isinstance(filters, str):
        if not filters.strip():
            filters = None
        else:
            try:
                filters = json.loads(filters)
            except json.JSONDecodeError as e:
                return _structured_error("invalid_filter", [], None, f"Invalid JSON in filters: {e}")

    SUPPORTED = ["spec", "ideation", "refinement", "sprint", "story", "topic"]
    if entity_type not in SUPPORTED:
        return _structured_error(
            "unsupported_entity",
            SUPPORTED,
            None,
            f"entity_type='{entity_type}' is not supported by okto_pulse_list_by_board",
        )

    ok, err = validate_filters(entity_type, filters or {}, scope="by_board")
    if not ok:
        return _structured_error("invalid_filter", list((filters or {}).keys()), None, err)

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)
    filters = filters or {}

    async with get_db_for_mcp() as db:
        if entity_type == "spec":
            service = SpecService(db)
            items = await service.list_specs(board_id, filters.get("status"))
            if "labels" in filters and filters["labels"]:
                label_filter = filters["labels"] if isinstance(filters["labels"], list) else [filters["labels"]]
                items = [s for s in items if any(lbl in (s.labels or []) for lbl in label_filter)]
            if "assignee_id" in filters and filters["assignee_id"]:
                items = [s for s in items if s.assignee_id == filters["assignee_id"]]
            await db.commit()
            total = len(items)
            paginated = items[offset:offset + limit]
            return json.dumps({
                "board_id": board_id,
                "entity_type": entity_type,
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": [
                    {
                        "id": s.id,
                        "title": s.title,
                        "description": s.description,
                        "status": s.status.value,
                        "version": s.version,
                        "assignee_id": s.assignee_id,
                        "labels": s.labels,
                        "created_at": s.created_at.isoformat(),
                        "updated_at": s.updated_at.isoformat(),
                    }
                    for s in paginated
                ],
            }, default=str)

        elif entity_type == "ideation":
            service = IdeationService(db)
            items = await service.list_ideations(board_id, filters.get("status"))
            if "labels" in filters and filters["labels"]:
                label_filter = filters["labels"] if isinstance(filters["labels"], list) else [filters["labels"]]
                items = [i for i in items if any(lbl in (i.labels or []) for lbl in label_filter)]
            await db.commit()
            total = len(items)
            paginated = items[offset:offset + limit]
            return json.dumps({
                "board_id": board_id,
                "entity_type": entity_type,
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": [
                    {
                        "id": i.id,
                        "title": i.title,
                        "description": i.description,
                        "problem_statement": i.problem_statement,
                        "complexity": i.complexity.value if i.complexity else None,
                        "status": i.status.value,
                        "version": i.version,
                        "assignee_id": i.assignee_id,
                        "labels": i.labels,
                        "created_at": i.created_at.isoformat(),
                        "updated_at": i.updated_at.isoformat(),
                    }
                    for i in paginated
                ],
            }, default=str)

        elif entity_type == "refinement":
            ideation_id = filters.get("ideation_id", "")
            if not ideation_id:
                return _structured_error(
                    "missing_required_filter",
                    ["ideation_id"],
                    None,
                    "entity_type='refinement' requires filters.ideation_id",
                )
            service = RefinementService(db)
            items = await service.list_refinements(ideation_id)
            if "status" in filters and filters["status"]:
                items = [r for r in items if r.status.value == filters["status"]]
            if "labels" in filters and filters["labels"]:
                label_filter = filters["labels"] if isinstance(filters["labels"], list) else [filters["labels"]]
                items = [r for r in items if any(lbl in (r.labels or []) for lbl in label_filter)]
            await db.commit()
            total = len(items)
            paginated = items[offset:offset + limit]
            return json.dumps({
                "board_id": board_id,
                "entity_type": entity_type,
                "ideation_id": ideation_id,
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": [
                    {
                        "id": r.id,
                        "title": r.title,
                        "description": r.description,
                        "in_scope": r.in_scope,
                        "out_of_scope": r.out_of_scope,
                        "status": r.status.value,
                        "version": r.version,
                        "assignee_id": r.assignee_id,
                        "labels": r.labels,
                        "created_at": r.created_at.isoformat(),
                        "updated_at": r.updated_at.isoformat(),
                    }
                    for r in paginated
                ],
            }, default=str)

        elif entity_type == "sprint":
            spec_id = filters.get("spec_id", "")
            if not spec_id:
                return _structured_error(
                    "missing_required_filter",
                    ["spec_id"],
                    None,
                    "entity_type='sprint' requires filters.spec_id to identify the parent spec",
                )
            from okto_pulse.core.services.main import SprintService
            service = SprintService(db)
            items = await service.list_sprints(spec_id)
            if "status" in filters and filters["status"]:
                items = [s for s in items if s.status.value == filters["status"]]
            total = len(items)
            paginated = items[offset:offset + limit]
            return json.dumps({
                "board_id": board_id,
                "entity_type": entity_type,
                "spec_id": spec_id,
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": [
                    {
                        "id": s.id,
                        "title": s.title,
                        "status": s.status.value,
                        "lane_type": s.lane_type.value if s.lane_type else "normal",
                        "origin_sprint_id": s.origin_sprint_id,
                        "origin_bug_id": s.origin_bug_id,
                        "normal_sprint_created": s.normal_sprint_created,
                        "spec_version": s.spec_version,
                        "test_scenario_ids": s.test_scenario_ids,
                        "business_rule_ids": s.business_rule_ids,
                        "labels": s.labels,
                    }
                    for s in paginated
                ],
            }, default=str)

        elif entity_type == "story":
            def _optional_bool_filter(value: Any) -> bool | None:
                if value is None or value == "":
                    return None
                if isinstance(value, bool):
                    return value
                return _flag_enabled(str(value))

            service = StoryService(db)
            items = await service.list_stories(
                board_id,
                status_filter=filters.get("status") or None,
                topic_id=filters.get("topic_id") or None,
                linked=_optional_bool_filter(filters.get("linked")),
                converted=_optional_bool_filter(filters.get("converted")),
                include_archived=_flag_enabled(str(filters.get("include_archived", "false"))),
            )
            await db.commit()
            total = len(items)
            paginated = items[offset:offset + limit]
            return json.dumps({
                "board_id": board_id,
                "entity_type": entity_type,
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": [_story_payload(s) for s in paginated],
            }, default=str)

        else:  # topic
            service = StoryService(db)
            topics = await service.list_topics(
                board_id,
                include_archived=_flag_enabled(str(filters.get("include_archived", "false"))),
            )
            await db.commit()
            total = len(topics)
            paginated = topics[offset:offset + limit]
            return json.dumps({
                "board_id": board_id,
                "entity_type": entity_type,
                "total": total,
                "offset": offset,
                "limit": limit,
                "items": [_topic_payload(t) for t in paginated],
            }, default=str)


@mcp.tool()
async def okto_pulse_list_qa(
    board_id: str,
    entity_type: str,
    entity_id: str,
    filters: dict[str, Any] | str | None = None,
) -> str:
    """List Q&A items for a spec, ideation, or refinement.

    Consolidates: list_spec_qa, list_ideation_qa, list_refinement_qa."""
    from okto_pulse.core.mcp.filters import validate_filters

    # Auto-deserialize string JSON (MCP transport convention)
    if isinstance(filters, str):
        if not filters.strip():
            filters = None
        else:
            try:
                filters = json.loads(filters)
            except json.JSONDecodeError as e:
                return _structured_error("invalid_filter", [], None, f"Invalid JSON in filters: {e}")

    SUPPORTED = ["spec", "ideation", "refinement"]
    if entity_type not in SUPPORTED:
        return _structured_error(
            "unsupported_entity",
            SUPPORTED,
            None,
            f"entity_type='{entity_type}' is not supported by okto_pulse_list_qa",
        )

    ok, err = validate_filters(entity_type, filters or {}, scope="qa")
    if not ok:
        return _structured_error("invalid_filter", list((filters or {}).keys()), None, err)

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    filters = filters or {}

    def _qa_item(qa) -> dict:
        return {
            "id": qa.id,
            "question": qa.question,
            "question_type": qa.question_type,
            "choices": qa.choices,
            "allow_free_text": getattr(qa, "allow_free_text", None),
            "answer": qa.answer,
            "selected": qa.selected,
            "asked_by": qa.asked_by,
            "answered_by": qa.answered_by,
            "created_at": qa.created_at.isoformat(),
            "answered_at": qa.answered_at.isoformat() if qa.answered_at else None,
        }

    async with get_db_for_mcp() as db:
        if entity_type == "spec":
            service = SpecQAService(db)
            items = await service.list_qa(entity_id)
        elif entity_type == "ideation":
            service = IdeationQAService(db)
            items = await service.list_qa(entity_id)
        else:  # refinement
            service = RefinementQAService(db)
            items = await service.list_qa(entity_id)

        await db.commit()

        # Apply optional filters
        if filters.get("asked_by"):
            items = [q for q in items if q.asked_by == filters["asked_by"]]

        return json.dumps({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "count": len(items),
            "qa_items": [_qa_item(q) for q in items],
        }, default=str)


@mcp.tool()
async def okto_pulse_list_knowledge(
    board_id: str,
    entity_type: str,
    entity_id: str,
    filters: dict[str, Any] | str | None = None,
) -> str:
    """List knowledge base items for a spec, ideation, refinement, or card.

    Consolidates: list_spec_knowledge, list_ideation_knowledge,
    list_refinement_knowledge, list_card_knowledge."""
    from okto_pulse.core.mcp.filters import validate_filters

    # Auto-deserialize string JSON (MCP transport convention)
    if isinstance(filters, str):
        if not filters.strip():
            filters = None
        else:
            try:
                filters = json.loads(filters)
            except json.JSONDecodeError as e:
                return _structured_error("invalid_filter", [], None, f"Invalid JSON in filters: {e}")

    SUPPORTED = ["spec", "ideation", "refinement", "card"]
    if entity_type not in SUPPORTED:
        return _structured_error(
            "unsupported_entity",
            SUPPORTED,
            None,
            f"entity_type='{entity_type}' is not supported by okto_pulse_list_knowledge",
        )

    ok, err = validate_filters(entity_type, filters or {}, scope="knowledge")
    if not ok:
        return _structured_error("invalid_filter", list((filters or {}).keys()), None, err)

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    filters = filters or {}
    mime_filter: str | None = filters.get("mime_type")

    async with get_db_for_mcp() as db:
        if entity_type == "spec":
            service = SpecKnowledgeService(db)
            items = await service.list_knowledge(entity_id)
            await db.commit()
            if mime_filter:
                items = [kb for kb in items if getattr(kb, "mime_type", None) == mime_filter]
            return json.dumps({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(items),
                "knowledge_bases": [
                    {
                        "id": kb.id,
                        "title": kb.title,
                        "description": kb.description,
                        "mime_type": kb.mime_type,
                        "created_at": kb.created_at.isoformat(),
                    }
                    for kb in items
                ],
            }, default=str)

        elif entity_type == "ideation":
            ideation = await IdeationService(db).get_ideation(entity_id)
            if not ideation or ideation.board_id != board_id:
                return json.dumps({"error": "Ideation not found"})
            service = IdeationKnowledgeService(db)
            items = await service.list_knowledge(entity_id)
            await db.commit()
            if mime_filter:
                items = [kb for kb in items if getattr(kb, "mime_type", None) == mime_filter]
            return json.dumps({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(items),
                "knowledge_bases": [
                    _serialize_knowledge_base(kb, include_content=False)
                    for kb in items
                ],
            }, default=str)

        elif entity_type == "refinement":
            service = RefinementKnowledgeService(db)
            items = await service.list_knowledge(entity_id)
            await db.commit()
            if mime_filter:
                items = [kb for kb in items if getattr(kb, "mime_type", None) == mime_filter]
            return json.dumps({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(items),
                "knowledge_bases": [
                    {
                        "id": kb.id,
                        "title": kb.title,
                        "description": kb.description,
                        "mime_type": kb.mime_type,
                        "created_at": kb.created_at.isoformat(),
                    }
                    for kb in items
                ],
            }, default=str)

        else:  # card
            service = CardService(db)
            card = await service.get_card(entity_id)
            if not card or card.board_id != board_id:
                return json.dumps({"error": "Card not found"})
            kbs = list(card.knowledge_bases or [])
            if mime_filter:
                kbs = [kb for kb in kbs if kb.get("mime_type") == mime_filter]
            return json.dumps({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(kbs),
                "knowledge_bases": kbs,
            }, default=str)


@mcp.tool()
async def okto_pulse_list_snapshots(
    board_id: str,
    entity_type: str,
    entity_id: str,
) -> str:
    """List version snapshots for an ideation or refinement.

    Consolidates: list_ideation_snapshots, list_refinement_snapshots.

    Each snapshot is an immutable copy of the entity's state at the moment
    it was marked as 'done'."""
    SUPPORTED = ["ideation", "refinement"]
    if entity_type not in SUPPORTED:
        return _structured_error(
            "unsupported_entity",
            SUPPORTED,
            None,
            f"entity_type='{entity_type}' is not supported by okto_pulse_list_snapshots",
        )

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        if entity_type == "ideation":
            service = IdeationService(db)
            snapshots = await service.list_snapshots(entity_id)
            await db.commit()
            return json.dumps({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(snapshots),
                "snapshots": [
                    {
                        "version": s.version,
                        "title": s.title,
                        "complexity": s.complexity,
                        "created_by": s.created_by,
                        "created_at": s.created_at.isoformat(),
                    }
                    for s in snapshots
                ],
            }, default=str)

        else:  # refinement
            service = RefinementService(db)
            snapshots = await service.list_snapshots(entity_id)
            await db.commit()
            return json.dumps({
                "entity_type": entity_type,
                "entity_id": entity_id,
                "count": len(snapshots),
                "snapshots": [
                    {
                        "version": s.version,
                        "title": s.title,
                        "created_by": s.created_by,
                        "created_at": s.created_at.isoformat(),
                    }
                    for s in snapshots
                ],
            }, default=str)


# ============================================================================
# SERVER STARTUP
# ============================================================================


def build_mcp_asgi_app():
    """Build the MCP ASGI application wrapped with the API-key middleware.

    Returns the ASGI app that should be served by uvicorn (or mounted
    elsewhere). Single-process callers (``okto_pulse.community.main.serve``)
    use this to bind the MCP transport to its own port while sharing the
    same Python process as the API server, so the LadybugDB lock is held by a
    single process. The caller is responsible for invoking
    ``register_session_factory`` once before the first MCP request lands.

    ``_install_trace`` is idempotent (env-gated); calling this multiple
    times is safe.
    """
    _install_trace(mcp)
    http_app = mcp.http_app(transport="streamable-http")
    return ApiKeySessionMiddleware(http_app)


def mount_mcp(app, *, mount_path: str = "/mcp") -> None:
    """Mount the MCP sub-app at ``mount_path`` on a FastAPI/Starlette app.

    Kept for callers that prefer path-based routing on the same port as the
    API. The default deployment path (``okto_pulse.community.main.serve``)
    serves the MCP on its own port via :func:`build_mcp_asgi_app`.
    """
    app.mount(mount_path, build_mcp_asgi_app())


def run_mcp_server():
    """Run the MCP server standalone (compat shim for debug / legacy).

    Production path is :func:`okto_pulse.community.main.serve`, which runs
    the API server and the MCP server in the same Python process on
    separate ports. This function is preserved for stand-alone debug runs
    (``python -m okto_pulse.core.mcp.server``) only.
    """
    from okto_pulse.core.infra.config import get_settings
    from okto_pulse.core.infra.database import create_database, get_session_factory

    settings = get_settings()
    create_database(settings.database_url, echo=settings.debug)
    register_session_factory(get_session_factory())

    # Read port from environment (set by CLI) or use settings
    port = int(os.environ.get("MCP_PORT", str(settings.mcp_port)))
    # Read host from environment (set by CLI / Docker / compose) or fall back
    # to loopback so a stray binary doesn't accidentally expose the MCP server.
    # Override via MCP_HOST=0.0.0.0 in docker-compose.yml when port-mapping is
    # required from outside the container.
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    uvicorn.run(build_mcp_asgi_app(), host=host, port=port, ws="wsproto")


if __name__ == "__main__":
    run_mcp_server()

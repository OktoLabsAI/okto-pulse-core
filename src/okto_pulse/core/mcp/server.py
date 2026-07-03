"""MCP Server for Okto Pulse Core - enables AI agents to interact with the board."""

import base64
import binascii
import functools
import inspect
import json
import logging
import os
import re
import uuid as _uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import uvicorn
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.infra.config import get_mcp_settings, get_settings
from okto_pulse.core.infra.permissions import Permissions, check_permission
from okto_pulse.core.mcp.helpers import _structured_error, coerce_to_list_str, parse_multi_value, parse_options_json
from okto_pulse.core.mcp.trace_middleware import install_trace_sink as _install_trace
from okto_pulse.core.ports.content_ingestion import ContentIngestionError
from okto_pulse.core.ports.mcp_trace import McpTraceSink
from okto_pulse.core.models.db import Board
from okto_pulse.core.models.schemas import ArchitectureDesignCreate, ArchitectureDesignUpdate
from okto_pulse.core.services.activity_log import (
    activity_log_summary,
    sanitize_activity_details,
)
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitecturePropagationBlocked,
    ArchitectureDiagramAdapterRegistry,
    ArchitectureDiagramStore,
    ArchitecturePropagationService,
    ArchitectureWarningAcknowledgementRequired,
    CARD_ARCHITECTURE_READ_ONLY_MESSAGE,
    architecture_design_payload_schema,
)
from okto_pulse.core.services.gate_contracts import (
    GateContractError,
    human_control_required_envelope,
    operational_flow_for_test_card,
    spec_evaluation_success_envelope,
    spec_gate_readiness,
    task_gate_readiness,
)
from okto_pulse.core.services.human_control_metrics import (
    emit_human_control_required,
)
from okto_pulse.core.services.main import (
    AgentService,
    AttachmentService,
    BoardService,
    CARD_RESOURCE_READ_ONLY_MESSAGE,
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
from okto_pulse.core.services.skip_overrides import (
    ideation_skip_overrides,
    spec_skip_overrides,
)
from okto_pulse.core.services.story_permissions import (
    story_move_permission,
    story_state,
    story_update_permissions,
)


def _trs_to_objects(trs: list[str] | None) -> list | None:
    """Convert TR strings to objects with IDs for task linkage traceability."""
    if not trs:
        return None
    return [
        {"id": f"tr_{_uuid.uuid4().hex[:8]}", "text": tr, "linked_task_ids": []}
        if isinstance(tr, str) else tr
        for tr in trs
    ]


def _resolve_linked_requirement_tokens_to_fr_or_tr_ids(
    linked_tokens: list | None,
    frs: list,
    trs: list,
) -> tuple[list[str], list[str]]:
    """Re-export shim (MCP-FU6): the resolver moved to core
    (``analytics_service.resolve_linked_requirement_tokens_to_fr_or_tr_ids``) so the
    MCP-scoped api_contract use cases can resolve without importing this transport
    package. Kept here so the IR/OR/decision adapters keep their existing call-site
    during the migration. FR-first, then TR, with dedup — behavior unchanged."""
    from okto_pulse.core.services.analytics_service import (
        resolve_linked_requirement_tokens_to_fr_or_tr_ids,
    )

    return resolve_linked_requirement_tokens_to_fr_or_tr_ids(linked_tokens, frs, trs)


def _available_structured_ids(items: list) -> list[str]:
    return [rid for rid in (_structured_ref_id(item) for item in items) if rid]


def _qa_selected_labels(qa: Any) -> list[str]:
    selected = getattr(qa, "selected", None)
    choices = getattr(qa, "choices", None)
    if isinstance(qa, dict):
        selected = qa.get("selected")
        choices = qa.get("choices")
    selected_ids = [str(item) for item in (selected or [])]
    labels_by_id = {
        str(choice.get("id")): str(choice.get("label"))
        for choice in (choices or [])
        if isinstance(choice, dict) and choice.get("id") is not None
    }
    return [labels_by_id.get(item, item) for item in selected_ids]


def _qa_answer_text(qa: Any) -> str | None:
    answer = getattr(qa, "answer", None)
    if isinstance(qa, dict):
        answer = qa.get("answer")
    if answer:
        return str(answer)
    labels = _qa_selected_labels(qa)
    if labels:
        return ", ".join(labels)
    return None


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


_CORE_RESOURCE_TABLE = [
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


# ============================================================================
# R11-A — the EFFECTIVE resource catalog is the authority (not _CORE_RESOURCE_TABLE
# nor _RESOURCE_REGISTRY). The core built-in catalog is composed with any
# Community-injected operational catalog at the composition root, then FROZEN.
# ============================================================================
from okto_pulse.core.ports.mcp_resources import (  # noqa: E402
    CompositeMcpResourceCatalog,
    McpResourceCatalog,
    McpResourceSpec,
    StaticMcpResourceCatalog,
)


def _resource_category_for(uri: str) -> str:
    """Derive the resource category from its okto-pulse:// URI path."""
    rest = uri[len("okto-pulse://"):]
    parts = rest.split("/")
    if parts and parts[0] == "reference" and len(parts) >= 2 and parts[1] in (
        "tool-docs", "tool-families",
    ):
        return f"reference/{parts[1]}"
    return parts[0] if parts and parts[0] else "misc"


def _build_core_resource_catalog() -> StaticMcpResourceCatalog:
    """The CORE edition catalog, built from ``_CORE_RESOURCE_TABLE`` (path-loaders
    confined to the core resources dir)."""
    base = _get_resource_dir()
    specs = tuple(
        McpResourceSpec(
            uri=uri, path=path, description=desc,
            category=_resource_category_for(uri), edition="core", base_dir=base,
        )
        for uri, path, desc in _CORE_RESOURCE_TABLE
    )
    return StaticMcpResourceCatalog("core", specs)


_effective_resource_catalog = CompositeMcpResourceCatalog([_build_core_resource_catalog()])
_resource_catalog_frozen = False

#: (R11-A) READ-ONLY transitional PROJECTION of the EFFECTIVE catalog as the legacy
#: ``(uri, path, description)`` tuples. An IMMUTABLE tuple (NOT a mutable list) —
#: it is DERIVED from ``effective_resource_catalog()`` and ATOMICALLY REASSIGNED on
#: every injection/reset, so a consumer cannot mutate the public projection (or the
#: catalog) by accident. NOT an authority / extension point; kept only so existing
#: consumers keep working during register-before-remove.
_RESOURCE_REGISTRY: tuple = ()


def effective_resource_catalog() -> CompositeMcpResourceCatalog:
    """The AUTHORITATIVE effective MCP resource catalog (core + injected)."""
    return _effective_resource_catalog


def _projection_path(spec: McpResourceSpec) -> str:
    return spec.path if spec.path is not None else f"<content:{spec.uri}>"


def _rebuild_resource_registry_projection() -> None:
    global _RESOURCE_REGISTRY
    _RESOURCE_REGISTRY = tuple(
        (s.uri, _projection_path(s), s.description)
        for s in _effective_resource_catalog.specs()
    )


def _make_resource_handler(spec: McpResourceSpec) -> "Callable[[], str]":
    """Closure-safe resources/read handler bound to a catalog spec (the spec's
    deterministic loader; never exposes a filesystem path to the agent)."""
    def handler() -> str:
        return spec.read()
    return handler


def _register_resource_spec(spec: McpResourceSpec) -> None:
    handler = _make_resource_handler(spec)
    handler.__name__ = (
        "resource_"
        + spec.uri[len("okto-pulse://"):].replace("/", "_").replace("-", "_")
    )
    handler.__doc__ = spec.description
    mcp.resource(spec.uri, description=spec.description)(handler)


def register_resource_catalog(catalog: McpResourceCatalog) -> None:
    """(R11-A IMP2/IMP3) Composition-root injection of an additional edition
    catalog (e.g. the Community operational catalog), using the core CONTRACTS so
    the core never imports community. FAIL-CLOSED: raises after the freeze."""
    global _effective_resource_catalog
    if _resource_catalog_frozen:
        raise RuntimeError(
            "MCP resource catalog is FROZEN after composition; late "
            "registration/mutation is forbidden (R11-A IMP4 fail-closed freeze)."
        )
    existing = {s.uri for s in _effective_resource_catalog.specs()}
    _effective_resource_catalog = _effective_resource_catalog.with_catalog(catalog)
    for spec in catalog.specs():
        if spec.uri not in existing:  # first-wins dedupe; conflicts reported, not re-registered
            _register_resource_spec(spec)
    _rebuild_resource_registry_projection()


def freeze_resource_catalog() -> None:
    """(R11-A IMP4) Freeze the effective catalog AFTER composition (all providers
    registered) + prewarm every spec. Idempotent; later registration RAISES."""
    global _resource_catalog_frozen
    _resource_catalog_frozen = True
    for spec in _effective_resource_catalog.specs():
        spec.read()


def reset_resource_catalog_for_tests() -> None:
    """Tests only: rebuild the core-only effective catalog, clear the freeze, AND
    drop any FastMCP resource handlers registered beyond the core baseline so a
    previously-injected catalog leaves NO residual state (isolation)."""
    global _effective_resource_catalog, _resource_catalog_frozen
    _effective_resource_catalog = CompositeMcpResourceCatalog(
        [_build_core_resource_catalog()]
    )
    _resource_catalog_frozen = False
    _rebuild_resource_registry_projection()
    try:
        resources = mcp._resource_manager._resources
        for _uri in list(resources):
            if _uri not in _CORE_FASTMCP_RESOURCE_URIS:
                del resources[_uri]
    except Exception:  # pragma: no cover - FastMCP internals are best-effort here
        pass


# Register the CORE catalog with FastMCP + build the initial projection at import.
for _spec in _effective_resource_catalog.specs():
    _register_resource_spec(_spec)
_rebuild_resource_registry_projection()
# Pre-warm so first resources/read latency is minimal.
for _spec in _effective_resource_catalog.specs():
    _spec.read()

#: FastMCP resource URIs registered by the CORE catalog at import — the baseline a
#: test reset restores to (any injected-catalog handler beyond this is dropped).
try:
    _CORE_FASTMCP_RESOURCE_URIS = frozenset(mcp._resource_manager._resources.keys())
except Exception:  # pragma: no cover
    _CORE_FASTMCP_RESOURCE_URIS = frozenset()


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

_MCP_CREDENTIAL_SCOPE_KEY = "okto_pulse.mcp_credential"


class ApiKeySessionMiddleware:
    """ASGI middleware that stores the MCP credential on the request scope."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] == "http":
            request = Request(scope)
            credential = extract_mcp_credential_from_request(request)
            if credential is not None:
                scope[_MCP_CREDENTIAL_SCOPE_KEY] = credential

        await self.app(scope, receive, send)


# ----------------------------------------------------------------------------
# R08-A: transport -> McpCredential conversion shims (tr_7d105709).
#
# These convert the inbound transports into the pure ``McpCredential`` port DTO
# and keep it attached to the ASGI request scope. No Okto-owned ContextVar or
# process-global credential carrier participates in request authentication.
# ----------------------------------------------------------------------------
def extract_mcp_credential_from_request(request):
    """Build an ``McpCredential`` from a Starlette ``Request`` preserving the
    canonical precedence (query param > X-API-Key > Authorization Bearer) — the
    SAME order ``ApiKeySessionMiddleware`` applies above."""
    from okto_pulse.core.ports import mcp_credential_from_sources

    return mcp_credential_from_sources(
        query_param=request.query_params.get("api_key"),
        x_api_key_header=request.headers.get("x-api-key"),
        authorization_header=request.headers.get("authorization"),
    )


def request_scope_mcp_credential(scope):
    """Return the credential attached to an ASGI request scope, if any."""
    return scope.get(_MCP_CREDENTIAL_SCOPE_KEY)


def active_api_key_credential():
    """Resolve the current MCP credential from FastMCP's HTTP request context.

    The credential is request-scoped: ``ApiKeySessionMiddleware`` stores the
    transport-extracted :class:`McpCredential` on the ASGI ``scope`` and this
    shim reads it through FastMCP's current request provider. If the middleware is
    absent, the function can still extract from the active request headers/query;
    if no request context exists it fails closed with ``None``.
    """
    try:
        from fastmcp.server.dependencies import get_http_request
    except ImportError:
        return None

    try:
        request = get_http_request()
    except RuntimeError:
        return None

    credential = request_scope_mcp_credential(request.scope)
    if credential is not None:
        return credential
    return extract_mcp_credential_from_request(request)


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


def get_unit_of_work_factory_for_mcp():
    """UnitOfWorkFactory for the MCP strangler path (spec #04; R01B FR3 repoint).

    Resolves the edition-owned relational ``UnitOfWorkFactory`` from the
    process-level registry (:mod:`okto_pulse.core.runtime_registry`) instead of
    constructing a core ``SQLAlchemyUnitOfWorkFactory`` — the core no longer owns
    the relational adapter (R01B / TR4 / AC4). The edition composition root
    (Community) registers its factory over the same live session source; the
    resolution fails closed if no edition provider was registered.
    """
    from okto_pulse.core.runtime_registry import resolve_unit_of_work_factory

    return resolve_unit_of_work_factory()


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


async def _authenticate_mcp_credential(credential):
    """Resolve an agent from a request-scoped MCP credential."""
    if credential is None:
        return None
    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agent = await service.get_agent_by_key(credential.value)
        await db.commit()
        return agent


async def _get_authenticated_agent():
    """Get the agent authenticated via the current request-scoped MCP key."""
    return await _authenticate_mcp_credential(active_api_key_credential())


async def _get_agent_ctx_for_credential(board_id: str, credential) -> AgentContext | None:
    """Authenticate a provided MCP credential and verify board access.

    Resolves granular PermissionSet (agent_flags ∩ board_overrides) with 60s
    cache. Falls back to legacy flat permissions if permission_flags is not set.
    """
    if credential is None:
        return None
    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agent = await service.get_agent_by_key(credential.value)
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


async def _get_agent_ctx(board_id: str) -> AgentContext | None:
    """Authenticate agent from the current request-scoped MCP key."""
    return await _get_agent_ctx_for_credential(board_id, active_api_key_credential())


async def _get_global_agent_ctx() -> AgentContext | None:
    """Authenticate an MCP agent without granting implicit all-board scope."""
    credential = active_api_key_credential()
    if credential is None:
        return None
    async with get_db_for_mcp() as db:
        service = AgentService(db)
        agent = await service.get_agent_by_key(credential.value)
        if not agent:
            return None

        agent_flags = getattr(agent, "permission_flags", None)
        if agent_flags is not None:
            from okto_pulse.core.infra.permissions import resolve_permissions

            preset_flags = None
            preset_id = getattr(agent, "preset_id", None)
            if preset_id:
                from okto_pulse.core.models.db import PermissionPreset

                preset = await db.get(PermissionPreset, preset_id)
                if preset:
                    preset_flags = preset.flags
            perm_set = resolve_permissions(agent_flags, preset_flags, None)
        else:
            perm_set = agent.permissions

        await db.commit()
        return AgentContext(
            agent_id=agent.id,
            agent_name=agent.name,
            board_id="",
            permissions=perm_set,
        )


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


def _canonical_sprint_validation_error(exc) -> str:
    """Render a SprintCreate/SprintUpdate ValidationError as a canonical MCP error.

    S-LANE-01: a ``lane_type`` enum failure becomes the shared
    ``invalid_lane_type`` envelope — identical to the REST surface — so the agent
    never sees the raw Pydantic text (no ``errors.pydantic.dev`` URL, class name,
    or traceback). Any other field failure becomes a clean, non-leaking error via
    ``errors(include_url=False)`` (mirrors ``_canonical_api_contract_error``).
    """
    from okto_pulse.core.inbound.enum_error_envelope import canonical_enum_error

    envelope = canonical_enum_error(exc.errors())
    if envelope is not None:
        return json.dumps(envelope)
    details = "; ".join(
        ((".".join(str(p) for p in e.get("loc", ())) + ": ") if e.get("loc") else "")
        + str(e.get("msg", "invalid value"))
        for e in exc.errors(include_url=False)
    )
    return json.dumps({"error": "invalid_sprint", "detail": details})


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


def _refuse_human_control(
    *, board_id: str, blocked_tool: str, blocked_action: str, target_ref: str | None = None,
) -> str:
    """R5-IMP5 — single agent-boundary choke point for a human-only skip/no_action
    refusal. Emits the bounded-label rejection counter, then returns the read-only
    ``human_control_required`` envelope (gate_contracts stays pure). Fail-closed: no
    DB / ledger / skip_ambiguity_gate mutation — the metric is in-process only."""
    emit_human_control_required(
        board_id=board_id, blocked_tool=blocked_tool, blocked_action=blocked_action,
    )
    return json.dumps(human_control_required_envelope(
        blocked_tool=blocked_tool, blocked_action=blocked_action, target_ref=target_ref,
    ))


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
    if design.parent_type == "card":
        return None, CARD_ARCHITECTURE_READ_ONLY_MESSAGE
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


# Maximum bytes accepted by content ingestion (16 MB).
_MAX_CONTENT_BYTES = 16 * 1024 * 1024


async def _resolve_text_content(
    *,
    content: str,
    content_reference: str | None,
) -> tuple[str | None, str | None]:
    """Resolve text content from inline payload or an abstract runtime reference."""
    provided = [bool(content), bool(content_reference)]
    if sum(provided) == 0:
        return None, "One of 'content' or 'content_reference' must be provided"
    if sum(provided) > 1:
        return None, "Only one of 'content' or 'content_reference' may be provided"

    if content:
        normalized = content.replace("\\n", "\n")
        if len(normalized.encode("utf-8")) > _MAX_CONTENT_BYTES:
            return None, f"content exceeds {_MAX_CONTENT_BYTES} bytes"
        return normalized, None

    from okto_pulse.core.runtime_registry import resolve_content_ingestion_resolver

    resolver = resolve_content_ingestion_resolver()
    if resolver is None:
        return None, "content_reference requires a registered content ingestion resolver"
    try:
        resolved = await resolver.resolve_text(content_reference, max_bytes=_MAX_CONTENT_BYTES)
        return resolved.text, None
    except ContentIngestionError as e:
        return None, f"{e.code}: {e.message}"
    except Exception:
        return None, "content_reference resolution failed"


async def _resolve_binary_content(
    *,
    content_base64: str,
    content_reference: str | None,
) -> tuple[bytes | None, str | None]:
    """Resolve binary content from base64 payload or an abstract runtime reference."""
    provided = [bool(content_base64), bool(content_reference)]
    if sum(provided) == 0:
        return None, "One of 'content_base64' or 'content_reference' must be provided"
    if sum(provided) > 1:
        return None, "Only one of 'content_base64' or 'content_reference' may be provided"

    if content_base64:
        try:
            decoded = base64.b64decode(content_base64, validate=True)
        except (binascii.Error, ValueError) as e:
            return None, f"Invalid base64 content: {e}"
        if len(decoded) > _MAX_CONTENT_BYTES:
            return None, f"content_base64 exceeds {_MAX_CONTENT_BYTES} bytes ({len(decoded)})"
        return decoded, None

    from okto_pulse.core.runtime_registry import resolve_content_ingestion_resolver

    resolver = resolve_content_ingestion_resolver()
    if resolver is None:
        return None, "content_reference requires a registered content ingestion resolver"
    try:
        resolved = await resolver.resolve_binary(content_reference, max_bytes=_MAX_CONTENT_BYTES)
        return resolved.data, None
    except ContentIngestionError as e:
        return None, f"{e.code}: {e.message}"
    except Exception:
        return None, "content_reference resolution failed"


# D-8: helpers canônicos em services/analytics_service.py — re-exports para
# preservar import paths existentes (tests + callers legados).
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    _structured_ref_id,  # noqa: F401  (used to enumerate available ac_ids in errors)
    resolve_linked_criteria_to_ids,  # noqa: F401  (write-path strict resolver — spec aafcc73f)
    resolve_linked_fr_indices,  # noqa: F401  (read-path tolerant FR resolver — FR4)
    resolve_linked_requirements_to_ids,  # noqa: F401  (write-path strict FR resolver — spec 9d66847f)
)
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    decisions_stats as _decisions_stats,  # noqa: F401
)
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    filter_decisions_by_status as _filter_decisions_by_status,  # noqa: F401
)
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    render_decisions_markdown as _render_decisions_markdown,  # noqa: F401
)

# D-7: spec_coverage agora canônico em services/analytics_service.py — re-export
# preserva callers existentes em mcp/server.py + tests.
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    resolve_linked_criteria_to_indices as _resolve_linked_criteria_to_indices,  # noqa: F401
)
from okto_pulse.core.services.analytics_service import (  # noqa: E402
    spec_coverage_summary as _spec_coverage,  # noqa: F401
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
async def okto_pulse_get_publish_health() -> str:
    """Get the local telemetry publish-health status (R5C-A).

    Reports whether this install's anonymous usage publishing is healthy,
    degraded, recovering, failing, stale, disabled, or unavailable — plus the
    last success/failure timestamps, the scheduled next retry, and freshness.
    This is the agent-facing twin of the `GET /metrics/publish-health` endpoint;
    it reads the install-local failure-state and is NOT board-scoped. The
    response is the allowlisted, redacted projection only (install id appears
    solely as `install_id_redacted` — never a token/secret). No parameters."""
    agent = await _get_authenticated_agent()
    if not agent:
        return json.dumps({"error": "Authentication failed"})

    from okto_pulse.core.telemetry.telemetry_port_registry import get_telemetry_port

    result = get_telemetry_port(get_settings()).publish_health()
    return json.dumps(result, default=str)


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

    from okto_pulse.core.application.use_cases import (
        McpGetBoardCommand,
        McpGetBoardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: the board + agents/specs/ideations + effective design-
    # system fetch moves into McpGetBoardUseCase over the MCP UoW; the adapter keeps
    # the include-aware payload shaping below (built INSIDE the context so lazy
    # board.cards/board.settings load while the session is live). Tool no longer
    # opens get_db_for_mcp nor builds the services.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpGetBoardUseCase().execute(
                McpGetBoardCommand(board_id), actor=actor, uow=uow
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Board not found"})
        board = _r.board
        board_agents = _r.agents
        board_specs = _r.specs
        board_ideations = _r.ideations
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

        # Surface the board's effective Design System + canonical gate mode so an agent
        # sees the mockup mandate in the board overview (spec 24f2e786, FR1-FR4) instead of
        # only discovering it by being rejected at the MockupDesignSystemGate. The resolver
        # dict shape differs by source (board_link carries title/status/scope/exists;
        # default_snapshot carries gate_mode but NOT title), so normalize to a stable
        # {design_system_id, title|None, version, source}. gate_mode is read ONLY from
        # BoardSettings (canonical) — never the snapshot mirror. Read-only: the gate itself
        # is untouched.
        _ds_effective_raw = _r.ds_effective_raw
        _ds_effective = (
            {
                "design_system_id": _ds_effective_raw.get("design_system_id"),
                "title": _ds_effective_raw.get("title"),
                "version": _ds_effective_raw.get("version"),
                "source": _ds_effective_raw.get("source"),
            }
            if _ds_effective_raw
            else None
        )
        _ds_gate_mode = (board.settings or {}).get("design_system_gate_mode", "off") or "off"
        payload["design_system"] = {
            "effective": _ds_effective,
            "gate_mode": _ds_gate_mode,
            "mandate": bool(_ds_effective) and _ds_gate_mode == "blocking",
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

    from okto_pulse.core.application.use_cases import (
        McpListBoardMembersCommand,
        McpListBoardMembersUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpListBoardMembersUseCase().execute(
                McpListBoardMembersCommand(board_id), actor=actor, uow=uow
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Board not found"})
        board = _r.board
        board_agents = _r.agents

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
    Create a new card on the board. Every card MUST be linked to a spec.

    For card_type='test', test_scenario_ids is mandatory and is limited by the
    board setting max_scenarios_per_card (default 3). Split larger scenario
    sets into separate test cards before creating/linking them.
    """
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

    from okto_pulse.core.domain.enums import (
        BugSeverity,
        CardPriority,
        CardStatus,
        CardType,
    )
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

    from okto_pulse.core.application.use_cases import (
        McpCreateCardCommand,
        McpCreateCardUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: the full create orchestration (create skip_ownership →
    # commit → scenario backlink → card_created log → commit; 2 commits + flush)
    # moves into McpCreateCardUseCase over the MCP UoW; the adapter keeps the
    # coercion + envelope + resp_card. Tool no longer opens get_db_for_mcp nor
    # builds CardService.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
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
            card = (
                await McpCreateCardUseCase().execute(
                    McpCreateCardCommand(
                        board_id,
                        spec_id,
                        card_create,
                        scenario_ids_list,
                        {"title": title, "status": status, "priority": priority},
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).card
        except CardOperationError as e:
            # Preserve the legacy MCP envelope for max_scenarios_per_card_exceeded
            # while CardService remains the canonical enforcement point.
            return json.dumps({"error": e.code, **e.to_dict(), **e.facts})
        except ValueError as e:
            return json.dumps({"error": str(e)})

        if not card:
            return json.dumps({"error": "Failed to create card"})

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

    from okto_pulse.core.application.use_cases import (
        McpGetCardCommand,
        McpGetCardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: board-scope read via McpGetCardUseCase over the MCP UoW;
    # the JSON (incl. lazy attachments/qa/comments) is built INSIDE the context so
    # relationships load while the session is alive. Tool no longer opens
    # get_db_for_mcp nor builds CardService.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            card = (
                await McpGetCardUseCase().execute(
                    McpGetCardCommand(card_id, board_id), actor=actor, uow=uow
                )
            ).card
        except EntityNotFoundError:
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
    Task card context: card body, linked spec requirements/scenarios/BRs/contracts,
    validations, resources and Q&A. Use `summary` for exploration and `profile="full"`
    before card work or status-changing moves.

    Test cards expose `test_card_operational_flow`: update linked scenarios with
    `okto_pulse_update_test_scenario_status`, then move the card to done; task
    validation is not used. `gate_readiness` mirrors the active done-gate and
    cognitive-readiness verdict without mutating or skipping anything.
    Full docs: okto-pulse://reference/tool-docs/misc."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.mcp.projection_envelope import (
        resolve_profile as _resolve_profile,
    )
    from okto_pulse.core.mcp.projection_envelope import (
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
        from okto_pulse.core.models.db import Board as _Board
        from okto_pulse.core.models.db import Spec as _Spec
        from okto_pulse.core.models.db import Sprint as _Sprint
        board_obj = await db.get(_Board, card.board_id)
        board_settings = board_obj.settings or {} if board_obj else {}
        spec_for_gate = await db.get(_Spec, card.spec_id) if card.spec_id else None
        sprint_for_gate = await db.get(_Sprint, card.sprint_id) if card.sprint_id else None
        result["validation_config"] = card_service._resolve_validation_config(
            card, spec_for_gate, sprint_for_gate, board_settings
        )

        from okto_pulse.core.mcp.context_projection import project_task_context
        projected = project_task_context(result, card_id=card_id, profile=profile)
        # R4-IMP3: proactive read-only operational-flow block for test cards (reuses
        # the R4-IMP1 test_card_completion contract). Injected AFTER projection so it
        # is present in every profile — the operator sees the completion path before
        # hitting the gate. No state-machine change.
        operational_flow = None
        if card.card_type and card.card_type.value == "test":
            linked_scenarios = []
            if spec and card.test_scenario_ids:
                linked_scenarios = [
                    ts for ts in (spec.test_scenarios or [])
                    if ts.get("id") in card.test_scenario_ids
                ]
            operational_flow = operational_flow_for_test_card(
                card_id=card.id,
                board_id=board_id,
                spec_id=card.spec_id,
                current_status=card.status.value,
                linked_scenarios=linked_scenarios,
            )
            projected["test_card_operational_flow"] = operational_flow

        # R4-IMP4: read-only gate/readiness block — surfaces the SAME enforcement /
        # cognitive verdict / gate fields the done-gate enforces (parity by
        # construction), so an agent sees would_block_done (enforcement-aware), the
        # active gate and its required tool BEFORE acting. Injected post-projection so
        # it survives every profile. No state-machine change; mutation_allowed=False.
        cognitive_enforcement_active = await _cognitive_enforcement_active(db, board_id)
        cognitive_verdict = await _evaluate_card_cognitive_verdict(
            db, board_id, card, cognitive_enforcement_active
        )
        projected["gate_readiness"] = task_gate_readiness(
            card_status=card.status.value,
            cognitive_enforcement_active=cognitive_enforcement_active,
            cognitive_verdict=cognitive_verdict,
            operational_flow=operational_flow,
        )
        return json.dumps(projected, default=str)


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
    are REJECTED. For bidirectional scenario linking, use
    okto_pulse_link_task(target_type='scenario', ...).
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        McpUpdateCardCommand,
        McpUpdateCardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.domain.enums import BugSeverity, CardPriority
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.models.schemas import CardUpdate

    # MCP-FU6 strangler: board-scope + atomic card_updated activity log + commit
    # move into McpUpdateCardUseCase over the MCP UoW. Input coercion stays in the
    # adapter; the adapter keeps the exact envelope + except order. Tool no longer
    # opens get_db_for_mcp nor builds CardService.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
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
        try:
            updated = (
                await McpUpdateCardUseCase().execute(
                    McpUpdateCardCommand(
                        card_id, board_id, card_update, update_data
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).card
        except EntityNotFoundError:
            return json.dumps({"error": "Card not found"})
        except CardOperationError as e:
            return json.dumps({"error": e.code, **e.to_dict(), **e.facts})
        except ValueError as e:
            return json.dumps({"error": str(e)})

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

    from okto_pulse.core.domain.enums import CardStatus
    from okto_pulse.core.models.schemas import CardMove

    try:
        card_status = CardStatus(status)
    except ValueError:
        return json.dumps(
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in CardStatus]}"
            }
        )

    from okto_pulse.core.application.use_cases import (
        McpMoveCardCommand,
        McpMoveCardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: board-scope + state-machine move + commit move into
    # McpMoveCardUseCase over the MCP UoW; the adapter keeps the exact envelope,
    # except order and _resource_gate_error_response. A None move yields card=None
    # (commit skipped) → "Failed to move card", preserving the legacy early-return.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
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
            updated = (
                await McpMoveCardUseCase().execute(
                    McpMoveCardCommand(card_id, board_id, move_data),
                    actor=actor,
                    uow=uow,
                )
            ).card
        except EntityNotFoundError:
            return json.dumps({"error": "Card not found"})
        except CardOperationError as e:
            return json.dumps({
                "error": e.code,
                **e.to_dict(),
                "blocked_by_dependencies": True,
            })
        except GateContractError as e:
            return json.dumps(e.to_dict())
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        except ValueError as e:
            return json.dumps({"error": str(e), "blocked_by_dependencies": True})

        if not updated:
            return json.dumps({"error": "Failed to move card"})

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

    from okto_pulse.core.application.use_cases import (
        McpDeleteCardCommand,
        McpDeleteCardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: board-scope + atomic card_deleted activity log + delete +
    # commit move into McpDeleteCardUseCase over the MCP UoW; the adapter keeps the
    # exact envelope. Tool no longer opens get_db_for_mcp nor builds CardService.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            deleted = (
                await McpDeleteCardUseCase().execute(
                    McpDeleteCardCommand(card_id, board_id), actor=actor, uow=uow
                )
            ).deleted
    except EntityNotFoundError:
        return json.dumps({"error": "Card not found"})

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

    from okto_pulse.core.application.use_cases import (
        AddCardDependencyCommand,
        AddCardDependencyUseCase,
    )
    from okto_pulse.core.application.use_cases.base import ConflictError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: reuse the REST AddCardDependencyUseCase (same semantics —
    # None on cycle/self-ref → ConflictError); adapter maps it to the legacy MCP
    # message. Tool no longer opens get_db_for_mcp nor builds CardService.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            await AddCardDependencyUseCase().execute(
                AddCardDependencyCommand(card_id, depends_on_id),
                actor=actor,
                uow=uow,
            )
    except ConflictError:
        return json.dumps(
            {"error": "Dependência circular detectada ou auto-referência"}
        )
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

    from okto_pulse.core.application.use_cases import (
        McpRemoveCardDependencyCommand,
        McpRemoveCardDependencyUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        removed = (
            await McpRemoveCardDependencyUseCase().execute(
                McpRemoveCardDependencyCommand(card_id, depends_on_id),
                actor=actor,
                uow=uow,
            )
        ).removed
    return json.dumps({"success": removed})


@mcp.tool()
async def okto_pulse_get_card_dependencies(board_id: str, card_id: str) -> str:
    """
    List cards that this card depends on and cards that depend on it."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases import (
        McpGetCardDependenciesCommand,
        McpGetCardDependenciesUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpGetCardDependenciesUseCase().execute(
            McpGetCardDependenciesCommand(card_id), actor=actor, uow=uow
        )
        return json.dumps(
            {
                "card_id": card_id,
                "can_advance": result.can_advance,
                "blocking_titles": result.blocking_titles,
                "depends_on": [
                    {"id": d.id, "title": d.title, "status": d.status.value}
                    for d in result.dependencies
                ],
                "dependents": [
                    {"id": d.id, "title": d.title, "status": d.status.value}
                    for d in result.dependents
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
    options: list[str] | str = "",
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
    content_reference: str | None = None,
) -> str:
    """
    Upload a file attachment to a card.

    Provide exactly ONE of: content_base64 or content_reference."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.ATTACHMENTS_UPLOAD)
    if perm_err:
        return _perm_error(perm_err)

    content, err = await _resolve_binary_content(
        content_base64=content_base64, content_reference=content_reference
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
    from okto_pulse.core.domain.enums import StoryStatus
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
    from okto_pulse.core.domain.enums import StoryStatus
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
    from okto_pulse.core.application.use_cases import (
        McpLinkStoryToIdeationCommand,
        McpLinkStoryToIdeationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation link_story, VARIANT opt-C): board-scope + the
    # story-state permission (via actor.permissions — preserves the MCP auth source) +
    # the link (mark_converted=True) live in McpLinkStoryToIdeationUseCase; the adapter
    # maps perm_err to _mcp_permission_error_response and builds the envelope.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpLinkStoryToIdeationUseCase().execute(
                McpLinkStoryToIdeationCommand(board_id, story_id, ideation_id),
                actor=actor,
                uow=uow,
            )
            if _r.not_found:
                return json.dumps({"error": "Story or Ideation not found"})
            if _r.perm_err is not None:
                return _mcp_permission_error_response(_r.perm_err)
            link = _r.link
            story = _r.story
            return json.dumps(
                {
                    "success": True,
                    "link": {"id": link.id, "story_id": link.story_id, "ideation_id": link.ideation_id},
                    "story": _story_payload(story) if story else None,
                    "mark_converted_input_ignored": not _flag_enabled(mark_converted),
                },
                default=str,
            )
    except ValueError as e:
        return json.dumps({"error": str(e)})


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

    data = StoryConversionRequest(
        story_ids=story_id_list,
        ideation_id=ideation_id or None,
        title=title or None,
        description=description.replace("\\n", "\n") if description else None,
        problem_statement=problem_statement.replace("\\n", "\n") if problem_statement else None,
        proposed_approach=proposed_approach.replace("\\n", "\n") if proposed_approach else None,
        mockup_ids=mockup_id_list,
    )

    from okto_pulse.core.application.use_cases import (
        McpConvertStoriesCommand,
        McpConvertStoriesUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation convert_stories, VARIANT opt-C): per-story board-scope
    # + per-story conversion-state permission (via actor.permissions) + convert run in
    # McpConvertStoriesUseCase. The adapter keeps the board-level permission (above),
    # the coercion + StoryConversionRequest build, and maps perm_err + envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpConvertStoriesUseCase().execute(
                McpConvertStoriesCommand(board_id, story_id_list, data),
                actor=actor,
                uow=uow,
            )
            if _r.out_of_board:
                return json.dumps({"error": "One or more Stories were not found in this board"})
            if _r.perm_err is not None:
                return _mcp_permission_error_response(_r.perm_err)
            if _r.board_not_found:
                return json.dumps({"error": "Board not found"})
            return json.dumps(
                {
                    "success": True,
                    "ideation": {"id": _r.ideation.id, "title": _r.ideation.title, "status": _r.ideation.status.value},
                    "links": [{"id": link.id, "story_id": link.story_id, "ideation_id": link.ideation_id} for link in _r.links],
                    "propagated_mockups": _r.propagated,
                },
                default=str,
            )
    except ValueError as e:
        return json.dumps({"error": str(e)})


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

    from okto_pulse.core.application.use_cases import (
        McpCreateIdeationCommand,
        McpCreateIdeationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.models.schemas import IdeationCreate

    # MCP-FU6 strangler (ideation create, VARIANT): McpCreateIdeationUseCase runs the
    # skip_ownership create + commit; the adapter keeps the IdeationCreate build and
    # the id/title/status/version envelope.
    ideation_data = IdeationCreate(
        title=title,
        description=description.replace("\\n", "\n") if description else None,
        problem_statement=problem_statement.replace("\\n", "\n") if problem_statement else None,
        proposed_approach=proposed_approach.replace("\\n", "\n") if proposed_approach else None,
        assignee_id=assignee_id or None,
        labels=coerce_to_list_str(labels) or None,
    )
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpCreateIdeationUseCase().execute(
            McpCreateIdeationCommand(board_id, ideation_data), actor=actor, uow=uow
        )
        if not _r.ideation:
            return json.dumps({"error": "Failed to create ideation"})
        ideation = _r.ideation
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

    from okto_pulse.core.application.use_cases import (
        McpGetIdeationCommand,
        McpGetIdeationUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation get, VARIANT board-scoped): McpGetIdeationUseCase does
    # board-scope + get; the refinements/specs/qa_items aggregation envelope is built
    # below over uow.session (lazy ORM relationships — get_spec_context precedent).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            ideation = (
                await McpGetIdeationUseCase().execute(
                    McpGetIdeationCommand(ideation_id, board_id), actor=actor, uow=uow
                )
            ).ideation
        except EntityNotFoundError:
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

    from okto_pulse.core.application.use_cases import (
        McpGetIdeationCommand,
        McpGetIdeationUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation get_context, VARIANT): reuses McpGetIdeationUseCase
    # (board-scope + get); the heavy aggregation below stays in the adapter over
    # db = uow.session (server helpers / lazy relationships — get_spec_context pattern).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            ideation = (
                await McpGetIdeationUseCase().execute(
                    McpGetIdeationCommand(ideation_id, board_id), actor=actor, uow=uow
                )
            ).ideation
        except EntityNotFoundError:
            return json.dumps({"error": "Ideation not found"})

        db = uow.session

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
            # R5-IMP2: read-only skip-override read-model (ambiguity gate skip).
            "skip_overrides": await ideation_skip_overrides(db, ideation, board_id),
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

    from okto_pulse.core.application.use_cases import (
        McpUpdateIdeationCommand,
        McpUpdateIdeationUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation update, VARIANT no-board-scope): McpUpdateIdeation-
    # UseCase does update + commit; the adapter keeps the IdeationUpdate build (above)
    # and the envelope (built inside the context — status/complexity are lazy).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            ideation = (
                await McpUpdateIdeationUseCase().execute(
                    McpUpdateIdeationCommand(ideation_id, ideation_update),
                    actor=actor,
                    uow=uow,
                )
            ).ideation
        except EntityNotFoundError:
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

    from okto_pulse.core.domain.enums import IdeationStatus
    from okto_pulse.core.models.schemas import IdeationMove

    try:
        ideation_status = IdeationStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in IdeationStatus]}"}
        )

    from okto_pulse.core.application.use_cases import (
        EntityNotFoundError,
        MoveIdeationCommand,
        MoveIdeationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec #04 (MCP strangler): obtain a PulseUnitOfWork from the MCP
    # UnitOfWorkFactory instead of opening a raw get_db_for_mcp() session — the
    # tool no longer calls get_db_for_mcp directly. uow.session is used only as a
    # transitional bridge for the board-scoped pre-check.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        service = IdeationService(uow.session)
        existing = await service.get_ideation(ideation_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})
        old_status = existing.status.value
        try:
            # Delegate to the shared transport-free use case (it commits +
            # re-fetches via the uow). board pre-check, old_status, compact MCP
            # payload, error envelopes and actor_name are preserved.
            result = await MoveIdeationUseCase().execute(
                MoveIdeationCommand(ideation_id, IdeationMove(status=ideation_status)),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            # Defensive: the pre-check covers not-found, but guard the race where
            # the ideation is removed between pre-check and the use case — preserve
            # the original "Ideation not found" envelope (not a ValueError).
            return json.dumps({"error": "Ideation not found"})
        except ValueError as e:
            return MCPAdapterContract.error(e)

        return json.dumps(
            {
                "success": True,
                "ideation_id": result.ideation.id,
                "from_status": old_status,
                "to_status": status,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_set_ideation_ambiguity_gate_skip(
    board_id: str, ideation_id: str, skip_ambiguity_gate: bool
) -> str:
    """
    Human-only control (R5-IMP1): the per-ideation Max ambiguity gate skip is a
    human decision and is NOT applicable from the agent-facing MCP surface.

    This tool fails closed with ``human_control_required`` (mutation_allowed=false,
    state_changed=false) and never touches ``skip_ambiguity_gate``. To skip an
    ideation's ambiguity gate, a human operator uses the IDE control / the human
    REST surface (PATCH /api/v1/ideations/{ideation_id}/ambiguity-gate-skip)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    # R5-IMP1: fail closed BEFORE any service call — no skip_ambiguity_gate
    # mutation, no state change. The human UI / REST surface remains the path.
    # R5-IMP5: the choke point also emits the bounded-label rejection counter.
    return _refuse_human_control(
        board_id=board_id,
        blocked_tool="okto_pulse_set_ideation_ambiguity_gate_skip",
        blocked_action="set_ambiguity_gate_skip",
        target_ref=f"ideation:{ideation_id}",
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteIdeationCommand,
        McpDeleteIdeationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation delete, VARIANT): McpDeleteIdeationUseCase deletes +
    # cascades + commits; the adapter maps deleted=False -> "Ideation not found".
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteIdeationUseCase().execute(
            McpDeleteIdeationCommand(ideation_id), actor=actor, uow=uow
        )

    if not _r.deleted:
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

    # Build the scope-score dict (int coercion + \n unescape) — transport.
    scope: dict = {}
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

    from okto_pulse.core.application.use_cases import (
        McpEvaluateIdeationCommand,
        McpEvaluateIdeationUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation evaluate, VARIANT board-scoped): the scope merge +
    # flag_modified + evaluate_complexity run in McpEvaluateIdeationUseCase; the adapter
    # builds the scope dict and the envelope (inside the context — lazy).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            ideation = (
                await McpEvaluateIdeationUseCase().execute(
                    McpEvaluateIdeationCommand(ideation_id, board_id, scope),
                    actor=actor,
                    uow=uow,
                )
            ).ideation
        except EntityNotFoundError:
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
    architecture_design_ids to select specific ones (default: all).
    architecture_propagation_mode accepts copy, derive, reference_only, or
    none; "snapshot" is not a mode."""
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

    from okto_pulse.core.application.use_cases import (
        McpDeriveSpecCommand,
        McpDeriveSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            spec = (
                await McpDeriveSpecUseCase().execute(
                    McpDeriveSpecCommand(
                        "ideation",
                        ideation_id,
                        mockup_ids=_mockup_ids,
                        kb_ids=_kb_ids,
                        architecture_design_ids=_architecture_ids,
                        architecture_propagation_mode=architecture_propagation_mode,
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).spec
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
    except EntityNotFoundError:
        return json.dumps({"error": "Ideation not found"})
    except ValueError as e:
        return json.dumps({"error": str(e)})


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

    from okto_pulse.core.application.use_cases import (
        McpGetIdeationSnapshotCommand,
        McpGetIdeationSnapshotUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        snapshot = (
            await McpGetIdeationSnapshotUseCase().execute(
                McpGetIdeationSnapshotCommand(ideation_id, int(version)),
                actor=actor,
                uow=uow,
            )
        ).snapshot
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

    from okto_pulse.core.application.use_cases import (
        McpGetIdeationHistoryCommand,
        McpGetIdeationHistoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        entries = (
            await McpGetIdeationHistoryUseCase().execute(
                McpGetIdeationHistoryCommand(ideation_id, int(limit)),
                actor=actor,
                uow=uow,
            )
        ).entries

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

    from okto_pulse.core.application.use_cases import (
        McpGetIdeationKnowledgeCommand,
        McpGetIdeationKnowledgeUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpGetIdeationKnowledgeUseCase().execute(
                McpGetIdeationKnowledgeCommand(ideation_id, board_id, knowledge_id),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Ideation not found"})
        if _r.kb_not_found:
            return json.dumps({"error": "Knowledge base item not found"})
        return json.dumps(_serialize_knowledge_base(_r.kb), default=str)


@mcp.tool()
async def okto_pulse_add_ideation_knowledge(
    board_id: str,
    ideation_id: str,
    title: str,
    content: str = "",
    description: str = "",
    mime_type: str = "text/markdown",
    content_reference: str | None = None,
) -> str:
    """
    Add a knowledge base item to an ideation.

    Provide exactly ONE of content or content_reference. Ideation KBs are
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
        content=content, content_reference=content_reference
    )
    if err:
        return json.dumps({"error": err})

    from okto_pulse.core.models.schemas import IdeationKnowledgeCreate

    kb_data = IdeationKnowledgeCreate(
        title=title,
        description=description or None,
        content=resolved_content,
        mime_type=mime_type,
    )

    from okto_pulse.core.application.use_cases import (
        McpAddIdeationKnowledgeCommand,
        McpAddIdeationKnowledgeUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpAddIdeationKnowledgeUseCase().execute(
                McpAddIdeationKnowledgeCommand(ideation_id, board_id, kb_data),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Ideation not found"})
        if not _r.kb:
            return json.dumps({"error": "Failed to create knowledge base item"})
        return json.dumps(
            {"success": True, "knowledge": _serialize_knowledge_base(_r.kb)},
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteIdeationKnowledgeCommand,
        McpDeleteIdeationKnowledgeUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpDeleteIdeationKnowledgeUseCase().execute(
                McpDeleteIdeationKnowledgeCommand(ideation_id, board_id, knowledge_id),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Ideation not found"})

    if _r.kb_not_found:
        return json.dumps({"error": "Knowledge base item not found"})
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
    options: list[str] | str = "",
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

    data = IdeationQACreate(
        question=question,
        question_type=question_type if question_type in ("choice", "multi_choice") else "choice",
        choices=choice_list,
        allow_free_text=allow_free_text.lower() == "true",
    )

    from okto_pulse.core.application.use_cases import (
        McpAskIdeationChoiceQuestionCommand,
        McpAskIdeationChoiceQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation Q&A ask, ATOMIC activity-log): create + the
    # ideation_choice_question_added log + commit run atomically in the use case; the
    # adapter parses options_json / coerces options into the IdeationQACreate.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpAskIdeationChoiceQuestionUseCase().execute(
            McpAskIdeationChoiceQuestionCommand(board_id, ideation_id, data),
            actor=actor,
            uow=uow,
        )
        if _r.ideation_not_found:
            return json.dumps({"error": "Ideation not found"})
        qa = _r.qa
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

    try:
        selected_list = coerce_to_list_str(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    from okto_pulse.core.application.use_cases import (
        McpAnswerIdeationQuestionCommand,
        McpAnswerIdeationQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation Q&A answer, ATOMIC activity-log): answer + the
    # ideation_question_answered log + commit run atomically in the use case, which
    # also catches QASelfAnsweringNotAllowedError (committing — legacy parity). The
    # adapter builds the IdeationQAAnswer and renders the envelopes.
    answer_payload = IdeationQAAnswer(answer=answer or None, selected=selected_list)
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpAnswerIdeationQuestionUseCase().execute(
            McpAnswerIdeationQuestionCommand(
                board_id,
                ideation_id,
                qa_id,
                answer_payload=answer_payload,
                answer_text=answer,
                selected_list=selected_list,
            ),
            actor=actor,
            uow=uow,
        )
        if _r.self_answer_error is not None:
            return json.dumps(
                {"error": _r.self_answer_error.reason, "detail": str(_r.self_answer_error)}
            )
        if _r.qa_not_found:
            return json.dumps({"error": "Q&A item not found or invalid selection"})
        qa = _r.qa
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
    to select specific ones (default: all). architecture_propagation_mode
    accepts copy, derive, reference_only, or none; "snapshot" is not a mode."""
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

    from okto_pulse.core.application.use_cases import (
        McpCreateRefinementCommand,
        McpCreateRefinementUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement create, VARIANT): McpCreateRefinementUseCase runs
    # the skip_ownership create + commit; the adapter keeps the RefinementCreate build
    # and the envelope. ValueError (ideation not done / propagation) -> {"error": str}.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpCreateRefinementUseCase().execute(
                McpCreateRefinementCommand(ideation_id, refinement_data),
                actor=actor,
                uow=uow,
            )
            if not _r.refinement:
                return json.dumps({"error": "Failed to create refinement (ideation not found)"})
            refinement = _r.refinement
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
    except ValueError as e:
        return json.dumps({"error": str(e)})


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

    from okto_pulse.core.application.use_cases import (
        McpGetRefinementCommand,
        McpGetRefinementUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement get, VARIANT board-scoped): McpGetRefinementUseCase
    # does board-scope + get; the specs/qa_items aggregation envelope is built below
    # over the live session (lazy ORM relationships — get_spec_context precedent).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            refinement = (
                await McpGetRefinementUseCase().execute(
                    McpGetRefinementCommand(refinement_id, board_id), actor=actor, uow=uow
                )
            ).refinement
        except EntityNotFoundError:
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

    from okto_pulse.core.application.use_cases import (
        McpGetRefinementCommand,
        McpGetRefinementUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement get_context, VARIANT): reuses McpGetRefinementUseCase
    # (board-scope + get); the heavy aggregation below stays in the adapter over
    # db = uow.session (server helpers / lazy relationships — get_spec_context pattern).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            refinement = (
                await McpGetRefinementUseCase().execute(
                    McpGetRefinementCommand(refinement_id, board_id), actor=actor, uow=uow
                )
            ).refinement
        except EntityNotFoundError:
            return json.dumps({"error": "Refinement not found"})

        db = uow.session

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

    from okto_pulse.core.application.use_cases import (
        McpUpdateRefinementCommand,
        McpUpdateRefinementUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement update, VARIANT no-board-scope): McpUpdateRefinement-
    # UseCase does update + commit; the adapter keeps the RefinementUpdate build (above)
    # + the envelope (inside the context — status is lazy).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            refinement = (
                await McpUpdateRefinementUseCase().execute(
                    McpUpdateRefinementCommand(refinement_id, refinement_update),
                    actor=actor,
                    uow=uow,
                )
            ).refinement
        except EntityNotFoundError:
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

    from okto_pulse.core.domain.enums import RefinementStatus
    from okto_pulse.core.models.schemas import RefinementMove

    try:
        refinement_status = RefinementStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in RefinementStatus]}"}
        )

    from okto_pulse.core.application.use_cases import (
        McpMoveRefinementCommand,
        McpMoveRefinementUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement move, VARIANT board-scoped): McpMoveRefinementUseCase
    # captures old_status BEFORE the move (board-scoped) and runs the state-machine move;
    # the adapter keeps the status enum validation + the from/to envelope. ValueError
    # (illegal transition) -> {"error": str}.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpMoveRefinementUseCase().execute(
                McpMoveRefinementCommand(
                    refinement_id, board_id, RefinementMove(status=refinement_status)
                ),
                actor=actor,
                uow=uow,
            )
            return json.dumps(
                {
                    "success": True,
                    "refinement_id": _r.refinement.id,
                    "from_status": _r.old_status,
                    "to_status": status,
                },
                default=str,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Refinement not found"})
    except ValueError as e:
        return json.dumps({"error": str(e)})


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

    from okto_pulse.core.application.use_cases import (
        McpDeleteRefinementCommand,
        McpDeleteRefinementUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement delete, VARIANT): McpDeleteRefinementUseCase deletes
    # + cascades Q&A + commits; the adapter maps deleted=False -> "Refinement not found".
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteRefinementUseCase().execute(
            McpDeleteRefinementCommand(refinement_id), actor=actor, uow=uow
        )

    if not _r.deleted:
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
    architecture_design_ids to select specific ones (default: all).
    architecture_propagation_mode accepts copy, derive, reference_only, or
    none; "snapshot" is not a mode."""
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

    from okto_pulse.core.application.use_cases import (
        McpDeriveSpecCommand,
        McpDeriveSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            spec = (
                await McpDeriveSpecUseCase().execute(
                    McpDeriveSpecCommand(
                        "refinement",
                        refinement_id,
                        mockup_ids=_mockup_ids,
                        kb_ids=_kb_ids,
                        architecture_design_ids=_architecture_ids,
                        architecture_propagation_mode=architecture_propagation_mode,
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).spec
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
    except EntityNotFoundError:
        return json.dumps({"error": "Refinement not found"})
    except ValueError as e:
        return json.dumps({"error": str(e)})


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

    from okto_pulse.core.application.use_cases import (
        McpGetRefinementHistoryCommand,
        McpGetRefinementHistoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        entries = (
            await McpGetRefinementHistoryUseCase().execute(
                McpGetRefinementHistoryCommand(refinement_id, int(limit)),
                actor=actor,
                uow=uow,
            )
        ).entries

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
    options: list[str] | str = "",
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

    data = RefinementQACreate(
        question=question,
        question_type=question_type if question_type in ("choice", "multi_choice") else "choice",
        choices=choice_list,
        allow_free_text=allow_free_text.lower() == "true",
    )

    from okto_pulse.core.application.use_cases import (
        McpAskRefinementChoiceQuestionCommand,
        McpAskRefinementChoiceQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement Q&A ask, ATOMIC activity-log): create + the
    # refinement_choice_question_added log + commit run atomically in the use case.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpAskRefinementChoiceQuestionUseCase().execute(
            McpAskRefinementChoiceQuestionCommand(board_id, refinement_id, data),
            actor=actor,
            uow=uow,
        )
        if _r.refinement_not_found:
            return json.dumps({"error": "Refinement not found"})
        qa = _r.qa
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

    try:
        selected_list = coerce_to_list_str(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    answer_payload = RefinementQAAnswer(answer=answer or None, selected=selected_list)

    from okto_pulse.core.application.use_cases import (
        McpAnswerRefinementQuestionCommand,
        McpAnswerRefinementQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement Q&A answer, ATOMIC activity-log): answer + the
    # refinement_question_answered log + commit run atomically in the use case, which
    # also catches QASelfAnsweringNotAllowedError (committing — legacy parity).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpAnswerRefinementQuestionUseCase().execute(
            McpAnswerRefinementQuestionCommand(
                board_id,
                refinement_id,
                qa_id,
                answer_payload=answer_payload,
                answer_text=answer,
                selected_list=selected_list,
            ),
            actor=actor,
            uow=uow,
        )
        if _r.self_answer_error is not None:
            return json.dumps(
                {"error": _r.self_answer_error.reason, "detail": str(_r.self_answer_error)}
            )
        if _r.qa_not_found:
            return json.dumps({"error": "Q&A item not found or invalid selection"})
        qa = _r.qa
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

    from okto_pulse.core.domain.enums import SpecStatus
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

    from okto_pulse.core.application.use_cases import (
        McpCreateSpecCommand,
        McpCreateSpecUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec VARIANT, decision #4): create + R3-IMP1 effective-resource
    # propagation run in McpCreateSpecUseCase over the MCP UoW; the propagation runs
    # BEFORE the single commit so a lineage/propagation failure rolls the just-flushed
    # spec back (returned in the result, not committed). The adapter keeps the SpecCreate
    # build, the invalid-status / multi-value envelopes and renders to_error_dict().
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
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

        _r = await McpCreateSpecUseCase().execute(
            McpCreateSpecCommand(board_id, spec_data, refinement_id or ""),
            actor=actor,
            uow=uow,
        )
        if _r.spec is None:
            return json.dumps({"error": "Failed to create spec"})
        if _r.lineage_error is not None:
            return json.dumps(_r.lineage_error.to_error_dict())
        if _r.propagation_error is not None:
            return json.dumps(
                _r.propagation_error.to_error_dict(spec_id=_r.spec.id)
            )
        spec = _r.spec

        response: dict[str, Any] = {
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
        }
        if _r.resource_propagation is not None:
            response["resource_propagation"] = _r.resource_propagation
        return json.dumps(response, default=str)


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

    from okto_pulse.core.application.use_cases import (
        GetSpecCommand,
        GetSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec REUSE): GetSpecUseCase over the MCP UoW; payload (incl.
    # lazy spec.cards + the IR/OR permission-gated fields via the adapter-only
    # _mcp_check_permission) is built INSIDE the context. No board-scope (matches REST).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            spec = (
                await GetSpecUseCase().execute(
                    GetSpecCommand(spec_id), actor=actor, uow=uow
                )
            ).spec
        except EntityNotFoundError:
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
    Consolidated spec context: requirements, scenarios, rules, contracts, IR/OR,
    decisions, resources, Q&A, evaluations, cards and sprints. Use `summary` for
    exploration and `profile="full"` before evaluating, moving, or deriving cards.

    Includes read-only `gate_readiness` for spec transition gates. Cognitive
    readiness is per-card; call `okto_pulse_get_task_context` for card verdicts.
    Full docs: okto-pulse://reference/tool-docs/misc."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.mcp.projection_envelope import (
        resolve_profile as _resolve_profile,
    )
    from okto_pulse.core.mcp.projection_envelope import (
        unsupported_projection_error as _unsupported_projection_error,
    )
    if _resolve_profile(profile) is None:
        return json.dumps(_unsupported_projection_error(profile))

    _inc_kb = _flag_enabled(include_knowledge)
    _inc_mockups = _flag_enabled(include_mockups)
    _inc_qa = _flag_enabled(include_qa)
    _inc_architecture = _flag_enabled(include_architecture)
    _inc_superseded = _flag_enabled(include_superseded)

    from okto_pulse.core.application.use_cases import (
        McpGetSpecContextCommand,
        McpGetSpecContextUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec VARIANT): board-scope + get_spec move into the pure
    # McpGetSpecContextUseCase; the heavy presentation aggregation below stays in the
    # adapter over uow.session (Codex-approved projection exception — server helpers /
    # the sprint swallow / gate_readiness stay here, NOT pushed into the core).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            spec = (
                await McpGetSpecContextUseCase().execute(
                    McpGetSpecContextCommand(spec_id, board_id), actor=actor, uow=uow
                )
            ).spec
        except EntityNotFoundError:
            return json.dumps({"error": "Spec not found"})

        db = uow.session

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
            # R5-IMP2: read-only skip-override read-model (cognitive skips on this
            # spec/its cards). Parent ideation ambiguity skip is lineage, not an
            # effective override of the spec, so it is excluded here.
            "skip_overrides": await spec_skip_overrides(db, spec, board_id),
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
        projected = project_spec_context(result, profile=profile)
        # R4-IMP4: read-only gate/readiness block. Spec context does NOT aggregate
        # per-card cognitive verdicts (codex Q1) — cognitive readiness is per-card and
        # lives in get_task_context. Here: the board's cognitive enforcement posture +
        # the spec's OWN spec_validation gate (derived from the SAME builder move_spec
        # raises). Injected post-projection so it survives every profile.
        from okto_pulse.core.models.db import Board as _Board
        board_obj = await db.get(_Board, spec.board_id)
        board_settings = (board_obj.settings or {}) if board_obj else {}
        cognitive_enforcement_active = await _cognitive_enforcement_active(
            db, spec.board_id
        )
        projected["gate_readiness"] = spec_gate_readiness(
            spec_id=spec.id,
            spec_status=spec.status.value,
            require_spec_validation=bool(
                board_settings.get("require_spec_validation", True)
            ),
            cognitive_enforcement_active=cognitive_enforcement_active,
        )
        return json.dumps(projected, default=str)


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

    from okto_pulse.core.application.use_cases import (
        McpUpdateSpecCommand,
        McpUpdateSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec VARIANT): the mutation moves into McpUpdateSpecUseCase
    # over the MCP UoW; the adapter keeps the legacy _safe_spec_update envelope
    # ({"error": str(exc)} for the linked-ref ValueError) + "Spec not found". The
    # flat SPECS_UPDATE check above is preserved (NOT the REST field-level perms).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            spec = (
                await McpUpdateSpecUseCase().execute(
                    McpUpdateSpecCommand(spec_id, spec_update), actor=actor, uow=uow
                )
            ).spec
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
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})


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

    from okto_pulse.core.domain.enums import SpecStatus
    from okto_pulse.core.models.schemas import SpecMove

    try:
        spec_status = SpecStatus(status)
    except ValueError:
        return json.dumps(
            {"error": f"Invalid status. Must be one of: {[s.value for s in SpecStatus]}"}
        )

    from okto_pulse.core.application.use_cases import (
        McpMoveSpecCommand,
        McpMoveSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec VARIANT): board-scope + old_status capture + move move
    # into McpMoveSpecUseCase over the MCP UoW; the adapter keeps the SpecMove build,
    # the from_status/to_status envelope and the except order. EntityNotFoundError
    # (missing/cross-board/None move) -> "Spec not found".
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpMoveSpecUseCase().execute(
                McpMoveSpecCommand(spec_id, board_id, SpecMove(status=spec_status)),
                actor=actor,
                uow=uow,
            )
            return json.dumps(
                {
                    "success": True,
                    "spec_id": _r.spec.id,
                    "from_status": _r.old_status,
                    "to_status": status,
                },
                default=str,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})
    except GateContractError as e:
        return json.dumps(e.to_dict())
    except ValueError as e:
        return json.dumps({"error": str(e)})


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
    concrete Given/When/Then test plans.

    scenario_type accepts exactly: unit, integration, e2e, manual, negative.
    Unsupported values fail closed with invalid_scenario_type and no mutation.
    Use negative for expected denial/error-path behavior.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    from okto_pulse.core.application.use_cases import (
        McpAddTestScenarioCommand,
        McpAddTestScenarioUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity test_scenario add, USE-CASE COMMIT): the
    # fail-closed scenario_type + AC token resolution (core) + build + persist live in
    # McpAddTestScenarioUseCase. The adapter renders the typed envelopes
    # (invalid_scenario_type with VALID_SCENARIO_TYPES, unresolved with available ids).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpAddTestScenarioUseCase().execute(
                McpAddTestScenarioCommand(
                    spec_id,
                    f"ts_{_uuid.uuid4().hex[:8]}",
                    title,
                    given.replace("\\n", "\n"),
                    when.replace("\\n", "\n"),
                    then.replace("\\n", "\n"),
                    scenario_type=scenario_type,
                    linked_criteria_tokens=(
                        parse_multi_value(linked_criteria) if linked_criteria else None
                    ),
                    notes=notes.replace("\\n", "\n") if notes else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.invalid_scenario_type is not None:
        return json.dumps({
            "error": "invalid_scenario_type",
            "message": (
                f"Invalid scenario_type {_r.invalid_scenario_type!r}. "
                f"Allowed values: {', '.join(VALID_SCENARIO_TYPES)}. "
                f"No scenario was appended."
            ),
        })
    if _r.unresolved_criteria is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_criteria token(s): {_r.unresolved_criteria}. "
                f"Valid indices: 0..{_r.criteria_count - 1}. "
                f"Available ac_ids: {_r.available_ac_ids}. "
                f"No scenario was appended."
            )
        })

    return json.dumps(
        {"success": True, "scenario": _r.scenario, **_saturation_or_coverage(_r.coverage)},
        default=str,
    )


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

    from okto_pulse.core.application.use_cases import (
        McpListTestScenariosCommand,
        McpListTestScenariosUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity test_scenario list): fetch is the domain
    # (use case); the filter / pagination / coverage-map / summary projection below
    # stays in the adapter (presentation).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _ts = await McpListTestScenariosUseCase().execute(
                McpListTestScenariosCommand(spec_id), actor=actor, uow=uow
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Spec not found"})

        all_scenarios = _ts.all_scenarios
        criteria = _ts.criteria

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
                    "by_type": {t: sum(1 for s in all_scenarios if s.get("scenario_type") == t) for t in VALID_SCENARIO_TYPES if any(s.get("scenario_type") == t for s in all_scenarios)},
                    # Historical/invalid persisted scenario_types are surfaced
                    # EXPLICITLY (spec ac16b3c9 FR5/AC5) rather than silently
                    # folded into a supported bucket or dropped — so a stale value
                    # like 'regression'/'exploratory' is visible for deliberate
                    # remediation. New writes already fail closed (card 58844a26).
                    "unsupported_types": {
                        st: sum(1 for s in all_scenarios if s.get("scenario_type") == st)
                        for st in sorted({
                            s.get("scenario_type")
                            for s in all_scenarios
                            if isinstance(s.get("scenario_type"), str)
                            and not is_valid_scenario_type(s.get("scenario_type"))
                        })
                    },
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
    InvalidScenarioTypeError,
    StatusNotMutableError,
    VALID_SCENARIO_STATUSES,
    VALID_SCENARIO_TYPES,
    is_valid_scenario_type,
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
passed/failed require either an explicit evidence_class with its required fields
or unclassed run-log evidence with evidence.last_run_at AND
(output_snippet OR test_run_id) AND expected_output_snapshot AND
non_replayable_justification; draft/ready optional. When skip is True the gate is bypassed but a
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

    from okto_pulse.core.application.use_cases import (
        SetTestScenarioStatusCommand,
        SetTestScenarioStatusUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec REUSE): SetTestScenarioStatusUseCase calls the service
    # which SELF-COMMITS its narrow update + audit — the use case does NOT commit the
    # UoW (no double-commit). The NC-9 envelopes stay in the adapter.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = (
                await SetTestScenarioStatusUseCase().execute(
                    SetTestScenarioStatusCommand(
                        spec_id, scenario_id, status, evidence_dict
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).result
    except StatusNotMutableError as exc:
        return json.dumps({
            "error": "status_not_mutable",
            "spec_status": exc.spec_status,
            "message": str(exc),
        })
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("evidence_required"):
            _ok, missing = validate_test_scenario_evidence(
                status, evidence_dict, for_write=True
            )
            return json.dumps({
                "error": "evidence_required",
                "required": missing,
                "message": (
                    f"Cannot mark scenario as {status} without structured "
                    f"evidence ({', '.join(missing)}). This prevents the test "
                    "theater anti-pattern by requiring replayable or justified "
                    "evidence. To bypass, enable "
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

    from okto_pulse.core.application.use_cases import (
        McpUpdateTestScenarioCommand,
        McpUpdateTestScenarioUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity test_scenario update, SERVICE SELF-COMMIT):
    # McpUpdateTestScenarioUseCase calls SpecService.update_test_scenario (which
    # commits internally) — NO use-case commit (double-commit). The domain exceptions
    # propagate here for the adapter to map to the legacy envelopes (NC-9 parity).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = (
                await McpUpdateTestScenarioUseCase().execute(
                    McpUpdateTestScenarioCommand(
                        spec_id,
                        scenario_id,
                        title=title,
                        given=given,
                        when=when,
                        then=then,
                        scenario_type=scenario_type,
                        linked_criteria_tokens=lc,
                        notes=notes,
                        clear_fields=clear_fields,
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).result
    except SpecLockedError:
        return json.dumps({
            "error": "spec_locked",
            "message": (
                "Spec is locked by a passed validation; the scenario body "
                "cannot be edited. Move the spec back to draft/approved first."
            ),
        })
    except InvalidScenarioTypeError as exc:
        # Fail-closed scenario_type on the body-edit path (spec ac16b3c9): must
        # precede the generic ValueError handler (it subclasses ValueError).
        return json.dumps({
            "error": "invalid_scenario_type",
            "message": f"{exc} No scenario was updated.",
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteTestScenarioCommand,
        McpDeleteTestScenarioUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity test_scenario delete, SERVICE SELF-COMMIT):
    # McpDeleteTestScenarioUseCase calls SpecService.delete_test_scenario (CASCADE +
    # internal commit) — NO use-case commit. Domain exceptions propagate for mapping.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = (
                await McpDeleteTestScenarioUseCase().execute(
                    McpDeleteTestScenarioCommand(spec_id, scenario_id),
                    actor=actor,
                    uow=uow,
                )
            ).result
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


_LINK_TASK_TARGET_TYPES = ("scenario", "fr", "rule", "decision", "tr", "contract", "ir", "or", "spec")
_LINK_TASK_TARGET_ALIASES = {
    "test_scenario": "scenario",
    "functional_requirement": "fr",
    "business_rule": "rule",
    "technical_requirement": "tr",
    "api_contract": "contract",
    "integration_requirement": "ir",
    "observability_requirement": "or",
}
_LINK_TASK_ACCEPTED_TARGET_TYPES = _LINK_TASK_TARGET_TYPES + tuple(_LINK_TASK_TARGET_ALIASES)


@mcp.tool()
async def okto_pulse_link_task(
    board_id: str,
    target_type: str,
    target_id: str,
    card_id: str,
    spec_id: str = "",
) -> str:
    """
    Generic task-linking tool — dispatches on `target_type`. Short codes
    (`fr`, `tr`, `ir`, `or`) and their long names (`functional_requirement`,
    `technical_requirement`, `integration_requirement`,
    `observability_requirement`) are accepted.
    Equivalent to the former per-type task-linking tools plus direct Functional
    Requirement task linking. Note: direct FR task links are traceability links;
    the FR coverage gate is satisfied by Business Rules linked to FRs, not by
    FR `linked_task_ids`.
    exposes a single entry point so agents don't have to pre-load eight near-
    identical tool schemas.

    Ideação MCP-token-optimization Story 5."""
    target_type = (target_type or "").strip().lower()
    target_type = _LINK_TASK_TARGET_ALIASES.get(target_type, target_type)
    if target_type not in _LINK_TASK_TARGET_TYPES:
        return json.dumps({
            "error": f"Unknown target_type '{target_type}'. Must be one of: {', '.join(_LINK_TASK_ACCEPTED_TARGET_TYPES)}"
        })
    # Dispatch to internal helpers (no @mcp.tool() decoration — see commit
    # removing 8 link_task_to_* shims in favor of this unified entry point).
    if target_type == "spec":
        return await _link_card_to_spec_internal(board_id, target_id, card_id)
    if not spec_id:
        return json.dumps({"error": f"spec_id is required when target_type='{target_type}'"})
    if target_type == "scenario":
        return await _link_task_to_scenario_internal(board_id, spec_id, target_id, card_id)
    if target_type == "fr":
        return await _link_task_to_fr_internal(board_id, spec_id, target_id, card_id)
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

        # Update card's test_scenario_ids. CardService owns the board cap and
        # spec membership validation so REST/MCP keep the same contract.
        if card:
            existing_ids = list(card.test_scenario_ids or [])
            if scenario_id not in existing_ids:
                existing_ids.append(scenario_id)
            try:
                await card_service.update_card(
                    card_id,
                    ctx.agent_id,
                    CardUpdate(test_scenario_ids=existing_ids),
                )
            except CardOperationError as e:
                return json.dumps({"error": e.code, **e.to_dict(), **e.facts})
            except ValueError as e:
                return json.dumps({"error": str(e)})

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


async def _link_task_to_fr_internal(
    board_id: str, spec_id: str, fr_id: str, card_id: str
) -> str:
    """Internal helper for link_task target_type='fr'."""
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

        frs = list(spec.functional_requirements or [])
        found = False
        for fr in frs:
            if isinstance(fr, dict) and fr.get("id") == fr_id:
                task_ids = list(fr.get("linked_task_ids") or [])
                if card_id not in task_ids:
                    task_ids.append(card_id)
                fr["linked_task_ids"] = task_ids
                found = True
                break

        if not found:
            return json.dumps({
                "error": f"Functional requirement '{fr_id}' not found in spec. "
                f"FRs may be in legacy string format — update the spec via "
                f"okto_pulse_update_spec to convert them to objects with IDs."
            })

        from okto_pulse.core.models.schemas import SpecUpdate
        _, err = await _safe_spec_update(
            spec_service, spec_id, ctx.agent_id, SpecUpdate(functional_requirements=frs)
        )
        if err:
            return err
        await db.commit()

        cov = _spec_coverage(spec)
        return json.dumps({
            "success": True,
            "fr_id": fr_id,
            "card_id": card_id,
            "coverage_note": (
                "Direct FR task link persisted. The FR coverage gate is still "
                "computed from business_rules[].linked_requirements."
            ),
            **_saturation_or_coverage(cov),
        })


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
async def okto_pulse_list_architecture_propagation_legacy(
    board_id: str,
    limit: int = 100,
    offset: int = 0,
    include_clean: str = "false",
    parent_type_filter: str = "",
) -> str:
    """List legacy Architecture Design snapshots whose SOURCE is now ineligible for
    propagation (Spec C). READ-ONLY / forward-only: never backfills, resolves findings,
    mutates snapshots, or changes SDLC status. Each item carries legacy_status
    (source_blocked|verdict_missing|source_unavailable), source identity, finding_keys and
    remediation. architecture_warning_acknowledgement is audit-only and is NOT a
    propagation bypass."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = _mcp_check_architecture_permission(ctx.permissions, "spec", "read")
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.architecture_propagation_legacy import (
            build_propagation_legacy_report,
        )

        report = await build_propagation_legacy_report(
            db,
            board_id=board_id,
            limit=limit,
            offset=offset,
            include_clean=_flag_enabled(include_clean),
            parent_type_filter=parent_type_filter or None,
            surface="mcp",
        )
        return json.dumps({"success": True, **report}, default=str)


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
    """Create an Architecture Design on an ideation, refinement, or spec. Card
Architecture Designs are read-only governed snapshots; use
okto_pulse_copy_architecture_to_card to refresh card context. Use whenever
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
        repo = ArchitectureDesignRepository(db)
        # R3-IMP2/RG-ARCH-RO: copy DIRECT spec designs, or fall back to every
        # effective inherited Architecture source when the spec has none direct.
        # Card Architecture remains a read-only governed snapshot, but the
        # propagation service is allowed to create/re-sync those snapshots and
        # persists source_design_id (the gate-readable origin identity).
        from okto_pulse.core.services.effective_resource_propagation import (
            ResourcePropagationError,
            ResourceLineageResolutionError,
        )
        try:
            designs, _plan = await service.copy_effective_spec_to_card(
                board_id=board_id,
                spec_id=spec_id,
                card_id=card_id,
                actor_id=ctx.agent_id,
                design_ids=ids,
                architecture_warning_acknowledgement=acknowledgement,
            )
        except ResourceLineageResolutionError as exc:
            return json.dumps(exc.to_error_dict())
        except ResourcePropagationError as exc:
            return json.dumps(exc.to_error_dict(spec_id=spec_id))
        except ArchitecturePropagationBlocked as exc:
            # Spec B: same canonical structured error as REST/internal services.
            return json.dumps(exc.to_error_dict())
        except Exception as exc:
            return _mcp_architecture_error(exc)
        try:
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
    cards to carry the relevant mockups into the card for the implementer's context.

    R3-IMP2: when the spec has no DIRECT mockup, falls back to the effective
    inherited mockup (refinement/ideation), preserving the mockup ``id`` (the
    identity the Resource Gate reads)."""
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

        source_mockups = [m for m in (spec.screen_mockups or []) if isinstance(m, dict)]
        fallback = False
        if not source_mockups:
            from okto_pulse.core.services.effective_resource_propagation import (
                ResourceLineageResolutionError,
                load_effective_mockup_items,
                resolve_effective_card_copy_plan,
            )
            try:
                plan = await resolve_effective_card_copy_plan(
                    db, board_id=board_id, spec_id=spec_id, resource_type="mockup",
                )
            except ResourceLineageResolutionError as exc:
                return json.dumps(exc.to_error_dict())
            if not plan["fallback"]:
                return _effective_empty_copy_response("mockup", plan)
            source_mockups = await load_effective_mockup_items(
                db, plan["source_entity_type"], plan["source_entity_id"]
            )
            if not source_mockups:
                return _effective_empty_copy_response("mockup", plan)
            fallback = True

        if screen_ids:
            try:
                ids = set(coerce_to_list_str(screen_ids))
            except ValueError as e:
                return json.dumps({"error": f"Invalid screen_ids: {e}"})
            source_mockups = [m for m in source_mockups if m.get("id") in ids]

        existing = list(card.screen_mockups or [])
        existing_ids = {m.get("id") for m in existing if isinstance(m, dict)}
        copied = 0
        for m in source_mockups:
            if m.get("id") not in existing_ids:
                existing.append(m)
                existing_ids.add(m.get("id"))
                copied += 1

        from okto_pulse.core.models.schemas import CardUpdate
        await card_service.update_card(
            card_id,
            ctx.agent_id,
            CardUpdate(screen_mockups=existing),
            allow_card_resource_write=True,
        )
        await db.commit()

    return json.dumps({"success": True, "copied": copied, "total_on_card": len(existing), "fallback": fallback})


def _effective_empty_copy_response(resource_type: str, plan: dict) -> str:
    """R3-IMP2 shared resolution of a copy with no DIRECT and no effective
    fallback resource: an N/A or genuinely-absent resource is an honest empty
    (NOT an error); a resource the gate reports provided but that cannot be
    resolved is an actionable error — never a generic "no resources to copy"."""
    if plan.get("not_applicable"):
        return json.dumps({
            "success": True, "copied": 0, "reason": "not_applicable",
            "resource_type": resource_type,
        })
    if not plan.get("has_obligation"):
        return json.dumps({
            "success": True, "copied": 0, "reason": "no_resource_required",
            "resource_type": resource_type,
        })
    return json.dumps({
        "error": "resource_propagation_failed",
        "resource_type": resource_type,
        "coverage_obligation_id": plan.get("coverage_obligation_id"),
        "accepted_identity_fields": plan.get("accepted_identity_fields", []),
        "retryable": True,
        "detail": (
            f"Spec {resource_type} is reported provided by the Resource Gate but no "
            "copyable resource (direct or effective/inherited) could be resolved."
        ),
    })


@mcp.tool()
async def okto_pulse_copy_knowledge_to_card(
    board_id: str, spec_id: str, card_id: str, knowledge_ids: list[str] | str = ""
) -> str:
    """
    Copy knowledge base entries from a spec to a card as inline card KEs.
    Each copied entry is stored in Card.knowledge_bases with stable provenance.

    R3-IMP2: when the spec has no DIRECT knowledge base, falls back to the
    effective inherited resource (refinement/ideation) so a card linked to a
    manual/legacy spec still carries the gate-required knowledge with an identity
    the Resource Gate reads (``source_kb_id``)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    try:
        id_filter = set(coerce_to_list_str(knowledge_ids)) if knowledge_ids else None
    except ValueError as e:
        return json.dumps({"error": f"Invalid knowledge_ids: {e}"})

    from okto_pulse.core.application.use_cases import (
        McpCopyKnowledgeToCardCommand,
        McpCopyKnowledgeToCardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.effective_resource_propagation import (
        ResourceLineageResolutionError,
    )

    # MCP-FU6 strangler: the spec/card lookup + R3-IMP2 effective fallback + dedup +
    # update + commit move into McpCopyKnowledgeToCardUseCase over the MCP UoW. The
    # adapter keeps the exact envelopes: spec/card not-found, the resolver
    # exc.to_error_dict(), the empty-plan _effective_empty_copy_response, and the
    # success payload. Tool no longer opens get_db_for_mcp nor builds the services.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpCopyKnowledgeToCardUseCase().execute(
                McpCopyKnowledgeToCardCommand(
                    board_id, spec_id, card_id, id_filter
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as e:
        return json.dumps(
            {
                "error": (
                    "Spec not found" if e.entity_type == "spec" else "Card not found"
                )
            }
        )
    except ResourceLineageResolutionError as exc:
        return json.dumps(exc.to_error_dict())

    if result.empty_plan is not None:
        return _effective_empty_copy_response("knowledge_base", result.empty_plan)

    return json.dumps({
        "success": True, "copied": result.copied, "knowledge_ids": result.copied_ids,
        "total_on_card": result.total_on_card, "fallback": result.fallback,
    })


# ============================================================================
# Card.knowledge_bases — inline JSONB lifecycle (symmetric to spec_knowledge)
# ============================================================================


def _card_resource_read_only_error() -> str:
    return json.dumps(
        {
            "error": "card_resource_read_only",
            "message": CARD_RESOURCE_READ_ONLY_MESSAGE,
            "retryable": False,
        }
    )


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
    Deprecated: card Knowledge Base resources are read-only governed snapshots.
    Use okto_pulse_copy_knowledge_to_card to refresh card context from the spec."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    return _card_resource_read_only_error()


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
    """Deprecated: card Knowledge Base resources are read-only governed snapshots."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    return _card_resource_read_only_error()


@mcp.tool()
async def okto_pulse_delete_card_knowledge(board_id: str, card_id: str, knowledge_id: str) -> str:
    """Deprecated: card Knowledge Base resources are read-only governed snapshots."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    return _card_resource_read_only_error()


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

        # Choice/multi-choice answers store selected option ids while
        # ``answer`` may stay NULL. Treat either shape as answered.
        qa_items = [qa for qa in (spec.qa_items or []) if _qa_answer_text(qa)]
        if not qa_items:
            return json.dumps({"error": "No answered Q&A to copy"})

        lines = ["## Spec Q&A Context\n"]
        for qa in qa_items:
            lines.append(f"**Q:** {qa.question}\n**A:** {_qa_answer_text(qa)}\n")

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

    from sqlalchemy import select

    from okto_pulse.core.domain.enums import CardStatus
    from okto_pulse.core.models.db import (
        Board,
        Card,
        Ideation,
        Refinement,
        Spec,
    )

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
            )
            from okto_pulse.core.services.analytics_service import (
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

    from okto_pulse.core.application.use_cases import (
        McpApplyStructuredSpecEntityCommand,
        McpApplyStructuredSpecEntityUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec structured-entity shared helper): the StructuredSpec-
    # EntityService.apply + the CONDITIONAL commit/rollback (success+changed -> commit,
    # else rollback) live in McpApplyStructuredSpecEntityUseCase. This adapter keeps
    # auth, the api_contract gating, entity_type validation, the payload_json /
    # expected_spec_version parsing (all above) and renders result.as_dict().
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpApplyStructuredSpecEntityUseCase().execute(
            McpApplyStructuredSpecEntityCommand(
                board_id=board_id,
                spec_id=spec_id,
                entity_type=entity_type,
                entity_id=entity_id,
                operation=operation,
                payload=payload,
                expected_spec_version=expected,
                task_id=task_id,
                ack_token=ack_token,
                permission_set=ctx.permissions,
            ),
            actor=actor,
            uow=uow,
        )
        return json.dumps(_r.result.as_dict(), default=str)


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
    Add a business rule to a spec. Business rules define system behavior
    constraints using When/Then format. linked_requirements accepts 0-based FR
    indices, fr_ ids, or exact FR text; display labels like FR-1 are rejected."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    from okto_pulse.core.application.use_cases import (
        McpAddBusinessRuleCommand,
        McpAddBusinessRuleUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity, Codex-corrected): the fetch + fail-closed FR
    # token resolution + JSON-list build/persist live in McpAddBusinessRuleUseCase
    # (domain). The adapter keeps ONLY parse/coercion, the unresolved-token message
    # (via the api-layer _structured_ref_id) and the _saturation_or_coverage envelope.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpAddBusinessRuleUseCase().execute(
                McpAddBusinessRuleCommand(
                    spec_id,
                    f"br_{_uuid.uuid4().hex[:8]}",
                    title,
                    rule.replace("\\n", "\n"),
                    when.replace("\\n", "\n"),
                    then.replace("\\n", "\n"),
                    notes.replace("\\n", "\n") if notes else None,
                    parse_multi_value(linked_requirements) if linked_requirements else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    if _r.unresolved_tokens is not None:
        frs = _r.frs
        _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                f"Available fr_ids: {_available_fr_ids}. "
                f"No business rule was appended."
            )
        })

    return json.dumps(
        {
            "success": True,
            "business_rule": _r.business_rule,
            **_saturation_or_coverage(_r.coverage),
        },
        default=str,
    )


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
    Update an existing business rule on a spec. linked_requirements accepts
    0-based FR indices, fr_ ids, or exact FR text; display labels like FR-1
    are rejected."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        McpUpdateBusinessRuleCommand,
        McpUpdateBusinessRuleUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity, Codex-corrected): locate-by-id + apply edits/
    # CLEAR + fail-closed token resolution + persist live in McpUpdateBusinessRuleUseCase.
    # The adapter pre-coerces (\n, parse_multi_value), detects the CLEAR sentinels, and
    # renders the envelopes (not-found / unresolved via _structured_ref_id / saturation).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpUpdateBusinessRuleUseCase().execute(
                McpUpdateBusinessRuleCommand(
                    spec_id,
                    rule_id,
                    title=title,
                    rule=rule.replace("\\n", "\n") if rule else "",
                    when=when.replace("\\n", "\n") if when else "",
                    then=then.replace("\\n", "\n") if then else "",
                    notes=(notes.replace("\\n", "\n") if (notes and notes != "CLEAR") else ""),
                    notes_clear=(notes == "CLEAR"),
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements)
                        if (linked_requirements and linked_requirements != "CLEAR")
                        else None
                    ),
                    linked_clear=(linked_requirements == "CLEAR"),
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})

    if _r.not_found:
        return json.dumps({"error": f"Business rule '{rule_id}' not found"})
    if _r.unresolved_tokens is not None:
        frs = _r.frs
        _available_fr_ids = [fid for fid in (_structured_ref_id(f) for f in frs) if fid]
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                f"Available fr_ids: {_available_fr_ids}. "
                f"No business rule was updated."
            )
        })

    return json.dumps({
        "success": True,
        "business_rule": _r.business_rule,
        "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
        **_saturation_or_coverage(_r.coverage),
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
# ("We chose graph store A over graph store B because..."); a BusinessRule is a prescriptive
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

    from okto_pulse.core.application.use_cases import (
        McpListIntegrationRequirementsCommand,
        McpListIntegrationRequirementsUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity IR list, board-scoped): fetch + board-scope +
    # active filter are the domain (use case); the adapter just wraps the envelope.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpListIntegrationRequirementsUseCase().execute(
                McpListIntegrationRequirementsCommand(
                    spec_id, board_id, _flag_enabled(include_inactive)
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    return json.dumps(
        {"spec_id": spec_id, "integration_requirements": _r.requirements},
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

    Use IR for APIs, queues, stored procedures, MCP tools, events, files,
    external services, and data contracts that need traceability beyond a
    single endpoint.
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

    allowed_types = {
        "api",
        "queue",
        "stored_procedure",
        "data_contract",
        "event",
        "file",
        "external_service",
        "mcp_tool",
        "other",
    }
    if integration_type not in allowed_types:
        return json.dumps({"error": f"Invalid integration_type. Use one of: {sorted(allowed_types)}"})

    import uuid as _uuid

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

    from okto_pulse.core.application.use_cases import (
        McpAddIntegrationRequirementCommand,
        McpAddIntegrationRequirementUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity IR add, board-scoped): board-scope, FR/TR
    # resolution (core), build and persist live in McpAddIntegrationRequirementUseCase.
    # The adapter validates integration_type (above), parses data_contract / coerces
    # linked_api_contracts and renders the envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpAddIntegrationRequirementUseCase().execute(
                McpAddIntegrationRequirementCommand(
                    spec_id,
                    board_id,
                    f"ir_{_uuid.uuid4().hex[:8]}",
                    title,
                    integration_type,
                    description=description.replace("\\n", "\n") if description else "",
                    provider=provider,
                    consumer=consumer,
                    contract_ref=contract_ref,
                    endpoint=endpoint,
                    method=method,
                    data_contract=data_contract,
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements) if linked_requirements else None
                    ),
                    linked_api_contracts=linked_api_contracts_list,
                    notes=notes.replace("\\n", "\n") if notes else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.unresolved_tokens is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                f"Available fr_ids: {_r.available_fr_ids}. "
                f"Available tr_ids: {_r.available_tr_ids}. "
                f"No integration requirement was appended."
            )
        })

    return json.dumps(
        {"success": True, "integration_requirement": _r.requirement, **_saturation_or_coverage(_r.coverage)},
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

    from okto_pulse.core.application.use_cases import (
        McpListObservabilityRequirementsCommand,
        McpListObservabilityRequirementsUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity OR list, board-scoped): fetch + board-scope +
    # active filter are the domain (use case); the adapter just wraps the envelope.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpListObservabilityRequirementsUseCase().execute(
                McpListObservabilityRequirementsCommand(
                    spec_id, board_id, _flag_enabled(include_inactive)
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    return json.dumps(
        {"spec_id": spec_id, "observability_requirements": _r.requirements},
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
    """
    Add an Observability Requirement (OR) to a spec. signal_type accepts
    metric, log, trace, dashboard, alert, slo, or other; use log for audit
    logs instead of audit_log."""
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

    linked_irs_list = None
    if linked_integration_requirements:
        try:
            linked_irs_list = coerce_to_list_str(linked_integration_requirements) or None
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    from okto_pulse.core.application.use_cases import (
        McpAddObservabilityRequirementCommand,
        McpAddObservabilityRequirementUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity OR add, board-scoped): board-scope, FR/TR
    # resolution (core), build and persist live in McpAddObservabilityRequirementUseCase.
    # The adapter validates signal_type (above), coerces linked_integration_requirements
    # and renders the envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpAddObservabilityRequirementUseCase().execute(
                McpAddObservabilityRequirementCommand(
                    spec_id,
                    board_id,
                    f"or_{_uuid.uuid4().hex[:8]}",
                    title,
                    signal_type,
                    description=description.replace("\\n", "\n") if description else "",
                    target=target,
                    metric_name=metric_name,
                    threshold=threshold,
                    severity=severity,
                    owner=owner,
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements) if linked_requirements else None
                    ),
                    linked_integration_requirements=linked_irs_list,
                    notes=notes.replace("\\n", "\n") if notes else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.unresolved_tokens is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                f"Available fr_ids: {_r.available_fr_ids}. "
                f"Available tr_ids: {_r.available_tr_ids}. "
                f"No observability requirement was appended."
            )
        })

    return json.dumps(
        {"success": True, "observability_requirement": _r.requirement, **_saturation_or_coverage(_r.coverage)},
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
    Decision to have ≥1 linked task. linked_requirements accepts FR refs and
    structured TR refs."""
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

    from okto_pulse.core.application.use_cases import (
        McpAddDecisionCommand,
        McpAddDecisionUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity decision, Codex opt-C): FR/TR resolution
    # (core), auto-supersede flip, build and persist live in McpAddDecisionUseCase.
    # The adapter coerces alternatives / tokens and renders the envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpAddDecisionUseCase().execute(
                McpAddDecisionCommand(
                    spec_id,
                    f"dec_{_uuid.uuid4().hex[:8]}",
                    title,
                    rationale.replace("\\n", "\n"),
                    context=context.replace("\\n", "\n") if context else None,
                    alternatives=alts,
                    supersedes_decision_id=supersedes_decision_id,
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements) if linked_requirements else None
                    ),
                    notes=notes.replace("\\n", "\n") if notes else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.unresolved_tokens is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                f"Available fr_ids: {_r.available_fr_ids}. "
                f"Available tr_ids: {_r.available_tr_ids}. "
                f"No decision was appended."
            )
        })
    if _r.supersede_not_found is not None:
        return json.dumps({
            "error": f"supersedes_decision_id '{_r.supersede_not_found}' "
                     f"not found in spec.decisions"
        })

    return json.dumps(
        {"success": True, "decision": _r.decision, "decisions_total": _r.decisions_total},
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
    to wipe optional string/list fields. linked_requirements accepts FR refs
    and structured TR refs."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        McpUpdateDecisionCommand,
        McpUpdateDecisionUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity decision update, Codex opt-C): locate-by-id,
    # in-place apply, supersede flip, FR/TR resolution, status validation and persist
    # live in McpUpdateDecisionUseCase. The adapter pre-encodes CLEAR sentinels (via
    # .strip().upper()) and coerces alternatives into field_updates.
    field_updates: dict = {}
    if title:
        field_updates["title"] = title
    if rationale:
        field_updates["rationale"] = rationale.replace("\\n", "\n")
    if context:
        field_updates["context"] = (
            None if context.strip().upper() == "CLEAR" else context.replace("\\n", "\n")
        )
    if alternatives_considered:
        if (
            isinstance(alternatives_considered, str)
            and alternatives_considered.strip().upper() == "CLEAR"
        ):
            field_updates["alternatives_considered"] = None
        else:
            try:
                field_updates["alternatives_considered"] = (
                    coerce_to_list_str(alternatives_considered) or None
                )
            except ValueError as e:
                return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if notes:
        field_updates["notes"] = (
            None if notes.strip().upper() == "CLEAR" else notes.replace("\\n", "\n")
        )

    supersedes_clear = (
        bool(supersedes_decision_id) and supersedes_decision_id.strip().upper() == "CLEAR"
    )
    supersedes_value = (
        supersedes_decision_id if (supersedes_decision_id and not supersedes_clear) else ""
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpUpdateDecisionUseCase().execute(
                McpUpdateDecisionCommand(
                    spec_id,
                    decision_id,
                    field_updates=field_updates,
                    supersedes_clear=supersedes_clear,
                    supersedes_value=supersedes_value,
                    status=status,
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements) if linked_requirements else None
                    ),
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.not_found:
        return json.dumps({"error": f"Decision '{decision_id}' not found"})
    if _r.unresolved_tokens is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                f"Available fr_ids: {_r.available_fr_ids}. "
                f"Available tr_ids: {_r.available_tr_ids}. "
                f"No decision was updated."
            )
        })
    if _r.invalid_status is not None:
        return json.dumps({
            "error": f"Invalid status '{_r.invalid_status}'. Use active/superseded/revoked."
        })

    return json.dumps({
        "success": True,
        "decision": _r.decision,
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

    from okto_pulse.core.application.use_cases import (
        McpMigrateSpecDecisionsCommand,
        McpMigrateSpecDecisionsUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec migrate_spec_decisions): the extract / build / persist
    # migration logic is domain, so it lives in McpMigrateSpecDecisionsUseCase; this
    # adapter is a thin auth + render shell. ``no_block`` -> "nothing to migrate".
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpMigrateSpecDecisionsUseCase().execute(
                McpMigrateSpecDecisionsCommand(spec_id), actor=actor, uow=uow
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.no_block:
        return json.dumps({
            "success": True,
            "decisions_added": 0,
            "context_modified": False,
            "note": "No '## Decisions' block found — nothing to migrate.",
        })

    return json.dumps({
        "success": True,
        "decisions_added": _r.decisions_added,
        "context_modified": _r.context_modified,
        "added": _r.added,
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

    from okto_pulse.core.application.use_cases import (
        McpListBusinessRulesCommand,
        McpListBusinessRulesUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity list, Codex-corrected): the fetch + active
    # filter are the domain (use case); the FR-resolution projection + the
    # emit_compaction_metric below stay in the adapter (transport).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpListBusinessRulesUseCase().execute(
                McpListBusinessRulesCommand(spec_id, _flag_enabled(include_inactive)),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Spec not found"})
        rules = _r.rules
        frs = _r.frs

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
    shapes, and link to FR/TR requirements and business rules."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    from okto_pulse.core.application.use_cases import (
        McpAddApiContractCommand,
        McpAddApiContractUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity api_contract, Codex opt-C): the FR/TR
    # resolution (now a core helper), linked_rules existence, build, F9 on-write
    # validation and persist live in McpAddApiContractUseCase. The adapter keeps
    # the JSON parse, the multi-value coercion, _canonical_api_contract_error (F10)
    # and the envelopes (it renders unresolved with the use case's available ids,
    # never re-reading the spec).
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

    linked_rule_tokens = None
    if linked_rules:
        try:
            linked_rule_tokens = coerce_to_list_str(linked_rules)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpAddApiContractUseCase().execute(
                McpAddApiContractCommand(
                    spec_id,
                    f"api_{_uuid.uuid4().hex[:8]}",
                    method,
                    path,
                    description.replace("\\n", "\n") if description else "",
                    request_body=request_body,
                    response_success=response_success,
                    response_errors=response_errors,
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements) if linked_requirements else None
                    ),
                    linked_rule_tokens=linked_rule_tokens,
                    notes=notes.replace("\\n", "\n") if notes else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.unresolved_tokens is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                f"Available fr_ids: {_r.available_fr_ids}. "
                f"Available tr_ids: {_r.available_tr_ids}. "
                f"No API contract was appended."
            )
        })
    if _r.bad_rule_token is not None:
        return json.dumps({"error": f"Business rule '{_r.bad_rule_token}' not found in spec"})
    if _r.invalid_contract_exc is not None:
        return _canonical_api_contract_error(_r.invalid_contract_exc)

    return json.dumps(
        {"success": True, "api_contract": _r.contract, **_saturation_or_coverage(_r.coverage)},
        default=str,
    )


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
    Update an existing API contract on a spec. linked_requirements accepts
    FR index/fr_id/text and structured TR id/text."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        McpUpdateApiContractCommand,
        McpUpdateApiContractUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity api_contract update, Codex opt-C): the
    # locate-by-id, in-place apply, FR/TR resolution (core), linked_rules existence,
    # F9 on-write validation and persist live in McpUpdateApiContractUseCase. The
    # adapter pre-parses JSON / CLEAR sentinels into field_updates and canonicalizes.
    field_updates: dict = {}
    if description == "CLEAR":
        field_updates["description"] = ""
    elif description:
        field_updates["description"] = description.replace("\\n", "\n")

    if isinstance(request_body_json, dict):
        field_updates["request_body"] = request_body_json
    elif request_body_json == "CLEAR":
        field_updates["request_body"] = None
    elif request_body_json:
        try:
            field_updates["request_body"] = json.loads(request_body_json)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid request_body_json: {e}"})

    if isinstance(response_success_json, dict):
        field_updates["response_success"] = response_success_json
    elif response_success_json == "CLEAR":
        field_updates["response_success"] = None
    elif response_success_json:
        try:
            field_updates["response_success"] = json.loads(response_success_json)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid response_success_json: {e}"})

    if isinstance(response_errors_json, list):
        field_updates["response_errors"] = response_errors_json
    elif response_errors_json == "CLEAR":
        field_updates["response_errors"] = None
    elif response_errors_json:
        try:
            field_updates["response_errors"] = json.loads(response_errors_json)
        except json.JSONDecodeError as e:
            return json.dumps({"error": f"Invalid response_errors_json: {e}"})

    if notes == "CLEAR":
        field_updates["notes"] = None
    elif notes:
        field_updates["notes"] = notes.replace("\\n", "\n")

    linked_rule_clear = isinstance(linked_rules, str) and linked_rules == "CLEAR"
    linked_rule_tokens = None
    if not linked_rule_clear and linked_rules:
        try:
            linked_rule_tokens = coerce_to_list_str(linked_rules)
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpUpdateApiContractUseCase().execute(
                McpUpdateApiContractCommand(
                    spec_id,
                    contract_id,
                    method=method,
                    path=path,
                    field_updates=field_updates,
                    linked_req_clear=(linked_requirements == "CLEAR"),
                    linked_requirement_tokens=(
                        parse_multi_value(linked_requirements)
                        if (linked_requirements and linked_requirements != "CLEAR")
                        else None
                    ),
                    linked_rule_clear=linked_rule_clear,
                    linked_rule_tokens=linked_rule_tokens,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.not_found:
        return json.dumps({"error": f"API contract '{contract_id}' not found"})
    if _r.unresolved_tokens is not None:
        return json.dumps({
            "error": (
                f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                f"Available fr_ids: {_r.available_fr_ids}. "
                f"Available tr_ids: {_r.available_tr_ids}. "
                f"No API contract was updated."
            )
        })
    if _r.bad_rule_token is not None:
        return json.dumps({"error": f"Business rule '{_r.bad_rule_token}' not found in spec"})
    if _r.invalid_contract_exc is not None:
        return _canonical_api_contract_error(_r.invalid_contract_exc)

    return json.dumps({
        "success": True,
        "api_contract": _r.contract,
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

    from okto_pulse.core.application.use_cases import (
        McpRemoveSpecEntityCommand,
        McpRemoveSpecEntityUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity, Codex-corrected): the per-type hard/soft
    # remove + persist live in McpRemoveSpecEntityUseCase; this adapter keeps the
    # REGISTRY validation (above), the _telemetry emit (by outcome) and the envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpRemoveSpecEntityUseCase().execute(
                McpRemoveSpecEntityCommand(spec_id, target_type, entity_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        _telemetry("error")
        return json.dumps({"error": "Spec not found"})
    except ValueError as exc:
        _telemetry("error")
        return json.dumps({"error": str(exc)})

    if _r.not_found:
        _telemetry("error")
        _not_found_msg = {
            "business_rule": f"Business rule '{entity_id}' not found",
            "api_contract": f"API contract '{entity_id}' not found",
            "decision": f"Decision '{entity_id}' not found",
        }[target_type]
        return json.dumps({"error": _not_found_msg})

    _telemetry("ok")
    if target_type == "decision":
        return json.dumps(
            {"success": True, "revoked": entity_id, "decision": _r.revoked_decision}
        )
    return json.dumps({
        "success": True, "removed": entity_id, "remaining": _r.remaining,
        **_saturation_or_coverage(_r.coverage),
    })


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

    from okto_pulse.core.application.use_cases import (
        McpListApiContractsCommand,
        McpListApiContractsUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec sub-entity list, Codex opt-C): fetch + active filter are
    # the domain (use case); the FR/TR-resolution projection + emit_compaction_metric
    # below stay in the adapter (transport).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            _r = await McpListApiContractsUseCase().execute(
                McpListApiContractsCommand(spec_id, _flag_enabled(include_inactive)),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Spec not found"})
        contracts = _r.contracts
        existing_rules = _r.existing_rules
        frs = _r.frs
        trs = _r.trs

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

            # FR7 dedup: same as list_business_rules — emit canonical ids
            # under ``linked_requirements`` (IMPL-2: projection now emits fr_id,
            # not re-derived index; legacy FRs without id fall back to str(idx))
            # and carry the human ``[FR-n] <text>`` / ``[TR-id] <text>`` only
            # under ``resolved_requirements`` so full requirement text is not
            # serialized twice.
            linked_reqs = c.get("linked_requirements") or []
            idxs = sorted(resolve_linked_fr_indices(linked_reqs, frs))
            tr_ids: list[str] = []
            unresolved = []
            for ref in linked_reqs:
                if resolve_linked_fr_indices([ref], frs):
                    continue
                resolved_tr_ids, unresolved_trs = resolve_linked_requirements_to_ids([ref], trs)
                if resolved_tr_ids:
                    for tr_id in resolved_tr_ids:
                        if tr_id not in tr_ids:
                            tr_ids.append(tr_id)
                else:
                    unresolved.extend(unresolved_trs)
            tr_text_by_id = {
                _structured_ref_id(tr) or _structured_ref_text(tr): _structured_ref_text(tr)
                for tr in trs
            }
            entry["linked_requirements"] = [
                _structured_ref_id(frs[i]) or str(i)
                for i in idxs
                if 0 <= i < len(frs)
            ] + tr_ids
            entry["resolved_requirements"] = [
                f"[FR-{i}] {_structured_ref_text(frs[i])}"
                for i in idxs
                if 0 <= i < len(frs)
            ] + [
                f"[TR-{tr_id}] {tr_text_by_id.get(tr_id, '')}"
                for tr_id in tr_ids
            ]
            # Robustness: preserve legacy refs that don't resolve to any FR/TR.
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


def _mockup_gate_imports():
    from okto_pulse.core.services.design_system import (
        DesignSystemError,
        MockupDesignSystemGate,
        normalize_design_system_ref,
    )
    return MockupDesignSystemGate, DesignSystemError, normalize_design_system_ref


@mcp.tool()
async def okto_pulse_add_screen_mockup(
    board_id: str,
    entity_id: str,
    title: str,
    entity_type: str = "spec",
    description: str = "",
    screen_type: str = "page",
    html_content: str = "",
    design_system_ref: str = "",
    design_system_version: int | None = None,
    design_system_evidence=None,
) -> str:
    """
    Add a screen mockup to a source entity (spec, ideation, refinement, or story).
    Card mockups are read-only governed snapshots; use okto_pulse_copy_mockups_to_card
    to refresh card context.
    Screens contain HTML+Tailwind content that renders as visual mockups in the dashboard.

    design_system_ref (Design System id), design_system_version and design_system_evidence
    feed the MockupDesignSystemGate (spec 3a006f65): when the board has an effective Design
    System and design_system_gate_mode=blocking, an invalid/missing ref is rejected BEFORE
    persistence (design_system_required / design_system_not_found /
    design_system_version_mismatch / design_system_evidence_missing); advisory persists +
    returns a design_system_gate warning; off / no Design System does not block."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})
    if entity_type == "card":
        return _card_resource_read_only_error()

    import hashlib
    import time

    Gate, GateErr, normalize_ref = _mockup_gate_imports()
    screen_id = "sm_" + hashlib.md5(f"{entity_id}{title}{time.time()}".encode()).hexdigest()[:8]

    screen = {
        "id": screen_id,
        "title": title,
        "description": description or None,
        "screen_type": screen_type,
        "html_content": _sanitize_html(html_content),
        "annotations": [],
        "order": 0,
        "design_system_ref": normalize_ref(design_system_ref, design_system_version),
        "design_system_evidence": design_system_evidence,
    }

    async with get_db_for_mcp() as db:
        entity, service, UpdateClass = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        # MockupDesignSystemGate runs BEFORE persistence (blocking aborts the txn).
        try:
            gate_outcome = await Gate(db).evaluate_screen(
                board_id, screen, entity_type=entity_type, entity_id=entity_id
            )
        except GateErr as e:
            return json.dumps(e.to_dict())

        screens = list(entity.screen_mockups or [])
        screen["order"] = len(screens)
        screens.append(screen)

        await _save_entity_mockups(service, entity_type, entity_id, ctx.agent_id, screens, UpdateClass)
        await db.commit()

    return json.dumps(
        {"success": True, "entity_type": entity_type, "screen": screen, "design_system_gate": gate_outcome},
        default=str,
    )


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
    design_system_ref: str = "",
    design_system_version: int | None = None,
    design_system_evidence=None,
) -> str:
    """
    Update an existing screen mockup's fields on a source entity. Card mockups are
    read-only governed snapshots. When a gate-relevant field changes
    (html_content / design_system_ref / design_system_evidence) the
    MockupDesignSystemGate re-evaluates this mockup BEFORE persistence (delta-only):
    blocking rejects an invalid Design System ref/version/evidence with an actionable
    error; advisory persists + returns a design_system_gate warning; off / no Design
    System does not block."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})
    if entity_type == "card":
        return _card_resource_read_only_error()

    Gate, GateErr, normalize_ref = _mockup_gate_imports()

    async with get_db_for_mcp() as db:
        entity, service, UpdateClass = await _load_entity_mockups(db, entity_type, entity_id)
        if not entity:
            return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})

        screens = list(entity.screen_mockups or [])
        screen = next((s for s in screens if s.get("id") == screen_id), None)
        if not screen:
            return json.dumps({"error": f"Screen '{screen_id}' not found"})

        original = dict(screen)  # pre-edit snapshot for the delta comparison
        if title:
            screen["title"] = title
        if description:
            screen["description"] = description
        if screen_type:
            screen["screen_type"] = screen_type
        if html_content:
            screen["html_content"] = _sanitize_html(html_content)
        if design_system_ref:
            screen["design_system_ref"] = normalize_ref(design_system_ref, design_system_version)
        if design_system_evidence is not None:
            screen["design_system_evidence"] = design_system_evidence

        # delta-only: re-gate this mockup only if a gate-relevant field changed.
        try:
            gate_outcomes = await Gate(db).gate_delta(
                board_id, [original], [screen], entity_type=entity_type, entity_id=entity_id
            )
        except GateErr as e:
            return json.dumps(e.to_dict())
        gate_outcome = gate_outcomes[0] if gate_outcomes else {"outcome": "not_applicable"}

        await _save_entity_mockups(service, entity_type, entity_id, ctx.agent_id, screens, UpdateClass)
        await db.commit()

    return json.dumps(
        {"success": True, "screen": screen, "design_system_gate": gate_outcome}, default=str
    )


@mcp.tool()
async def okto_pulse_annotate_mockup(
    board_id: str,
    entity_id: str,
    screen_id: str,
    text: str,
    entity_type: str = "spec",
) -> str:
    """
    Add a design annotation/note to a screen mockup on a source entity. Card
    mockups are read-only governed snapshots."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})
    if entity_type == "card":
        return _card_resource_read_only_error()

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
    Delete a screen mockup from a source entity. Card mockups are read-only governed snapshots."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    entity_type_error = _validate_screen_mockup_entity_type(entity_type)
    if entity_type_error:
        return json.dumps({"error": entity_type_error})
    if entity_type == "card":
        return _card_resource_read_only_error()

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

    from okto_pulse.core.application.use_cases import (
        McpGetBoardGuidelinesCommand,
        McpGetBoardGuidelinesUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpGetBoardGuidelinesUseCase().execute(
            McpGetBoardGuidelinesCommand(board_id), actor=actor, uow=uow
        )
        items = result.data
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

    from okto_pulse.core.application.use_cases import (
        McpLinkGuidelineToBoardCommand,
        McpLinkGuidelineToBoardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpLinkGuidelineToBoardUseCase().execute(
                McpLinkGuidelineToBoardCommand(
                    board_id, guideline_id, int(priority)
                ),
                actor=actor,
                uow=uow,
            )
            link = result.data
            return json.dumps(
                {
                    "success": True,
                    "board_id": board_id,
                    "guideline_id": guideline_id,
                    "priority": link.priority,
                },
                default=str,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Guideline not found"})


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

    from okto_pulse.core.application.use_cases import (
        McpUnlinkGuidelineFromBoardCommand,
        McpUnlinkGuidelineFromBoardUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            await McpUnlinkGuidelineFromBoardUseCase().execute(
                McpUnlinkGuidelineFromBoardCommand(board_id, guideline_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Link not found"})
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

    from okto_pulse.core.application.use_cases import (
        DeleteSpecCommand,
        DeleteSpecUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (spec REUSE): DeleteSpecUseCase raises EntityNotFoundError
    # when nothing was deleted — adapter maps it to the legacy "Spec not found".
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            await DeleteSpecUseCase().execute(
                DeleteSpecCommand(spec_id), actor=actor, uow=uow
            )
    except EntityNotFoundError:
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
        # R4-IMP1: capture status BEFORE the append-only submit so a future
        # auto-transition regression surfaces as state_changed in the envelope.
        spec_before = await service.get_spec(spec_id)
        status_before = spec_before.status.value if spec_before else "validated"
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
        # R4-IMP1: evaluation is append-only — capture status AFTER independently;
        # the envelope reports both (state_changed=false in the happy path) and the
        # operator's next step (move_spec to in_progress), never an auto-transition.
        spec_after = await service.get_spec(spec_id)
        status_after = spec_after.status.value if spec_after else status_before
        await db.commit()

    return json.dumps(
        spec_evaluation_success_envelope(
            spec_id=spec_id, status_before=status_before, status_after=status_after,
            evaluation=evaluation,
        ),
        default=str,
    )


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

    from okto_pulse.core.application.use_cases import (
        ListSpecHistoryCommand,
        ListSpecHistoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        entries = (
            await ListSpecHistoryUseCase().execute(
                ListSpecHistoryCommand(spec_id, int(limit)), actor=actor, uow=uow
            )
        ).history

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
    options: list[str] | str = "",
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
    content_reference: str | None = None,
) -> str:
    """
    Add a knowledge base item to a spec. Use this to attach reference documents,
    design docs, API specs, or any context that helps agents understand the spec.

    Provide exactly ONE of: content or content_reference."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    resolved_content, err = await _resolve_text_content(
        content=content, content_reference=content_reference
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

    from okto_pulse.core.application.use_cases import (
        McpGetRefinementSnapshotCommand,
        McpGetRefinementSnapshotUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        snapshot = (
            await McpGetRefinementSnapshotUseCase().execute(
                McpGetRefinementSnapshotCommand(refinement_id, int(version)),
                actor=actor,
                uow=uow,
            )
        ).snapshot
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

    from okto_pulse.core.application.use_cases import (
        McpGetRefinementKnowledgeCommand,
        McpGetRefinementKnowledgeUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpGetRefinementKnowledgeUseCase().execute(
            McpGetRefinementKnowledgeCommand(refinement_id, knowledge_id),
            actor=actor,
            uow=uow,
        )
        if _r.kb_not_found:
            return json.dumps({"error": "Knowledge base item not found"})
        kb = _r.kb
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
    content_reference: str | None = None,
) -> str:
    """
    Add a knowledge base item to a refinement. Use this to attach reference documents,
    design docs, analysis notes, or any context that helps agents understand the refinement.

    Provide exactly ONE of: content or content_reference."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    resolved_content, err = await _resolve_text_content(
        content=content, content_reference=content_reference
    )
    if err:
        return json.dumps({"error": err})

    from okto_pulse.core.models.schemas import RefinementKnowledgeCreate

    kb_data = RefinementKnowledgeCreate(
        title=title,
        description=description or None,
        content=resolved_content,
        mime_type=mime_type,
    )

    from okto_pulse.core.application.use_cases import (
        McpAddRefinementKnowledgeCommand,
        McpAddRefinementKnowledgeUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpAddRefinementKnowledgeUseCase().execute(
            McpAddRefinementKnowledgeCommand(refinement_id, kb_data),
            actor=actor,
            uow=uow,
        )
        if not _r.kb:
            return json.dumps({"error": "Failed to create knowledge base item — refinement not found"})
        kb = _r.kb
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteRefinementKnowledgeCommand,
        McpDeleteRefinementKnowledgeUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteRefinementKnowledgeUseCase().execute(
            McpDeleteRefinementKnowledgeCommand(refinement_id, knowledge_id),
            actor=actor,
            uow=uow,
        )

    if _r.kb_not_found:
        return json.dumps({"error": "Knowledge base item not found"})
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
    Create a new sprint for a spec. Sprints break specs into incremental
    deliverables. lane_type accepts normal or hotfix; release_validation is an
    objective/label on a normal sprint, not a lane."""
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

    from pydantic import ValidationError

    from okto_pulse.core.models.schemas import SprintCreate
    from okto_pulse.core.services.main import SprintOperationError

    # S-LANE-01: build the DTO under its own guard so an invalid lane_type surfaces the
    # canonical envelope (fail-closed: no service call, nothing persists) instead of
    # leaking raw Pydantic text. A non-validation ValueError (coerce) keeps its shape.
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
    except ValidationError as exc:
        return _canonical_sprint_validation_error(exc)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    from okto_pulse.core.application.use_cases import (
        McpCreateSprintCommand,
        McpCreateSprintUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint create, VARIANT): McpCreateSprintUseCase runs the
    # skip_ownership create + commit (the service only flushes + emits its own log/
    # history/event). The adapter keeps the permission gate, the S-LANE-01 DTO build
    # and the SprintOperationError-before-ValueError envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpCreateSprintUseCase().execute(
                McpCreateSprintCommand(board_id, data), actor=actor, uow=uow
            )
            if not _r.sprint:
                return json.dumps({"error": "Failed to create sprint (spec not found or wrong board)"})
            sprint = _r.sprint
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
    Update sprint fields. lane_type accepts normal or hotfix; release_validation
    is an objective/label on a normal sprint, not a lane."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.models.schemas import SprintUpdate

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

    # S-LANE-01: fail-closed DTO build (an invalid lane_type surfaces the canonical
    # envelope, nothing persists) BEFORE the use case.
    from pydantic import ValidationError
    try:
        data = SprintUpdate(**kwargs)
    except ValidationError as exc:
        return _canonical_sprint_validation_error(exc)
    except ValueError as e:
        return json.dumps({"error": str(e)})

    from okto_pulse.core.application.use_cases import (
        UpdateSprintCommand,
        UpdateSprintUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint update, REUSE:UpdateSprintUseCase — the service self-
    # commits; a None result is EntityNotFoundError -> "Sprint not found"; ValueError
    # (archived / lane-in-draft / invalid scoped ids) -> {"error": str}).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            sprint = (
                await UpdateSprintUseCase().execute(
                    UpdateSprintCommand(sprint_id, data), actor=actor, uow=uow
                )
            ).sprint
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
    except EntityNotFoundError:
        return json.dumps({"error": "Sprint not found"})
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
    Gates: draft→active requires cards, active→review requires scoped test
    scenarios in passed status, review→closed requires evaluation. Automated
    test pointers alone do not satisfy sprint review."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.domain.enums import SprintStatus
    from okto_pulse.core.models.schemas import SprintMove

    try:
        sprint_status = SprintStatus(status)
    except ValueError:
        return json.dumps({"error": f"Invalid status. Must be one of: {[s.value for s in SprintStatus]}"})

    from okto_pulse.core.application.use_cases import (
        MoveSprintCommand,
        MoveSprintUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint move, REUSE:MoveSprintUseCase — the service self-commits,
    # runs the BG-01 critical-context guard + the state-machine gates internally; a None
    # result is EntityNotFoundError -> "Sprint not found"; ValueError (incl. the
    # SprintOperationError subclass) -> {"error": str(e)} — the single except matches the
    # legacy, which does NOT surface to_dict() for move).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            sprint = (
                await MoveSprintUseCase().execute(
                    MoveSprintCommand(sprint_id, SprintMove(status=sprint_status)),
                    actor=actor,
                    uow=uow,
                )
            ).sprint
            return json.dumps({
                "success": True,
                "sprint": {"id": sprint.id, "title": sprint.title, "status": sprint.status.value},
            })
    except EntityNotFoundError:
        return json.dumps({"error": "Sprint not found"})
    except ValueError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_get_sprint(board_id: str, sprint_id: str) -> str:
    """
    Get full sprint details including cards, evaluations, and Q&A."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases import (
        McpGetSprintCommand,
        McpGetSprintUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint get, Clean Core): McpGetSprintUseCase builds the full
    # presentation dict (cards/qa_items) in the application layer; the adapter stays
    # THIN — call + render (no composed read query in the wrapper, no direct service).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpGetSprintUseCase().execute(
            McpGetSprintCommand(sprint_id), actor=actor, uow=uow
        )

    if _r.not_found:
        return json.dumps({"error": "Sprint not found"})
    return json.dumps(_r.result, default=str)


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

    from okto_pulse.core.application.use_cases import (
        McpGetSprintContextCommand,
        McpGetSprintContextUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint get_context, Clean Core): McpGetSprintContextUseCase
    # builds the WHOLE aggregation (sprint dict + the CROSS-FAMILY parent-spec read +
    # the scoped-item filtering) in the application layer; the adapter stays THIN —
    # parse include_spec + call + render (no composed read / cross-family leak here).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await McpGetSprintContextUseCase().execute(
                McpGetSprintContextCommand(sprint_id, board_id, _inc_spec),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Sprint not found"})

    return json.dumps(_r.result, default=str)


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

    from okto_pulse.core.application.use_cases import (
        AssignSprintTasksCommand,
        AssignSprintTasksUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.main import SprintOperationError

    # MCP-FU6 strangler (sprint assign, REUSE:AssignSprintTasksUseCase — assign +
    # commit + re-fetch live in the use case; the service logs/records-history
    # atomically inside the txn). The adapter keeps the pre-UoW coerce/empty-guard, the
    # lane SUCCESS envelope shaped from the returned sprint (a scalar, no query), and
    # the SprintOperationError-before-ValueError order (the typed e.code/to_dict must
    # not be shadowed by the bare ValueError).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await AssignSprintTasksUseCase().execute(
                AssignSprintTasksCommand(sprint_id, ids), actor=actor, uow=uow
            )
            count = _r.assigned
            sprint = _r.sprint
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

    from okto_pulse.core.application.use_cases import (
        SubmitSprintEvaluationCommand,
        SubmitSprintEvaluationUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError, commit
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.critical_context_guard import FullContextGuardError

    # MCP-FU6 strangler (sprint submit_evaluation, REUSE:SubmitSprintEvaluationUseCase —
    # the service self-commits + logs/records-history). The recommendation validation +
    # the evaluation dict are built above (input parsing). A None result is
    # EntityNotFoundError -> "Sprint not found".
    #
    # FullContextGuardError IS a ValueError subclass (the critical-context guard persists
    # a denial-decision audit, then raises). It MUST be caught BEFORE the generic
    # ValueError so it is not swallowed into the bare {error: str(e)}. We mirror the MCP
    # sibling okto_pulse_submit_spec_evaluation EXACTLY: commit the persisted decision
    # then surface the guard's reason + decision audit as a STRUCTURED envelope. (move_*
    # tools, by contrast, intentionally convert the guard via ValueError -> {error}.)
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            try:
                _r = await SubmitSprintEvaluationUseCase().execute(
                    SubmitSprintEvaluationCommand(sprint_id, evaluation), actor=actor, uow=uow
                )
            except FullContextGuardError as exc:
                await commit(uow)
                return json.dumps({
                    "error": str(exc),
                    "reason": exc.reason,
                    "decision": exc.decision.audit_details(),
                })
            sprint = _r.sprint
            last_eval = sprint.evaluations[-1] if sprint.evaluations else {}
            return json.dumps({
                "success": True,
                "evaluation_id": last_eval.get("id"),
                "overall_score": overall_score,
                "recommendation": recommendation,
            })
    except EntityNotFoundError:
        return json.dumps({"error": "Sprint not found"})
    except ValueError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_list_sprint_evaluations(board_id: str, sprint_id: str) -> str:
    """
    List all evaluations for a sprint."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases import (
        McpListSprintEvaluationsCommand,
        McpListSprintEvaluationsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint list_evaluations, Clean Core): the aggregation lives in
    # the use case; the adapter stays thin — call + render.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpListSprintEvaluationsUseCase().execute(
            McpListSprintEvaluationsCommand(sprint_id), actor=actor, uow=uow
        )

    if _r.not_found:
        return json.dumps({"error": "Sprint not found"})
    return json.dumps(_r.result)


@mcp.tool()
async def okto_pulse_get_sprint_evaluation(
    board_id: str, sprint_id: str, evaluation_id: str,
) -> str:
    """
    Get full details of a specific sprint evaluation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases import (
        McpGetSprintEvaluationCommand,
        McpGetSprintEvaluationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint get_evaluation, Clean Core): the scan lives in the use
    # case; the adapter stays thin — call + render (the eval dict is returned UNWRAPPED).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpGetSprintEvaluationUseCase().execute(
            McpGetSprintEvaluationCommand(sprint_id, evaluation_id), actor=actor, uow=uow
        )

    if _r.sprint_not_found:
        return json.dumps({"error": "Sprint not found"})
    if _r.eval_not_found:
        return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})
    return json.dumps(_r.evaluation)


@mcp.tool()
async def okto_pulse_delete_sprint_evaluation(
    board_id: str, sprint_id: str, evaluation_id: str,
) -> str:
    """
    Delete your own sprint evaluation."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases import (
        McpDeleteSprintEvaluationCommand,
        McpDeleteSprintEvaluationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint delete_evaluation, Clean Core, option A): the load +
    # ownership gate + the Sprint.evaluations JSON mutation + the dirty-flag live in the
    # new SprintService.delete_evaluation; the use case commits iff "deleted"; the
    # adapter only maps the status to the legacy envelopes.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteSprintEvaluationUseCase().execute(
            McpDeleteSprintEvaluationCommand(sprint_id, evaluation_id), actor=actor, uow=uow
        )

    if _r.status == "sprint_not_found":
        return json.dumps({"error": "Sprint not found"})
    if _r.status == "eval_not_found":
        return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})
    if _r.status == "not_owner":
        return json.dumps({"error": "You can only delete your own evaluations"})
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

    from okto_pulse.core.application.use_cases import (
        McpAnswerSprintQuestionCommand,
        McpAnswerSprintQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint answer Q&A, VARIANT): NO permission gate, NO activity log
    # (unique among the Q&A-answer family), a plain answer string, and an UNCONDITIONAL
    # commit (the use case commits even on a None qa — legacy parity). Self-answer ->
    # {error, detail}; a None result -> "Q&A item not found".
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpAnswerSprintQuestionUseCase().execute(
            McpAnswerSprintQuestionCommand(qa_id, answer), actor=actor, uow=uow
        )
        if _r.self_answer_error is not None:
            return json.dumps(
                {"error": _r.self_answer_error.reason, "detail": str(_r.self_answer_error)}
            )
        if _r.qa_not_found:
            return json.dumps({"error": "Q&A item not found"})
        qa = _r.qa
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteIdeationQuestionCommand,
        McpDeleteIdeationQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (ideation Q&A delete, ATOMIC activity-log): delete + the
    # ideation_question_deleted log + commit run atomically in the use case.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteIdeationQuestionUseCase().execute(
            McpDeleteIdeationQuestionCommand(board_id, ideation_id, qa_id),
            actor=actor,
            uow=uow,
        )

    if _r.qa_not_found:
        return json.dumps({"error": "Q&A item not found"})
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteRefinementQuestionCommand,
        McpDeleteRefinementQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (refinement Q&A delete, ATOMIC activity-log): delete + the
    # refinement_question_deleted log + commit run atomically in the use case.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteRefinementQuestionUseCase().execute(
            McpDeleteRefinementQuestionCommand(board_id, refinement_id, qa_id),
            actor=actor,
            uow=uow,
        )

    if _r.qa_not_found:
        return json.dumps({"error": "Q&A item not found"})
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

    from okto_pulse.core.application.use_cases import (
        McpDeleteSprintQuestionCommand,
        McpDeleteSprintQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint delete Q&A, VARIANT): the sprint_question_deleted log +
    # commit run ATOMICALLY in the use case; a falsy delete short-circuits (no log/
    # commit). The QA_DELETE gate stays in the adapter (above).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _r = await McpDeleteSprintQuestionUseCase().execute(
            McpDeleteSprintQuestionCommand(board_id, sprint_id, qa_id), actor=actor, uow=uow
        )

    if _r.qa_not_found:
        return json.dumps({"error": "Q&A item not found"})
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

    from okto_pulse.core.application.use_cases import (
        SuggestSprintsCommand,
        SuggestSprintsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler (sprint suggest, REUSE:SuggestSprintsUseCase — read; a ValueError
    # (spec not found / not ready) propagates and the adapter maps it to {error: str}).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            _r = await SuggestSprintsUseCase().execute(
                SuggestSprintsCommand(spec_id, threshold), actor=actor, uow=uow
            )
            return json.dumps({"suggestions": _r.suggestions, "count": len(_r.suggestions)})
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
        except GateContractError as e:
            return json.dumps(e.to_dict())
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_confirm_amendment_coverage(
    board_id: str,
    amendment_id: str,
    regression_test_task_id: str,
    regression_scenario_id: str,
) -> str:
    """
    Confirm Path B amendment coverage (validator-only · G2 · card c9cf9781).

    Writes the single non-forgeable attestation letting the bug gate treat a
    cross-spec Path B regression artifact as closure-ready. Fail-closed: the
    test task + scenario MUST be declared by THIS amendment, the test task MUST
    be done with SPEC3 reexecutable evidence, and the caller MUST hold validator
    critical-context authorization — all NECESSARY but NOT sufficient; the gate
    derives coverage from the persisted attestation.

    Preflight (BUG-01): BEFORE persisting, runs the SAME eligibility predicate
    the bug regression gate uses; success implies the attestation is persisted
    AND gate-consumable. An inert tuple fails closed with
    `coverage_not_gate_consumable` (bounded facts, no-bypass remediation) —
    distinct from `coverage_pending` (validator confirmation not yet recorded).
    Full docs: okto-pulse://reference/tool-docs/card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "card.validation.submit")
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        card_service = CardService(db)
        try:
            result = await card_service.confirm_amendment_coverage(
                amendment_id=amendment_id,
                regression_test_task_id=regression_test_task_id,
                regression_scenario_id=regression_scenario_id,
                reviewer_id=ctx.agent_id,
                reviewer_name=ctx.agent_name,
            )
            await db.commit()
            return json.dumps(
                {"success": True, "coverage_confirmation": result}, default=str
            )
        except CardOperationError as e:
            # BUG-03: preserve the STRUCTURED fail-closed error (e.g.
            # coverage_not_gate_consumable with bounded facts: amendment_id,
            # bug_id, original_spec_id, regression_test_task_id,
            # regression_scenario_id, scenario_spec_id, routed_path,
            # resolver_reason, coverage_state, missing_links). Must precede the
            # ValueError arm — CardOperationError subclasses ValueError — so it
            # never degrades to a textual {"error": str(e)}.
            return json.dumps({"error": e.code, **e.to_dict()})
        except GateContractError as e:
            return json.dumps(e.to_dict())
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_create_amendment_revision(
    board_id: str,
    bug_id: str,
    original_spec_id: str = "",
    revision_spec_id: str = "",
    origin_task_ids: list[str] | None = None,
    affected_task_ids: list[str] | None = None,
    regression_scenario_ids: list[str] | None = None,
    regression_test_task_ids: list[str] | None = None,
    automated_regression_refs: list[str] | None = None,
) -> str:
    """
    Create a Path B AmendmentHotfixRevision for a bug (spec be089cd3 · FR1 ·
    ir_54ceb69b). Twin of POST /boards/{board_id}/bugs/{bug_id}/amendment-revisions.

    The amendment binds to the bug's OWN content-locked spec (done/validated, OR
    in_progress still content-locked by an active passed validation -
    current_validation_id -> outcome=success) and always starts as 'draft'. An
    in_progress spec that is still editable (no active success validation, or a
    failed/stale/superseded one) is rejected (original_spec_not_done_or_locked) -
    edit it directly there. This tool ONLY remediates — it NEVER skips/overrides the
    bug regression gate, and it cannot set coverage confirmation (validator-only).
    Returns the structured amendment payload (status, lineage_state, eligibility,
    artifacts) or a structured error (no raw exception text)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
        AmendmentRevisionApiService,
    )

    async with get_db_for_mcp() as db:
        try:
            result = await AmendmentRevisionApiService(db).create(
                board_id=board_id,
                bug_id=bug_id,
                author=ctx.agent_id,
                original_spec_id=original_spec_id or None,
                revision_spec_id=revision_spec_id or None,
                origin_task_ids=origin_task_ids,
                affected_task_ids=affected_task_ids,
                regression_scenario_ids=regression_scenario_ids,
                regression_test_task_ids=regression_test_task_ids,
                automated_regression_refs=automated_regression_refs,
            )
            await db.commit()
            return json.dumps({"success": True, "amendment_revision": result}, default=str)
        except AmendmentRevisionApiError as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_list_amendment_revisions(board_id: str, bug_id: str) -> str:
    """
    List Path B amendment revisions for a bug + the bug-level lineage/coverage
    resolution (twin of GET .../amendment-revisions). Read-only. The payload
    exposes lineage_state, missing_links, safe_next_actions, rejected/eligible
    regression artifacts and coverage_state so an agent knows the next safe
    action without parsing raw exceptions (FR2/AC3)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
        AmendmentRevisionApiService,
    )

    async with get_db_for_mcp() as db:
        try:
            result = await AmendmentRevisionApiService(db).list_for_bug(
                board_id=board_id, bug_id=bug_id
            )
            return json.dumps(result, default=str)
        except AmendmentRevisionApiError as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_get_amendment_revision(
    board_id: str, bug_id: str, amendment_id: str
) -> str:
    """
    Get one Path B amendment revision scoped to a bug (twin of GET
    .../amendment-revisions/{amendment_id}). Read-only. A revision that does not
    belong to this bug/board fails structured (amendment_bug_mismatch), never
    leaks as success."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
        AmendmentRevisionApiService,
    )

    async with get_db_for_mcp() as db:
        try:
            result = await AmendmentRevisionApiService(db).get(
                board_id=board_id, bug_id=bug_id, amendment_id=amendment_id
            )
            return json.dumps({"amendment_revision": result}, default=str)
        except AmendmentRevisionApiError as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_associate_amendment_revision_artifacts(
    board_id: str,
    bug_id: str,
    amendment_id: str,
    regression_scenario_ids: list[str] | None = None,
    regression_test_task_ids: list[str] | None = None,
    automated_regression_refs: list[str] | None = None,
) -> str:
    """
    Additively associate regression artifacts/evidence (scenarios, test tasks,
    automated refs) to an existing amendment (twin of POST
    .../amendment-revisions/{amendment_id}/associate). NEVER reparents the bug/spec
    and NEVER skips the gate. Requires at least one artifact list; audit-backed,
    structured failure on mismatch/empty (no silent no-op)."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
        AmendmentRevisionApiService,
    )

    async with get_db_for_mcp() as db:
        try:
            result = await AmendmentRevisionApiService(db).associate(
                board_id=board_id,
                bug_id=bug_id,
                amendment_id=amendment_id,
                actor=ctx.agent_id,
                regression_scenario_ids=regression_scenario_ids,
                regression_test_task_ids=regression_test_task_ids,
                automated_regression_refs=automated_regression_refs,
            )
            await db.commit()
            return json.dumps({"success": True, "amendment_revision": result}, default=str)
        except AmendmentRevisionApiError as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_transition_amendment_revision(
    board_id: str,
    bug_id: str,
    amendment_id: str,
    status: str = "",
    lineage_state: str = "",
) -> str:
    """
    Transition a Path B amendment status/lineage for a bug. It promotes a created
    or associated amendment toward complete lineage and approved/done status, but
    never confirms coverage.

    Unknown status/lineage fail closed; complete lineage needs declared regression
    artifacts and authoritative origin/affected tasks; approved/done need complete
    lineage; cancelled/superseded are terminal. Validator coverage still uses
    `okto_pulse_confirm_amendment_coverage`.
    Full docs: okto-pulse://reference/tool-docs/card."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.CARDS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.services.amendment_revision_api import (
        AmendmentRevisionApiError,
        AmendmentRevisionApiService,
    )

    async with get_db_for_mcp() as db:
        try:
            result = await AmendmentRevisionApiService(db).transition_lifecycle(
                board_id=board_id,
                bug_id=bug_id,
                amendment_id=amendment_id,
                actor=ctx.agent_id,
                status=status or None,
                lineage_state=lineage_state or None,
            )
            await db.commit()
            return json.dumps({"success": True, "amendment_revision": result}, default=str)
        except AmendmentRevisionApiError as e:
            return json.dumps(e.to_dict())


def _default_board_config_imports():
    from okto_pulse.core.services.default_board_config_api import (
        DefaultBoardConfigApiService,
    )
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )
    return DefaultBoardConfigApiService, DefaultBoardConfigurationError


_TASK_REQUIREMENT_GATE_DEFAULT_SKIP_FIELD = "skip_task_requirement_link_gate_global"


def _template_task_requirement_skip_value(settings_payload: Any) -> bool:
    if isinstance(settings_payload, dict):
        return bool(settings_payload.get(_TASK_REQUIREMENT_GATE_DEFAULT_SKIP_FIELD, False))
    return False


async def _refuse_mcp_default_config_activation_if_human_skip_changes(
    *,
    board_id: str,
    template_id: str,
    blocked_tool: str,
    blocked_action: str,
) -> str | None:
    from sqlalchemy import select

    from okto_pulse.core.models.db import DefaultBoardConfiguration

    async with get_db_for_mcp() as db:
        target = await db.get(DefaultBoardConfiguration, template_id)
        if target is None:
            return None
        active = (
            await db.execute(
                select(DefaultBoardConfiguration).where(
                    DefaultBoardConfiguration.scope == target.scope,
                    DefaultBoardConfiguration.is_active.is_(True),
                )
            )
        ).scalars().first()
        current_value = (
            _template_task_requirement_skip_value(active.settings_payload)
            if active is not None
            else False
        )
        next_value = _template_task_requirement_skip_value(target.settings_payload)
        if current_value != next_value:
            return _refuse_human_control(
                board_id=board_id,
                blocked_tool=blocked_tool,
                blocked_action=blocked_action,
                target_ref=f"default_board_config:{template_id}",
            )
    return None


async def _refuse_mcp_default_config_deactivation_if_human_skip_changes(
    *,
    board_id: str,
    template_id: str,
    blocked_tool: str,
    blocked_action: str,
) -> str | None:
    from okto_pulse.core.models.db import DefaultBoardConfiguration

    async with get_db_for_mcp() as db:
        target = await db.get(DefaultBoardConfiguration, template_id)
        if target is None or not target.is_active:
            return None
        current_value = _template_task_requirement_skip_value(target.settings_payload)
        if current_value:
            return _refuse_human_control(
                board_id=board_id,
                blocked_tool=blocked_tool,
                blocked_action=blocked_action,
                target_ref=f"default_board_config:{template_id}",
            )
    return None


@mcp.tool()
async def okto_pulse_get_active_default_board_config(board_id: str, scope: str = "global") -> str:
    """Get the active default board-configuration template for a scope (admin read,
    spec 9df814bc / FR7). REST twin: GET /default-board-config/active. Returns
    {scope, active|null}. board_id anchors the agent permission context."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    from okto_pulse.core.application.use_cases import (
        McpGetActiveDefaultBoardConfigCommand,
        McpGetActiveDefaultBoardConfigUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpGetActiveDefaultBoardConfigUseCase().execute(
                McpGetActiveDefaultBoardConfigCommand(scope), actor=actor, uow=uow
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_list_default_board_config_versions(board_id: str, scope: str = "global") -> str:
    """List default board-configuration template versions for a scope + the active id
    (admin read). REST twin: GET /default-board-config/versions."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    from okto_pulse.core.application.use_cases import (
        McpListDefaultBoardConfigVersionsCommand,
        McpListDefaultBoardConfigVersionsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpListDefaultBoardConfigVersionsUseCase().execute(
                McpListDefaultBoardConfigVersionsCommand(scope), actor=actor, uow=uow
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_get_board_default_config_diff(board_id: str) -> str:
    """Field-level diff between the template snapshot applied to a board and its
    current settings (admin read, FR7). REST twin: GET
    /boards/{board_id}/default-config-diff. board_not_found if the board is missing
    or inaccessible."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    from okto_pulse.core.application.use_cases import (
        McpGetBoardDefaultConfigDiffCommand,
        McpGetBoardDefaultConfigDiffUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpGetBoardDefaultConfigDiffUseCase().execute(
                McpGetBoardDefaultConfigDiffCommand(board_id), actor=actor, uow=uow
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_create_default_board_config_version(
    board_id: str,
    settings_payload: dict | None = None,
    scope: str = "global",
    guideline_default_refs: list | None = None,
    design_system_default_ref: dict | None = None,
    activate: bool = False,
) -> str:
    """Create a new default board-configuration template version (admin write).
    REST twin: POST /default-board-config/versions. Validated as BoardSettings;
    guideline defaults must be global; design_system gate_mode must be valid.
    activate=True activates it (single-active enforced). Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    if (
        isinstance(settings_payload, dict)
        and "skip_task_requirement_link_gate_global" in settings_payload
    ):
        return _refuse_human_control(
            board_id=board_id,
            blocked_tool="okto_pulse_create_default_board_config_version",
            blocked_action="set_task_requirement_link_gate_default_skip",
            target_ref="default_board_config:global",
        )
    from okto_pulse.core.application.use_cases import (
        McpCreateDefaultBoardConfigVersionCommand,
        McpCreateDefaultBoardConfigVersionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpCreateDefaultBoardConfigVersionUseCase().execute(
                McpCreateDefaultBoardConfigVersionCommand(
                    settings_payload=settings_payload,
                    scope=scope,
                    guideline_default_refs=guideline_default_refs,
                    design_system_default_ref=design_system_default_ref,
                    activate=activate,
                ),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_activate_default_board_config_version(board_id: str, template_id: str) -> str:
    """Activate a default board-configuration template version (admin write);
    deactivates every other active version in the scope. REST twin: POST
    /default-board-config/versions/{template_id}/activate. Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    human_control_refusal = await _refuse_mcp_default_config_activation_if_human_skip_changes(
        board_id=board_id,
        template_id=template_id,
        blocked_tool="okto_pulse_activate_default_board_config_version",
        blocked_action="activate_template_changes_task_requirement_link_gate_default_skip",
    )
    if human_control_refusal:
        return human_control_refusal
    from okto_pulse.core.application.use_cases import (
        McpActivateDefaultBoardConfigVersionCommand,
        McpActivateDefaultBoardConfigVersionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpActivateDefaultBoardConfigVersionUseCase().execute(
                McpActivateDefaultBoardConfigVersionCommand(template_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_deactivate_default_board_config_version(board_id: str, template_id: str) -> str:
    """Deactivate a default board-configuration template version (admin write).
    REST twin: POST /default-board-config/versions/{template_id}/deactivate.
    Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    human_control_refusal = await _refuse_mcp_default_config_deactivation_if_human_skip_changes(
        board_id=board_id,
        template_id=template_id,
        blocked_tool="okto_pulse_deactivate_default_board_config_version",
        blocked_action="deactivate_template_changes_task_requirement_link_gate_default_skip",
    )
    if human_control_refusal:
        return human_control_refusal
    from okto_pulse.core.application.use_cases import (
        McpDeactivateDefaultBoardConfigVersionCommand,
        McpDeactivateDefaultBoardConfigVersionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpDeactivateDefaultBoardConfigVersionUseCase().execute(
                McpDeactivateDefaultBoardConfigVersionCommand(template_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_list_default_guideline_candidates(
    board_id: str, scope: str = "global", template_id: str | None = None
) -> str:
    """List GLOBAL catalog guidelines with derived eligibility + current default
    status from the umbrella template (spec 8a2fad91 / FR1, admin read). REST twin:
    GET /guidelines/default-candidates. Uses the active template by default;
    template_id inspects a specific version. Perm: BOARD_READ."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err = _default_board_config_imports()
    async with get_db_for_mcp() as db:
        try:
            return json.dumps(
                await Svc(db).list_default_candidates(scope=scope, template_id=template_id),
                default=str,
            )
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_update_default_guideline_refs(
    board_id: str, template_id: str, guideline_default_refs: list | None = None
) -> str:
    """Update a template's guideline_default_refs using only global catalog guidelines
    (spec 8a2fad91 / FR1, admin write). REST twin: POST
    /default-board-configurations/{template_id}/guidelines. Inline/missing/non-global
    refs are rejected fail-closed (structured error). An ACTIVE template is
    copy-on-write (a new version is created + activated); a draft mutates in-place.
    Returns the EFFECTIVE template. Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err = _default_board_config_imports()
    async with get_db_for_mcp() as db:
        try:
            result = await Svc(db).update_template_guidelines(
                template_id=template_id,
                guideline_default_refs=guideline_default_refs,
                actor=ctx.agent_id,
            )
            await db.commit()
            return json.dumps(result, default=str)
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_set_default_design_system(
    board_id: str,
    template_id: str,
    design_system_id: str,
    gate_mode: str = "off",
    version: int | None = None,
    snapshot: dict | None = None,
) -> str:
    """Set the Design System default reference + canonical gate mode on a template
    (spec 3a006f65 / FR3, admin write). REST twin: POST
    /default-board-configurations/{template_id}/design-system. The design_system_id must
    be a real global active DesignSystem (inline/synthetic rejected fail-closed). An
    active template is copy-on-write (new version). Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err = _default_board_config_imports()
    async with get_db_for_mcp() as db:
        try:
            result = await Svc(db).set_template_design_system(
                template_id=template_id,
                design_system_id=design_system_id,
                actor=ctx.agent_id,
                version=version,
                snapshot=snapshot,
                gate_mode=gate_mode,
            )
            await db.commit()
            return json.dumps(result, default=str)
        except Err as e:
            return json.dumps(e.to_dict())


def _design_system_imports():
    from okto_pulse.core.services.design_system import (
        DesignSystemError,
        DesignSystemService,
        serialize_design_system,
    )
    return DesignSystemService, DesignSystemError, serialize_design_system


@mcp.tool()
async def okto_pulse_list_design_systems(board_id: str, scope: str = "global") -> str:
    """List Design Systems (spec 3a006f65 / FR2, admin read). scope='global' lists the
    global catalog; scope='inline' lists THIS board's inline Design Systems. REST twin:
    GET /design-systems. Perm: BOARD_READ."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err, ser = _design_system_imports()
    async with get_db_for_mcp() as db:
        try:
            items = await Svc(db).list_catalog(scope=scope, board_id=board_id)
            return json.dumps([ser(d) for d in items], default=str)
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_get_design_system(board_id: str, design_system_id: str) -> str:
    """Get a Design System by id (admin read). REST twin: GET /design-systems/{id}.
    Perm: BOARD_READ."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err, ser = _design_system_imports()
    async with get_db_for_mcp() as db:
        try:
            return json.dumps(ser(await Svc(db).require_design_system(design_system_id)), default=str)
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_create_design_system(
    board_id: str,
    title: str,
    scope: str = "global",
    payload: dict | None = None,
    status: str = "active",
) -> str:
    """Create a Design System (spec 3a006f65 / FR1, admin write). scope='global' = a
    catalog entry; scope='inline' = bound to THIS board (board_id). Inline can never be
    a global default. REST twin: POST /design-systems. Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err, ser = _design_system_imports()
    async with get_db_for_mcp() as db:
        try:
            ds = await Svc(db).create_design_system(
                ctx.agent_id,
                title=title,
                scope=scope,
                board_id=board_id if scope == "inline" else None,
                payload=payload,
                status=status,
            )
            await db.commit()
            return json.dumps(ser(ds), default=str)
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_update_design_system(
    board_id: str,
    design_system_id: str,
    title: str | None = None,
    payload: dict | None = None,
    status: str | None = None,
) -> str:
    """Update a Design System (admin write); a title/payload change bumps version.
    REST twin: PATCH /design-systems/{id}. Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err, ser = _design_system_imports()
    kwargs = {
        k: v for k, v in (("title", title), ("payload", payload), ("status", status))
        if v is not None
    }
    async with get_db_for_mcp() as db:
        try:
            ds = await Svc(db).update_design_system(design_system_id, ctx.agent_id, **kwargs)
            await db.commit()
            return json.dumps(ser(ds), default=str)
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_delete_design_system(board_id: str, design_system_id: str) -> str:
    """Delete a Design System (admin write). REST twin: DELETE /design-systems/{id}.
    Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    Svc, Err, _ser = _design_system_imports()
    async with get_db_for_mcp() as db:
        try:
            deleted = await Svc(db).delete_design_system(design_system_id, ctx.agent_id)
            if not deleted:
                return json.dumps(
                    {"error": "design_system_not_found", "code": "design_system_not_found"}
                )
            await db.commit()
            return json.dumps({"deleted": True, "id": design_system_id})
        except Err as e:
            return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_link_board_design_system(board_id: str, design_system_id: str) -> str:
    """Set the board's single effective Design System (admin write). REST twin: POST
    /boards/{board_id}/design-system. Inline systems can only link to their own board.
    Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    from okto_pulse.core.application.use_cases import (
        McpLinkBoardDesignSystemCommand,
        McpLinkBoardDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpLinkBoardDesignSystemUseCase().execute(
                McpLinkBoardDesignSystemCommand(board_id, design_system_id),
                actor=actor,
                uow=uow,
            )
            link = result.data
            return json.dumps(
                {
                    "board_id": link.board_id,
                    "design_system_id": link.design_system_id,
                    "design_system_version": link.design_system_version,
                },
                default=str,
            )
    except DesignSystemError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_unlink_board_design_system(board_id: str) -> str:
    """Remove the board's effective Design System link (admin write). REST twin: DELETE
    /boards/{board_id}/design-system. Perm: SPECS_UPDATE."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)
    from okto_pulse.core.application.use_cases import (
        McpUnlinkBoardDesignSystemCommand,
        McpUnlinkBoardDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpUnlinkBoardDesignSystemUseCase().execute(
                McpUnlinkBoardDesignSystemCommand(board_id), actor=actor, uow=uow
            )
        return json.dumps({"unlinked": result.data})
    except DesignSystemError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_get_board_design_system(board_id: str) -> str:
    """Resolve the board's EFFECTIVE Design System from real persisted state (admin
    read) — explicit board link else umbrella default snapshot, or null. REST twin: GET
    /boards/{board_id}/design-system. Perm: BOARD_READ."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)
    from okto_pulse.core.application.use_cases import (
        McpGetBoardDesignSystemCommand,
        McpGetBoardDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpGetBoardDesignSystemUseCase().execute(
                McpGetBoardDesignSystemCommand(board_id), actor=actor, uow=uow
            )
        return json.dumps({"board_id": board_id, "effective": result.data}, default=str)
    except DesignSystemError as e:
        return json.dumps(e.to_dict())


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
okto-pulse://reference/tool-docs/spec.

Scores are 0-100 integers, NOT 1-5: completeness/assertiveness are higher-is-better
and ambiguity is lower-is-better. A 1-5 style value is treated literally and will
usually violate the configured thresholds."""
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

    from okto_pulse.core.application.use_cases import (
        EntityNotFoundError,
        SubmitSpecValidationCommand,
        SubmitSpecValidationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    async with get_db_for_mcp() as db:
        try:
            # Thin MCP adapter (spec #09): delegate to the shared use case (it
            # validates the payload, resolves the reviewer name from the MCP agent,
            # submits and commits). The MCP-specific input checks above are kept so
            # the tool's error envelopes/order are unchanged.
            result = await SubmitSpecValidationUseCase().execute(
                SubmitSpecValidationCommand(spec_id, data),
                actor=MCPAdapterContract.actor(ctx, board_id=board_id),
                uow=db,
            )
            return json.dumps(result.payload, default=str)
        except (EntityNotFoundError, ValueError) as e:
            return MCPAdapterContract.error(e)


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

_register_kg_tools(mcp, get_agent=_get_authenticated_agent, get_uow=get_unit_of_work_factory_for_mcp)
_register_kg_query_tools(mcp, get_agent=_get_authenticated_agent, get_uow=get_unit_of_work_factory_for_mcp)

from okto_pulse.core.mcp.kg_power_tools import register_kg_power_tools as _register_kg_power_tools  # noqa: E402

_register_kg_power_tools(mcp, get_agent=_get_authenticated_agent)


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

    from okto_pulse.core.application.use_cases import (
        GetKgHealthCommand,
        GetKgHealthUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.mcp.kg_query_safety import KGHealthMCPProjection
    from okto_pulse.core.services.kg_health_service import BoardNotFoundError

    # Spec R01A MCP-FU4 (MCP strangler): read the board KG health snapshot through
    # the transport-free use case + MCP UnitOfWorkFactory instead of a raw
    # get_db_for_mcp() session — this tool no longer calls get_db_for_mcp. The MCP
    # slim/full projection, the BoardNotFoundError envelope and the
    # ``_get_agent_ctx`` permission-cache path are unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await GetKgHealthUseCase().execute(
                GetKgHealthCommand(board_id), actor=actor, uow=uow,
            )
    except BoardNotFoundError as exc:
        return json.dumps({"error": str(exc)})
    # FR4: slim default projection — keep the stop-rule fields, omit verbose
    # diagnostics until profile=full/legacy is requested.
    data = KGHealthMCPProjection().project(result.data, profile=profile)
    return json.dumps(data, default=str)


@mcp.tool()
async def okto_pulse_kg_health_readiness(
    board_id: str, profile: str = "summary", artifact_ref: str = "",
) -> str:
    """Canonical NON-MASKABLE KG health/readiness (gemelar do REST GET
/api/v1/kg/health-readiness, RKG-05). Both summary and full expose
`technical_signals` (scalar counters dead_letter_count / technical_dlq_count /
canonical_debt_open_count / active_queue_count, SEPARATE domains), `readiness`
(`blocking` vs `would_block_done` + reasons + policy_reason), top-level
`cognitive_enforcement_mode` / `enforcement_active`, and `non_maskable_items`
(per-item drill_down_tool / last_error / next_action). A summary never hides a
technical blocker; full adds prose health_issues + root_cause. Optional
artifact_ref scopes items. Full guide: okto-pulse://reference/tool-docs/kg."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.application.use_cases import (
        GetKgHealthReadinessCommand,
        GetKgHealthReadinessUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.kg_health_readiness_service import (
        InvalidProfileError,
    )
    from okto_pulse.core.services.kg_health_service import BoardNotFoundError

    # Spec R01A MCP-FU4 (MCP strangler): build the non-maskable readiness through
    # the transport-free use case + MCP UnitOfWorkFactory instead of a raw
    # get_db_for_mcp() session — this tool no longer calls get_db_for_mcp.
    # surface="mcp", the InvalidProfile/BoardNotFound envelopes and the
    # ``_get_agent_ctx`` permission-cache path are unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await GetKgHealthReadinessUseCase().execute(
                GetKgHealthReadinessCommand(
                    board_id, profile=profile, surface="mcp",
                    artifact_ref=(artifact_ref or None),
                ),
                actor=actor, uow=uow,
            )
    except InvalidProfileError:
        return json.dumps({"error": "invalid_profile"})
    except BoardNotFoundError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(result.data, default=str)


@mcp.tool()
async def okto_pulse_kg_canonical_debt_list(
    board_id: str,
    artifact_type: str | None = None,
    state: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List canonical-debt ledger rows for KG health drill-down.

    Use this when `okto_pulse_kg_health` reports `canonical_debt.open_count`
    and you need to inspect which artifacts are pending, blocked, failed, or
    retry-scheduled before deciding whether a rebuild or retry is appropriate.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps({
            "error": "invalid_pagination",
            "detail": "limit and offset must be integers",
        })

    from okto_pulse.core.application.use_cases import (
        ListCanonicalDebtCommand,
        ListCanonicalDebtUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU5 strangler: obtain a PulseUnitOfWork from the MCP UnitOfWorkFactory
    # instead of opening a raw get_db_for_mcp() session — the tool no longer calls
    # get_db_for_mcp directly. The use case delegates to the same reader so the
    # payload (items/counts/total) stays byte-identical. Read-only: no commit.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = (
            await ListCanonicalDebtUseCase().execute(
                ListCanonicalDebtCommand(
                    board_id,
                    artifact_type=artifact_type or None,
                    state=state or None,
                    limit=bounded_limit,
                    offset=bounded_offset,
                ),
                actor=actor,
                uow=uow,
            )
        ).data

    from okto_pulse.core.kg.rebuild_audit import emit_operational_inspection_sample
    emit_operational_inspection_sample(
        signal="canonical_debt", surface="mcp", outcome="success",
        board_id=board_id, item_count=len(result.items),
    )
    return json.dumps({
        "board_id": board_id,
        "items": result.items,
        "counts": result.counts,
        "total": result.total,
        "limit": bounded_limit,
        "offset": bounded_offset,
    }, default=str)


@mcp.tool()
async def okto_pulse_kg_canonical_partition_integrity_list(
    board_id: str,
    reason_code: str | None = None,
    graph_layer: str | None = None,
    source_ref: str | None = None,
    node_id: str | None = None,
    status: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List canonical Learning partition-integrity signals for KG health drill-down
    (R7). READ-ONLY: surfaces go-forward cognitive holds, historical canonical
    debt, mixed-evidence deferred and provenance-only observed Learnings. Each
    item carries an S-KG-02 ``classification`` (Learning-centric: missing_source,
    unresolved_source, canonical_learning_resolved, weak_provenance,
    invalid_orphan_learning) plus the response's ``classification_counts`` census;
    the existing ``status`` / ``counts`` are preserved.

    Mirrors REST `GET /api/v1/kg/{board_id}/canonical-partition-integrity` (and the
    per-node `.../{node_id}` detail, which carries the same ``classification`` so
    the two surfaces stay consistent). This tool NEVER skips, clears, force-closes
    or resolves an R7 hold/debt — those are human-only (use the human REST
    surface). Filters: reason_code, graph_layer, source_ref, node_id, status.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps({
            "error": "invalid_pagination",
            "detail": "limit and offset must be integers",
        })

    from okto_pulse.core.application.use_cases import (
        ListCanonicalPartitionIntegrityCommand,
        ListCanonicalPartitionIntegrityUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError

    # MCP-FU5 strangler: UoW factory instead of get_db_for_mcp(); the use case
    # lets CognitiveReadinessError propagate so the tool keeps its legacy
    # exc.to_dict() envelope. Read-only: no commit.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = (
                await ListCanonicalPartitionIntegrityUseCase().execute(
                    ListCanonicalPartitionIntegrityCommand(
                        board_id,
                        reason_code=reason_code or None,
                        graph_layer=graph_layer or None,
                        source_ref=source_ref or None,
                        node_id=node_id or None,
                        status=status or None,
                        limit=bounded_limit,
                        offset=bounded_offset,
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).data
    except CognitiveReadinessError as exc:
        return json.dumps(exc.to_dict())

    # OR1 metric (kg_canonical_partition_integrity_total) is emitted inside
    # list_canonical_partition_integrity (the single enumeration point), so REST
    # and MCP share the same dedicated signal without double-emitting here.
    return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_kg_digest_layer_mismatch_list(
    board_id: str,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List Global Discovery DecisionDigest rows whose published graph_layer diverges
    from the expected_digest_layer recomputed from the board graph
    (digest_vs_board_layer_mismatch, R1). READ-ONLY drill-down for the KG Health
    issue. Mirrors REST `GET /api/v1/kg/{board_id}/digest-layer-mismatch`. Each
    item carries board_id, digest_id, original_node_id, node_type, expected_layer,
    actual_layer, source_artifact_ref. NEVER mutates / remediates (the R1-IMP1
    reconciler corrects mismatches on drain).
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps({
            "error": "invalid_pagination",
            "detail": "limit and offset must be integers",
        })

    from okto_pulse.core.application.use_cases import (
        ListDigestLayerMismatchCommand,
        ListDigestLayerMismatchUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU5 strangler: UoW factory instead of get_db_for_mcp(). Read-only: no
    # commit. The use case delegates to the same reader (single metric-emit point).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = (
            await ListDigestLayerMismatchUseCase().execute(
                ListDigestLayerMismatchCommand(
                    board_id, limit=bounded_limit, offset=bounded_offset,
                ),
                actor=actor,
                uow=uow,
            )
        ).data
    # kg_discovery_digest_layer_mismatch_total is emitted inside
    # list_digest_layer_mismatches (single enumeration point) so REST + MCP share
    # the metric without double-emitting here.
    return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_kg_originates_from_contract_audit(
    board_id: str,
    limit: int = 50,
    offset: int = 0,
    include_ok: bool = False,
) -> str:
    """
    Read-only advisory audit for persisted KG `originates_from` edges whose
    endpoint labels violate the Bug->Entity contract. Unknown/missing endpoint
    labels are returned as low-confidence advisory warnings. This tool never
    mutates, rebuilds, reprocesses, skips, or remediates graph data.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps({
            "error": "invalid_pagination",
            "detail": "limit and offset must be integers",
        })

    from okto_pulse.core.application.use_cases import (
        AuditOriginatesFromContractCommand,
        AuditOriginatesFromContractUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = (
            await AuditOriginatesFromContractUseCase().execute(
                AuditOriginatesFromContractCommand(
                    board_id,
                    limit=bounded_limit,
                    offset=bounded_offset,
                    include_ok=bool(include_ok),
                ),
                actor=actor,
                uow=uow,
            )
        ).data
    return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_kg_stale_canonical_parity_list(
    board_id: str,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List stale-canonical parity signals for KG health drill-down (R2). READ-ONLY:
    canonical deterministic board-graph nodes whose SQL source regressed below
    canonical eligibility, each annotated with whether its Global Discovery digest
    is also stale (R1 parity). Mirrors REST
    `GET /api/v1/kg/{board_id}/stale-canonical-parity`. Items carry board_graph_stale,
    global_discovery_stale_digest, expected_graph_layer, expected_maturity_status,
    current_source_status, recommended_action. NEVER demotes/reconciles/syncs — this
    is a diagnostic only (no mutating path).
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps({
            "error": "invalid_pagination",
            "detail": "limit and offset must be integers",
        })

    from okto_pulse.core.application.use_cases import (
        ListStaleCanonicalParityCommand,
        ListStaleCanonicalParityUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A IMP5 (MCP strangler): obtain a PulseUnitOfWork from the MCP
    # UnitOfWorkFactory instead of opening a raw get_db_for_mcp() session — this
    # tool no longer calls get_db_for_mcp. The transport-free use case (shared with
    # the REST endpoint migrated in R01A IMP4) reads the parity signals; the
    # payload, the pagination bounds and the ``_get_agent_ctx`` permission-cache
    # path are unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ListStaleCanonicalParityUseCase().execute(
            ListStaleCanonicalParityCommand(
                board_id, limit=bounded_limit, offset=bounded_offset
            ),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.data, default=str)


@mcp.tool()
async def okto_pulse_kg_evaluate_bug_cognitive_closure(
    board_id: str,
    bug_id: str,
    evidence: dict[str, Any] | None = None,
    requested_action: str = "evaluate",
    reason_code: str | None = None,
    justification: str | None = None,
    evidence_refs: list[str] | None = None,
    revisit_at: str | None = None,
) -> str:
    """
    Read-only bug cognitive-closure evaluation. Mirrors the REST/UI classifier
    and central CognitiveReadinessService verdict; this tool does not recompute
    precedence.

    Allowed agent actions: `evaluate` (default) and `create_learning`.
    Agent-facing `skip`/`no_action` fails closed with `human_control_required`
    and never writes the ledger. Human skip/no_action remains on the authorized
    UI/REST path and cannot mask technical debt.
    Full docs: okto-pulse://reference/tool-docs/kg."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.kg.bug_cognitive_closure import (
        NO_ACTION,
        SKIP,
    )
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError

    # R5-IMP1: a skip / no_action requested_action is a HUMAN-only mutation. Fail
    # closed BEFORE the write-path so no skip/no_action is persisted (state
    # unchanged). evaluate / create_learning (no skip persistence) stay
    # agent-facing — only the mutating actions are human-only.
    if str(requested_action or "").strip().lower() in (SKIP, NO_ACTION):
        return _refuse_human_control(
            board_id=board_id,
            blocked_tool="okto_pulse_kg_evaluate_bug_cognitive_closure",
            blocked_action=f"evaluate_bug_cognitive_closure:{str(requested_action).strip().lower()}",
            target_ref=f"bug:{bug_id}",
        )
    # Spec R01A MCP-FU3 (MCP strangler): read-only bug closure verdict via the
    # transport-free use case + MCP UnitOfWorkFactory instead of a raw
    # get_db_for_mcp() session. The human-control fail-closed (above), the actor
    # name and the CognitiveReadinessError envelope are unchanged.
    from okto_pulse.core.application.use_cases import (
        EvaluateBugCognitiveClosureCommand,
        EvaluateBugCognitiveClosureUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await EvaluateBugCognitiveClosureUseCase().execute(
                EvaluateBugCognitiveClosureCommand(
                    board_id, bug_id, evidence=evidence,
                    requested_action=requested_action, reason_code=reason_code,
                    justification=justification, evidence_refs=evidence_refs,
                    revisit_at=revisit_at,
                ),
                actor=actor, uow=uow,
            )
        return json.dumps(result.data, default=str)
    except CognitiveReadinessError as exc:
        return json.dumps(exc.to_dict())


# ============================================================================
# COGNITIVE ACTION CENTER — operational MCP tools (spec 2731a346, card 3979c220)
#
# Agent-facing surface over the S3.1 read-model + the central
# CognitiveReadinessService. NEVER reimplements precedence (tr_b9595c79 /
# dec_af630079): list/evaluate mirror the service verbatim; write tools (skip /
# clear) drive the central ledger-only path — no own store, no KG node/edge, no
# connectivity guard for skip/no_action. Enforcement (would-this-block-done) is
# NOT inferred from ``blocking`` alone: it is delegated to the existing two-key
# wiring (``_cognitive_readiness_blocking_active`` + ``GATE_BLOCKING_TIERS``),
# never recomputed here (S3.1 validator carry-forward).
# ============================================================================


def _build_cognitive_readiness_service():
    """Central readiness service over the shared item store (no own store)."""
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessService
    from okto_pulse.core.kg.rebuild_audit import (
        CognitiveConsolidationItemStore,
        default_rebuild_base_dir,
    )

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(base_dir=default_rebuild_base_dir())
    )


async def _cognitive_enforcement_active(db, board_id: str) -> bool:
    """Whether the board's done-gate is ACTUALLY enforcing cognitive readiness
    (two-key rollout). Delegates to the transport-free service reader
    (spec R01A MCP-FU3) — never recomputed; the 4 in-server callers are unchanged."""
    from okto_pulse.core.services.main import cognitive_enforcement_active

    return await cognitive_enforcement_active(db, board_id)


def _would_block_done(item_or_tier, enforcement_active: bool) -> bool:
    """``blocking`` means a pending/unresolved verdict; it does NOT by itself mean
    the done-gate will block (S3.1 validator carry-forward). The gate only
    enforces GATE_BLOCKING_TIERS, and only when the board policy is active."""
    from okto_pulse.core.kg.cognitive_readiness import GATE_BLOCKING_TIERS

    if isinstance(item_or_tier, dict):
        tier = (item_or_tier.get("precedence_explanation") or {}).get("tier")
    else:
        tier = item_or_tier
    return bool(enforcement_active and tier in GATE_BLOCKING_TIERS)


async def _evaluate_card_cognitive_verdict(db, board_id: str, card, enforcement_active: bool):
    """R4-IMP4 — the card's OWN per-artifact cognitive readiness verdict, mirroring
    the done-gate's evaluation (``resolve_cognitive_source_refs`` + the central
    ``CognitiveReadinessService`` on the card's own ref) — NEVER a duplicate store.

    Read-only and best-effort: returns ``None`` on any failure, so the context tool
    can never break because readiness is momentarily unavailable. ``would_block_done``
    is enforcement-aware via the shared ``_would_block_done`` (``blocking`` alone is
    advisory)."""
    try:
        from okto_pulse.core.kg.cognitive_closeout_gate import (
            resolve_cognitive_source_refs,
        )
        from okto_pulse.core.services.main import _card_cognitive_entity_type

        refs = resolve_cognitive_source_refs(
            entity_type=_card_cognitive_entity_type(card),
            entity=card,
            entity_id=card.id,
        ).source_refs
        if not refs:
            return None
        ref = refs[0]
        # Carve-out identical to the done-gate: task/test carry no reusable
        # cognition -> advisory (never blocks on cognitive tiers).
        has_reusable = ref.split(":", 1)[0] not in ("task", "test")
        verdict = await _build_cognitive_readiness_service().evaluate_artifact(
            db, board_id=board_id, source_ref=ref, has_reusable_cognition=has_reusable,
        )
    except Exception:
        return None
    return {
        "source_ref": ref,
        "readiness_effect": getattr(verdict, "readiness_effect", None),
        "readiness_signal": getattr(verdict, "readiness_signal", None),
        "tier": getattr(verdict, "tier", None),
        "reason_code": getattr(verdict, "reason_code", None),
        "blocking": bool(getattr(verdict, "blocking", False)),
        "revisit_at": getattr(verdict, "revisit_at", None),
        "would_block_done": _would_block_done(
            getattr(verdict, "tier", None), enforcement_active
        ),
    }


@mcp.tool()
async def okto_pulse_kg_list_cognitive_readiness_items(
    board_id: str,
    signal: str = "all",
    artifact_id: str | None = None,
    source_ref: str | None = None,
    reason_code: str | None = None,
    status: str | None = None,
    search: str | None = None,
    limit: int = 50,
    offset: int = 0,
    kg_generation_id: str | None = None,
) -> str:
    """
    List board cognitive-readiness rows: cognitive items, canonical debt and
    technical DLQ, reconciled by normalized artifact_id. Rows mirror the central
    CognitiveReadinessService verdict; cognitive `reason_code` stays distinct
    from technical `error_cause`.

    `would_block_done` is enforcement-aware. Filters: signal, artifact_id,
    source_ref, reason_code, status, search, limit<=200, offset.
    Full docs: okto-pulse://reference/tool-docs/kg."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    # Spec R01A MCP-FU3 (MCP strangler): read-only signal list + enforcement via the
    # transport-free use case + MCP UnitOfWorkFactory instead of a raw
    # get_db_for_mcp() session. The would_block_done post-processing below, the
    # CognitiveReadinessError envelope and the enforcement flag are unchanged.
    from okto_pulse.core.application.use_cases import (
        ListCognitiveReadinessItemsCommand,
        ListCognitiveReadinessItemsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            uc_result = await ListCognitiveReadinessItemsUseCase().execute(
                ListCognitiveReadinessItemsCommand(
                    board_id, signal=signal, artifact_id=artifact_id or None,
                    source_ref=source_ref or None, reason_code=reason_code or None,
                    status=status or None, search=search or None, limit=limit,
                    offset=offset, kg_generation_id=kg_generation_id or None,
                ),
                actor=actor, uow=uow,
            )
    except CognitiveReadinessError as exc:
        return json.dumps(exc.to_dict())
    result = uc_result.result
    enforcement_active = uc_result.enforcement_active

    for item in result["items"]:
        item["would_block_done"] = _would_block_done(item, enforcement_active)
    result["summary"]["enforcement_active"] = enforcement_active
    result["board_id"] = board_id
    return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_kg_evaluate_cognitive_readiness(
    board_id: str,
    source_ref: str,
    kg_generation_id: str | None = None,
) -> str:
    """
    Evaluate ONE artifact's cognitive readiness via the central service.

    Returns the 6-tier verdict verbatim (`readiness_effect`, `blocking`,
    `tier`, `readiness_signal`, `reason_code`, `revisit_at`,
    `precedence_explanation` = the blocked-by source) — precedence is NEVER
    recomputed here. `would_block_done` is enforcement-aware (see the list tool).

    `source_ref` is `<type>:<id>` (a `bug:<uuid>` reconciles to its
    `card:<uuid>`). task/test carry no reusable cognition -> advisory.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.kg.cognitive_readiness import CognitiveReadinessError

    primary_type = str(source_ref).split(":", 1)[0].lower()
    # Spec R01A MCP-FU3 (MCP strangler): central readiness verdict + enforcement via
    # the transport-free use case + MCP UnitOfWorkFactory instead of a raw
    # get_db_for_mcp() session. The payload assembly below and the
    # CognitiveReadinessError envelope are unchanged.
    from okto_pulse.core.application.use_cases import (
        EvaluateCognitiveReadinessCommand,
        EvaluateCognitiveReadinessUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            uc_result = await EvaluateCognitiveReadinessUseCase().execute(
                EvaluateCognitiveReadinessCommand(
                    board_id, source_ref=source_ref,
                    kg_generation_id=kg_generation_id or None,
                    has_reusable_cognition=primary_type not in ("task", "test"),
                ),
                actor=actor, uow=uow,
            )
    except CognitiveReadinessError as exc:
        return json.dumps(exc.to_dict())
    verdict = uc_result.verdict
    enforcement_active = uc_result.enforcement_active

    payload = verdict.to_api()
    payload["would_block_done"] = _would_block_done(verdict.tier, enforcement_active)
    payload["enforcement_active"] = enforcement_active
    return json.dumps(payload, default=str)


@mcp.tool()
async def okto_pulse_kg_record_cognitive_skip(
    board_id: str,
    source_ref: str,
    reason_code: str,
    justification: str | None = None,
    evidence_refs: list[str] | None = None,
    revisit_at: str | None = None,
    kg_generation_id: str | None = None,
) -> str:
    """
    Agent-facing cognitive skip/no_action control. This surface is HUMAN-only:
    it fails closed with `human_control_required`, performs no state change and
    never writes the ledger or KG. Human REST/UI keeps the canonical validations
    for invalid reason, missing revisit date and technical debt masking.
    Full docs: okto-pulse://reference/tool-docs/kg."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    # Fail closed BEFORE the central write-path — no ledger mutation, no state
    # change. The human REST surface (actor_is_human) remains the path.
    return _refuse_human_control(
        board_id=board_id,
        blocked_tool="okto_pulse_kg_record_cognitive_skip",
        blocked_action="record_cognitive_skip",
        target_ref=source_ref,
    )


@mcp.tool()
async def okto_pulse_kg_clear_cognitive_skip(
    board_id: str,
    source_ref: str,
    kg_generation_id: str | None = None,
) -> str:
    """
    Clear a cognitive skip / no_action, REOPENING the item to pending via the
    central ledger path (CognitiveReadinessService.clear_cognitive_skip). The
    clearing actor + timestamp are stamped so the audit trail is preserved; the
    stale reason_code / revisit_at are dropped. Ledger-only — no KG mutation.

    R5-IMP1 — HUMAN-only control: clearing/reopening a cognitive skip is a human
    decision and is NOT applicable from the agent-facing MCP surface. This tool
    fails closed with ``human_control_required`` (mutation_allowed=false,
    state_changed=false) and never reopens the ledger item. A human operator
    clears the skip via the IDE control / the human REST surface.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    # Fail closed BEFORE the central write-path — no ledger mutation, no state
    # change. The human REST surface (actor_is_human) remains the path.
    return _refuse_human_control(
        board_id=board_id,
        blocked_tool="okto_pulse_kg_clear_cognitive_skip",
        blocked_action="clear_cognitive_skip",
        target_ref=source_ref,
    )


@mcp.tool()
async def okto_pulse_kg_list_cognitive_dlq(
    board_id: str,
    limit: int = 50,
    offset: int = 0,
) -> str:
    """
    List the board's TECHNICAL dead-letter (DLQ) blockers for cognitive
    consolidation — diagnosis + action surface, no UI dependency.

    Each row carries the normalized `artifact_id`, the `errors` history, and the
    central readiness framing (`error_cause`=technical_dlq, `signal`=dlq,
    `readiness_effect`=blocking_technical). A technical DLQ is NEVER a selectable
    cognitive reason_code; resolve it (don't skip it). Open canonical-debt
    blockers are surfaced by `okto_pulse_kg_list_cognitive_readiness_items`
    (signal=open_canonical_debt) and `okto_pulse_kg_canonical_debt_list`.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps({
            "error": "invalid_pagination",
            "detail": "limit and offset must be integers",
        })

    from okto_pulse.core.application.use_cases import (
        ListCognitiveDlqCommand,
        ListCognitiveDlqUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.kg.cognitive_readiness import TECHNICAL_DLQ_SIGNAL
    from okto_pulse.core.kg.rebuild_audit import normalize_cognitive_artifact_id

    # Spec R01A MCP-FU3B (MCP strangler): the inline DLQ query is now a dedicated
    # reader behind the transport-free use case + MCP UnitOfWorkFactory — this tool
    # no longer issues SQL or opens a raw get_db_for_mcp() session. The row
    # projection below (normalized artifact id, technical_dlq framing) is unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        uc_result = await ListCognitiveDlqUseCase().execute(
            ListCognitiveDlqCommand(board_id, limit=bounded_limit, offset=bounded_offset),
            actor=actor, uow=uow,
        )
    total = uc_result.total
    rows = uc_result.rows

    items = []
    for row in rows:
        ref = f"{row.artifact_type}:{row.artifact_id}"
        items.append({
            "id": row.id,
            "artifact_type": str(row.artifact_type or ""),
            "artifact_id": normalize_cognitive_artifact_id(ref),
            "source_ref_original": ref,
            "original_queue_id": getattr(row, "original_queue_id", None),
            "attempts": getattr(row, "attempts", None),
            "errors": getattr(row, "errors", None),
            "signal": "dlq",
            "signal_source": "dlq",
            "error_cause": TECHNICAL_DLQ_SIGNAL,
            "readiness_effect": "blocking_technical",
            "blocking": True,
        })
    return json.dumps({
        "board_id": board_id,
        "items": items,
        "total": total,
        "limit": bounded_limit,
        "offset": bounded_offset,
        "note": (
            "Technical DLQ — resolve/reprocess; never skippable as a cognitive "
            "reason_code. Open canonical debt is in the readiness list "
            "(signal=open_canonical_debt) and okto_pulse_kg_canonical_debt_list."
        ),
    }, default=str)


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

    from okto_pulse.core.application.use_cases import (
        ListDeadLetterRowsCommand,
        ListDeadLetterRowsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A IMP3 (MCP strangler): obtain a PulseUnitOfWork from the MCP
    # UnitOfWorkFactory instead of opening a raw get_db_for_mcp() session — this
    # tool no longer calls get_db_for_mcp. The transport-free use case (shared
    # with the REST endpoint migrated in R01A IMP2) reads the DLQ rows; the
    # payload, the audit sample and the ``_get_agent_ctx`` permission-cache path
    # are unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ListDeadLetterRowsUseCase().execute(
            ListDeadLetterRowsCommand(board_id, limit=limit, offset=offset),
            actor=actor,
            uow=uow,
        )
    data = result.data
    from okto_pulse.core.kg.rebuild_audit import emit_operational_inspection_sample
    emit_operational_inspection_sample(
        signal="dead_letter", surface="mcp", outcome="success",
        board_id=board_id, item_count=len(data.get("rows", [])),
    )
    return json.dumps(data, default=str)


@mcp.tool()
async def okto_pulse_kg_queue_drilldown(board_id: str) -> str:
    """Drill down into the ACTIVE operational queue depth (R6-IMP2).

    Use this when `okto_pulse_kg_health` reports an `active_queue` backlog (a
    health issue with `drill_down_tool='okto_pulse_kg_queue_drilldown'`) and you
    need to know WHERE the queue depth comes from. Read-only.

    Returns `worker_mode`, `total_active_depth`, an overall `classification`
    (transient | stuck | backpressure | idle) and per-source breakdowns:
      - `consolidation_queue` — pending/claimed by status + by artifact category +
        oldest_age_seconds;
      - `global_update_outbox` — pending (retry-window) depth + oldest_age_seconds.

    This is the ACTIVE queue only: dead-letter (DLQ), outbox dead_letter and
    canonical debt are TERMINAL and intentionally NOT counted here — inspect those
    via `okto_pulse_kg_dead_letter_list` / `okto_pulse_kg_canonical_debt_list`."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        GetQueueDrilldownCommand,
        GetQueueDrilldownUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A IMP5 (MCP strangler): PulseUnitOfWork from the MCP UnitOfWorkFactory
    # instead of a raw get_db_for_mcp() session — this tool no longer calls
    # get_db_for_mcp. The transport-free use case (shared with the REST endpoint
    # migrated in R01A IMP4) computes the active-queue drilldown; the payload, the
    # BOARD_READ permission check above and the ``_get_agent_ctx`` permission-cache
    # path are unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await GetQueueDrilldownUseCase().execute(
            GetQueueDrilldownCommand(board_id), actor=actor, uow=uow
        )
    return json.dumps(result.data, default=str)


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

    from okto_pulse.core.application.use_cases import (
        ReprocessDeadLetterRowsCommand,
        ReprocessDeadLetterRowsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A MCP-FU2 (MCP strangler): requeue DLQ rows via the transport-free use
    # case + MCP UnitOfWorkFactory instead of a raw get_db_for_mcp() session. The
    # explicit commit is preserved inside the use case; the process_now worker
    # signalling below is unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ReprocessDeadLetterRowsUseCase().execute(
            ReprocessDeadLetterRowsCommand(
                board_id, dead_letter_ids=ids or None, limit=limit
            ),
            actor=actor,
            uow=uow,
        )
    data = result.data

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
# RKG-04 — connectivity-guard technical_dlq class: diagnose / reprocess / verify
# (fail-closed, class-scoped; never a broad reprocess of unanalysed DLQs).
# ============================================================================


@mcp.tool()
async def okto_pulse_kg_connectivity_dlq_diagnose(board_id: str) -> str:
    """okto_pulse_kg_connectivity_dlq_diagnose — diagnose the LIVE connectivity-
    guard technical_dlq class (RKG-04) BEFORE any reprocess. Read-only.

    Returns each member's dead_letter_id, artifact_id, attempts, errors,
    last_error, the source_artifact_ref involved, the probable root cause and the
    next_action — the input you must feed to
    `okto_pulse_kg_connectivity_dlq_reprocess` (which only accepts in-class ids)."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        DiagnoseConnectivityDlqCommand,
        DiagnoseConnectivityDlqUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A MCP-FU2 (MCP strangler): read-only diagnose via the transport-free
    # use case + MCP UnitOfWorkFactory instead of a raw get_db_for_mcp() session.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await DiagnoseConnectivityDlqUseCase().execute(
            DiagnoseConnectivityDlqCommand(board_id), actor=actor, uow=uow
        )
    return json.dumps(result.data, default=str)


@mcp.tool()
async def okto_pulse_kg_connectivity_dlq_reprocess(
    board_id: str,
    dead_letter_ids: list[str] | str = "",
    process_now: str = "true",
) -> str:
    """okto_pulse_kg_connectivity_dlq_reprocess — fail-closed reprocess of the
    connectivity-guard technical_dlq class (RKG-04).

    Requires EXPLICIT in-class `dead_letter_ids` (from
    `okto_pulse_kg_connectivity_dlq_diagnose`). Blocks — removing NO DLQ — when the
    selection is empty (`no_dlq_selected`), missing (`selected_dlq_missing`),
    out-of-class (`selected_dlq_out_of_class`), the RKG-02/RKG-03 fixes are absent
    (`rkg02_rkg03_not_applied`) or the KG is quarantined (`kg_quarantined`). It is
    NEVER a broad reprocess. On success it reuses the idempotent DLQ→queue path
    (ConsolidationQueue dedup) and, with process_now, runs one worker batch."""
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

    from okto_pulse.core.application.use_cases import (
        ReprocessConnectivityDlqCommand,
        ReprocessConnectivityDlqUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A MCP-FU2 (MCP strangler): fail-closed reprocess via the transport-free
    # use case + MCP UnitOfWorkFactory instead of a raw get_db_for_mcp() session. The
    # use case commits ONLY when the service did not block (a blocked selection
    # removes no DLQ and must not commit) — identical to the legacy tool. The
    # process_now worker signalling below is unchanged.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ReprocessConnectivityDlqUseCase().execute(
            ReprocessConnectivityDlqCommand(board_id, ids), actor=actor, uow=uow
        )
    data = result.data

    if not data.get("blocked") and _flag_enabled(process_now):
        from okto_pulse.core.kg.workers.consolidation import (
            get_consolidation_worker,
            signal_consolidation_worker,
        )

        worker = get_consolidation_worker(_mcp_session_factory)
        signal_consolidation_worker()
        data["worker_running"] = worker.is_running
        if not worker.is_running:
            data["processed_now_count"] = await worker.process_batch()
            data["process_now_mode"] = "singleton_direct_batch"
        else:
            data["processed_now_count"] = 0
            data["process_now_mode"] = "signalled_singleton"

    return json.dumps(data, default=str)


@mcp.tool()
async def okto_pulse_kg_connectivity_dlq_verify(
    board_id: str,
    artifact_refs: list[str] | str = "",
) -> str:
    """okto_pulse_kg_connectivity_dlq_verify — after the worker drains the queue,
    confirm the connectivity-guard class is cleared for the given
    `artifact_refs` (or the whole class when empty). Read-only.

    A member that returned to the DLQ stays VISIBLE (`class_cleared=false` +
    `remaining_dlq`) — partial success is never masked."""
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    try:
        refs = coerce_to_list_str(artifact_refs) if artifact_refs else None
    except ValueError as exc:
        return json.dumps({"error": f"Invalid artifact_refs: {exc}"})

    from okto_pulse.core.application.use_cases import (
        VerifyConnectivityClassCommand,
        VerifyConnectivityClassUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec R01A MCP-FU2 (MCP strangler): read-only verify via the transport-free use
    # case + MCP UnitOfWorkFactory instead of a raw get_db_for_mcp() session.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await VerifyConnectivityClassUseCase().execute(
            VerifyConnectivityClassCommand(board_id, artifact_refs=refs),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.data, default=str)


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

    NUNCA apague o armazenamento persistente do KG para "consertar" —
    destruiria todo o KG do board. Use esta tool em vez disso."""
    if not board_id and not all_boards:
        return json.dumps({"error": "missing_board_or_all_boards"})

    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.kg.interfaces.registry import get_kg_registry

    schema_manager = get_kg_registry().graph_schema_manager

    if all_boards:
        ctx = await _get_global_agent_ctx()
        if ctx is None:
            return _auth_error()
        actor = MCPAdapterContract.actor(ctx)
        actor_scope = ActorScope.from_context(actor)
        perm_err = check_permission(
            actor_scope.permissions, "kg.admin.historical_consolidation"
        )
        if perm_err:
            return _perm_error(perm_err)

        from sqlalchemy import select as _select

        from okto_pulse.core.models.db import Board as _Board

        results: list[dict[str, Any]] = []
        async with get_db_for_mcp() as db:
            rows = await db.execute(_select(_Board.id, _Board.name))
            board_pairs = list(rows.all())
        query_scope = actor_scope.query_scope(
            allowed_board_ids=[bid for bid, _name in board_pairs],
            require_ownership=False,
        )
        for bid, _bname in board_pairs:
            if not query_scope.allows_board_id(bid):
                continue
            try:
                summary = await schema_manager.migrate(bid)
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
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    actor_scope = ActorScope.from_context(actor)
    query_scope = actor_scope.query_scope(
        target_board_id=board_id,
        allowed_board_ids=[board_id],
        require_ownership=False,
    )
    if not query_scope.allows_board_id(board_id):
        return _perm_error("Permission denied: board outside query scope")
    perm_err = check_permission(
        actor_scope.permissions, "kg.admin.historical_consolidation"
    )
    if perm_err:
        return _perm_error(perm_err)
    summary = await schema_manager.migrate(board_id)
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

        async with get_db_for_mcp() as _health_session:
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
            session_scope_factory=get_db_for_mcp,
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
    BoardSourceReader, classifica o estado de saúde do KG e persiste
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

    # Admission gate + FR9 health probe — MCP-FU5 strangler: the single session
    # block is extracted into RebuildAdmissionGateUseCase over the MCP
    # UnitOfWork (admission helper injected so the use case stays Clean Core).
    # The threadpool enumeration / RebuildPreflightService / manifest persistence
    # below are UNCHANGED — no transactional-scope change.
    from okto_pulse.core.application.use_cases import (
        RebuildAdmissionGateCommand,
        RebuildAdmissionGateUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.kg.rebuild_preflight import (
        RebuildHealthSummary,
        RebuildPreflightService,
        RebuildSourceSummary,
    )
    from okto_pulse.core.kg.rebuild_sources import (
        KGRebuildSourceManifest,
        RebuildSourceEnumerator,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        _gate = await RebuildAdmissionGateUseCase().execute(
            RebuildAdmissionGateCommand(
                board_id,
                refuse_fn=_refuse_rebuild_if_quarantined,
                include_health=True,
            ),
            actor=actor,
            uow=uow,
        )
    if _gate.refusal is not None:
        return json.dumps(_gate.refusal)
    _raw_health = _gate.raw_health

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
            canonical_source_count=source_set.canonical_source_count,
            working_source_count=source_set.working_source_count,
            skipped_by_maturity_count=source_set.skipped_by_maturity_count,
            skipped_expired_working_count=(
                source_set.skipped_expired_working_count
            ),
            legacy_unknown_count=source_set.legacy_unknown_count,
            layer_counts=source_set.layer_counts,
            source_partition_counts=source_set.source_partition_counts,
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
        operation      — operação canônica (ex: 'rebuild')
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
        _build_rebuild_step_adapter,
        _build_source_store,
        _provider_missing_payload,
        _refuse_rebuild_if_quarantined,
    )

    # FR8 — admission gate before consuming the token. MCP-FU5 strangler: the
    # single session block is extracted into RebuildAdmissionGateUseCase over the
    # MCP UnitOfWork (admission helper injected → use case stays Clean Core). The
    # token-consumption / KGRebuildService orchestration below is UNCHANGED.
    from okto_pulse.core.application.use_cases import (
        RebuildAdmissionGateCommand,
        RebuildAdmissionGateUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    _gate_actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=_gate_actor) as _gate_uow:
        refusal = (
            await RebuildAdmissionGateUseCase().execute(
                RebuildAdmissionGateCommand(
                    board_id, refuse_fn=_refuse_rebuild_if_quarantined
                ),
                actor=_gate_actor,
                uow=_gate_uow,
            )
        ).refusal
    if refusal is not None:
        return json.dumps(refusal)

    from okto_pulse.core.kg.interfaces import get_kg_registry
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
    from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock

    lock = KGSingleWriterLock(base_dir=_REBUILD_BASE_DIR / "locks")

    def _always_owner(bid: str, owner_token: str) -> bool:
        m = lock.inspect(board_id=bid)
        return m is not None and m.owner_token == owner_token

    safe_lifecycle = KGSafeWriteLifecycle(
        step_adapter=get_kg_registry().safe_write_step_adapter,
        owner_probe=LockOwnerProbe(is_active_owner=_always_owner),
        health_probe=HealthProbe(classify=lambda b, g, status, step: "at_risk"),
    )

    source_store_fetch = _build_source_store()
    enumerator = RebuildSourceEnumerator(source_store=source_store_fetch)
    manifest_store_obj = KGRebuildSourceManifest(base_dir=_REBUILD_BASE_DIR)
    try:
        _step_adapter_with_sources = _build_rebuild_step_adapter(
            manifest_store_obj=manifest_store_obj,
        )
    except Exception as exc:
        from okto_pulse.core.composition import RuntimeProviderMissing

        if isinstance(exc, RuntimeProviderMissing):
            return json.dumps(_provider_missing_payload(exc))
        raise

    audit_recorder = ConfirmationConsumptionAuditRecorder(base_dir=_REBUILD_BASE_DIR)
    event_publisher = KGRebuiltEventPublisher(base_dir=_REBUILD_BASE_DIR)
    cognitive_marker = CognitivePendingMarker(base_dir=_REBUILD_BASE_DIR)

    def _source_resolver(event_payload):
        m = manifest_store_obj.load(event_payload.get("manifest_ref", ""))
        if m is None:
            return ()
        return tuple(row.to_dict() for row in m.materializable_sources)

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

    # Required-filter checks stay in the adapter (Codex: validate before the call
    # when simple) → legacy _structured_error envelopes preserved exactly.
    if entity_type == "refinement" and not filters.get("ideation_id"):
        return _structured_error(
            "missing_required_filter",
            ["ideation_id"],
            None,
            "entity_type='refinement' requires filters.ideation_id",
        )
    if entity_type == "sprint" and not filters.get("spec_id"):
        return _structured_error(
            "missing_required_filter",
            ["spec_id"],
            None,
            "entity_type='sprint' requires filters.spec_id to identify the parent spec",
        )

    # story/topic bool-arg computation uses server-local transport helpers — keep
    # it in the adapter and pass the pre-computed kwargs into the use case.
    story_args = None
    topic_args = None
    if entity_type == "story":
        def _optional_bool_filter(value: Any) -> bool | None:
            if value is None or value == "":
                return None
            if isinstance(value, bool):
                return value
            return _flag_enabled(str(value))

        story_args = {
            "status_filter": filters.get("status") or None,
            "topic_id": filters.get("topic_id") or None,
            "linked": _optional_bool_filter(filters.get("linked")),
            "converted": _optional_bool_filter(filters.get("converted")),
            "include_archived": _flag_enabled(
                str(filters.get("include_archived", "false"))
            ),
        }
    elif entity_type == "topic":
        topic_args = {
            "include_archived": _flag_enabled(
                str(filters.get("include_archived", "false"))
            ),
        }

    from okto_pulse.core.application.use_cases import (
        McpListByBoardCommand,
        McpListByBoardUseCase,
    )
    from okto_pulse.core.application.use_cases.mcp_board_crud import (
        is_derivation_pending_ideation,
        is_derivation_pending_refinement,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # MCP-FU6 strangler: the per-entity_type fetch + pure-data post-filters move
    # into McpListByBoardUseCase over the MCP UoW; the adapter keeps the validation
    # (above), the required-filter checks + story/topic helper args (above),
    # pagination and the per-type JSON shaping (below). Tool no longer opens
    # get_db_for_mcp nor builds the entity services.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        items = (
            await McpListByBoardUseCase().execute(
                McpListByBoardCommand(
                    board_id,
                    entity_type,
                    filters,
                    story_args=story_args,
                    topic_args=topic_args,
                ),
                actor=actor,
                uow=uow,
            )
        ).data

        if entity_type == "spec":
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
                        "active_refinement_count": getattr(i, "active_refinement_count", 0),
                        "derivation_pending": is_derivation_pending_ideation(i),
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
                        "active_spec_count": getattr(r, "active_spec_count", 0),
                        "derivation_pending": is_derivation_pending_refinement(r),
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
            total = len(items)
            paginated = items[offset:offset + limit]
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


def build_mcp_asgi_app(trace_sink: McpTraceSink | None = None):
    """Build the MCP ASGI application wrapped with the API-key middleware.

    Returns the ASGI app that should be served by uvicorn (or mounted
    elsewhere). Single-process callers (``okto_pulse.community.main.serve``)
    use this to bind the MCP transport to its own port while sharing the
    same Python process as the API server, so the graph-store lock is held by a
    single process. The caller is responsible for invoking
    ``register_session_factory`` once before the first MCP request lands.

    The core factory never resolves local trace paths. When ``trace_sink`` is
    ``None`` no trace middleware is installed; editions that want replay traces
    must inject a concrete sink from their composition root.
    """
    _install_trace(mcp, trace_sink)
    http_app = mcp.http_app(transport="streamable-http")
    return ApiKeySessionMiddleware(http_app)


def mount_mcp(
    app,
    *,
    mount_path: str = "/mcp",
    trace_sink: McpTraceSink | None = None,
) -> None:
    """Mount the MCP sub-app at ``mount_path`` on a FastAPI/Starlette app.

    Kept for callers that prefer path-based routing on the same port as the
    API. The default deployment path (``okto_pulse.community.main.serve``)
    serves the MCP on its own port via :func:`build_mcp_asgi_app`. Tracing is
    disabled unless the caller injects a sink.
    """
    app.mount(mount_path, build_mcp_asgi_app(trace_sink=trace_sink))


def run_mcp_server():
    """Run the MCP server standalone (compat shim for debug / legacy).

    Production path is :func:`okto_pulse.community.main.serve`, which runs
    the API server and the MCP server in the same Python process on
    separate ports. This function is preserved for stand-alone debug runs
    (``python -m okto_pulse.core.mcp.server``) only.

    R01B REPLAN-IMP2 (TR5): this standalone shim does NOT register a Community
    SQLite PRAGMA installer, so ``create_database`` below resolves the EXPLICIT
    core-default fallback (the three historical PRAGMAs: WAL + busy_timeout=30000
    + synchronous=NORMAL, no foreign_keys). That is the documented core-only /
    transitional path. The production MCP listener (``community.main.serve``) does
    NOT call ``create_database`` again — it shares the SAME engine built by
    ``create_app``, which was hardened with the Community UNION installer (adds
    foreign_keys=ON) — so the production MCP path inherits the edition registration.

    This core-only standalone path also does not inject a trace sink; local
    MCP replay JSONL is enabled by the Community composition root.
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

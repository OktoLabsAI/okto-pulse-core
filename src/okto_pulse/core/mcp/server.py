"""MCP Server for Okto Pulse Core - enables AI agents to interact with the board."""

import asyncio
import base64
import binascii
import functools
import inspect
import json
import logging
import os
import re
import warnings
import uuid as _uuid
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import datetime
from importlib.resources import files as package_files
from typing import Annotated, Any, Callable

from pydantic import Field

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    reset_runtime_values,
    resolve_runtime_value,
)

from okto_pulse.core.application.scope import ActorScope
from okto_pulse.core.infra.config import get_mcp_settings, get_settings
from okto_pulse.core.infra.permissions import Permissions, check_permission
from okto_pulse.core.mcp.catalog import CoreMcpCatalog
from okto_pulse.core.mcp.helpers import (
    _structured_error,
    coerce_to_list_str,
    parse_multi_value,
    parse_options_json,
)
from okto_pulse.core.ports.content_ingestion import ContentIngestionError
from okto_pulse.core.ports.mcp_instructions import (
    McpInstructionProvider,
    StaticMcpInstructionProvider,
)
from okto_pulse.core.ports.mcp_trace import McpTraceSink
from okto_pulse.core.ports.mcp_host import (
    McpHostProviderMissing,
    get_mcp_host_provider,
)
from okto_pulse.core.ports.mcp_auth import (
    AuthSession,
    MCP_CREDENTIAL_SCOPE_KEY,
    McpAuthError,
    McpAuthenticator,
    principal_from_auth_session,
    require_authenticator,
)
from okto_pulse.core.models.schemas import (
    ArchitectureDesignCreate,
    ArchitectureDesignUpdate,
)
from okto_pulse.core.services.activity_log import (
    activity_log_summary,
)
from okto_pulse.core.services.architecture import (
    ArchitectureDesignRepository,
    ArchitecturePropagationBlocked,
    ArchitectureDiagramAdapterRegistry,
    ArchitectureWarningAcknowledgementRequired,
    CARD_ARCHITECTURE_READ_ONLY_MESSAGE,
    architecture_design_payload_schema,
)
from okto_pulse.core.services.cancellation import CancellationReasonRequiredError
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
    BoardService,
    CARD_RESOURCE_READ_ONLY_MESSAGE,
    CardOperationError,
    CardService,
    IdeationService,
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
    story_state,
)


def _trs_to_objects(trs: list[str] | None) -> list | None:
    """Convert TR strings to objects with IDs for task linkage traceability."""
    if not trs:
        return None
    return [
        {"id": f"tr_{_uuid.uuid4().hex[:8]}", "text": tr, "linked_task_ids": []}
        if isinstance(tr, str)
        else tr
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


def _load_bundled_text(relative_path: str) -> str:
    """Read immutable text packaged with Core, independent of runtime paths."""

    try:
        resource = package_files("okto_pulse.core.mcp")
        for part in relative_path.split("/"):
            resource = resource.joinpath(part)
        return resource.read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return ""


_core_instruction_provider = StaticMcpInstructionProvider(
    provider_id="core",
    content=_load_bundled_text("agent_instructions.md"),
)
_INSTRUCTION_PROVIDERS_KEY = "mcp.instruction_providers"
_INSTRUCTION_FROZEN_KEY = "mcp.instruction_providers_frozen"


def _instruction_providers() -> tuple[McpInstructionProvider, ...]:
    return resolve_runtime_value(_INSTRUCTION_PROVIDERS_KEY) or (
        _core_instruction_provider,
    )


def _instruction_providers_frozen() -> bool:
    return bool(resolve_runtime_value(_INSTRUCTION_FROZEN_KEY))


def _refresh_catalog_instructions() -> None:
    mcp.instructions = _load_instructions()


def _load_instructions() -> str:
    """Load command-catalog instructions through registered edition providers."""
    for provider in _instruction_providers():
        text = provider.load_instructions()
        if text:
            return text
    return ""


def register_instruction_provider(provider: McpInstructionProvider) -> None:
    """Register an edition-owned instruction provider before MCP startup freezes."""
    if _instruction_providers_frozen():
        raise RuntimeError(
            "MCP instruction providers are FROZEN after composition; late "
            "registration/mutation is forbidden."
        )
    register_runtime_value(
        _INSTRUCTION_PROVIDERS_KEY,
        (provider, *_instruction_providers()),
    )
    _refresh_catalog_instructions()


def freeze_instruction_providers() -> None:
    """Freeze MCP instruction provider registration after composition."""
    register_runtime_value(_INSTRUCTION_FROZEN_KEY, True)
    _refresh_catalog_instructions()


def has_instruction_provider(provider_id: str) -> bool:
    """Return whether an instruction provider id is already registered."""
    return any(
        provider.provider_id == provider_id for provider in _instruction_providers()
    )


def reset_instruction_providers_for_tests() -> None:
    """Tests only: restore the core bundled instruction provider."""
    reset_runtime_values(_INSTRUCTION_PROVIDERS_KEY, _INSTRUCTION_FROZEN_KEY)
    _refresh_catalog_instructions()


# Initialize the transport-neutral command catalog.
mcp = CoreMcpCatalog(
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


def _load_resource_file(relative_path: str) -> str:
    """Load an immutable bundled MCP resource by package-relative name."""

    return _load_bundled_text(f"resources/{relative_path}")


_CORE_RESOURCE_TABLE = [
    (
        "okto-pulse://workflows/stories",
        "workflows/stories.md",
        "Stories & Topics workflow — pre-ideation intake.",
    ),
    (
        "okto-pulse://workflows/ideations",
        "workflows/ideations.md",
        "Ideations workflow — scope + ambiguity-killer.",
    ),
    (
        "okto-pulse://workflows/refinements",
        "workflows/refinements.md",
        "Refinements workflow — deep investigation.",
    ),
    (
        "okto-pulse://workflows/specs",
        "workflows/specs.md",
        "Specs workflow — saturation, gate, evaluation.",
    ),
    (
        "okto-pulse://workflows/cards",
        "workflows/cards.md",
        "Cards workflow — impl/bug/test execution.",
    ),
    (
        "okto-pulse://workflows/sprints",
        "workflows/sprints.md",
        "Sprints workflow — lifecycle e evaluation.",
    ),
    (
        "okto-pulse://workflows/kg",
        "workflows/kg.md",
        "KG workflow — consolidation, query, governance.",
    ),
    (
        "okto-pulse://workflows/preflight",
        "workflows/preflight.md",
        "Pre-Flight Checklist — session/entity/card/resource-gate pre-flight sequences (READ FIRST).",
    ),
    (
        "okto-pulse://reference/errors",
        "reference/errors.md",
        "MCP errors matrix com fixes canônicos.",
    ),
    (
        "okto-pulse://reference/multivalue",
        "reference/multivalue.md",
        "Multi-value parameter input shapes.",
    ),
    (
        "okto-pulse://reference/destructive_ops",
        "reference/destructive_ops.md",
        "Destructive operations governance.",
    ),
    (
        "okto-pulse://reference/card_types",
        "reference/card_types.md",
        "Card types — normal/test/bug rules.",
    ),
    (
        "okto-pulse://reference/spec_gates",
        "reference/spec_gates.md",
        "Spec validation gate + evaluation gates.",
    ),
    (
        "okto-pulse://reference/transitions",
        "reference/transitions.md",
        "Status transitions matrix — cards/sprints/specs.",
    ),
    (
        "okto-pulse://reference/list_tools",
        "reference/list_tools.md",
        "Consolidated polymorphic list_* tools.",
    ),
    (
        "okto-pulse://reference/tools_catalog",
        "reference/tools_catalog.md",
        "Full MCP tool catalog grouped by domain.",
    ),
    (
        "okto-pulse://reference/projection-profiles",
        "reference/projection_profiles.md",
        "Projection profiles (summary/detail/full/legacy) + response envelope (SC1).",
    ),
    (
        "okto-pulse://reference/kg-health",
        "reference/kg-health.md",
        "Full KG health contract: payload fields, when to consult, must-not-do.",
    ),
    # R1.1 — lazy long-form tool docs (args/returns/examples) moved off the
    # compact tools/list surface; one resource per tool family (api_fd7c5878).
    (
        "okto-pulse://reference/tool-docs/activity",
        "reference/tool-docs/activity.md",
        "Full long-form docs for activity tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/agent",
        "reference/tool-docs/agent.md",
        "Full long-form docs for agent tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/analytics",
        "reference/tool-docs/analytics.md",
        "Full long-form docs for analytics tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/api-contract",
        "reference/tool-docs/api-contract.md",
        "Full long-form docs for api-contract tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/architecture",
        "reference/tool-docs/architecture.md",
        "Full long-form docs for architecture tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/attachment",
        "reference/tool-docs/attachment.md",
        "Full long-form docs for attachment tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/board",
        "reference/tool-docs/board.md",
        "Full long-form docs for board tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/business-rule",
        "reference/tool-docs/business-rule.md",
        "Full long-form docs for business-rule tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/card",
        "reference/tool-docs/card.md",
        "Full long-form docs for card tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/comment",
        "reference/tool-docs/comment.md",
        "Full long-form docs for comment tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/decision",
        "reference/tool-docs/decision.md",
        "Full long-form docs for decision tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/guideline",
        "reference/tool-docs/guideline.md",
        "Full long-form docs for guideline tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/ideation",
        "reference/tool-docs/ideation.md",
        "Full long-form docs for ideation tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/integration-requirement",
        "reference/tool-docs/integration-requirement.md",
        "Full long-form docs for integration-requirement tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/kg",
        "reference/tool-docs/kg.md",
        "Full long-form docs for kg tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/knowledge",
        "reference/tool-docs/knowledge.md",
        "Full long-form docs for knowledge tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/misc",
        "reference/tool-docs/misc.md",
        "Full long-form docs for misc tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/mockup",
        "reference/tool-docs/mockup.md",
        "Full long-form docs for mockup tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/observability-requirement",
        "reference/tool-docs/observability-requirement.md",
        "Full long-form docs for observability-requirement tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/qa",
        "reference/tool-docs/qa.md",
        "Full long-form docs for qa tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/refinement",
        "reference/tool-docs/refinement.md",
        "Full long-form docs for refinement tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/snapshot",
        "reference/tool-docs/snapshot.md",
        "Full long-form docs for snapshot tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/spec",
        "reference/tool-docs/spec.md",
        "Full long-form docs for spec tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/story",
        "reference/tool-docs/story.md",
        "Full long-form docs for story tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/sprint",
        "reference/tool-docs/sprint.md",
        "Full long-form docs for sprint tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/test-scenario",
        "reference/tool-docs/test-scenario.md",
        "Full long-form docs for test-scenario tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/topic",
        "reference/tool-docs/topic.md",
        "Full long-form docs for topic tools (args/returns/examples).",
    ),
    (
        "okto-pulse://reference/tool-docs/traceability",
        "reference/tool-docs/traceability.md",
        "Full long-form docs for traceability tools (args/returns/examples).",
    ),
    # R4.2 — lazy tool-family consolidation/migration docs (fr_589a9977 / ir_a1db20f3).
    # Compact tool descriptions point here instead of embedding migration prose.
    (
        "okto-pulse://reference/tool-families/spec_entity_remove",
        "reference/tool-families/spec_entity_remove.md",
        "R4 consolidated spec-entity removal: target_types, aliases, soft-delete asymmetry.",
    ),
    (
        "okto-pulse://reference/tool-families/qa_ask",
        "reference/tool-families/qa_ask.md",
        "R4 consolidated Q&A ask: target_types, aliases, sprint asymmetry.",
    ),
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
    rest = uri[len("okto-pulse://") :]
    parts = rest.split("/")
    if (
        parts
        and parts[0] == "reference"
        and len(parts) >= 2
        and parts[1]
        in (
            "tool-docs",
            "tool-families",
        )
    ):
        return f"reference/{parts[1]}"
    return parts[0] if parts and parts[0] else "misc"


def _build_core_resource_catalog() -> StaticMcpResourceCatalog:
    """The CORE edition catalog, built from ``_CORE_RESOURCE_TABLE`` (path-loaders
    confined to the core resources dir)."""
    specs = tuple(
        McpResourceSpec(
            uri=uri,
            path=path,
            loader=functools.partial(_load_resource_file, path),
            description=desc,
            category=_resource_category_for(uri),
            edition="core",
        )
        for uri, path, desc in _CORE_RESOURCE_TABLE
    )
    return StaticMcpResourceCatalog("core", specs)


_RESOURCE_CATALOG_KEY = "mcp.resource_catalog"
_RESOURCE_CATALOG_FROZEN_KEY = "mcp.resource_catalog_frozen"


#: (R11-A) READ-ONLY transitional PROJECTION of the EFFECTIVE catalog as the legacy
#: ``(uri, path, description)`` tuples. An IMMUTABLE tuple (NOT a mutable list) —
#: it is DERIVED from ``effective_resource_catalog()`` and ATOMICALLY REASSIGNED on
#: every injection/reset, so a consumer cannot mutate the public projection (or the
#: catalog) by accident. NOT an authority / extension point; kept only so existing
#: consumers keep working during register-before-remove.
def _resource_catalog() -> CompositeMcpResourceCatalog:
    catalog = resolve_runtime_value(_RESOURCE_CATALOG_KEY)
    if catalog is None:
        catalog = CompositeMcpResourceCatalog([_build_core_resource_catalog()])
        register_runtime_value(_RESOURCE_CATALOG_KEY, catalog)
    return catalog


def _resource_catalog_frozen() -> bool:
    return bool(resolve_runtime_value(_RESOURCE_CATALOG_FROZEN_KEY))


def effective_resource_catalog() -> CompositeMcpResourceCatalog:
    """The AUTHORITATIVE effective MCP resource catalog (core + injected)."""
    return _resource_catalog()


def _projection_path(spec: McpResourceSpec) -> str:
    return spec.path if spec.path is not None else f"<content:{spec.uri}>"


def resource_registry_projection() -> tuple[tuple[str, str, str], ...]:
    """Return the immutable legacy projection of the effective catalog."""

    return tuple(
        (s.uri, _projection_path(s), s.description) for s in _resource_catalog().specs()
    )


def _make_resource_handler(spec: McpResourceSpec) -> "Callable[[], str]":
    """Closure-safe resources/read handler bound to a catalog spec (the spec's
    deterministic loader; never exposes a filesystem path to the agent)."""

    def handler() -> str:
        return spec.read()

    return handler


def _register_resource_spec(spec: McpResourceSpec) -> None:
    handler = _make_resource_handler(spec)
    handler.__name__ = "resource_" + spec.uri[len("okto-pulse://") :].replace(
        "/", "_"
    ).replace("-", "_")
    handler.__doc__ = spec.description
    mcp.resource(spec.uri, description=spec.description)(handler)


def register_resource_catalog(catalog: McpResourceCatalog) -> None:
    """(R11-A IMP2/IMP3) Composition-root injection of an additional edition
    catalog (e.g. the Community operational catalog), using the core CONTRACTS so
    the core never imports community. FAIL-CLOSED: raises after the freeze."""
    if _resource_catalog_frozen():
        raise RuntimeError(
            "MCP resource catalog is FROZEN after composition; late "
            "registration/mutation is forbidden (R11-A IMP4 fail-closed freeze)."
        )
    current = _resource_catalog()
    existing = {s.uri for s in current.specs()}
    register_runtime_value(_RESOURCE_CATALOG_KEY, current.with_catalog(catalog))
    for spec in catalog.specs():
        if (
            spec.uri not in existing
        ):  # first-wins dedupe; conflicts reported, not re-registered
            _register_resource_spec(spec)


def freeze_resource_catalog() -> None:
    """(R11-A IMP4) Freeze the effective catalog AFTER composition (all providers
    registered) + prewarm every spec. Idempotent; later registration RAISES."""
    register_runtime_value(_RESOURCE_CATALOG_FROZEN_KEY, True)
    for spec in _resource_catalog().specs():
        spec.read()


def reset_resource_catalog_for_tests() -> None:
    """Tests only: rebuild the core-only effective catalog, clear the freeze, AND
    drop any catalog resource handlers registered beyond the core baseline so a
    previously-injected catalog leaves NO residual state (isolation)."""
    reset_instruction_providers_for_tests()
    register_runtime_value(
        _RESOURCE_CATALOG_KEY,
        CompositeMcpResourceCatalog([_build_core_resource_catalog()]),
    )
    reset_runtime_values(_RESOURCE_CATALOG_FROZEN_KEY)
    resources = mcp._resource_manager._resources
    for _uri in list(resources):
        if _uri not in _CORE_MCP_RESOURCE_URIS:
            del resources[_uri]


# Register the Core resources with the command catalog and build the initial
# projection at import.
for _spec in _resource_catalog().specs():
    _register_resource_spec(_spec)
# Pre-warm so first resources/read latency is minimal.
for _spec in _resource_catalog().specs():
    _spec.read()

#: Core catalog resource URIs registered at import — the baseline a
#: test reset restores to (any injected-catalog handler beyond this is dropped).
_CORE_MCP_RESOURCE_URIS = frozenset(mcp._resource_manager._resources.keys())


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

# Compatibility alias retained for callers that imported the old server symbol.
_MCP_CREDENTIAL_SCOPE_KEY = MCP_CREDENTIAL_SCOPE_KEY


def ApiKeySessionMiddleware(app):
    """Compatibility factory for the edition-owned MCP credential middleware."""

    return get_mcp_host_provider().wrap_session_middleware(app)


# ----------------------------------------------------------------------------
# R08-A: transport -> McpCredential conversion shims (tr_7d105709).
#
# Community's MCP host converts inbound HTTP data into the pure
# ``McpCredential`` DTO and attaches it to this scope key. No Okto-owned
# ContextVar or process-global credential carrier participates in request
# authentication.
# ----------------------------------------------------------------------------
def extract_mcp_credential_from_request(request):
    """Build an ``McpCredential`` from an HTTP request-like object.

    The Community MCP host owns the concrete Starlette middleware and uses the
    same precedence: query parameter, ``X-API-Key``, then Bearer header.
    """
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
    """Resolve a request credential through the composed edition host.

    The HTTP request context belongs to the host adapter.  Core only consumes
    its already-extracted credential and fails closed outside a composed
    edition runtime.
    """
    try:
        return get_mcp_host_provider().active_credential()
    except McpHostProviderMissing:
        return None


# ============================================================================
# AUTH HELPERS (tools call these instead of passing api_key)
# ============================================================================


# Session factory registration for MCP server
_MCP_SESSION_FACTORY_KEY = "mcp.session_factory"
_MCP_AUTHENTICATOR_KEY = "mcp.authenticator"


class _McpSessionFactoryRuntime:
    """Callable MCP session factory plus edition runtime ports.

    The legacy MCP surface has one process-level composition hook,
    ``_mcp_session_factory``. Keep that single hook callable for all existing
    tools/tests while allowing request-less MCP tools to use edition-owned ports
    that REST resolves from ``app.state.runtime_composition``.
    """

    def __init__(self, session_factory, *, scheduler_control=None):
        self.session_factory = session_factory
        self.scheduler_control = scheduler_control

    def __call__(self, *args, **kwargs):
        return self.session_factory(*args, **kwargs)


class _PortBackedSessionFactoryAuthenticator:
    """Compatibility authenticator backed by the composed relational port.

    Productive Community composition supplies its own ``McpAuthenticator``.
    This adapter only preserves legacy ``register_session_factory(factory)``
    callers while keeping credential lookup behind the edition-owned relational
    application adapter rather than importing ORM models into the MCP catalog.
    """

    def __init__(self, session_factory) -> None:
        self._session_factory = session_factory

    async def authenticate(self, credential):
        if credential is None or not getattr(credential, "value", None):
            return None
        from okto_pulse.core.ports.relational_application import (
            require_relational_application_adapter,
        )

        async with self._session_factory() as session:
            gateway = require_relational_application_adapter().agent_authentication(
                session
            )
            return await gateway.authenticate_agent_by_api_key(
                credential.value,
                credential_source=str(getattr(credential, "source", "mcp")),
            )


def register_session_factory(
    factory,
    *,
    scheduler_control=None,
    mcp_authenticator: McpAuthenticator | None = None,
):
    """Register the MCP session factory and optional edition runtime ports."""
    register_runtime_value(
        _MCP_SESSION_FACTORY_KEY,
        _McpSessionFactoryRuntime(factory, scheduler_control=scheduler_control),
    )
    register_runtime_value(
        _MCP_AUTHENTICATOR_KEY,
        mcp_authenticator
        if mcp_authenticator is not None
        else _PortBackedSessionFactoryAuthenticator(factory),
    )


def register_mcp_authenticator(authenticator: McpAuthenticator | None) -> None:
    """Register the edition-owned MCP authenticator port."""
    if authenticator is None:
        reset_runtime_values(_MCP_AUTHENTICATOR_KEY)
    else:
        register_runtime_value(_MCP_AUTHENTICATOR_KEY, authenticator)


def get_mcp_authenticator_for_mcp() -> McpAuthenticator:
    """Return the registered MCP authenticator, failing closed when absent."""
    return require_authenticator(resolve_runtime_value(_MCP_AUTHENTICATOR_KEY))


def get_scheduler_control_for_mcp():
    """Return the edition-owned scheduler port for request-less MCP tools."""
    session_factory = resolve_runtime_value(_MCP_SESSION_FACTORY_KEY)
    if session_factory is None:
        return None
    return getattr(session_factory, "scheduler_control", None)


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
    session = await _authenticate_mcp_session(credential)
    if session is None:
        return None
    return _agent_from_auth_session(session)


async def _authenticate_mcp_session(credential) -> AuthSession | None:
    """Authenticate a request-scoped credential through the registered port."""
    if credential is None:
        return None
    try:
        authenticator = get_mcp_authenticator_for_mcp()
    except McpAuthError:
        return None
    session = await authenticator.authenticate(credential)
    if session is None or not bool(getattr(session, "is_active", False)):
        return None
    return session


class _AuthenticatedMcpAgent:
    """Secret-free compatibility shape for legacy MCP helper consumers."""

    def __init__(self, session: AuthSession) -> None:
        self.id = session.agent_id
        self.name = session.agent_name
        self.is_active = session.is_active
        self.api_key = "<redacted>"
        self.api_key_hash = None
        self.permissions = None
        self.metadata = dict(getattr(session, "metadata", {}) or {})

    def __repr__(self) -> str:
        return (
            f"_AuthenticatedMcpAgent(id={self.id!r}, name={self.name!r}, "
            f"is_active={self.is_active!r}, api_key='<redacted>')"
        )


def _agent_from_auth_session(session: AuthSession) -> _AuthenticatedMcpAgent:
    return _AuthenticatedMcpAgent(session)


async def _get_authenticated_agent():
    """Get the agent authenticated via the current request-scoped MCP key."""
    return await _authenticate_mcp_credential(active_api_key_credential())


async def _get_agent_ctx_for_credential(
    board_id: str, credential
) -> AgentContext | None:
    """Authenticate a provided MCP credential and verify board access.

    Resolves granular PermissionSet (agent_flags ∩ board_overrides) with 60s
    cache. Falls back to legacy flat permissions if permission_flags is not set.
    """
    auth_session = await _authenticate_mcp_session(credential)
    if auth_session is None:
        return None

    from okto_pulse.core.application.use_cases.base import (
        actor_context_from_principal,
    )

    principal = principal_from_auth_session(auth_session)
    if principal is None:
        return None
    actor = actor_context_from_principal(
        principal,
        source="mcp",
        board_id=board_id,
    )
    async with AsyncExitStack() as stack:
        uow = await stack.enter_async_context(
            get_unit_of_work_factory_for_mcp()(actor=actor)
        )
        auth_gateway = uow.services.agent_authentication
        if not await auth_gateway.agent_has_board_access(
            auth_session.agent_id,
            board_id,
        ):
            await uow.commit()
            return None

        cached = _cache_get(auth_session.agent_id, board_id)
        if cached:
            await uow.commit()
            return cached

        resolved = await auth_gateway.resolve_agent_permission_context(
            auth_session.agent_id,
            board_id=board_id,
        )
        if resolved is None:
            await uow.commit()
            return None
        await uow.commit()
        ctx = AgentContext(
            agent_id=resolved.agent_id,
            agent_name=resolved.agent_name,
            board_id=board_id,
            permissions=resolved.permissions,
        )
        _cache_set(auth_session.agent_id, board_id, ctx)
        return ctx


async def _get_agent_ctx(board_id: str) -> AgentContext | None:
    """Authenticate agent from the current request-scoped MCP key."""
    return await _get_agent_ctx_for_credential(board_id, active_api_key_credential())


async def _get_global_agent_ctx() -> AgentContext | None:
    """Authenticate an MCP agent without granting implicit all-board scope."""
    credential = active_api_key_credential()
    auth_session = await _authenticate_mcp_session(credential)
    if auth_session is None:
        return None

    from okto_pulse.core.application.use_cases.base import actor_context_from_principal

    principal = principal_from_auth_session(auth_session)
    if principal is None:
        return None
    actor = actor_context_from_principal(principal, source="mcp", board_id="")
    async with AsyncExitStack() as stack:
        uow = await stack.enter_async_context(
            get_unit_of_work_factory_for_mcp()(actor=actor)
        )
        resolved = (
            await uow.services.agent_authentication.resolve_agent_permission_context(
                auth_session.agent_id,
                board_id=None,
            )
        )
        if resolved is None:
            await uow.commit()
            return None
        await uow.commit()
        return AgentContext(
            agent_id=resolved.agent_id,
            agent_name=resolved.agent_name,
            board_id="",
            permissions=resolved.permissions,
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
    *,
    board_id: str,
    blocked_tool: str,
    blocked_action: str,
    target_ref: str | None = None,
) -> str:
    """R5-IMP5 — single agent-boundary choke point for a human-only skip/no_action
    refusal. Emits the bounded-label rejection counter, then returns the read-only
    ``human_control_required`` envelope (gate_contracts stays pure). Fail-closed: no
    DB / ledger / skip_ambiguity_gate mutation — the metric is in-process only."""
    emit_human_control_required(
        board_id=board_id,
        blocked_tool=blocked_tool,
        blocked_action=blocked_action,
    )
    return json.dumps(
        human_control_required_envelope(
            blocked_tool=blocked_tool,
            blocked_action=blocked_action,
            target_ref=target_ref,
        )
    )


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


def _serialize_knowledge_base(
    kb: Any, *, include_content: bool = True
) -> dict[str, Any]:
    """Serialize refinement/spec/ideation KB rows without assuming shape drift."""
    if isinstance(kb, dict):
        data = {
            "id": kb.get("id"),
            "title": kb.get("title") or kb.get("name"),
            "description": kb.get("description"),
            "mime_type": kb.get("mime_type")
            or kb.get("content_type")
            or "text/markdown",
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
        "cards_total": coverage.get("cards_total", len(cards)),
        "cards_done": coverage.get("cards_done", 0),
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


def _mcp_architecture_conflict_error(exc: Any) -> str:
    if getattr(exc, "entity_type", "") == "card_architecture_readonly":
        return json.dumps({"error": CARD_ARCHITECTURE_READ_ONLY_MESSAGE})
    return json.dumps(
        {
            "error": (
                "Spec is locked because validation passed. Move it back to "
                "draft or approved to edit architecture."
            )
        }
    )


def _mcp_entity_not_found_error(exc: Any) -> str:
    return json.dumps({"error": f"{exc.entity_type} not found"})


async def _mcp_architecture_for_parent(
    services,
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

    repo = services.architecture_designs
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
        return (
            None,
            "content_reference requires a registered content ingestion resolver",
        )
    try:
        resolved = await resolver.resolve_text(
            content_reference, max_bytes=_MAX_CONTENT_BYTES
        )
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
        return (
            None,
            "Only one of 'content_base64' or 'content_reference' may be provided",
        )

    if content_base64:
        try:
            decoded = base64.b64decode(content_base64, validate=True)
        except (binascii.Error, ValueError) as e:
            return None, f"Invalid base64 content: {e}"
        if len(decoded) > _MAX_CONTENT_BYTES:
            return (
                None,
                f"content_base64 exceeds {_MAX_CONTENT_BYTES} bytes ({len(decoded)})",
            )
        return decoded, None

    from okto_pulse.core.runtime_registry import resolve_content_ingestion_resolver

    resolver = resolve_content_ingestion_resolver()
    if resolver is None:
        return (
            None,
            "content_reference requires a registered content ingestion resolver",
        )
    try:
        resolved = await resolver.resolve_binary(
            content_reference, max_bytes=_MAX_CONTENT_BYTES
        )
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


_XML_SAFETY_COUNT_KEY = "mcp.xml_safety_decorated_count"


def _increment_xml_safety_decorated_count() -> None:
    count = int(resolve_runtime_value(_XML_SAFETY_COUNT_KEY) or 0)
    register_runtime_value(_XML_SAFETY_COUNT_KEY, count + 1)


def __getattr__(name: str) -> Any:
    """Expose read-only legacy diagnostics without module-global authorities."""

    if name == "_RESOURCE_REGISTRY":
        return resource_registry_projection()
    if name == "_XML_SAFETY_DECORATED_COUNT":
        return int(resolve_runtime_value(_XML_SAFETY_COUNT_KEY) or 0)
    if name == "_mcp_session_factory":
        return resolve_runtime_value(_MCP_SESSION_FACTORY_KEY)
    if name == "_mcp_authenticator":
        return resolve_runtime_value(_MCP_AUTHENTICATOR_KEY)
    raise AttributeError(name)


def _patch_mcp_tool_for_xml_safety() -> None:
    """Wrap every registered command while preserving its public signature.

    The catalog supports both direct and decorator registration. Passing the
    wrapped function through its direct-registration path ensures the module
    receives a catalog tool whose ``.fn`` retains the original signature via
    ``functools.wraps``.
    """
    if getattr(mcp.tool, "_xml_safety_patched", False):
        return

    _original_mcp_tool = mcp.tool

    def _patched_mcp_tool(*args, **kwargs):
        # ``@mcp.tool`` (no parens) — first positional arg is the function.
        if args and inspect.isroutine(args[0]):
            func = args[0]
            wrapped = _xml_safety_log_decorator(func)
            _increment_xml_safety_decorated_count()
            return _original_mcp_tool(wrapped, *args[1:], **kwargs)

        # ``@mcp.tool()`` / ``@mcp.tool("name")`` / ``@mcp.tool(name=...)`` —
        # route decoration through the same direct-registration path.
        def _wrap(func):
            wrapped = _xml_safety_log_decorator(func)
            _increment_xml_safety_decorated_count()
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

    from okto_pulse.core.application.use_cases import ActorContext
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpUpdateMyProfileCommand,
        McpUpdateMyProfileUseCase,
    )

    actor = ActorContext(
        agent.id,
        "mcp",
        actor_name=agent.name,
        permissions=agent.permissions,
    )
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        try:
            result = await McpUpdateMyProfileUseCase().execute(
                McpUpdateMyProfileCommand(
                    description=description,
                    objective=objective,
                ),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            return json.dumps({"error": "Agent not found"})

        updated = result.agent
        return json.dumps(
            {
                "success": True,
                "profile": {
                    "id": updated.id,
                    "name": updated.name,
                    "description": updated.description,
                    "objective": updated.objective,
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

    from okto_pulse.core.application.use_cases import ActorContext
    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpListMyBoardsCommand,
        McpListMyBoardsUseCase,
    )

    actor = ActorContext(
        agent.id,
        "mcp",
        actor_name=agent.name,
        permissions=agent.permissions,
    )
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListMyBoardsUseCase().execute(
            McpListMyBoardsCommand(),
            actor=actor,
            uow=uow,
        )
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
                    for b in result.boards
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
async def okto_pulse_list_my_mentions(
    board_id: str, include_seen: str = "false"
) -> str:
    """
    List comments and Q&A items where you are mentioned via @name.
    By default only returns UNSEEN mentions. Use include_seen="true" to get all."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpListMyMentionsCommand,
        McpListMyMentionsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListMyMentionsUseCase().execute(
            McpListMyMentionsCommand(
                board_id,
                include_seen=include_seen.lower() == "true",
            ),
            actor=actor,
            uow=uow,
        )

        return json.dumps(
            {
                "agent_name": ctx.agent_name,
                "unseen_count": len(result.mentions),
                "filter": "unseen_only" if not result.show_all else "all",
                "mentions": result.mentions,
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

    try:
        ids = coerce_to_list_str(item_ids)
    except ValueError as e:
        return json.dumps({"error": f"Invalid item_ids: {e}"})
    if not ids:
        return json.dumps({"error": "No item_ids provided"})

    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpMarkMentionsSeenCommand,
        McpMarkMentionsSeenUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpMarkMentionsSeenUseCase().execute(
            McpMarkMentionsSeenCommand(board_id, ids),
            actor=actor,
            uow=uow,
        )
        return json.dumps(
            {
                "success": True,
                "marked_count": result.marked_count,
                "total_requested": result.total_requested,
            }
        )


@mcp.tool()
async def okto_pulse_get_unseen_summary(board_id: str) -> str:
    """
    Quick summary of unseen mentions and activity for the agent on this board.
    Use this to check if there's anything new without fetching full details."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpGetUnseenSummaryCommand,
        McpGetUnseenSummaryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpGetUnseenSummaryUseCase().execute(
            McpGetUnseenSummaryCommand(board_id),
            actor=actor,
            uow=uow,
        )
        return json.dumps(result.payload)


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
    """Get board details. Defaults to a minimal overview envelope; pass
    `include` to inline collections. Token-optimized default: the response
    carries id, name, description, owner_id, settings, counts{} and
    timestamps — ~200B vs ~10KB on a typical board.
    """
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
        _ds_gate_mode = (board.settings or {}).get(
            "design_system_gate_mode", "off"
        ) or "off"
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
async def okto_pulse_get_allowed_transitions(
    board_id: str,
    entity_type: str,
    entity_id: str = "",
    current_status: str = "",
) -> str:
    """
    Return allowed lifecycle transitions for ideation, refinement, or spec from
    the backend transition authority used by move tools/endpoints.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        CommandValidationError,
        EntityNotFoundError,
        ListAllowedTransitionsCommand,
        ListAllowedTransitionsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await ListAllowedTransitionsUseCase().execute(
                ListAllowedTransitionsCommand(
                    board_id,
                    entity_type,
                    entity_id=entity_id or None,
                    current_status=current_status or None,
                ),
                actor=actor,
                uow=uow,
            )
            return json.dumps(result.read_model.to_dict(), default=str)
    except EntityNotFoundError as exc:
        detail = (
            "Board not found"
            if exc.entity_type == "board"
            else f"{exc.entity_type.title()} not found"
        )
        return json.dumps({"error": detail})
    except CommandValidationError as exc:
        return json.dumps({"error": str(exc)})


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

    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpListAgentsCommand,
        McpListAgentsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.infra.permissions import generate_role_summary

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListAgentsUseCase().execute(
            McpListAgentsCommand(board_id),
            actor=actor,
            uow=uow,
        )
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
                for a in result.agents
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
    """Get the board activity log with optional filtering and pagination.
    Default rows carry id, action, trigger, card_id, created_at + a
    deterministic server-built `summary` (~120B per row vs ~1.5KB); pass
    include_details=true for the full nested details object (legacy shape).
    Pass `cursor` (opaque base64 from a prior next_cursor) for O(1) keyset
    pagination, and envelope=true to receive {items, next_cursor} instead of a
    raw list. Legacy `offset` is silently ignored unless
    OKTO_PULSE_LEGACY_OFFSET=1.
    """
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

    from okto_pulse.core.application.use_cases.mcp_profile_activity import (
        McpGetActivityLogCommand,
        McpGetActivityLogUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpGetActivityLogUseCase().execute(
            McpGetActivityLogCommand(
                board_id,
                limit=limit,
                cursor_pair=cursor_pair,
                effective_offset=effective_offset,
                action=action,
                card_id=card_id,
                include_details=include_details,
            ),
            actor=actor,
            uow=uow,
        )

        if envelope:
            return json.dumps(
                {"items": result.rows, "next_cursor": result.next_cursor}, default=str
            )
        return json.dumps(result.rows, default=str)


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
    """Create a new card on the board. Every card MUST be linked to a spec.
    The spec must be approved/in_progress/done (test cards also accept
    validated); create test cards BEFORE requesting spec validation.

    For card_type='test', test_scenario_ids is mandatory and limited by the
    board setting max_scenarios_per_card (default 3); split larger scenario
    sets into separate test cards. Errors: max_scenarios_per_card_exceeded.
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
            name
            for name, val in (
                ("origin_task_id", origin_task_id),
                ("severity", severity),
                ("expected_behavior", expected_behavior),
                ("observed_behavior", observed_behavior),
            )
            if not (val or "").strip()
        ]
        if missing:
            return json.dumps(
                {"error": f"Bug cards require non-empty: {', '.join(missing)}"}
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
            expected_behavior=expected_behavior.replace("\\n", "\n")
            if expected_behavior
            else None,
            observed_behavior=observed_behavior.replace("\\n", "\n")
            if observed_behavior
            else None,
            steps_to_reproduce=steps_to_reproduce.replace("\\n", "\n")
            if steps_to_reproduce
            else None,
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
            resp_card.update(
                {
                    "origin_task_id": card.origin_task_id,
                    "severity": getattr(card, "severity", None),
                    "expected_behavior": card.expected_behavior,
                    "observed_behavior": card.observed_behavior,
                    "spec_id": card.spec_id,
                }
            )

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
            compact_and_emit(
                {
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
                    "due_date": (card.due_date.isoformat() if card.due_date else None),
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
                },
                tool_name="okto_pulse_get_card",
            ),
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
        return json.dumps(
            {
                "error": "invalid_multivalue",
                "message": str(exc),
            }
        )

    from okto_pulse.core.services.bug_regression_preview import (
        BugRegressionScenarioPreviewError,
        BugRegressionScenarioPreviewService,
    )

    async with get_unit_of_work_factory_for_mcp()() as uow:
        try:
            payload = await uow.services.bug_regression_preview.resolve(
                board_id=board_id,
                bug_id=bug_id,
                affected_task_ids=affected_ids,
                candidate_scenario_ids=candidate_ids,
                surface="mcp",
            )
            await uow.commit()
            return json.dumps(payload, default=str)
        except BugRegressionScenarioPreviewError as exc:
            await uow.rollback()
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
    profile: Annotated[str, Field(description="summary | detail | full | legacy — full is MANDATORY before any status-changing move")] = "summary",
) -> str:
    """Task card context: card body, linked spec
    requirements/scenarios/BRs/contracts, validations, resources and Q&A. Use
    `summary` for exploration and `profile="full"` before card work or
    status-changing moves.

    Test cards expose `test_card_operational_flow`: update linked scenarios
    with okto_pulse_update_test_scenario_status, then move the card to done;
    task validation is not used. `gate_readiness` mirrors the active done-gate
    and cognitive-readiness verdict without mutating or skipping anything.
    Profiles: okto-pulse://reference/projection-profiles.
    Docs: okto-pulse://reference/tool-docs/misc.
    """
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        card_service = uow.services.cards
        card = await card_service.get_card(card_id)
        await uow.commit()

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
                uow.services, "card", card_id, permissions=ctx.permissions
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
        await uow.commit()
        if deps:
            result["card"]["depends_on"] = [
                {"id": d.id, "title": d.title, "status": d.status.value} for d in deps
            ]

        # Spec context (the core of task context)
        spec = None
        spec_architecture_designs: list[dict[str, Any]] = []
        if card.spec_id:
            spec_service = uow.services.specs
            spec = await spec_service.get_spec(card.spec_id)
            await uow.commit()

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
                if (
                    _mcp_check_permission(
                        ctx.permissions,
                        "spec.integration_requirements.read",
                        Permissions.BOARD_READ,
                    )
                    is None
                ):
                    spec_data["integration_requirements"] = (
                        getattr(spec, "integration_requirements", None) or []
                    )
                if (
                    _mcp_check_permission(
                        ctx.permissions,
                        "spec.observability_requirements.read",
                        Permissions.BOARD_READ,
                    )
                    is None
                ):
                    spec_data["observability_requirements"] = (
                        getattr(spec, "observability_requirements", None) or []
                    )

                if _inc_mockups and spec.screen_mockups:
                    spec_data["screen_mockups"] = spec.screen_mockups

                if _inc_architecture:
                    spec_architecture_designs = await _mcp_architecture_for_parent(
                        uow.services, "spec", spec.id, permissions=ctx.permissions
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
                        ts
                        for ts in spec.test_scenarios
                        if ts.get("id") in card.test_scenario_ids
                    ]

        resolved_references = resolve_task_context_references(
            card,
            spec,
            include_superseded=_inc_superseded,
            include_content=_inc_kb,
            card_architecture_designs=card_architecture_designs
            if _inc_architecture
            else [],
            spec_architecture_designs=spec_architecture_designs
            if _inc_architecture
            else [],
        )
        if not _inc_kb:
            resolved_references["knowledge_bases"] = []
        if not _inc_mockups:
            resolved_references["screen_mockups"] = []
        if not _inc_architecture:
            resolved_references["architecture_designs"] = []
        result["resolved_references"] = resolved_references
        result["resource_gate_summary"] = await uow.services.resource_gate.get_summary(
            board_id,
            "card",
            card_id,
        )
        if spec:
            result["spec"][
                "resource_gate_summary"
            ] = await uow.services.resource_gate.get_summary(
                board_id,
                "spec",
                spec.id,
            )

        # Task validations — critical for agents picking up cards that failed validation
        result["validations"] = list(card.validations or [])

        # Validation gate config (resolved from sprint -> spec -> board hierarchy)
        board_obj = await uow.services.get_application_record(
            entity="board",
            record_id=card.board_id,
        )
        board_settings = board_obj.settings or {} if board_obj else {}
        spec_for_gate = (
            await uow.services.get_application_record(
                entity="spec",
                record_id=card.spec_id,
            )
            if card.spec_id
            else None
        )
        sprint_for_gate = (
            await uow.services.get_application_record(
                entity="sprint",
                record_id=card.sprint_id,
            )
            if card.sprint_id
            else None
        )
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
                    ts
                    for ts in (spec.test_scenarios or [])
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
        cognitive_enforcement_active = await _cognitive_enforcement_active(
            uow.services.kg,
            board_id,
        )
        cognitive_verdict = await _evaluate_card_cognitive_verdict(
            uow.services.kg,
            board_id,
            card,
            cognitive_enforcement_active,
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.cards
        card = await service.get_card(card_id)
        await uow.commit()

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
            result["note"] = (
                "No conclusions recorded. Conclusions are required when moving a card to 'done'."
            )

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
                update_data["linked_test_task_ids"] = coerce_to_list_str(
                    linked_test_task_ids
                )
            except ValueError as e:
                return json.dumps({"error": f"Invalid linked_test_task_ids: {e}"})

        card_update = CardUpdate(**update_data)
        try:
            updated = (
                await McpUpdateCardUseCase().execute(
                    McpUpdateCardCommand(card_id, board_id, card_update, update_data),
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
    status: Annotated[str, Field(description="Target column; moving to validation/done requires the execution-report fields")],
    position: int = -1,
    conclusion: Annotated[str, Field(description="Execution report — required for validation/done moves")] = "",
    completeness: Annotated[int, Field(description="0-100; -1 when no execution report is required")] = -1,
    completeness_justification: str = "",
    drift: Annotated[int, Field(description="0-100 deviation from spec; -1 when not required")] = -1,
    drift_justification: str = "",
    cancellation_reason: Annotated[str, Field(description="Required when status=cancelled; cleared on reopen")] = "",
) -> str:
    """Move a card to a different column/position on the board.

    Moving to 'validation' or 'done' REQUIRES conclusion, completeness
    (0-100), completeness_justification, drift (0-100), and
    drift_justification so the reviewer can validate the claim. Use -1 for
    completeness/drift when no execution report is required (e.g. moving to
    on_hold or started). status='cancelled' requires cancellation_reason;
    reopening clears it. Errors: resource_gate_missing_resources;
    missing_regression_test_task (bug -> in_progress).
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
            cancellation_reason=cancellation_reason or None,
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
            return json.dumps(
                {
                    "error": e.code,
                    **e.to_dict(),
                    "blocked_by_dependencies": True,
                }
            )
        except GateContractError as e:
            return json.dumps(e.to_dict())
        except ResourceGateError as e:
            return _resource_gate_error_response(e)
        except CancellationReasonRequiredError as e:
            return json.dumps({"error": e.code, **e.to_dict()})
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
    """Remove a dependency link between two cards. Permanent, no undo — re-add
    with okto_pulse_add_card_dependency if needed.
    Docs: okto-pulse://reference/destructive_ops
    """
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.boards
        board = await service.get_board(board_id)
        await uow.commit()

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
        paginated = sorted_cards[offset : offset + limit]

        from okto_pulse.core.mcp.payload_compaction import compact_and_emit

        return json.dumps(
            compact_and_emit(
                {
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
                },
                tool_name="okto_pulse_list_cards_by_status",
                truncated=total_filtered > offset + len(paginated),
            ),
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
        board_id,
        "card",
        card_id,
        question,
        alias_kind="legacy",
        tool_name="okto_pulse_ask_question",
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
        return json.dumps(
            {
                "error": "unsupported_target_type",
                "message": type_err,
                "allowed": list(fam.target_types) if fam else [],
            }
        )

    # Sprint is asymmetric: no QA_CREATE permission gate (preserve legacy behavior).
    if target_type != "sprint":
        perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
        if perm_err:
            _telemetry("error")
            return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpAskQuestionCommand,
        McpAskQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpAskQuestionUseCase().execute(
            McpAskQuestionCommand(board_id, target_type, parent_id, question),
            actor=actor,
            uow=uow,
        )
    _telemetry("ok" if result.payload.get("success") else "error")
    return json.dumps(result.payload, default=str)


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
        board_id,
        target_type,
        parent_id,
        question,
        alias_kind="consolidated",
        tool_name="okto_pulse_ask",
    )


@mcp.tool()
async def okto_pulse_answer_question(board_id: str, qa_id: str, answer: str) -> str:
    """
    Answer a question on a card's Q&A board."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpAnswerQuestionCommand,
        McpAnswerQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpAnswerQuestionUseCase().execute(
            McpAnswerQuestionCommand(board_id, qa_id, answer),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_delete_question(board_id: str, qa_id: str) -> str:
    """Delete a Q&A item from a card. Permanent, no undo — the question and any
    recorded answer are removed. Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpDeleteQuestionCommand,
        McpDeleteQuestionUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpDeleteQuestionUseCase().execute(
            McpDeleteQuestionCommand(qa_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload)


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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpAddCommentCommand,
        McpAddCommentUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpAddCommentUseCase().execute(
            McpAddCommentCommand(board_id, card_id, content),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.models.schemas import ChoiceOption

    try:
        parsed_objects = parse_options_json(options_json or None)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options_json: {e}"})

    if parsed_objects is not None:
        choice_list = [
            ChoiceOption(
                id=f"opt_{i}",
                label=obj["label"],
                recommended=obj["recommended"],
                tradeoff=obj["tradeoff"],
            )
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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpAddChoiceCommentCommand,
        McpAddChoiceCommentUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpAddChoiceCommentUseCase().execute(
            McpAddChoiceCommentCommand(
                board_id,
                card_id,
                question,
                comment_type,
                choice_list,
                allow_free_text.lower() == "true",
            ),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpRespondToChoiceCommand,
        McpRespondToChoiceUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpRespondToChoiceUseCase().execute(
            McpRespondToChoiceCommand(comment_id, selected_ids, free_text),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_get_choice_responses(board_id: str, comment_id: str) -> str:
    """
    Get all responses for a choice board comment."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpGetChoiceResponsesCommand,
        McpGetChoiceResponsesUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpGetChoiceResponsesUseCase().execute(
            McpGetChoiceResponsesCommand(comment_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpListCommentsCommand,
        McpListCommentsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListCommentsUseCase().execute(
            McpListCommentsCommand(board_id, card_id),
            actor=actor,
            uow=uow,
        )
    if isinstance(result.payload, dict) and "error" in result.payload:
        return json.dumps(result.payload)
    from okto_pulse.core.mcp.payload_compaction import compact_and_emit

    return json.dumps(
        compact_and_emit(result.payload, tool_name="okto_pulse_list_comments"),
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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpUpdateCommentCommand,
        McpUpdateCommentUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpUpdateCommentUseCase().execute(
            McpUpdateCommentCommand(board_id, comment_id, content),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_delete_comment(board_id: str, comment_id: str) -> str:
    """Delete the agent's own comment. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.COMMENTS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpDeleteCommentCommand,
        McpDeleteCommentUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpDeleteCommentUseCase().execute(
            McpDeleteCommentCommand(board_id, comment_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload)


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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpUploadAttachmentCommand,
        McpUploadAttachmentUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpUploadAttachmentUseCase().execute(
            McpUploadAttachmentCommand(card_id, filename, content, mime_type),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpListAttachmentsCommand,
        McpListAttachmentsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListAttachmentsUseCase().execute(
            McpListAttachmentsCommand(board_id, card_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_delete_attachment(board_id: str, attachment_id: str) -> str:
    """Delete an attachment. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.ATTACHMENTS_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpDeleteAttachmentCommand,
        McpDeleteAttachmentUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpDeleteAttachmentUseCase().execute(
            McpDeleteAttachmentCommand(attachment_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload)


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
            {
                "id": link.id,
                "ideation_id": link.ideation_id,
                "created_at": link.created_at.isoformat(),
            }
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
        "active_count": getattr(
            topic, "active_count", getattr(topic, "story_count", 0)
        ),
        "archived_count": getattr(topic, "archived_count", 0),
        "total_associated_count": getattr(
            topic, "total_associated_count", getattr(topic, "story_count", 0)
        ),
        "created_by": topic.created_by,
        "created_at": topic.created_at.isoformat(),
        "updated_at": topic.updated_at.isoformat(),
    }


def _topic_impact(topic) -> dict:
    return {
        "topic_id": topic.id,
        "story_count": getattr(topic, "story_count", 0),
        "active_count": getattr(
            topic, "active_count", getattr(topic, "story_count", 0)
        ),
        "archived_count": getattr(topic, "archived_count", 0),
        "total_associated_count": getattr(
            topic, "total_associated_count", getattr(topic, "story_count", 0)
        ),
    }


def _topic_operation_error_response(exc: TopicOperationError) -> str:
    return json.dumps(
        {"success": False, "error": str(exc), "code": exc.code, **exc.details},
        default=str,
    )


@mcp.tool()
async def okto_pulse_create_topic(
    board_id: str, name: str, description: str = ""
) -> str:
    """Create a board-scoped Topic for grouping Stories."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions, "topic.entity.create", Permissions.SPECS_CREATE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpCreateTopicCommand,
        McpCreateTopicUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpCreateTopicUseCase().execute(
            McpCreateTopicCommand(board_id, name, description),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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
    perm_err = _mcp_check_permission(
        ctx.permissions, "topic.entity.edit_fields", Permissions.SPECS_UPDATE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    update_data = {}
    if name:
        update_data["name"] = name
    if description:
        update_data["description"] = description
    if not update_data:
        return json.dumps(
            {"success": False, "error": "Provide at least one field to update"}
        )

    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpUpdateTopicCommand,
        McpUpdateTopicUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpUpdateTopicUseCase().execute(
            McpUpdateTopicCommand(board_id, topic_id, update_data),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_archive_topic(board_id: str, topic_id: str) -> str:
    """Archive a Topic without archiving its Stories. Not a delete — reversible
    via okto_pulse_restore_topic. Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions, "topic.entity.archive", Permissions.SPECS_UPDATE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpSetTopicArchivedCommand,
        McpSetTopicArchivedUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpSetTopicArchivedUseCase().execute(
            McpSetTopicArchivedCommand(board_id, topic_id, True),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_restore_topic(board_id: str, topic_id: str) -> str:
    """Restore an archived Topic."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions, "topic.entity.restore", Permissions.SPECS_UPDATE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpSetTopicArchivedCommand,
        McpSetTopicArchivedUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpSetTopicArchivedUseCase().execute(
            McpSetTopicArchivedCommand(board_id, topic_id, False),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_delete_topic(board_id: str, topic_id: str) -> str:
    """Delete a Topic only when it has no associated Stories, including archived Stories."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions, "topic.entity.delete", Permissions.SPECS_DELETE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpDeleteTopicCommand,
        McpDeleteTopicUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpDeleteTopicUseCase().execute(
            McpDeleteTopicCommand(board_id, topic_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


@mcp.tool()
async def okto_pulse_merge_topics(
    board_id: str, source_topic_id: str, target_topic_id: str
) -> str:
    """Merge a source Topic into an active target Topic while preserving Story-Ideation links."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = _mcp_check_permission(
        ctx.permissions, "topic.entity.merge", Permissions.SPECS_UPDATE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.application.use_cases.mcp_collaboration import (
        McpMergeTopicsCommand,
        McpMergeTopicsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpMergeTopicsUseCase().execute(
            McpMergeTopicsCommand(board_id, source_topic_id, target_topic_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpGetResourceGateSummaryUseCase,
        McpResourceGateSummaryCommand,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpGetResourceGateSummaryUseCase().execute(
                McpResourceGateSummaryCommand(board_id, entity_type, entity_id),
                actor=actor,
                uow=uow,
            )
    except ResourceGateError as e:
        return _resource_gate_error_response(e)
    return json.dumps(result.payload, default=str)


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
    perm_err = _mcp_check_resource_gate_permission(
        ctx.permissions, entity_type, "write"
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)

    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpMarkResourceNotApplicableCommand,
        McpMarkResourceNotApplicableUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpMarkResourceNotApplicableUseCase().execute(
                McpMarkResourceNotApplicableCommand(
                    board_id,
                    entity_type,
                    entity_id,
                    resource_type,
                    justification,
                ),
                actor=actor,
                uow=uow,
            )
    except ResourceGateError as e:
        return _resource_gate_error_response(e)
    return json.dumps(result.payload, default=str)


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
    perm_err = _mcp_check_resource_gate_permission(
        ctx.permissions, entity_type, "write"
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)

    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpClearResourceNotApplicableCommand,
        McpClearResourceNotApplicableUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpClearResourceNotApplicableUseCase().execute(
                McpClearResourceNotApplicableCommand(
                    board_id,
                    entity_type,
                    entity_id,
                    resource_type,
                    reason,
                ),
                actor=actor,
                uow=uow,
            )
    except ResourceGateError as e:
        return _resource_gate_error_response(e)
    return json.dumps(result.payload, default=str)


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
    perm_err = _mcp_check_permission(
        ctx.permissions, "story.entity.create", Permissions.SPECS_CREATE
    )
    if perm_err:
        return _mcp_permission_error_response(perm_err)
    from okto_pulse.core.domain.enums import StoryStatus
    from okto_pulse.core.models.schemas import StoryCreate

    try:
        story_status = StoryStatus(status)
        label_list = coerce_to_list_str(labels) or None
    except ValueError as e:
        return json.dumps({"error": str(e)})

    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpCreateStoryCommand,
        McpCreateStoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    data = StoryCreate(
        topic_id=topic_id,
        title=title,
        description=description.replace("\\n", "\n"),
        actor=actor or None,
        goal=goal or None,
        benefit=benefit or None,
        labels=label_list,
        status=story_status,
    )
    actor_ctx = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor_ctx) as uow:
            result = await McpCreateStoryUseCase().execute(
                McpCreateStoryCommand(board_id, data),
                actor=actor_ctx,
                uow=uow,
            )
    except ValueError as e:
        return json.dumps({"error": str(e)})
    if result.board_not_found:
        return json.dumps({"error": "Board not found"})
    return json.dumps(
        {"success": True, "story": _story_payload(result.story)}, default=str
    )


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
        return json.dumps(
            {"success": False, "error": "Provide at least one field to update"}
        )

    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpUpdateStoryCommand,
        McpUpdateStoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpUpdateStoryUseCase().execute(
                McpUpdateStoryCommand(
                    board_id,
                    story_id,
                    update_data,
                    StoryUpdate(**update_data),
                ),
                actor=actor,
                uow=uow,
            )
    except ValueError as e:
        return json.dumps({"error": str(e)})
    if result.not_found:
        return json.dumps({"error": "Story not found"})
    if result.perm_err is not None:
        return _mcp_permission_error_response(result.perm_err)
    return json.dumps(
        {"success": True, "story": _story_payload(result.story)}, default=str
    )


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
        return json.dumps(
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in StoryStatus]}"
            }
        )

    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpMoveStoryCommand,
        McpMoveStoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpMoveStoryUseCase().execute(
                McpMoveStoryCommand(
                    board_id,
                    story_id,
                    story_status,
                    StoryMove(status=story_status),
                ),
                actor=actor,
                uow=uow,
            )
    except ValueError as e:
        return json.dumps({"error": str(e)})
    if result.not_found:
        return json.dumps({"error": "Story not found"})
    if result.perm_err is not None:
        return _mcp_permission_error_response(result.perm_err)
    return json.dumps(
        {"success": True, "story": _story_payload(result.story)}, default=str
    )


@mcp.tool()
async def okto_pulse_archive_story(board_id: str, story_id: str) -> str:
    """Archive a Story without deleting lineage or linked Ideations."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpArchiveStoryCommand,
        McpArchiveStoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpArchiveStoryUseCase().execute(
            McpArchiveStoryCommand(board_id, story_id, True),
            actor=actor,
            uow=uow,
        )
    if result.not_found:
        return json.dumps({"error": "Story not found"})
    if result.perm_err is not None:
        return _mcp_permission_error_response(result.perm_err)
    return json.dumps(
        {"success": True, "story": _story_payload(result.story)}, default=str
    )


@mcp.tool()
async def okto_pulse_restore_story(board_id: str, story_id: str) -> str:
    """Restore an archived Story."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    from okto_pulse.core.application.use_cases.mcp_resource_stories import (
        McpArchiveStoryCommand,
        McpArchiveStoryUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpArchiveStoryUseCase().execute(
            McpArchiveStoryCommand(board_id, story_id, False),
            actor=actor,
            uow=uow,
        )
    if result.not_found:
        return json.dumps({"error": "Story not found"})
    if result.perm_err is not None:
        return _mcp_permission_error_response(result.perm_err)
    return json.dumps(
        {"success": True, "story": _story_payload(result.story)}, default=str
    )


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
                    "link": {
                        "id": link.id,
                        "story_id": link.story_id,
                        "ideation_id": link.ideation_id,
                    },
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
    perm_err = _mcp_check_permission(
        ctx.permissions, "story.conversion.to_ideation", Permissions.SPECS_CREATE
    )
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
        problem_statement=problem_statement.replace("\\n", "\n")
        if problem_statement
        else None,
        proposed_approach=proposed_approach.replace("\\n", "\n")
        if proposed_approach
        else None,
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
                return json.dumps(
                    {"error": "One or more Stories were not found in this board"}
                )
            if _r.perm_err is not None:
                return _mcp_permission_error_response(_r.perm_err)
            if _r.board_not_found:
                return json.dumps({"error": "Board not found"})
            return json.dumps(
                {
                    "success": True,
                    "ideation": {
                        "id": _r.ideation.id,
                        "title": _r.ideation.title,
                        "status": _r.ideation.status.value,
                    },
                    "links": [
                        {
                            "id": link.id,
                            "story_id": link.story_id,
                            "ideation_id": link.ideation_id,
                        }
                        for link in _r.links
                    ],
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
        problem_statement=problem_statement.replace("\\n", "\n")
        if problem_statement
        else None,
        proposed_approach=proposed_approach.replace("\\n", "\n")
        if proposed_approach
        else None,
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
    # below through the legacy presentation context (lazy relationships).
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
                "complexity": ideation.complexity.value
                if ideation.complexity
                else None,
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
    # The presentation projection still uses the F12 legacy adapter context.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with AsyncExitStack() as stack:
        uow = await stack.enter_async_context(
            get_unit_of_work_factory_for_mcp()(actor=actor)
        )
        try:
            ideation = (
                await McpGetIdeationUseCase().execute(
                    McpGetIdeationCommand(ideation_id, board_id), actor=actor, uow=uow
                )
            ).ideation
        except EntityNotFoundError:
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
            "created_at": ideation.created_at.isoformat()
            if ideation.created_at
            else None,
            "updated_at": ideation.updated_at.isoformat()
            if ideation.updated_at
            else None,
            "labels": ideation.labels or [],
            # R5-IMP2: read-only skip-override read-model (ambiguity gate skip).
            "skip_overrides": await uow.services.ideation_skip_overrides(
                ideation,
                board_id,
            ),
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

        if (
            _inc_mockups
            and hasattr(ideation, "screen_mockups")
            and ideation.screen_mockups
        ):
            result["screen_mockups"] = ideation.screen_mockups

        architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            architecture_designs = await _mcp_architecture_for_parent(
                uow.services, "ideation", ideation_id, permissions=ctx.permissions
            )
            result["architecture_designs"] = architecture_designs

        if _inc_kb and hasattr(ideation, "knowledge_bases"):
            result["knowledge_bases"] = [
                _serialize_knowledge_base(kb) for kb in (ideation.knowledge_bases or [])
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
                    "complexity": ideation.complexity.value
                    if ideation.complexity
                    else None,
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_move_ideation(
    board_id: str, ideation_id: str, status: str, cancellation_reason: str = ""
) -> str:
    """
    Change an ideation's status (draft -> review -> approved -> evaluating -> done).

    Allowed transitions:
    - draft → review, cancelled
    - review → draft, approved, cancelled
    - approved → review, evaluating, cancelled
    - evaluating → approved, done, cancelled
    - done → draft (new version)
    status='cancelled' requires cancellation_reason; reopening clears it."""
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
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in IdeationStatus]}"
            }
        )

    from okto_pulse.core.application.use_cases import (
        EntityNotFoundError,
        MoveIdeationCommand,
        MoveIdeationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    # Spec #04 (MCP strangler): obtain a PulseUnitOfWork from the MCP
    # UnitOfWorkFactory instead of opening a raw get_db_for_mcp() session — the
    # The board-scoped pre-check uses the typed service catalog.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        service = uow.services.ideations
        existing = await service.get_ideation(ideation_id)
        if not existing or existing.board_id != board_id:
            return json.dumps({"error": "Ideation not found"})
        old_status = existing.status.value
        try:
            # Delegate to the shared transport-free use case (it commits +
            # re-fetches via the uow). board pre-check, old_status, compact MCP
            # payload, error envelopes and actor_name are preserved.
            result = await MoveIdeationUseCase().execute(
                MoveIdeationCommand(
                    ideation_id,
                    IdeationMove(
                        status=ideation_status,
                        cancellation_reason=cancellation_reason or None,
                    ),
                ),
                actor=actor,
                uow=uow,
            )
        except EntityNotFoundError:
            # Defensive: the pre-check covers not-found, but guard the race where
            # the ideation is removed between pre-check and the use case — preserve
            # the original "Ideation not found" envelope (not a ValueError).
            return json.dumps({"error": "Ideation not found"})
        except CancellationReasonRequiredError as e:
            return json.dumps({"error": e.code, **e.to_dict()})
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
        scope["dependencies_justification"] = dependencies_justification.replace(
            "\\n", "\n"
        )

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
                "complexity": ideation.complexity.value
                if ideation.complexity
                else None,
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
    """Create a spec draft from a DONE ideation (fully reviewed and
    snapshotted). The spec gets rich context compiled from the ideation —
    its description/context embed the FULL parent ideation context (large) —
    while structured fields (requirements, criteria) stay empty for deliberate
    analysis.

    Artifacts (mockups, KBs, Architecture Designs) from the ideation are
    automatically propagated to the spec; use mockup_ids/kb_ids/
    architecture_design_ids to select specific ones (default: all).
    architecture_propagation_mode accepts copy, derive, reference_only, or
    none; "snapshot" is not a mode.
    """
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
async def okto_pulse_get_ideation_snapshot(
    board_id: str, ideation_id: str, version: str
) -> str:
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
async def okto_pulse_get_ideation_history(
    board_id: str, ideation_id: str, limit: str = "30"
) -> str:
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
    """Delete a knowledge base item from an ideation. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
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
async def okto_pulse_ask_ideation_question(
    board_id: str, ideation_id: str, question: str
) -> str:
    """
    Ask a question on an ideation's Q&A board. Use @Name to direct the question."""
    return await _ask_question_impl(
        board_id,
        "ideation",
        ideation_id,
        question,
        alias_kind="legacy",
        tool_name="okto_pulse_ask_ideation_question",
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
            IdeationQAChoiceOption(
                id=f"opt_{i}",
                label=obj["label"],
                recommended=obj["recommended"],
                tradeoff=obj["tradeoff"],
            )
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
        question_type=question_type
        if question_type in ("choice", "multi_choice")
        else "choice",
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
async def okto_pulse_answer_ideation_question(
    board_id: str,
    ideation_id: str,
    qa_id: str,
    answer: str = "",
    selected: list[str] | str = "",
) -> str:
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
                {
                    "error": _r.self_answer_error.reason,
                    "detail": str(_r.self_answer_error),
                }
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
                return json.dumps(
                    {"error": "Failed to create refinement (ideation not found)"}
                )
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
    """Get full details of a refinement including its specs and Q&A items. The
    refinement description embeds the full parent ideation context — responses
    can be large.
    """
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
                    McpGetRefinementCommand(refinement_id, board_id),
                    actor=actor,
                    uow=uow,
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
    # The presentation projection still uses the F12 legacy adapter context.
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with AsyncExitStack() as stack:
        uow = await stack.enter_async_context(
            get_unit_of_work_factory_for_mcp()(actor=actor)
        )
        try:
            refinement = (
                await McpGetRefinementUseCase().execute(
                    McpGetRefinementCommand(refinement_id, board_id),
                    actor=actor,
                    uow=uow,
                )
            ).refinement
        except EntityNotFoundError:
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
            "created_at": refinement.created_at.isoformat()
            if refinement.created_at
            else None,
            "updated_at": refinement.updated_at.isoformat()
            if refinement.updated_at
            else None,
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

        if (
            _inc_mockups
            and hasattr(refinement, "screen_mockups")
            and refinement.screen_mockups
        ):
            result["screen_mockups"] = refinement.screen_mockups

        architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            architecture_designs = await _mcp_architecture_for_parent(
                uow.services, "refinement", refinement_id, permissions=ctx.permissions
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
async def okto_pulse_move_refinement(
    board_id: str, refinement_id: str, status: str, cancellation_reason: str = ""
) -> str:
    """
    Change a refinement's status (draft -> review -> approved -> done).

    Allowed transitions:
    - draft → review, cancelled
    - review → draft, approved, cancelled
    - approved → review, done, cancelled
    - done → draft (new version)
    status='cancelled' requires cancellation_reason; reopening clears it."""
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
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in RefinementStatus]}"
            }
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
                    refinement_id,
                    board_id,
                    RefinementMove(
                        status=refinement_status,
                        cancellation_reason=cancellation_reason or None,
                    ),
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
    except CancellationReasonRequiredError as e:
        return json.dumps({"error": e.code, **e.to_dict()})
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
    """Create a spec draft from a DONE refinement. Context is compiled from the
    refinement's scope, analysis, decisions, and Q&A — the refinement
    description (and thus the spec) embeds the FULL parent ideation context
    (large).

    Artifacts (mockups, KBs, Architecture Designs) from the refinement are
    automatically propagated to the spec; use mockup_ids/kb_ids/
    architecture_design_ids to select specific ones (default: all).
    architecture_propagation_mode accepts copy, derive, reference_only, or
    none; "snapshot" is not a mode.
    """
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
async def okto_pulse_get_refinement_history(
    board_id: str, refinement_id: str, limit: str = "30"
) -> str:
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
async def okto_pulse_ask_refinement_question(
    board_id: str, refinement_id: str, question: str
) -> str:
    """
    Ask a question on a refinement's Q&A board. Use @Name to direct the question."""
    return await _ask_question_impl(
        board_id,
        "refinement",
        refinement_id,
        question,
        alias_kind="legacy",
        tool_name="okto_pulse_ask_refinement_question",
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

    from okto_pulse.core.models.schemas import (
        RefinementQAChoiceOption,
        RefinementQACreate,
    )

    try:
        parsed_objects = parse_options_json(options_json or None)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options_json: {e}"})

    if parsed_objects is not None:
        choice_list = [
            RefinementQAChoiceOption(
                id=f"opt_{i}",
                label=obj["label"],
                recommended=obj["recommended"],
                tradeoff=obj["tradeoff"],
            )
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
        question_type=question_type
        if question_type in ("choice", "multi_choice")
        else "choice",
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
async def okto_pulse_answer_refinement_question(
    board_id: str,
    refinement_id: str,
    qa_id: str,
    answer: str = "",
    selected: list[str] | str = "",
) -> str:
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
                {
                    "error": _r.self_answer_error.reason,
                    "detail": str(_r.self_answer_error),
                }
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
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in SpecStatus]}"
            }
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
            return json.dumps(_r.propagation_error.to_error_dict(spec_id=_r.spec.id))
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
                "integration_requirements": getattr(
                    spec, "integration_requirements", None
                )
                or [],
                "observability_requirements": getattr(
                    spec, "observability_requirements", None
                )
                or [],
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
        if (
            _mcp_check_permission(
                ctx.permissions,
                "spec.integration_requirements.read",
                Permissions.BOARD_READ,
            )
            is None
        ):
            payload["integration_requirements"] = (
                getattr(spec, "integration_requirements", None) or []
            )
        if (
            _mcp_check_permission(
                ctx.permissions,
                "spec.observability_requirements.read",
                Permissions.BOARD_READ,
            )
            is None
        ):
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
    profile: Annotated[str, Field(description="summary | detail | full | legacy — full is MANDATORY before any status-changing move")] = "summary",
) -> str:
    """Consolidated spec context: requirements, scenarios, rules, contracts,
    IR/OR, decisions, resources, Q&A, evaluations, cards and sprints. Use
    `summary` for exploration and `profile="full"` before evaluating, moving,
    or deriving cards. Includes read-only `gate_readiness` for spec transition
    gates; cognitive readiness is per-card — call okto_pulse_get_task_context
    for card verdicts. Profiles: okto-pulse://reference/projection-profiles.
    Docs: okto-pulse://reference/tool-docs/misc.
    """
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
    # adapter through the F12 legacy presentation context (server helpers /
    # the sprint swallow / gate_readiness stay here, NOT pushed into the core).
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with AsyncExitStack() as stack:
        uow = await stack.enter_async_context(
            get_unit_of_work_factory_for_mcp()(actor=actor)
        )
        try:
            spec = (
                await McpGetSpecContextUseCase().execute(
                    McpGetSpecContextCommand(spec_id, board_id), actor=actor, uow=uow
                )
            ).spec
        except EntityNotFoundError:
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
            # R5-IMP2: read-only skip-override read-model (cognitive skips on this
            # spec/its cards). Parent ideation ambiguity skip is lineage, not an
            # effective override of the spec, so it is excluded here.
            "skip_overrides": await uow.services.spec_skip_overrides(spec, board_id),
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
            "decisions_stats": _decisions_stats(getattr(spec, "decisions", None) or []),
            # Evaluations
            "evaluations": spec.evaluations or [],
            # Skip flags
            "skip_test_coverage": spec.skip_test_coverage,
            "skip_rules_coverage": getattr(spec, "skip_rules_coverage", False),
            "skip_ir_coverage": getattr(spec, "skip_ir_coverage", False),
            "skip_or_coverage": getattr(spec, "skip_or_coverage", False),
            "skip_decisions_coverage": getattr(spec, "skip_decisions_coverage", True),
            "skip_qualitative_validation": getattr(
                spec, "skip_qualitative_validation", False
            ),
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
        if (
            _mcp_check_permission(
                ctx.permissions,
                "spec.integration_requirements.read",
                Permissions.BOARD_READ,
            )
            is None
        ):
            result["integration_requirements"] = (
                getattr(spec, "integration_requirements", None) or []
            )
        if (
            _mcp_check_permission(
                ctx.permissions,
                "spec.observability_requirements.read",
                Permissions.BOARD_READ,
            )
            is None
        ):
            result["observability_requirements"] = (
                getattr(spec, "observability_requirements", None) or []
            )

        if _inc_mockups and spec.screen_mockups:
            result["screen_mockups"] = spec.screen_mockups

        architecture_designs: list[dict[str, Any]] = []
        if _inc_architecture:
            architecture_designs = await _mcp_architecture_for_parent(
                uow.services, "spec", spec_id, permissions=ctx.permissions
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
                _serialize_knowledge_base(kb) for kb in (spec.knowledge_bases or [])
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
        result["resource_gate_summary"] = await uow.services.resource_gate.get_summary(
            board_id,
            "spec",
            spec_id,
        )

        result["coverage_summary"] = _mcp_spec_coverage_summary(spec)

        # Load sprints separately to avoid lazy-load error
        try:
            from okto_pulse.core.services.main import SprintService

            sprint_service = uow.services.sprints
            sprints = await sprint_service.list_board_sprints(board_id, spec_id=spec_id)
            await uow.commit()
            result["sprints"] = [
                {
                    "id": s.id,
                    "title": s.title,
                    "status": s.status.value,
                    "description": s.description,
                    "objective": getattr(s, "objective", None),
                    "expected_outcome": getattr(s, "expected_outcome", None),
                    "lane_type": s.lane_type.value
                    if getattr(s, "lane_type", None)
                    else "normal",
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
        board_obj = await uow.services.get_application_record(
            entity="board",
            record_id=spec.board_id,
        )
        board_settings = (board_obj.settings or {}) if board_obj else {}
        cognitive_enforcement_active = await _cognitive_enforcement_active(
            uow.services.kg,
            spec.board_id,
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
            update_kwargs["functional_requirements"] = coerce_to_list_str(
                functional_requirements
            )
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if technical_requirements:
        try:
            update_kwargs["technical_requirements"] = _trs_to_objects(
                coerce_to_list_str(technical_requirements)
            )
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    if acceptance_criteria:
        try:
            update_kwargs["acceptance_criteria"] = coerce_to_list_str(
                acceptance_criteria
            )
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
async def okto_pulse_move_spec(
    board_id: str, spec_id: str, status: str, cancellation_reason: str = ""
) -> str:
    """
    Change a spec's status (e.g. draft → review → approved → validated → in_progress → done).
    status='cancelled' requires cancellation_reason; reopening clears it."""
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
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in SpecStatus]}"
            }
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
                McpMoveSpecCommand(
                    spec_id,
                    board_id,
                    SpecMove(
                        status=spec_status,
                        cancellation_reason=cancellation_reason or None,
                    ),
                ),
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
    except CancellationReasonRequiredError as e:
        return json.dumps({"error": e.code, **e.to_dict()})
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
    """Add a test scenario to a spec. Test scenarios translate acceptance
    criteria into concrete Given/When/Then test plans. scenario_type accepts
    exactly: unit, integration, e2e, manual, negative (use negative for
    expected denial/error-path behavior); unsupported values fail closed with
    no mutation. Errors: invalid_scenario_type.
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
        return json.dumps(
            {
                "error": "invalid_scenario_type",
                "message": (
                    f"Invalid scenario_type {_r.invalid_scenario_type!r}. "
                    f"Allowed values: {', '.join(VALID_SCENARIO_TYPES)}. "
                    f"No scenario was appended."
                ),
            }
        )
    if _r.unresolved_criteria is not None:
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_criteria token(s): {_r.unresolved_criteria}. "
                    f"Valid indices: 0..{_r.criteria_count - 1}. "
                    f"Available ac_ids: {_r.available_ac_ids}. "
                    f"No scenario was appended."
                )
            }
        )

    return json.dumps(
        {
            "success": True,
            "scenario": _r.scenario,
            **_saturation_or_coverage(_r.coverage),
        },
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
        paginated = filtered[offset : offset + limit]

        # Build coverage map (always from full set)
        coverage: dict[int, list[str]] = {i: [] for i, _ in enumerate(criteria)}
        for scenario in all_scenarios:
            for index in _resolve_linked_criteria_to_indices(
                scenario.get("linked_criteria"),
                criteria,
            ):
                coverage.setdefault(index, []).append(scenario["id"])

        indexed_criteria = [{"index": i, "text": c} for i, c in enumerate(criteria)]

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
                    "uncovered_indices": [
                        i for i, _ in enumerate(criteria) if not coverage.get(i)
                    ],
                    "uncovered": [
                        c for i, c in enumerate(criteria) if not coverage.get(i)
                    ],
                    "details": {
                        str(i): coverage.get(i, []) for i, _ in enumerate(criteria)
                    },
                },
                "summary": {
                    "by_status": {
                        st: sum(1 for s in all_scenarios if s.get("status") == st)
                        for st in ("draft", "ready", "automated", "passed", "failed")
                        if any(s.get("status") == st for s in all_scenarios)
                    },
                    "by_type": {
                        t: sum(1 for s in all_scenarios if s.get("scenario_type") == t)
                        for t in VALID_SCENARIO_TYPES
                        if any(s.get("scenario_type") == t for s in all_scenarios)
                    },
                    # Historical/invalid persisted scenario_types are surfaced
                    # EXPLICITLY (spec ac16b3c9 FR5/AC5) rather than silently
                    # folded into a supported bucket or dropped — so a stale value
                    # like 'regression'/'exploratory' is visible for deliberate
                    # remediation. New writes already fail closed (card 58844a26).
                    "unsupported_types": {
                        st: sum(
                            1 for s in all_scenarios if s.get("scenario_type") == st
                        )
                        for st in sorted(
                            {
                                s.get("scenario_type")
                                for s in all_scenarios
                                if isinstance(s.get("scenario_type"), str)
                                and not is_valid_scenario_type(s.get("scenario_type"))
                            }
                        )
                    },
                    "linked": sum(1 for s in all_scenarios if s.get("linked_task_ids")),
                    "unlinked": sum(
                        1 for s in all_scenarios if not s.get("linked_task_ids")
                    ),
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
    status: Annotated[str, Field(description="draft | ready | automated | passed | failed")],
    evidence: Annotated[str, Field(description="JSON evidence; passed/failed require an evidence_class payload or a full run-log (NC-9 gate)")] = "",
) -> str:
    """Update a test scenario's status, optionally attaching structured evidence
    that the test exists/ran. Test-theater prevention gate (NC-9), active
    unless skip_test_evidence_global is set: status=automated requires
    evidence.test_file_path + test_function; passed/failed require an explicit
    evidence_class with its required fields, or unclassed run-log evidence
    (full matrix in the tool-doc); draft/ready optional. When skipped, a
    test_scenario.evidence_gate_skipped audit is emitted; evidence persists
    inline. Errors: status_not_mutable (scenario on a locked spec not linked
    to an executable test card).
    Docs: okto-pulse://reference/tool-docs/test-scenario.
    """
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
                return json.dumps(
                    {
                        "error": "invalid_evidence_json",
                        "message": "evidence must be a JSON object",
                    }
                )
            evidence_dict = parsed
        except json.JSONDecodeError as exc:
            return json.dumps(
                {
                    "error": "invalid_evidence_json",
                    "message": f"evidence is not valid JSON: {exc}",
                }
            )

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
        return json.dumps(
            {
                "error": "status_not_mutable",
                "spec_status": exc.spec_status,
                "message": str(exc),
            }
        )
    except ValueError as exc:
        msg = str(exc)
        if msg.startswith("evidence_required"):
            _ok, missing = validate_test_scenario_evidence(
                status, evidence_dict, for_write=True
            )
            return json.dumps(
                {
                    "error": "evidence_required",
                    "required": missing,
                    "message": (
                        f"Cannot mark scenario as {status} without structured "
                        f"evidence ({', '.join(missing)}). This prevents the test "
                        "theater anti-pattern by requiring replayable or justified "
                        "evidence. To bypass, enable "
                        "skip_test_evidence_global on the board."
                    ),
                }
            )
        if msg.startswith("scenario_not_found"):
            if "spec not found" in msg:
                return json.dumps({"error": "Spec not found"})
            return json.dumps({"error": f"Scenario '{scenario_id}' not found"})
        return json.dumps({"error": msg})

    return json.dumps(
        {
            "success": True,
            "scenario_id": result["scenario_id"],
            "old_status": result["old_status"],
            "new_status": result["new_status"],
            "evidence_provided": result["evidence_provided"],
            "evidence_gate_skipped": result["evidence_gate_skipped"],
        }
    )


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
    okto_pulse_update_test_scenario_status (no second NC-9 bypass).
    Empty-string params mean "leave unchanged"; to intentionally CLEAR a field
    list it in `clear` (pipe-separated; only notes and linked_criteria are
    clearable). linked_criteria is a pipe-separated list of AC index/id/text,
    resolved to AC ids fail-closed. Editing a SEMANTIC field
    (given/when/then/scenario_type/linked_criteria) of a scenario holding
    evidence resets status to `ready` and drops the evidence; cosmetic edits
    (title/notes) preserve both. Respects the spec content-lock.
    """
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
        return json.dumps(
            {
                "error": "spec_locked",
                "message": (
                    "Spec is locked by a passed validation; the scenario body "
                    "cannot be edited. Move the spec back to draft/approved first."
                ),
            }
        )
    except InvalidScenarioTypeError as exc:
        # Fail-closed scenario_type on the body-edit path (spec ac16b3c9): must
        # precede the generic ValueError handler (it subclasses ValueError).
        return json.dumps(
            {
                "error": "invalid_scenario_type",
                "message": f"{exc} No scenario was updated.",
            }
        )
    except ValueError as exc:
        msg = str(exc)
        code = "invalid_update"
        if msg.startswith("scenario_not_found"):
            code = "scenario_not_found"
        elif msg.startswith("unresolved_criteria"):
            code = "unresolved_criteria"
        return json.dumps({"error": code, "message": msg})

    return json.dumps(
        {
            "success": True,
            "scenario_id": result["scenario_id"],
            "updated_fields": result["updated_fields"],
            "evidence_invalidated": result["evidence_invalidated"],
        }
    )


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
        return json.dumps(
            {
                "error": "spec_locked",
                "message": (
                    "Spec is locked by a passed validation; scenarios cannot be "
                    "deleted. Move the spec back to draft/approved first."
                ),
            }
        )
    except ValueError as exc:
        return json.dumps({"error": "scenario_not_found", "message": str(exc)})

    return json.dumps(
        {
            "success": True,
            "scenario_id": result["scenario_id"],
            "cards_unlinked": result["cards_unlinked"],
        }
    )


_LINK_TASK_TARGET_TYPES = (
    "scenario",
    "fr",
    "rule",
    "decision",
    "tr",
    "contract",
    "ir",
    "or",
    "spec",
)
_LINK_TASK_TARGET_ALIASES = {
    "test_scenario": "scenario",
    "functional_requirement": "fr",
    "business_rule": "rule",
    "technical_requirement": "tr",
    "api_contract": "contract",
    "integration_requirement": "ir",
    "observability_requirement": "or",
}
_LINK_TASK_ACCEPTED_TARGET_TYPES = _LINK_TASK_TARGET_TYPES + tuple(
    _LINK_TASK_TARGET_ALIASES
)


@mcp.tool()
async def okto_pulse_link_task(
    board_id: str,
    target_type: str,
    target_id: str,
    card_id: str,
    spec_id: str = "",
) -> str:
    """Generic task-linking tool — dispatches on `target_type`; short codes
    (fr, tr, ir, or) and the long names (functional_requirement,
    technical_requirement, integration_requirement,
    observability_requirement) are accepted. Replaces the former per-type
    task-linking tools plus direct Functional Requirement linking in a single
    entry point. Note: direct FR task links are traceability links — the FR
    coverage gate is satisfied by Business Rules linked to FRs, not by FR
    linked_task_ids.
    """
    target_type = (target_type or "").strip().lower()
    target_type = _LINK_TASK_TARGET_ALIASES.get(target_type, target_type)
    if target_type not in _LINK_TASK_TARGET_TYPES:
        return json.dumps(
            {
                "error": f"Unknown target_type '{target_type}'. Must be one of: {', '.join(_LINK_TASK_ACCEPTED_TARGET_TYPES)}"
            }
        )
    # Dispatch to internal helpers (no @mcp.tool() decoration — see commit
    # removing 8 link_task_to_* shims in favor of this unified entry point).
    if target_type == "spec":
        return await _link_card_to_spec_internal(board_id, target_id, card_id)
    if not spec_id:
        return json.dumps(
            {"error": f"spec_id is required when target_type='{target_type}'"}
        )
    if target_type == "scenario":
        return await _link_task_to_scenario_internal(
            board_id, spec_id, target_id, card_id
        )
    if target_type == "fr":
        return await _link_task_to_fr_internal(board_id, spec_id, target_id, card_id)
    if target_type == "rule":
        return await _link_task_to_rule_internal(board_id, spec_id, target_id, card_id)
    if target_type == "decision":
        return await _link_task_to_decision_internal(
            board_id, spec_id, target_id, card_id
        )
    if target_type == "tr":
        return await _link_task_to_tr_internal(board_id, spec_id, target_id, card_id)
    if target_type == "contract":
        return await _link_task_to_contract_internal(
            board_id, spec_id, target_id, card_id
        )
    if target_type == "ir":
        return await _link_task_to_integration_requirement_internal(
            board_id, spec_id, target_id, card_id
        )
    if target_type == "or":
        return await _link_task_to_observability_requirement_internal(
            board_id, spec_id, target_id, card_id
        )
    return json.dumps(
        {"error": f"Internal dispatch error for target_type '{target_type}'"}
    )


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

    from okto_pulse.core.application.use_cases import (
        LinkTaskToScenarioCommand,
        LinkTaskToScenarioUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await LinkTaskToScenarioUseCase().execute(
                LinkTaskToScenarioCommand(spec_id, scenario_id, card_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as e:
        if e.entity_type == "spec":
            return json.dumps({"error": "Spec not found"})
        if e.entity_type == "card":
            return json.dumps(
                {
                    "error": f"Card '{card_id}' not found — cannot link a non-existent card."
                }
            )
        if e.entity_type == "scenario":
            return json.dumps({"error": f"Scenario '{scenario_id}' not found in spec."})
        return MCPAdapterContract.error(e)
    except CardOperationError as e:
        return json.dumps({"error": e.code, **e.to_dict(), **e.facts})
    except ValueError as e:
        return json.dumps({"error": str(e)})

    return json.dumps(
        {
            "success": True,
            "scenario_id": result.scenario_id,
            "card_id": result.card_id,
            **_saturation_or_coverage(result.coverage),
        }
    )


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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        # Verify card exists
        card_service = uow.services.cards
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

        _, err = await _safe_spec_update(
            spec_service, spec_id, ctx.agent_id, SpecUpdate(business_rules=rules)
        )
        if err:
            return err
        await uow.commit()

        cov = _spec_coverage(spec, rules=rules)
        return json.dumps(
            {
                "success": True,
                "rule_id": rule_id,
                "card_id": card_id,
                **_saturation_or_coverage(cov),
            }
        )


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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = uow.services.cards
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        frs = [
            dict(fr) if isinstance(fr, dict) else fr
            for fr in (spec.functional_requirements or [])
        ]
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
            return json.dumps(
                {
                    "error": f"Functional requirement '{fr_id}' not found in spec. "
                    f"FRs may be in legacy string format — update the spec via "
                    f"okto_pulse_update_spec to convert them to objects with IDs."
                }
            )

        from okto_pulse.core.models.schemas import SpecUpdate

        traceability_only = False
        link_changed = True
        try:
            _, err = await _safe_spec_update(
                spec_service,
                spec_id,
                ctx.agent_id,
                SpecUpdate(functional_requirements=frs),
            )
            if err:
                return err
        except SpecLockedError:
            try:
                (
                    locked_spec,
                    link_changed,
                    task_ids,
                ) = await spec_service.append_locked_traceability_task_link(
                    spec_id,
                    ctx.agent_id,
                    target_field="functional_requirements",
                    target_id=fr_id,
                    card_id=card_id,
                )
            except ValueError as exc:
                return json.dumps(
                    {"error": "traceability_link_rejected", "detail": str(exc)}
                )
            if locked_spec is None:
                return json.dumps({"error": "Spec not found"})
            traceability_only = True
        await uow.commit()

        cov = _spec_coverage(spec)
        return json.dumps(
            {
                "success": True,
                "fr_id": fr_id,
                "card_id": card_id,
                "traceability_only": traceability_only,
                "link_changed": link_changed,
                "coverage_note": (
                    "Direct FR task link persisted. The FR coverage gate is still "
                    "computed from business_rules[].linked_requirements."
                ),
                **_saturation_or_coverage(cov),
            }
        )


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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = uow.services.cards
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
            return json.dumps(
                {"error": f"API contract '{contract_id}' not found in spec"}
            )

        from okto_pulse.core.models.schemas import SpecUpdate

        _, err = await _safe_spec_update(
            spec_service, spec_id, ctx.agent_id, SpecUpdate(api_contracts=contracts)
        )
        if err:
            return err
        await uow.commit()

        cov = _spec_coverage(spec, contracts=contracts)
        return json.dumps(
            {
                "success": True,
                "contract_id": contract_id,
                "card_id": card_id,
                **_saturation_or_coverage(cov),
            }
        )


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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = uow.services.cards
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        trs = [
            dict(tr) if isinstance(tr, dict) else tr
            for tr in (spec.technical_requirements or [])
        ]
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
            return json.dumps(
                {
                    "error": f"Technical requirement '{tr_id}' not found in spec. "
                    f"TRs may be in legacy string format — update the spec via "
                    f"okto_pulse_update_spec to convert them to objects with IDs."
                }
            )

        from okto_pulse.core.models.schemas import SpecUpdate

        traceability_only = False
        link_changed = True
        try:
            _, err = await _safe_spec_update(
                spec_service,
                spec_id,
                ctx.agent_id,
                SpecUpdate(technical_requirements=trs),
            )
            if err:
                return err
        except SpecLockedError:
            try:
                (
                    locked_spec,
                    link_changed,
                    task_ids,
                ) = await spec_service.append_locked_traceability_task_link(
                    spec_id,
                    ctx.agent_id,
                    target_field="technical_requirements",
                    target_id=tr_id,
                    card_id=card_id,
                )
            except ValueError as exc:
                return json.dumps(
                    {"error": "traceability_link_rejected", "detail": str(exc)}
                )
            if locked_spec is None:
                return json.dumps({"error": "Spec not found"})
            traceability_only = True
        await uow.commit()

        cov = _spec_coverage(spec, trs=trs)
        return json.dumps(
            {
                "success": True,
                "tr_id": tr_id,
                "card_id": card_id,
                "traceability_only": traceability_only,
                "link_changed": link_changed,
                **_saturation_or_coverage(cov),
            }
        )


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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.archives
        try:
            counts = await service.archive_tree(entity_type, entity_id)
        except ValueError as e:
            return json.dumps({"error": str(e)})

        board_service = uow.services.boards
        await board_service._log_activity(
            board_id=board_id,
            card_id=None,
            action="tree_archived",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={
                "entity_type": entity_type,
                "entity_id": entity_id,
                "counts": counts,
            },
        )
        await uow.commit()

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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.archives
        try:
            counts = await service.restore_tree(entity_type, entity_id)
        except ValueError as e:
            return json.dumps({"error": str(e)})

        board_service = uow.services.boards
        await board_service._log_activity(
            board_id=board_id,
            card_id=None,
            action="tree_restored",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={
                "entity_type": entity_type,
                "entity_id": entity_id,
                "counts": counts,
            },
        )
        await uow.commit()

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

    from okto_pulse.core.application.use_cases.architecture_crud import (
        ListArchitectureCommand,
        ListArchitectureUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await ListArchitectureUseCase().execute(
                ListArchitectureCommand(
                    parent_type,
                    parent_id,
                    include_payloads=_flag_enabled(include_payloads),
                    board_id=board_id,
                ),
                actor=actor,
                uow=uow,
            )
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    except EntityNotFoundError:
        return json.dumps({"error": f"{parent_type} not found"})
    payload = [_dump_model(item) for item in result.summaries]
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

    from okto_pulse.core.application.use_cases.architecture_crud import (
        ArchitecturePropagationLegacyReportCommand,
        ArchitecturePropagationLegacyReportUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ArchitecturePropagationLegacyReportUseCase().execute(
            ArchitecturePropagationLegacyReportCommand(
                board_id,
                limit=limit,
                offset=offset,
                include_clean=_flag_enabled(include_clean),
                parent_type_filter=parent_type_filter,
                surface="mcp",
            ),
            actor=actor,
            uow=uow,
        )
    return json.dumps({"success": True, **result.report}, default=str)


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

    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await GetArchitectureDesignUseCase().execute(
                GetArchitectureDesignCommand(
                    design_id,
                    include_payloads=_flag_enabled(include_payloads),
                    board_id=board_id,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Architecture design not found"})
    action = "render" if _flag_enabled(include_payloads) else "read"
    parent_type = getattr(result.response, "parent_type", None)
    perm_err = _mcp_check_architecture_permission(ctx.permissions, parent_type, action)
    if perm_err:
        return _perm_error(perm_err)
    return json.dumps(
        {"success": True, "architecture_design": _dump_model(result.response)},
        default=str,
    )


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

    return json.dumps(
        {"success": True, "schema": architecture_design_payload_schema()}, default=str
    )


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
    for field_name, raw in (
        ("entities", entities),
        ("interfaces", interfaces),
        ("diagrams", diagrams),
    ):
        parsed, err = _parse_json_arg(raw, None)
        if err:
            return json.dumps({"error": f"Invalid {field_name}: {err}"})
        if parsed is not None:
            parsed_fields[field_name] = parsed
    acknowledgement, err = _parse_json_arg(architecture_warning_acknowledgement, None)
    if err:
        return json.dumps(
            {"error": f"Invalid architecture_warning_acknowledgement: {err}"}
        )

    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
        McpValidateArchitecturePayloadCommand,
        McpValidateArchitecturePayloadUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ConflictError,
        EntityNotFoundError,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            if design_id:
                design_result = await GetArchitectureDesignUseCase().execute(
                    GetArchitectureDesignCommand(
                        design_id, include_payloads=False, board_id=board_id
                    ),
                    actor=actor,
                    uow=uow,
                )
                perm_err = _mcp_check_architecture_permission(
                    ctx.permissions, design_result.response.parent_type, "edit"
                )
                if perm_err:
                    return _perm_error(perm_err)
            else:
                if not parent_type or not parent_id:
                    return json.dumps(
                        {
                            "error": (
                                "parent_type and parent_id are required when "
                                "design_id is omitted"
                            )
                        }
                    )
                perm_err = _mcp_check_architecture_permission(
                    ctx.permissions, parent_type, "create"
                )
                if perm_err:
                    return _perm_error(perm_err)
            result = await McpValidateArchitecturePayloadUseCase().execute(
                McpValidateArchitecturePayloadCommand(
                    board_id=board_id,
                    parent_type=parent_type,
                    parent_id=parent_id,
                    design_id=design_id,
                    title=title,
                    global_description=global_description,
                    parsed_fields=parsed_fields,
                    architecture_warning_acknowledgement=acknowledgement,
                    commit_requested=commit,
                    include_design=include_design,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "Architecture design":
            return json.dumps({"error": "Architecture design not found"})
        return json.dumps({"error": f"{exc.entity_type} not found"})
    except ConflictError as exc:
        return _mcp_architecture_conflict_error(exc)
    except ValueError as exc:
        return _mcp_architecture_error(exc)

    return json.dumps(_dump_model(result.payload), default=str)


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
    """Create an Architecture Design on an ideation, refinement, or spec (card
    designs are read-only governed snapshots — use
    okto_pulse_copy_architecture_to_card to refresh card context). Use
    whenever the artifact benefits from explicit architecture (services,
    modules, databases, queues, events, integrations, contracts, data flows,
    ownership). For non-trivial payloads call
    okto_pulse_get_architecture_design_schema then
    okto_pulse_validate_architecture_design_payload and persist only after
    valid=true + reviewed warnings; the server critiques the full payload
    before accepting — fix cited fields and retry rather than hiding
    architecture in prose. Docs: okto-pulse://reference/tool-docs/architecture.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = _mcp_check_architecture_permission(
        ctx.permissions, parent_type, "create"
    )
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
        return json.dumps(
            {"error": f"Invalid architecture_warning_acknowledgement: {err}"}
        )

    from okto_pulse.core.application.use_cases.architecture_crud import (
        CreateArchitectureCommand,
        CreateArchitectureUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ConflictError,
        EntityNotFoundError,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await CreateArchitectureUseCase().execute(
                CreateArchitectureCommand(
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
                    board_id=board_id,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        return json.dumps({"error": f"{exc.entity_type} not found"})
    except ConflictError as exc:
        return _mcp_architecture_conflict_error(exc)
    except Exception as exc:
        return _mcp_architecture_error(exc)
    return json.dumps(
        {"success": True, "architecture_design": _dump_model(result.response)},
        default=str,
    )


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
    for field_name, raw in (
        ("entities", entities),
        ("interfaces", interfaces),
        ("diagrams", diagrams),
    ):
        parsed, err = _parse_json_arg(raw, None)
        if err:
            return json.dumps({"error": f"Invalid {field_name}: {err}"})
        if parsed is not None:
            patch[field_name] = parsed
    if change_summary:
        patch["change_summary"] = change_summary
    acknowledgement, err = _parse_json_arg(architecture_warning_acknowledgement, None)
    if err:
        return json.dumps(
            {"error": f"Invalid architecture_warning_acknowledgement: {err}"}
        )
    if acknowledgement is not None:
        patch["architecture_warning_acknowledgement"] = acknowledgement
    if not patch:
        return json.dumps({"error": "No fields provided for update"})

    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
        UpdateArchitectureDesignCommand,
        UpdateArchitectureDesignUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ConflictError,
        EntityNotFoundError,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            design_result = await GetArchitectureDesignUseCase().execute(
                GetArchitectureDesignCommand(design_id, board_id=board_id),
                actor=actor,
                uow=uow,
            )
            perm_err = _mcp_check_architecture_permission(
                ctx.permissions, design_result.response.parent_type, "edit"
            )
            if perm_err:
                return _perm_error(perm_err)
            result = await UpdateArchitectureDesignUseCase().execute(
                UpdateArchitectureDesignCommand(
                    design_id, ArchitectureDesignUpdate(**patch), board_id=board_id
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "Architecture design":
            return json.dumps({"error": "Architecture design not found"})
        return _mcp_entity_not_found_error(exc)
    except ConflictError as exc:
        return _mcp_architecture_conflict_error(exc)
    except Exception as exc:
        return _mcp_architecture_error(exc)
    return json.dumps(
        {"success": True, "architecture_design": _dump_model(result.response)},
        default=str,
    )


@mcp.tool()
async def okto_pulse_delete_architecture_design(board_id: str, design_id: str) -> str:
    """Delete an Architecture Design. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases.architecture_crud import (
        DeleteArchitectureDesignCommand,
        DeleteArchitectureDesignUseCase,
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ConflictError,
        EntityNotFoundError,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            design_result = await GetArchitectureDesignUseCase().execute(
                GetArchitectureDesignCommand(design_id, board_id=board_id),
                actor=actor,
                uow=uow,
            )
            perm_err = _mcp_check_architecture_permission(
                ctx.permissions, design_result.response.parent_type, "delete"
            )
            if perm_err:
                return _perm_error(perm_err)
            await DeleteArchitectureDesignUseCase().execute(
                DeleteArchitectureDesignCommand(design_id, board_id=board_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "Architecture design":
            return json.dumps({"error": "Architecture design not found"})
        return _mcp_entity_not_found_error(exc)
    except ConflictError as exc:
        return _mcp_architecture_conflict_error(exc)
    return json.dumps({"success": True})


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
        return json.dumps(
            {"error": f"Invalid payload_json: {err or 'payload is required'}"}
        )

    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
        ImportExcalidrawArchitectureDiagramCommand,
        ImportExcalidrawArchitectureDiagramUseCase,
    )
    from okto_pulse.core.application.use_cases.base import (
        ConflictError,
        EntityNotFoundError,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            design_result = await GetArchitectureDesignUseCase().execute(
                GetArchitectureDesignCommand(design_id, board_id=board_id),
                actor=actor,
                uow=uow,
            )
            perm_err = _mcp_check_architecture_permission(
                ctx.permissions, design_result.response.parent_type, "import"
            )
            if perm_err:
                return _perm_error(perm_err)
            result = await ImportExcalidrawArchitectureDiagramUseCase().execute(
                ImportExcalidrawArchitectureDiagramCommand(
                    design_id,
                    title,
                    payload,
                    diagram_type,
                    description or None,
                    order_index,
                    replace_diagram_id or None,
                    change_summary,
                    None,
                    board_id=board_id,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "Diagram":
            return json.dumps({"error": "Diagram not found"})
        return json.dumps({"error": "Architecture design not found"})
    except ConflictError as exc:
        return _mcp_architecture_conflict_error(exc)
    except Exception as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(
        {"success": True, "architecture_design": _dump_model(result.response)},
        default=str,
    )


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

    from okto_pulse.core.application.use_cases.architecture_crud import (
        GetArchitectureDesignCommand,
        GetArchitectureDesignUseCase,
        GetArchitectureDiagramPayloadCommand,
        GetArchitectureDiagramPayloadUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            design_result = await GetArchitectureDesignUseCase().execute(
                GetArchitectureDesignCommand(design_id, board_id=board_id),
                actor=actor,
                uow=uow,
            )
            perm_err = _mcp_check_architecture_permission(
                ctx.permissions, design_result.response.parent_type, "render"
            )
            if perm_err:
                return _perm_error(perm_err)
            payload_result = await GetArchitectureDiagramPayloadUseCase().execute(
                GetArchitectureDiagramPayloadCommand(
                    design_id, diagram_id, board_id=board_id
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "Architecture design":
            return json.dumps({"error": "Architecture design not found"})
        return json.dumps({"error": "Diagram payload not found"})
    try:
        response = payload_result.response
        adapter = ArchitectureDiagramAdapterRegistry().get(response.format or "raw")
        return json.dumps(
            {
                "success": True,
                "design_id": design_id,
                "diagram_id": diagram_id,
                "format": response.format,
                "payload": response.payload,
                "dump": adapter.dump(response.payload),
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
    profile: Annotated[str, Field(description="summary | full | legacy (copy tools have no detail profile)")] = "summary",
) -> str:
    """Copy Architecture Designs from a spec to a card/task as deep-copy
    snapshots. profile=summary (default) returns copy metadata only (copied,
    design_ids, total_on_card + the projection envelope); full/legacy include
    the complete copied architecture_designs. Bodies are persisted on the
    card regardless of profile — read them with
    okto_pulse_get_task_context(profile=full).
    Profiles: okto-pulse://reference/projection-profiles.
    """
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
        return json.dumps(
            {"error": f"Invalid architecture_warning_acknowledgement: {err}"}
        )

    from okto_pulse.core.application.use_cases.architecture_crud import (
        CopyArchitectureFromSpecToCardCommand,
        CopyArchitectureFromSpecToCardUseCase,
        ListArchitectureCommand,
        ListArchitectureUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.effective_resource_propagation import (
        ResourceLineageResolutionError,
        ResourcePropagationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await CopyArchitectureFromSpecToCardUseCase().execute(
                CopyArchitectureFromSpecToCardCommand(
                    card_id,
                    spec_id,
                    ids,
                    acknowledgement,
                    board_id=board_id,
                ),
                actor=actor,
                uow=uow,
            )
            list_result = await ListArchitectureUseCase().execute(
                ListArchitectureCommand("card", card_id, board_id=board_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        return json.dumps({"error": f"{exc.entity_type} not found"})
    except ResourceLineageResolutionError as exc:
        return json.dumps(exc.to_error_dict())
    except ResourcePropagationError as exc:
        return json.dumps(exc.to_error_dict(spec_id=spec_id))
    except ArchitecturePropagationBlocked as exc:
        return json.dumps(exc.to_error_dict())
    except Exception as exc:
        return _mcp_architecture_error(exc)
    payload = [_dump_model(item) for item in result.responses]
    return json.dumps(
        project_copy_architecture_response(
            payload, total_on_card=len(list_result.summaries), profile=profile
        ),
        default=str,
    )


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

    try:
        id_filter = set(coerce_to_list_str(screen_ids)) if screen_ids else None
    except ValueError as e:
        return json.dumps({"error": f"Invalid screen_ids: {e}"})

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpCopyMockupsToCardCommand,
        McpCopyMockupsToCardUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.effective_resource_propagation import (
        ResourceLineageResolutionError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpCopyMockupsToCardUseCase().execute(
                McpCopyMockupsToCardCommand(board_id, spec_id, card_id, id_filter),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as e:
        return json.dumps(
            {"error": "Spec not found" if e.entity_type == "spec" else "Card not found"}
        )
    except ResourceLineageResolutionError as exc:
        return json.dumps(exc.to_error_dict())

    if result.empty_plan is not None:
        return _effective_empty_copy_response("mockup", result.empty_plan)
    return json.dumps(
        {
            "success": True,
            "copied": result.copied,
            "total_on_card": result.total_on_card,
            "fallback": result.fallback,
        }
    )


def _effective_empty_copy_response(resource_type: str, plan: dict) -> str:
    """R3-IMP2 shared resolution of a copy with no DIRECT and no effective
    fallback resource: an N/A or genuinely-absent resource is an honest empty
    (NOT an error); a resource the gate reports provided but that cannot be
    resolved is an actionable error — never a generic "no resources to copy"."""
    if plan.get("not_applicable"):
        return json.dumps(
            {
                "success": True,
                "copied": 0,
                "reason": "not_applicable",
                "resource_type": resource_type,
            }
        )
    if not plan.get("has_obligation"):
        return json.dumps(
            {
                "success": True,
                "copied": 0,
                "reason": "no_resource_required",
                "resource_type": resource_type,
            }
        )
    return json.dumps(
        {
            "error": "resource_propagation_failed",
            "resource_type": resource_type,
            "coverage_obligation_id": plan.get("coverage_obligation_id"),
            "accepted_identity_fields": plan.get("accepted_identity_fields", []),
            "retryable": True,
            "detail": (
                f"Spec {resource_type} is reported provided by the Resource Gate but no "
                "copyable resource (direct or effective/inherited) could be resolved."
            ),
        }
    )


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
                McpCopyKnowledgeToCardCommand(board_id, spec_id, card_id, id_filter),
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

    return json.dumps(
        {
            "success": True,
            "copied": result.copied,
            "knowledge_ids": result.copied_ids,
            "total_on_card": result.total_on_card,
            "fallback": result.fallback,
        }
    )


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
async def okto_pulse_get_card_knowledge(
    board_id: str, card_id: str, knowledge_id: str
) -> str:
    """Get a single KE by id from a card's inline knowledge_bases array."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpGetCardKnowledgeCommand,
        McpGetCardKnowledgeUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpGetCardKnowledgeUseCase().execute(
                McpGetCardKnowledgeCommand(board_id, card_id, knowledge_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "card":
            return json.dumps({"error": "Card not found"})
        return json.dumps({"error": "Knowledge entry not found"})
    return json.dumps(result.payload, default=str)


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
async def okto_pulse_delete_card_knowledge(
    board_id: str, card_id: str, knowledge_id: str
) -> str:
    """Deprecated: card Knowledge Base resources are read-only governed snapshots."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    return _card_resource_read_only_error()


@mcp.tool()
async def okto_pulse_copy_qa_to_card(board_id: str, spec_id: str, card_id: str) -> str:
    """
    Copy answered Q&A items from a spec to a card as a consolidated comment.
    Only copies Q&As that have been answered — unanswered questions are skipped."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpCopyQaToCardCommand,
        McpCopyQaToCardUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpCopyQaToCardUseCase().execute(
                McpCopyQaToCardCommand(spec_id, card_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as e:
        return json.dumps(
            {"error": "Spec not found" if e.entity_type == "spec" else "Card not found"}
        )
    return json.dumps(result.payload)


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

    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpGetAnalyticsCommand,
        McpGetAnalyticsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpGetAnalyticsUseCase().execute(
            McpGetAnalyticsCommand(
                board_id,
                metric_type=metric_type,
                from_date=from_date,
                to_date=to_date,
            ),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.data, default=str)


@mcp.tool()
async def okto_pulse_list_blockers(
    board_id: str,
    stale_hours: int = 72,
    filter_type: str = "",
) -> str:
    """Triage view of everything stalling the funnel, with root-cause
    classification. Each entry carries a `type` the agent can act on directly:
    dependency_blocked (active card with a not-DONE depends_on target),
    on_hold (explicitly paused card), stale (started/in_progress/validation
    card untouched beyond stale_hours), spec_pending_validation (approved spec
    with no 'approve' evaluation), spec_no_cards (validated/in_progress spec
    with zero linked cards), uncovered_scenario (test scenario with no linked
    test card — the test-coverage gate will fail).
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    if stale_hours < 1:
        return json.dumps({"error": "stale_hours must be >= 1"})

    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpListBlockersCommand,
        McpListBlockersUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListBlockersUseCase().execute(
            McpListBlockersCommand(
                board_id,
                stale_hours=stale_hours,
                filter_type=filter_type or None,
            ),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.data, default=str)


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
        raise ValueError(
            "expected_spec_version must be an integer when provided."
        ) from exc


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
        return json.dumps(
            {
                "error": "api_contract uses the dedicated okto_pulse_update_spec_api_contract wrapper.",
            }
        )
    if (
        entity_type != "api_contract"
        and entity_type not in _STRUCTURED_SPEC_ENTITY_MCP_TYPES
    ):
        return json.dumps(
            {"error": f"Unsupported structured spec entity type: {entity_type}"}
        )

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
                    parse_multi_value(linked_requirements)
                    if linked_requirements
                    else None,
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                    f"Available fr_ids: {_available_fr_ids}. "
                    f"No business rule was appended."
                )
            }
        )

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
                    notes=(
                        notes.replace("\\n", "\n")
                        if (notes and notes != "CLEAR")
                        else ""
                    ),
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid indices: 0..{max(0, len(frs) - 1)}. "
                    f"Available fr_ids: {_available_fr_ids}. "
                    f"No business rule was updated."
                )
            }
        )

    return json.dumps(
        {
            "success": True,
            "business_rule": _r.business_rule,
            "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
            **_saturation_or_coverage(_r.coverage),
        },
        default=str,
    )


@mcp.tool()
async def okto_pulse_remove_business_rule(
    board_id: str,
    spec_id: str,
    rule_id: str,
) -> str:
    """Remove a business rule from a spec. Permanent, no undo — FR->BR coverage
    provided by this rule is lost.
    Docs: okto-pulse://reference/destructive_ops
    """
    return await _remove_spec_entity_impl(
        board_id,
        spec_id,
        "business_rule",
        rule_id,
        alias_kind="legacy",
        tool_name="okto_pulse_remove_business_rule",
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
        return json.dumps(
            {"error": f"Invalid integration_type. Use one of: {sorted(allowed_types)}"}
        )

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
                        parse_multi_value(linked_requirements)
                        if linked_requirements
                        else None
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                    f"Available fr_ids: {_r.available_fr_ids}. "
                    f"Available tr_ids: {_r.available_tr_ids}. "
                    f"No integration requirement was appended."
                )
            }
        )

    return json.dumps(
        {
            "success": True,
            "integration_requirement": _r.requirement,
            **_saturation_or_coverage(_r.coverage),
        },
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        card_service = uow.services.cards
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        requirements = list(getattr(spec, "integration_requirements", None) or [])
        target = next(
            (item for item in requirements if item.get("id") == requirement_id), None
        )
        if target is None:
            return json.dumps(
                {"error": f"Integration requirement '{requirement_id}' not found"}
            )

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
        await uow.commit()

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
        return json.dumps(
            {"error": f"Invalid signal_type. Use one of: {sorted(allowed_types)}"}
        )

    import uuid as _uuid

    linked_irs_list = None
    if linked_integration_requirements:
        try:
            linked_irs_list = (
                coerce_to_list_str(linked_integration_requirements) or None
            )
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
                        parse_multi_value(linked_requirements)
                        if linked_requirements
                        else None
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                    f"Available fr_ids: {_r.available_fr_ids}. "
                    f"Available tr_ids: {_r.available_tr_ids}. "
                    f"No observability requirement was appended."
                )
            }
        )

    return json.dumps(
        {
            "success": True,
            "observability_requirement": _r.requirement,
            **_saturation_or_coverage(_r.coverage),
        },
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec or spec.board_id != board_id:
            return json.dumps({"error": "Spec not found"})
        card_service = uow.services.cards
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        requirements = list(getattr(spec, "observability_requirements", None) or [])
        target = next(
            (item for item in requirements if item.get("id") == requirement_id), None
        )
        if target is None:
            return json.dumps(
                {"error": f"Observability requirement '{requirement_id}' not found"}
            )

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
        await uow.commit()

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
    """Add a formalized Decision to a spec.

    A Decision records a contextual CHOICE — the reasoning behind picking one
    path over alternatives. Different from BusinessRule (which is a NORM, a
    prescriptive "MUST" statement): use a Decision to capture design
    intent, tradeoffs, or team consensus. The KG extracts Decisions into
    queryable nodes, and the optional coverage gate (opt-in) can require each
    Decision to have >=1 linked task. linked_requirements accepts FR refs and
    structured TR refs.
    """
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
                        parse_multi_value(linked_requirements)
                        if linked_requirements
                        else None
                    ),
                    notes=notes.replace("\\n", "\n") if notes else None,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Spec not found"})

    if _r.unresolved_tokens is not None:
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                    f"Available fr_ids: {_r.available_fr_ids}. "
                    f"Available tr_ids: {_r.available_tr_ids}. "
                    f"No decision was appended."
                )
            }
        )
    if _r.supersede_not_found is not None:
        return json.dumps(
            {
                "error": f"supersedes_decision_id '{_r.supersede_not_found}' "
                f"not found in spec.decisions"
            }
        )

    return json.dumps(
        {
            "success": True,
            "decision": _r.decision,
            "decisions_total": _r.decisions_total,
        },
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
                return json.dumps(
                    {"error": "invalid_multi_value_input", "detail": str(e)}
                )
    if notes:
        field_updates["notes"] = (
            None if notes.strip().upper() == "CLEAR" else notes.replace("\\n", "\n")
        )

    supersedes_clear = (
        bool(supersedes_decision_id)
        and supersedes_decision_id.strip().upper() == "CLEAR"
    )
    supersedes_value = (
        supersedes_decision_id
        if (supersedes_decision_id and not supersedes_clear)
        else ""
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
                        parse_multi_value(linked_requirements)
                        if linked_requirements
                        else None
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                    f"Available fr_ids: {_r.available_fr_ids}. "
                    f"Available tr_ids: {_r.available_tr_ids}. "
                    f"No decision was updated."
                )
            }
        )
    if _r.invalid_status is not None:
        return json.dumps(
            {
                "error": f"Invalid status '{_r.invalid_status}'. Use active/superseded/revoked."
            }
        )

    return json.dumps(
        {
            "success": True,
            "decision": _r.decision,
            "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
        },
        default=str,
    )


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
        board_id,
        spec_id,
        "decision",
        decision_id,
        alias_kind="legacy",
        tool_name="okto_pulse_remove_decision",
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        spec_service = uow.services.specs
        spec = await spec_service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        card_service = uow.services.cards
        card = await card_service.get_card(card_id)
        if not card:
            return json.dumps({"error": "Card not found"})

        decisions = [
            dict(decision) if isinstance(decision, dict) else decision
            for decision in (spec.decisions or [])
        ]
        target = next((d for d in decisions if d.get("id") == decision_id), None)
        if target is None:
            return json.dumps({"error": f"Decision '{decision_id}' not found"})

        task_ids = list(target.get("linked_task_ids") or [])
        if card_id not in task_ids:
            task_ids.append(card_id)
        target["linked_task_ids"] = task_ids

        from okto_pulse.core.models.schemas import SpecUpdate

        traceability_only = False
        link_changed = True
        try:
            _, _err = await _safe_spec_update(
                spec_service,
                spec_id,
                ctx.agent_id,
                SpecUpdate(decisions=decisions),
            )
            if _err:
                return _err
        except SpecLockedError:
            try:
                (
                    locked_spec,
                    link_changed,
                    task_ids,
                ) = await spec_service.append_locked_traceability_task_link(
                    spec_id,
                    ctx.agent_id,
                    target_field="decisions",
                    target_id=decision_id,
                    card_id=card_id,
                )
            except ValueError as exc:
                return json.dumps(
                    {"error": "traceability_link_rejected", "detail": str(exc)}
                )
            if locked_spec is None:
                return json.dumps({"error": "Spec not found"})
            traceability_only = True
        await uow.commit()

        cov = _spec_coverage(spec, decisions=decisions)
        return json.dumps(
            {
                "success": True,
                "decision_id": decision_id,
                "card_id": card_id,
                "linked_tasks": task_ids,
                "traceability_only": traceability_only,
                "link_changed": link_changed,
                **_saturation_or_coverage(cov),
            }
        )


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
        return json.dumps(
            {
                "success": True,
                "decisions_added": 0,
                "context_modified": False,
                "note": "No '## Decisions' block found — nothing to migrate.",
            }
        )

    return json.dumps(
        {
            "success": True,
            "decisions_added": _r.decisions_added,
            "context_modified": _r.context_modified,
            "added": _r.added,
        }
    )


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
                _structured_ref_id(frs[i]) or str(i) for i in idxs if 0 <= i < len(frs)
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
        return json.dumps(
            {
                "spec_id": spec_id,
                "total": len(result),
                "business_rules": result,
            },
            default=str,
        )


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
    """Add an API contract to a spec. API contracts define endpoints,
    request/response shapes, and link to FR/TR requirements and business
    rules. method must be an HTTP verb (GET/POST/..., upper-cased); for
    non-HTTP contracts pass method=TOOL/COMPONENT/EVENT, which infers
    contract_type. Docs: okto-pulse://reference/tool-docs/api-contract.
    """
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
                        parse_multi_value(linked_requirements)
                        if linked_requirements
                        else None
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                    f"Available fr_ids: {_r.available_fr_ids}. "
                    f"Available tr_ids: {_r.available_tr_ids}. "
                    f"No API contract was appended."
                )
            }
        )
    if _r.bad_rule_token is not None:
        return json.dumps(
            {"error": f"Business rule '{_r.bad_rule_token}' not found in spec"}
        )
    if _r.invalid_contract_exc is not None:
        return _canonical_api_contract_error(_r.invalid_contract_exc)

    return json.dumps(
        {
            "success": True,
            "api_contract": _r.contract,
            **_saturation_or_coverage(_r.coverage),
        },
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
        return json.dumps(
            {
                "error": (
                    f"Unresolved linked_requirements token(s): {_r.unresolved_tokens}. "
                    f"Valid FR indices: 0..{max(0, _r.fr_count - 1)}. "
                    f"Available fr_ids: {_r.available_fr_ids}. "
                    f"Available tr_ids: {_r.available_tr_ids}. "
                    f"No API contract was updated."
                )
            }
        )
    if _r.bad_rule_token is not None:
        return json.dumps(
            {"error": f"Business rule '{_r.bad_rule_token}' not found in spec"}
        )
    if _r.invalid_contract_exc is not None:
        return _canonical_api_contract_error(_r.invalid_contract_exc)

    return json.dumps(
        {
            "success": True,
            "api_contract": _r.contract,
            "deprecation_warning": _STRUCTURED_SPEC_ENTITY_LEGACY_WARNING,
        },
        default=str,
    )


@mcp.tool()
async def okto_pulse_remove_api_contract(
    board_id: str,
    spec_id: str,
    contract_id: str,
) -> str:
    """Remove an API contract from a spec. Permanent, no undo — contract
    coverage links are lost. Docs: okto-pulse://reference/destructive_ops
    """
    return await _remove_spec_entity_impl(
        board_id,
        spec_id,
        "api_contract",
        contract_id,
        alias_kind="legacy",
        tool_name="okto_pulse_remove_api_contract",
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
        return json.dumps(
            {
                "error": "unsupported_target_type",
                "message": type_err,
                "allowed": list(fam.target_types) if fam else [],
            }
        )

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
    return json.dumps(
        {
            "success": True,
            "removed": entity_id,
            "remaining": _r.remaining,
            **_saturation_or_coverage(_r.coverage),
        }
    )


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
        board_id,
        spec_id,
        target_type,
        entity_id,
        alias_kind="consolidated",
        tool_name="okto_pulse_remove_spec_entity",
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
                resolved_tr_ids, unresolved_trs = resolve_linked_requirements_to_ids(
                    [ref], trs
                )
                if resolved_tr_ids:
                    for tr_id in resolved_tr_ids:
                        if tr_id not in tr_ids:
                            tr_ids.append(tr_id)
                else:
                    unresolved.extend(unresolved_trs)
            tr_text_by_id = {
                _structured_ref_id(tr)
                or _structured_ref_text(tr): _structured_ref_text(tr)
                for tr in trs
            }
            entry["linked_requirements"] = [
                _structured_ref_id(frs[i]) or str(i) for i in idxs if 0 <= i < len(frs)
            ] + tr_ids
            entry["resolved_requirements"] = [
                f"[FR-{i}] {_structured_ref_text(frs[i])}"
                for i in idxs
                if 0 <= i < len(frs)
            ] + [f"[TR-{tr_id}] {tr_text_by_id.get(tr_id, '')}" for tr_id in tr_ids]
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
        return json.dumps(
            {
                "spec_id": spec_id,
                "total": len(result),
                "api_contracts": result,
            },
            default=str,
        )


# ==================== SCREEN MOCKUP TOOLS ====================


SCREEN_MOCKUP_ENTITY_TYPES = ("spec", "ideation", "refinement", "card", "story")


def _validate_screen_mockup_entity_type(entity_type: str) -> str | None:
    if entity_type in SCREEN_MOCKUP_ENTITY_TYPES:
        return None
    allowed = ", ".join(SCREEN_MOCKUP_ENTITY_TYPES)
    return f"Invalid entity_type '{entity_type}'. Must be one of: {allowed}"


def _sanitize_html(html: str) -> str:
    import re

    sanitized = re.sub(r"<script[\s\S]*?</script>", "", html, flags=re.IGNORECASE)
    sanitized = re.sub(
        r"\s+on\w+\s*=\s*[\"'][^\"']*[\"']", "", sanitized, flags=re.IGNORECASE
    )
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
    """Add a screen mockup (HTML+Tailwind, rendered in the dashboard) to a spec,
    ideation, refinement, or story. Card mockups are read-only governed
    snapshots — use okto_pulse_copy_mockups_to_card to refresh card context.
    design_system_ref / design_system_version / design_system_evidence feed
    the MockupDesignSystemGate: when the board has an effective Design System
    and design_system_gate_mode=blocking, an invalid/missing ref is rejected
    BEFORE persistence; advisory persists with a design_system_gate warning;
    off / no Design System does not block.
    Errors: design_system_required, design_system_not_found,
    design_system_version_mismatch, design_system_evidence_missing.
    """
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

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpAddScreenMockupUseCase,
        McpScreenMockupCommand,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpAddScreenMockupUseCase().execute(
                McpScreenMockupCommand(
                    board_id=board_id,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    title=title,
                    description=description,
                    screen_type=screen_type,
                    html_content=html_content,
                    design_system_ref=design_system_ref,
                    design_system_version=design_system_version,
                    design_system_evidence=design_system_evidence,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpScreenMockupCommand,
        McpUpdateScreenMockupUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpUpdateScreenMockupUseCase().execute(
                McpScreenMockupCommand(
                    board_id=board_id,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    screen_id=screen_id,
                    title=title,
                    description=description,
                    screen_type=screen_type,
                    html_content=html_content,
                    design_system_ref=design_system_ref,
                    design_system_version=design_system_version,
                    design_system_evidence=design_system_evidence,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "screen":
            return json.dumps({"error": f"Screen '{screen_id}' not found"})
        return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpAnnotateMockupUseCase,
        McpScreenMockupCommand,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpAnnotateMockupUseCase().execute(
                McpScreenMockupCommand(
                    board_id=board_id,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    screen_id=screen_id,
                    text=text,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "screen":
            return json.dumps({"error": f"Screen '{screen_id}' not found"})
        return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(result.payload)


@mcp.tool()
async def okto_pulse_list_screen_mockups(
    board_id: str,
    entity_id: str,
    entity_type: str = "spec",
    screen_type: str = "",
    offset: int = 0,
    limit: int = 50,
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

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpListScreenMockupsUseCase,
        McpScreenMockupCommand,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpListScreenMockupsUseCase().execute(
                McpScreenMockupCommand(
                    board_id=board_id,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    screen_type=screen_type,
                    offset=offset,
                    limit=limit,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpDeleteScreenMockupUseCase,
        McpScreenMockupCommand,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpDeleteScreenMockupUseCase().execute(
                McpScreenMockupCommand(
                    board_id=board_id,
                    entity_id=entity_id,
                    entity_type=entity_type,
                    screen_id=screen_id,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "screen":
            return json.dumps({"error": f"Screen '{screen_id}' not found"})
        return json.dumps({"error": f"{entity_type.title()} '{entity_id}' not found"})
    except ValueError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(result.payload)


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
        return json.dumps(
            {"board_id": board_id, "count": len(items), "guidelines": items},
            default=str,
        )


@mcp.tool()
async def okto_pulse_list_guidelines(
    board_id: str,
    offset: str = "0",
    limit: str = "50",
    tag: str = "",
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

    from okto_pulse.core.application.use_cases import (
        ListGuidelinesCommand,
        ListGuidelinesUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await ListGuidelinesUseCase().execute(
            ListGuidelinesCommand(
                offset=int(offset),
                limit=int(limit),
                tag=tag or None,
            ),
            actor=actor,
            uow=uow,
        )
        guidelines = result.guidelines
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
    board_id: str,
    title: str,
    content: str,
    tags: list[str] | str = "",
    scope: str = "global",
) -> str:
    """
    Create a new guideline. If scope is "global", it goes into the catalog and can be
    linked to any board. If scope is "inline", set a board_id to make it board-specific."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(
        ctx.permissions,
        Permissions.SPECS_UPDATE
        if hasattr(Permissions, "BOARD_UPDATE")
        else Permissions.BOARD_READ,
    )
    if perm_err:
        return _perm_error(perm_err)

    try:
        tag_list = coerce_to_list_str(tags) or None
    except ValueError as e:
        return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})

    from okto_pulse.core.application.use_cases import (
        CreateGuidelineCommand,
        CreateGuidelineUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.models.schemas import GuidelineCreate

    data = GuidelineCreate(
        title=title,
        content=content,
        tags=tag_list,
        scope=scope,
        board_id=board_id if scope == "inline" else None,
    )
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await CreateGuidelineUseCase().execute(
                CreateGuidelineCommand(data),
                actor=actor,
                uow=uow,
            )
            guideline = result.guideline
    except EntityNotFoundError:
        return json.dumps({"error": "Board not found"})

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
    board_id: str,
    guideline_id: str,
    title: str = "",
    content: str = "",
    tags: list[str] | str = "",
) -> str:
    """
    Update a guideline's title, content, or tags."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(
        ctx.permissions,
        Permissions.SPECS_UPDATE
        if hasattr(Permissions, "BOARD_UPDATE")
        else Permissions.BOARD_READ,
    )
    if perm_err:
        return _perm_error(perm_err)

    if tags:
        try:
            tags_list = coerce_to_list_str(tags) or None
        except ValueError as e:
            return json.dumps({"error": "invalid_multi_value_input", "detail": str(e)})
    else:
        tags_list = None

    from okto_pulse.core.application.use_cases import (
        UpdateGuidelineCommand,
        UpdateGuidelineUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.models.schemas import GuidelineUpdate

    data = GuidelineUpdate(
        title=title or None,
        content=content or None,
        tags=tags_list,
    )
    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await UpdateGuidelineUseCase().execute(
                UpdateGuidelineCommand(guideline_id, data),
                actor=actor,
                uow=uow,
            )
            guideline = result.guideline
    except EntityNotFoundError:
        return json.dumps({"error": "Guideline not found or not owned by actor"})

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
    """Delete a guideline. Permanent, no undo — also removes all board links in
    cascade. Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(
        ctx.permissions,
        Permissions.SPECS_UPDATE
        if hasattr(Permissions, "BOARD_UPDATE")
        else Permissions.BOARD_READ,
    )
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        DeleteGuidelineCommand,
        DeleteGuidelineUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            await DeleteGuidelineUseCase().execute(
                DeleteGuidelineCommand(guideline_id),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Guideline not found or not owned by actor"})

    return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_link_guideline_to_board(
    board_id: str,
    guideline_id: str,
    priority: str = "0",
) -> str:
    """
    Link a global guideline to a board so agents see it when loading board guidelines."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(
        ctx.permissions,
        Permissions.SPECS_UPDATE
        if hasattr(Permissions, "BOARD_UPDATE")
        else Permissions.BOARD_READ,
    )
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
                McpLinkGuidelineToBoardCommand(board_id, guideline_id, int(priority)),
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
async def okto_pulse_unlink_guideline_from_board(
    board_id: str, guideline_id: str
) -> str:
    """
    Unlink a guideline from a board. The guideline itself is not deleted."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(
        ctx.permissions,
        Permissions.SPECS_UPDATE
        if hasattr(Permissions, "BOARD_UPDATE")
        else Permissions.BOARD_READ,
    )
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
async def okto_pulse_update_board_guideline_priority(
    board_id: str,
    guideline_id: str,
    priority: str,
) -> str:
    """
    Update the priority of a guideline linked to a board."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(
        ctx.permissions,
        Permissions.SPECS_UPDATE
        if hasattr(Permissions, "BOARD_UPDATE")
        else Permissions.BOARD_READ,
    )
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        UpdateBoardGuidelinePriorityCommand,
        UpdateBoardGuidelinePriorityUseCase,
    )
    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    try:
        priority_value = int(priority)
    except (TypeError, ValueError):
        return json.dumps(
            {"error": "invalid_priority", "detail": "priority must be an integer"}
        )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            await UpdateBoardGuidelinePriorityUseCase().execute(
                UpdateBoardGuidelinePriorityCommand(
                    board_id,
                    guideline_id,
                    priority_value,
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Link not found"})
    return json.dumps(
        {
            "success": True,
            "board_id": board_id,
            "guideline_id": guideline_id,
            "priority": priority_value,
        }
    )


@mcp.tool()
async def okto_pulse_delete_spec(board_id: str, spec_id: str) -> str:
    """Delete a spec. Permanent, no undo — derived cards are unlinked but not
    deleted. Docs: okto-pulse://reference/destructive_ops
    """
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.specs
        linked = await service.link_card(spec_id, card_id, user_id=ctx.agent_id)
        await uow.commit()

        if not linked:
            return json.dumps(
                {"error": "Spec or card not found, or they belong to different boards"}
            )

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
    async with get_unit_of_work_factory_for_mcp()() as uow:
        from okto_pulse.core.services.critical_context_guard import (
            FullContextGuardError,
        )
        from okto_pulse.core.services.main import SpecService

        service = uow.services.specs
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
            await uow.commit()
            return json.dumps(
                {
                    "error": str(exc),
                    "reason": exc.reason,
                    "decision": exc.decision.audit_details(),
                }
            )
        except ValueError as exc:
            return json.dumps({"error": str(exc)})
        # R4-IMP1: evaluation is append-only — capture status AFTER independently;
        # the envelope reports both (state_changed=false in the happy path) and the
        # operator's next step (move_spec to in_progress), never an auto-transition.
        spec_after = await service.get_spec(spec_id)
        status_after = spec_after.status.value if spec_after else status_before
        await uow.commit()

    return json.dumps(
        spec_evaluation_success_envelope(
            spec_id=spec_id,
            status_before=status_before,
            status_after=status_after,
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        from okto_pulse.core.services.main import SpecService

        service = uow.services.specs
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
            "rejections": len(
                [e for e in non_stale if e.get("recommendation") == "reject"]
            ),
            "request_changes": len(
                [e for e in non_stale if e.get("recommendation") == "request_changes"]
            ),
            "avg_score_approvals": (
                sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
                if approvals
                else 0
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        from okto_pulse.core.services.main import SpecService

        service = uow.services.specs
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        for e in spec.evaluations or []:
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        from okto_pulse.core.services.main import SpecService

        service = uow.services.specs
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
            return json.dumps(
                {
                    "error": "Cannot delete evaluation: you can only delete your own evaluations"
                }
            )

        evaluations.remove(target)
        spec.evaluations = evaluations
        await uow.commit()

    return json.dumps({"success": True, "deleted_evaluation_id": evaluation_id})


# SPEC HISTORY TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_spec_history(
    board_id: str, spec_id: str, limit: str = "30"
) -> str:
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
async def okto_pulse_ask_spec_question(
    board_id: str, spec_id: str, question: str
) -> str:
    """
    Ask a question on a spec's Q&A board. Use @Name to direct the question.
    Both humans and agents can ask questions — this is for clarifying spec requirements
    BEFORE work begins on tasks."""
    return await _ask_question_impl(
        board_id,
        "spec",
        spec_id,
        question,
        alias_kind="legacy",
        tool_name="okto_pulse_ask_spec_question",
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
    """Ask a choice question (poll/form) on a spec's Q&A board — the respondent
    picks from predefined options. Use for structured answers, e.g. "Which
    auth approach?". options_json (optional, takes precedence over options):
    JSON array of option objects, each requiring a non-empty label;
    recommended defaults to false, tradeoff to null. Multi-value params
    (options/selected): JSON array (preferred — safe for labels containing
    commas) or pipe-separated string. Format rules:
    okto-pulse://reference/multivalue.
    """
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
            SpecQAChoiceOption(
                id=f"opt_{i}",
                label=obj["label"],
                recommended=obj["recommended"],
                tradeoff=obj["tradeoff"],
            )
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.spec_qa
        data = SpecQACreate(
            question=question,
            question_type=question_type
            if question_type in ("choice", "multi_choice")
            else "choice",
            choices=choice_list,
            allow_free_text=allow_free_text.lower() == "true",
        )
        qa = await service.create_question(spec_id, ctx.agent_id, data)
        if not qa:
            return json.dumps({"error": "Spec not found"})

        board_service = uow.services.boards
        await board_service._log_activity(
            board_id=board_id,
            action="spec_choice_question_added",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={
                "spec_id": spec_id,
                "question": question[:100],
                "option_count": len(choice_list),
            },
        )
        await uow.commit()

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
async def okto_pulse_answer_spec_question(
    board_id: str,
    spec_id: str,
    qa_id: str,
    answer: str = "",
    selected: list[str] | str = "",
) -> str:
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.spec_qa
        try:
            qa = await service.answer_question(
                qa_id,
                ctx.agent_id,
                SpecQAAnswer(answer=answer or None, selected=selected_list),
                actor_type="agent",
                surface="mcp",
            )
        except QASelfAnsweringNotAllowedError as e:
            await uow.commit()
            return json.dumps({"error": e.reason, "detail": str(e)})
        if not qa:
            return json.dumps({"error": "Q&A item not found or invalid selection"})

        board_service = uow.services.boards
        await board_service._log_activity(
            board_id=board_id,
            action="spec_question_answered",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={
                "spec_id": spec_id,
                "qa_id": qa_id,
                "answer": (answer or "")[:100],
                "selected": selected_list,
            },
        )
        await uow.commit()

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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        from okto_pulse.core.ports.traceability import TraceabilityReadError

        try:
            report = await uow.services.build_traceability_report(
                board_id,
                ideation_id=ideation_id,
                spec_id=spec_id,
                include_artifacts=_include_artifacts,
            )
        except TraceabilityReadError as exc:
            return json.dumps({"error": exc.message, "code": exc.code})
        await uow.commit()

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
async def okto_pulse_get_spec_knowledge(
    board_id: str, spec_id: str, knowledge_id: str
) -> str:
    """
    Get the full content of a knowledge base item."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.spec_knowledge
        kb = await service.get_knowledge(knowledge_id)
        await uow.commit()

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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.spec_knowledge
        kb_data = SpecKnowledgeCreate(
            title=title,
            description=description or None,
            content=resolved_content,
            mime_type=mime_type,
        )
        kb = await service.create_knowledge(spec_id, ctx.agent_id, kb_data)
        await uow.commit()

        if not kb:
            return json.dumps(
                {"error": "Failed to create knowledge base item — spec not found"}
            )

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
async def okto_pulse_delete_spec_knowledge(
    board_id: str, spec_id: str, knowledge_id: str
) -> str:
    """Delete a knowledge base item from a spec. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.spec_knowledge
        kb = await service.get_knowledge(knowledge_id)
        if not kb or kb.spec_id != spec_id:
            return json.dumps({"error": "Knowledge base item not found"})
        await service.delete_knowledge(knowledge_id)
        await uow.commit()

        return json.dumps({"success": True})


# ============================================================================
# REFINEMENT SNAPSHOT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_get_refinement_snapshot(
    board_id: str, refinement_id: str, version: str
) -> str:
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
async def okto_pulse_get_refinement_knowledge(
    board_id: str, refinement_id: str, knowledge_id: str
) -> str:
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
            return json.dumps(
                {"error": "Failed to create knowledge base item — refinement not found"}
            )
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
async def okto_pulse_delete_refinement_knowledge(
    board_id: str, refinement_id: str, knowledge_id: str
) -> str:
    """Delete a knowledge base item from a refinement. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
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
            title=title,
            description=description or None,
            spec_id=spec_id,
            objective=objective or None,
            expected_outcome=expected_outcome or None,
            lane_type=lane_type or "normal",
            origin_sprint_id=origin_sprint_id or None,
            origin_bug_id=origin_bug_id or None,
            test_scenario_ids=coerce_to_list_str(test_scenario_ids) or None,
            business_rule_ids=coerce_to_list_str(business_rule_ids) or None,
            start_date=start_date or None,
            end_date=end_date or None,
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
                return json.dumps(
                    {"error": "Failed to create sprint (spec not found or wrong board)"}
                )
            sprint = _r.sprint
            return json.dumps(
                {
                    "success": True,
                    "sprint": {
                        "id": sprint.id,
                        "title": sprint.title,
                        "status": sprint.status.value,
                        "spec_id": sprint.spec_id,
                        "lane_type": sprint.lane_type.value
                        if sprint.lane_type
                        else "normal",
                        "origin_sprint_id": sprint.origin_sprint_id,
                        "origin_bug_id": sprint.origin_bug_id,
                        "normal_sprint_created": sprint.normal_sprint_created,
                    },
                }
            )
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
        kwargs["skip_qualitative_validation"] = (
            skip_qualitative_validation.lower() == "true"
        )

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
            return json.dumps(
                {
                    "success": True,
                    "sprint": {
                        "id": sprint.id,
                        "title": sprint.title,
                        "version": sprint.version,
                        "lane_type": sprint.lane_type.value
                        if sprint.lane_type
                        else "normal",
                        "origin_sprint_id": sprint.origin_sprint_id,
                        "origin_bug_id": sprint.origin_bug_id,
                        "normal_sprint_created": sprint.normal_sprint_created,
                    },
                }
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Sprint not found"})
    except ValueError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_move_sprint(
    board_id: str,
    sprint_id: str,
    status: str,
    cancellation_reason: str = "",
) -> str:
    """
    Move a sprint to a new status. State machine: draft→active→review→closed.
    Gates: draft→active requires cards, active→review requires scoped test
    scenarios in passed status, review→closed requires evaluation. Automated
    test pointers alone do not satisfy sprint review.
    status='cancelled' requires cancellation_reason; reopening clears it."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from okto_pulse.core.domain.enums import SprintStatus
    from okto_pulse.core.models.schemas import SprintMove

    try:
        sprint_status = SprintStatus(status)
    except ValueError:
        return json.dumps(
            {
                "error": f"Invalid status. Must be one of: {[s.value for s in SprintStatus]}"
            }
        )

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
                    MoveSprintCommand(
                        sprint_id,
                        SprintMove(
                            status=sprint_status,
                            cancellation_reason=cancellation_reason or None,
                        ),
                    ),
                    actor=actor,
                    uow=uow,
                )
            ).sprint
            return json.dumps(
                {
                    "success": True,
                    "sprint": {
                        "id": sprint.id,
                        "title": sprint.title,
                        "status": sprint.status.value,
                    },
                }
            )
    except EntityNotFoundError:
        return json.dumps({"error": "Sprint not found"})
    except CancellationReasonRequiredError as e:
        return json.dumps({"error": e.code, **e.to_dict()})
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
                ["bug", "test"] if lane_type == "hotfix" else ["normal", "test", "bug"]
            )
            return json.dumps(
                {
                    "success": True,
                    "assigned": count,
                    "assigned_count": count,
                    "lane_type": lane_type,
                    "accepted_card_types": accepted_card_types,
                }
            )
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
        return json.dumps(
            {"error": "recommendation must be: approve, request_changes, or reject"}
        )

    evaluation = {
        "dimensions": {
            "breakdown_completeness": {
                "score": breakdown_completeness,
                "justification": breakdown_justification,
            },
            "granularity": {
                "score": granularity,
                "justification": granularity_justification,
            },
            "dependency_coherence": {
                "score": dependency_coherence,
                "justification": dependency_justification,
            },
            "test_coverage_quality": {
                "score": test_coverage_quality,
                "justification": test_coverage_justification,
            },
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
                    SubmitSprintEvaluationCommand(sprint_id, evaluation),
                    actor=actor,
                    uow=uow,
                )
            except FullContextGuardError as exc:
                await commit(uow)
                return json.dumps(
                    {
                        "error": str(exc),
                        "reason": exc.reason,
                        "decision": exc.decision.audit_details(),
                    }
                )
            sprint = _r.sprint
            last_eval = sprint.evaluations[-1] if sprint.evaluations else {}
            return json.dumps(
                {
                    "success": True,
                    "evaluation_id": last_eval.get("id"),
                    "overall_score": overall_score,
                    "recommendation": recommendation,
                }
            )
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
    board_id: str,
    sprint_id: str,
    evaluation_id: str,
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
            McpGetSprintEvaluationCommand(sprint_id, evaluation_id),
            actor=actor,
            uow=uow,
        )

    if _r.sprint_not_found:
        return json.dumps({"error": "Sprint not found"})
    if _r.eval_not_found:
        return json.dumps({"error": f"Evaluation '{evaluation_id}' not found"})
    return json.dumps(_r.evaluation)


@mcp.tool()
async def okto_pulse_delete_sprint_evaluation(
    board_id: str,
    sprint_id: str,
    evaluation_id: str,
) -> str:
    """Delete your own sprint evaluation. Permanent, no undo.
    Docs: okto-pulse://reference/destructive_ops
    """
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
            McpDeleteSprintEvaluationCommand(sprint_id, evaluation_id),
            actor=actor,
            uow=uow,
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
        board_id,
        "sprint",
        sprint_id,
        question,
        alias_kind="legacy",
        tool_name="okto_pulse_ask_sprint_question",
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
                {
                    "error": _r.self_answer_error.reason,
                    "detail": str(_r.self_answer_error),
                }
            )
        if _r.qa_not_found:
            return json.dumps({"error": "Q&A item not found"})
        qa = _r.qa
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
async def okto_pulse_delete_spec_question(
    board_id: str, spec_id: str, qa_id: str
) -> str:
    """
    Delete a Q&A item from a spec. Use this to invalidate outdated questions
    or remove resolved clarifications that no longer apply."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_DELETE)
    if perm_err:
        return _perm_error(perm_err)

    async with get_unit_of_work_factory_for_mcp()() as uow:
        service = uow.services.spec_qa
        deleted = await service.delete_question(qa_id)
        if not deleted:
            return json.dumps({"error": "Q&A item not found"})

        board_service = uow.services.boards
        await board_service._log_activity(
            board_id=board_id,
            action="spec_question_deleted",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={"spec_id": spec_id, "qa_id": qa_id},
        )
        await uow.commit()
        return json.dumps({"success": True})


@mcp.tool()
async def okto_pulse_delete_ideation_question(
    board_id: str, ideation_id: str, qa_id: str
) -> str:
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
async def okto_pulse_delete_refinement_question(
    board_id: str, refinement_id: str, qa_id: str
) -> str:
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
async def okto_pulse_delete_sprint_question(
    board_id: str, sprint_id: str, qa_id: str
) -> str:
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
            McpDeleteSprintQuestionCommand(board_id, sprint_id, qa_id),
            actor=actor,
            uow=uow,
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
            return json.dumps(
                {"suggestions": _r.suggestions, "count": len(_r.suggestions)}
            )
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

    from okto_pulse.core.application.use_cases import (
        EntityNotFoundError,
        SubmitTaskValidationCommand,
        SubmitTaskValidationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await SubmitTaskValidationUseCase().execute(
                SubmitTaskValidationCommand(card_id, data),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.validation, default=str)
    except EntityNotFoundError as e:
        if e.entity_type == "card":
            return json.dumps({"error": "Card not found"})
        return json.dumps({"error": str(e)})
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
    """Confirm Path B amendment coverage — validator-only writer of the
    non-forgeable attestation that lets the bug gate treat a cross-spec Path B
    regression artifact as closure-ready. Fail-closed preconditions: the test
    task + scenario MUST be declared by THIS amendment, the test task MUST be
    done with SPEC3 reexecutable evidence, and the caller MUST hold validator
    critical-context authorization — all necessary but NOT sufficient; the
    gate derives coverage from the persisted attestation. Preflight (BUG-01)
    runs the SAME eligibility predicate as the gate, so success implies the
    attestation is persisted AND gate-consumable.
    Errors: coverage_not_gate_consumable (inert tuple — nothing persisted),
    distinct from coverage_pending (confirmation not yet recorded).
    Docs: okto-pulse://reference/tool-docs/card.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "card.validation.submit")
    if perm_err:
        return _perm_error(perm_err)

    async with get_unit_of_work_factory_for_mcp()() as uow:
        card_service = uow.services.cards
        try:
            result = await card_service.confirm_amendment_coverage(
                amendment_id=amendment_id,
                regression_test_task_id=regression_test_task_id,
                regression_scenario_id=regression_scenario_id,
                reviewer_id=ctx.agent_id,
                reviewer_name=ctx.agent_name,
            )
            await uow.commit()
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
    """Create a Path B AmendmentHotfixRevision for a bug. REST twin: POST
    /boards/{board_id}/bugs/{bug_id}/amendment-revisions. The amendment binds
    to the bug's OWN content-locked spec (done/validated, or in_progress still
    content-locked by an active passed validation) and always starts as
    'draft'; an in_progress spec that is still editable is rejected — edit it
    directly. This tool ONLY remediates: it NEVER skips/overrides the bug
    regression gate and cannot set coverage confirmation (validator-only).
    Returns the structured amendment payload (status, lineage_state,
    eligibility, artifacts) or a structured error.
    Errors: original_spec_not_done_or_locked, bug_spec_mismatch,
    invalid_initial_status.
    """
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        try:
            result = await uow.services.amendments.create(
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
            await uow.commit()
            return json.dumps(
                {"success": True, "amendment_revision": result}, default=str
            )
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        try:
            result = await uow.services.amendments.list_for_bug(
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        try:
            result = await uow.services.amendments.get(
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        try:
            result = await uow.services.amendments.associate(
                board_id=board_id,
                bug_id=bug_id,
                amendment_id=amendment_id,
                actor=ctx.agent_id,
                regression_scenario_ids=regression_scenario_ids,
                regression_test_task_ids=regression_test_task_ids,
                automated_regression_refs=automated_regression_refs,
            )
            await uow.commit()
            return json.dumps(
                {"success": True, "amendment_revision": result}, default=str
            )
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

    async with get_unit_of_work_factory_for_mcp()() as uow:
        try:
            result = await uow.services.amendments.transition_lifecycle(
                board_id=board_id,
                bug_id=bug_id,
                amendment_id=amendment_id,
                actor=ctx.agent_id,
                status=status or None,
                lineage_state=lineage_state or None,
            )
            await uow.commit()
            return json.dumps(
                {"success": True, "amendment_revision": result}, default=str
            )
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
        return bool(
            settings_payload.get(_TASK_REQUIREMENT_GATE_DEFAULT_SKIP_FIELD, False)
        )
    return False


async def _refuse_mcp_default_config_activation_if_human_skip_changes(
    *,
    board_id: str,
    template_id: str,
    blocked_tool: str,
    blocked_action: str,
) -> str | None:
    async with get_unit_of_work_factory_for_mcp()() as uow:
        target = await uow.services.get_default_board_template(template_id)
        if target is None:
            return None
        active = await uow.services.resolve_active_default_board_template(target.scope)
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
    async with get_unit_of_work_factory_for_mcp()() as uow:
        target = await uow.services.get_default_board_template(template_id)
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
async def okto_pulse_get_active_default_board_config(
    board_id: str, scope: str = "global"
) -> str:
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
async def okto_pulse_list_default_board_config_versions(
    board_id: str, scope: str = "global"
) -> str:
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
async def okto_pulse_activate_default_board_config_version(
    board_id: str, template_id: str
) -> str:
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
async def okto_pulse_deactivate_default_board_config_version(
    board_id: str, template_id: str
) -> str:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpListDefaultGuidelineCandidatesCommand,
        McpListDefaultGuidelineCandidatesUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    # AF23 scope marker: ActorScope.from_context(actor).query_scope; query_scope=query_scope.
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpListDefaultGuidelineCandidatesUseCase().execute(
                McpListDefaultGuidelineCandidatesCommand(
                    board_id,
                    scope=scope,
                    template_id=template_id,
                ),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpUpdateDefaultGuidelineRefsCommand,
        McpUpdateDefaultGuidelineRefsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    # AF23 scope marker: ActorScope.from_context(actor).query_scope; query_scope=query_scope.
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpUpdateDefaultGuidelineRefsUseCase().execute(
                McpUpdateDefaultGuidelineRefsCommand(
                    board_id,
                    template_id=template_id,
                    guideline_default_refs=guideline_default_refs,
                ),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpSetDefaultDesignSystemCommand,
        McpSetDefaultDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.default_board_configuration import (
        DefaultBoardConfigurationError,
    )

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpSetDefaultDesignSystemUseCase().execute(
                McpSetDefaultDesignSystemCommand(
                    template_id=template_id,
                    design_system_id=design_system_id,
                    gate_mode=gate_mode,
                    version=version,
                    snapshot=snapshot,
                ),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DefaultBoardConfigurationError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpListDesignSystemsCommand,
        McpListDesignSystemsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpListDesignSystemsUseCase().execute(
                McpListDesignSystemsCommand(board_id, scope=scope),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DesignSystemError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpGetDesignSystemCommand,
        McpGetDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpGetDesignSystemUseCase().execute(
                McpGetDesignSystemCommand(design_system_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DesignSystemError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpCreateDesignSystemCommand,
        McpCreateDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpCreateDesignSystemUseCase().execute(
                McpCreateDesignSystemCommand(
                    board_id,
                    title=title,
                    scope=scope,
                    payload=payload,
                    status=status,
                ),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DesignSystemError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpUpdateDesignSystemCommand,
        McpUpdateDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpUpdateDesignSystemUseCase().execute(
                McpUpdateDesignSystemCommand(
                    design_system_id,
                    title=title,
                    payload=payload,
                    status=status,
                ),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DesignSystemError as e:
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
    from okto_pulse.core.application.use_cases.mcp_admin_validation_analytics import (
        McpDeleteDesignSystemCommand,
        McpDeleteDesignSystemUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract
    from okto_pulse.core.services.design_system import DesignSystemError

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpDeleteDesignSystemUseCase().execute(
                McpDeleteDesignSystemCommand(design_system_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.data, default=str)
    except DesignSystemError as e:
        return json.dumps(e.to_dict())


@mcp.tool()
async def okto_pulse_link_board_design_system(
    board_id: str, design_system_id: str
) -> str:
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

    from okto_pulse.core.application.use_cases import (
        EntityNotFoundError,
        ListTaskValidationsCommand,
        ListTaskValidationsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await ListTaskValidationsUseCase().execute(
                ListTaskValidationsCommand(card_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(
            {
                "card_id": card_id,
                "total": len(result.validations),
                "validations": result.validations,
            },
            default=str,
        )
    except EntityNotFoundError as e:
        return json.dumps(
            {"error": "Card not found" if e.entity_type == "card" else str(e)}
        )
    except ValueError as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_get_task_validation(
    board_id: str,
    card_id: str,
    validation_id: str,
) -> str:
    """
    Get full details of a specific task validation entry."""
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, "card.validation.read")
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases import (
        EntityNotFoundError,
        GetTaskValidationCommand,
        GetTaskValidationUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await GetTaskValidationUseCase().execute(
                GetTaskValidationCommand(card_id, validation_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(result.validation, default=str)
    except EntityNotFoundError as e:
        if e.entity_type == "task_validation":
            return json.dumps({"error": f"Validation '{validation_id}' not found"})
        return json.dumps(
            {"error": "Card not found" if e.entity_type == "card" else str(e)}
        )
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
    """Submit a Spec Validation Gate record for a spec in 'approved' status — a
    semantic quality gate that runs AFTER the deterministic coverage gates
    (AC/FR/TR/Contract); if any coverage fails the submit is rejected with the
    violation. Outcome is FAILED on any threshold violation or
    recommendation=reject; SUCCESS (all thresholds pass AND
    recommendation=approve) atomically promotes the spec approved->validated
    and content-locks it. Scores are 0-100 integers, NOT 1-5:
    completeness/assertiveness are higher-is-better, ambiguity is
    lower-is-better. ANTI-PATTERN: never inflate scores to pass the gate —
    iterate on content (scenarios, BRs, TRs) instead.
    Errors: resource_gate_missing_resources.
    Docs: okto-pulse://reference/tool-docs/spec.
    """
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
        return json.dumps(
            {"error": "completeness_justification must be at least 10 characters"}
        )
    if len(assertiveness_justification.strip()) < 10:
        return json.dumps(
            {"error": "assertiveness_justification must be at least 10 characters"}
        )
    if len(ambiguity_justification.strip()) < 10:
        return json.dumps(
            {"error": "ambiguity_justification must be at least 10 characters"}
        )
    if len(general_justification.strip()) < 20:
        return json.dumps(
            {"error": "general_justification must be at least 20 characters"}
        )

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

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            # Thin MCP adapter (spec #09): delegate to the shared use case (it
            # validates the payload, resolves the reviewer name from the MCP agent,
            # submits and commits). The MCP-specific input checks above are kept so
            # the tool's error envelopes/order are unchanged.
            result = await SubmitSpecValidationUseCase().execute(
                SubmitSpecValidationCommand(spec_id, data),
                actor=actor,
                uow=uow,
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

    from okto_pulse.core.application.use_cases import (
        ListSpecValidationsCommand,
        ListSpecValidationsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await ListSpecValidationsUseCase().execute(
                ListSpecValidationsCommand(spec_id),
                actor=actor,
                uow=uow,
            )
        return json.dumps(
            {
                "spec_id": spec_id,
                **result.data,
            },
            default=str,
        )
    except ValueError as e:
        return json.dumps({"error": str(e)})


# ============================================================================
# KG CONSOLIDATION PRIMITIVES (MVP Fase 0)
# ============================================================================

from okto_pulse.core.mcp.kg_tools import register_kg_tools as _register_kg_tools  # noqa: E402
from okto_pulse.core.mcp.kg_query_tools import (
    register_kg_query_tools as _register_kg_query_tools,
)  # noqa: E402

_register_kg_tools(
    mcp, get_agent=_get_authenticated_agent, get_uow=get_unit_of_work_factory_for_mcp
)
_register_kg_query_tools(
    mcp, get_agent=_get_authenticated_agent, get_uow=get_unit_of_work_factory_for_mcp
)

from okto_pulse.core.mcp.kg_power_tools import (
    register_kg_power_tools as _register_kg_power_tools,
)  # noqa: E402

_register_kg_power_tools(mcp, get_agent=_get_authenticated_agent)

from okto_pulse.core.mcp.kg_export_tools import (
    register_kg_export_tools as _register_kg_export_tools,
)

_register_kg_export_tools(mcp, get_agent=_get_authenticated_agent)


# ============================================================================
# KG HEALTH (spec 20f67c2a — Ideação #5, FR2)
# ============================================================================


@mcp.tool()
async def okto_pulse_kg_health(board_id: str, profile: str = "summary") -> str:
    """Snapshot of a board's KG health. REST twin: GET /api/v1/kg/health.
    Default profile=summary returns the slim stop-rule fields an agent needs
    before a KG mutation — graph_state, discovery_state, overall_state,
    metric_status, classification_reason, correlation_id,
    memory_pressure_status, recent_events — plus operational scalars,
    decay_scheduler_diagnostics and storage_footprint_proxy. Scheduler debt is
    operational and does not by itself require graph recovery. profile=full
    (or legacy) adds the complete dashboard payload.
    Profiles: okto-pulse://reference/projection-profiles.
    Docs: okto-pulse://reference/tool-docs/kg.
    """
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
                GetKgHealthCommand(
                    board_id,
                    scheduler_control=get_scheduler_control_for_mcp(),
                ),
                actor=actor,
                uow=uow,
            )
    except BoardNotFoundError as exc:
        return json.dumps({"error": str(exc)})
    # FR4: slim default projection — keep the stop-rule fields, omit verbose
    # diagnostics until profile=full/legacy is requested.
    data = KGHealthMCPProjection().project(result.data, profile=profile)
    return json.dumps(data, default=str)


@mcp.tool()
async def okto_pulse_kg_health_readiness(
    board_id: str,
    profile: str = "summary",
    artifact_ref: str = "",
) -> str:
    """Canonical NON-MASKABLE KG health/readiness. REST twin: GET
    /api/v1/kg/health-readiness (RKG-05). Both summary and full expose
    technical_signals (scalar counters dead_letter_count / technical_dlq_count
    / canonical_debt_open_count / active_queue_count — SEPARATE domains),
    readiness (blocking vs would_block_done + reasons + policy_reason),
    top-level cognitive_enforcement_mode / enforcement_active, and
    non_maskable_items (per-item drill_down_tool / last_error / next_action).
    A summary never hides a technical blocker; full adds prose health_issues +
    root_cause. Optional artifact_ref scopes items.
    Profiles: okto-pulse://reference/projection-profiles.
    Docs: okto-pulse://reference/tool-docs/kg.
    """
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
                    board_id,
                    profile=profile,
                    surface="mcp",
                    artifact_ref=(artifact_ref or None),
                    scheduler_control=get_scheduler_control_for_mcp(),
                ),
                actor=actor,
                uow=uow,
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
        return json.dumps(
            {
                "error": "invalid_pagination",
                "detail": "limit and offset must be integers",
            }
        )

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
        signal="canonical_debt",
        surface="mcp",
        outcome="success",
        board_id=board_id,
        item_count=len(result.items),
    )
    return json.dumps(
        {
            "board_id": board_id,
            "items": result.items,
            "counts": result.counts,
            "total": result.total,
            "limit": bounded_limit,
            "offset": bounded_offset,
        },
        default=str,
    )


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
    """List canonical Learning partition-integrity signals for KG health
    drill-down (R7). READ-ONLY: cognitive holds, canonical debt,
    mixed-evidence deferred and provenance-only Learnings. Each item carries
    an S-KG-02 classification (missing_source, unresolved_source,
    canonical_learning_resolved, weak_provenance, invalid_orphan_learning) +
    a classification_counts census. REST twin: GET
    /api/v1/kg/{board_id}/canonical-partition-integrity. NEVER skips, clears
    or resolves an R7 hold/debt — human-only. Filters: reason_code,
    graph_layer, source_ref, node_id, status.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    try:
        bounded_limit = max(1, min(int(limit), 200))
        bounded_offset = max(0, int(offset))
    except (TypeError, ValueError):
        return json.dumps(
            {
                "error": "invalid_pagination",
                "detail": "limit and offset must be integers",
            }
        )

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
        return json.dumps(
            {
                "error": "invalid_pagination",
                "detail": "limit and offset must be integers",
            }
        )

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
                    board_id,
                    limit=bounded_limit,
                    offset=bounded_offset,
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
        return json.dumps(
            {
                "error": "invalid_pagination",
                "detail": "limit and offset must be integers",
            }
        )

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
        return json.dumps(
            {
                "error": "invalid_pagination",
                "detail": "limit and offset must be integers",
            }
        )

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
                    board_id,
                    bug_id,
                    evidence=evidence,
                    requested_action=requested_action,
                    reason_code=reason_code,
                    justification=justification,
                    evidence_refs=evidence_refs,
                    revisit_at=revisit_at,
                ),
                actor=actor,
                uow=uow,
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
        require_rebuild_audit_artifact_store,
    )

    return CognitiveReadinessService(
        CognitiveConsolidationItemStore(
            artifact_store=require_rebuild_audit_artifact_store(),
        )
    )


async def _cognitive_enforcement_active(kg_operations, board_id: str) -> bool:
    """Whether the board's done-gate is ACTUALLY enforcing cognitive readiness
    (two-key rollout). Delegates to the transport-free service reader
    (spec R01A MCP-FU3) — never recomputed; the 4 in-server callers are unchanged."""
    return await kg_operations.cognitive_enforcement_active(board_id)


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


async def _evaluate_card_cognitive_verdict(
    kg_operations,
    board_id: str,
    card,
    enforcement_active: bool,
):
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
        verdict = await kg_operations.evaluate_cognitive_readiness(
            _build_cognitive_readiness_service(),
            board_id=board_id,
            source_ref=ref,
            has_reusable_cognition=has_reusable,
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
                    board_id,
                    signal=signal,
                    artifact_id=artifact_id or None,
                    source_ref=source_ref or None,
                    reason_code=reason_code or None,
                    status=status or None,
                    search=search or None,
                    limit=limit,
                    offset=offset,
                    kg_generation_id=kg_generation_id or None,
                ),
                actor=actor,
                uow=uow,
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
                    board_id,
                    source_ref=source_ref,
                    kg_generation_id=kg_generation_id or None,
                    has_reusable_cognition=primary_type not in ("task", "test"),
                ),
                actor=actor,
                uow=uow,
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
    """Clear a cognitive skip / no_action, reopening the item to pending via the
    central ledger path (audit-preserving; ledger-only, no KG mutation).
    R5-IMP1 — HUMAN-only control: from the agent-facing MCP surface this tool
    ALWAYS fails closed (mutation_allowed=false, state_changed=false) and
    never reopens the ledger item; a human operator clears the skip via the
    IDE control or the human REST surface. Errors: human_control_required.
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
        return json.dumps(
            {
                "error": "invalid_pagination",
                "detail": "limit and offset must be integers",
            }
        )

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
            ListCognitiveDlqCommand(
                board_id, limit=bounded_limit, offset=bounded_offset
            ),
            actor=actor,
            uow=uow,
        )
    total = uc_result.total
    rows = uc_result.rows

    items = []
    for row in rows:
        ref = f"{row.artifact_type}:{row.artifact_id}"
        items.append(
            {
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
            }
        )
    return json.dumps(
        {
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
        },
        default=str,
    )


# ============================================================================
# KG ORPHAN INTEGRITY (spec KG-ZO-02 — FR6/TR4)
# ============================================================================


def _kg_orphan_graph_unavailable_payload(
    board_id: str, exc: Exception
) -> dict[str, Any]:
    return {
        "error": "kg_orphan_graph_unavailable",
        "board_id": board_id,
        "error_type": type(exc).__name__,
        "operator_action": "inspect_kg_health",
    }


async def _kg_orphan_backfill_health_refusal(board_id: str) -> dict[str, Any] | None:
    async with get_unit_of_work_factory_for_mcp()() as uow:
        health = await uow.services.kg.health(
            board_id,
            scheduler_control=get_scheduler_control_for_mcp(),
        )
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
        return json.dumps(
            {
                "error": "invalid_node_ids",
                "reason": str(exc),
                "expected_format": "JSON array, native list, or pipe-separated string",
            }
        )

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

    return json.dumps(
        {
            "board_id": board_id,
            "generation_id": generation_id,
            "dry_run": dry_run,
            "backfill_summary": result.to_safe_dict(),
            "correlation_id": result.correlation_id,
        },
        default=str,
    )


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
        signal="dead_letter",
        surface="mcp",
        outcome="success",
        board_id=board_id,
        item_count=len(data.get("rows", [])),
    )
    return json.dumps(data, default=str)


@mcp.tool()
async def okto_pulse_kg_queue_drilldown(board_id: str) -> str:
    """Drill down into the ACTIVE operational queue depth (R6-IMP2). Read-only.
    Use when okto_pulse_kg_health reports an active_queue backlog and you need
    to know WHERE the depth comes from. Returns worker_mode,
    total_active_depth, a classification (transient|stuck|backpressure|idle)
    and per-source breakdowns (consolidation_queue by status + artifact
    category + oldest_age_seconds; global_update_outbox pending depth +
    oldest_age_seconds). ACTIVE queue only — dead-letter (DLQ), outbox
    dead_letter and canonical debt are TERMINAL and NOT counted here; inspect
    those via okto_pulse_kg_dead_letter_list /
    okto_pulse_kg_canonical_debt_list.
    """
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
        from okto_pulse.core.application.runtime_workers import (
            process_runtime_worker_once,
            runtime_worker_is_running,
            signal_runtime_worker,
        )

        worker_running = runtime_worker_is_running("consolidation_worker")
        signal_runtime_worker("consolidation_worker")
        data["worker_running"] = worker_running
        if worker_running:
            data["processed_now_count"] = 0
            data["process_now_mode"] = "signalled_app_runner"
        else:
            data["processed_now_count"] = await process_runtime_worker_once(
                "consolidation_worker"
            )
            data["process_now_mode"] = "app_runner_direct_batch"

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
    """Fail-closed reprocess of the connectivity-guard technical_dlq class
    (RKG-04). Requires EXPLICIT in-class dead_letter_ids from
    okto_pulse_kg_connectivity_dlq_diagnose — it is NEVER a broad reprocess.
    On success it reuses the idempotent DLQ->queue path (ConsolidationQueue
    dedup) and, with process_now, runs one worker batch.
    Errors (each blocks, removing NO DLQ): no_dlq_selected,
    selected_dlq_missing, selected_dlq_out_of_class, rkg02_rkg03_not_applied,
    kg_quarantined.
    """
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
        from okto_pulse.core.application.runtime_workers import (
            process_runtime_worker_once,
            runtime_worker_is_running,
            signal_runtime_worker,
        )

        worker_running = runtime_worker_is_running("consolidation_worker")
        signal_runtime_worker("consolidation_worker")
        data["worker_running"] = worker_running
        if not worker_running:
            data["processed_now_count"] = await process_runtime_worker_once(
                "consolidation_worker"
            )
            data["process_now_mode"] = "app_runner_direct_batch"
        else:
            data["processed_now_count"] = 0
            data["process_now_mode"] = "signalled_app_runner"

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
    """Force-apply schema migrations to fix legacy boards (pre v0.3.2). REST
    twin: POST /api/v1/kg/{board_id}/migrate-schema. Use when consolidation
    fails with `Binder exception: Cannot find property X for n` — usually an
    ALTER ADD missed on a board bootstrapped before that version. Idempotent:
    re-running on an already-migrated board returns migrated=true with empty
    columns_added (no-op). NEVER delete the KG's persistent storage to "fix"
    a board — that destroys the board's whole KG; use this tool instead.
    """
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

        from okto_pulse.core.ports.application_persistence import ApplicationQuery

        results: list[dict[str, Any]] = []
        async with get_unit_of_work_factory_for_mcp()() as uow:
            boards = await uow.services.list_application_records(
                ApplicationQuery(entity="board", order_by=(("name", False),)),
            )
            board_pairs = [(board.id, board.name) for board in boards]
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
                results.append(
                    {
                        "board_id": bid,
                        "migrated": False,
                        "columns_added": {},
                        "errors": [f"unhandled: {exc}"],
                        "duration_ms": 0,
                    }
                )
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
    """Trigger the KG decay tick manually. REST twin: POST
    /api/v1/kg/tick/run-now. Runs an immediate tick without waiting for the
    periodic cron: use after mass node rescoring, when default_score_ratio is
    above 0.7 and stale ranking is suspected, or when debugging a specific
    board's scoring (pass board_id). force_full_rebuild=true clears
    last_recomputed_at before the tick (ignores the staleness threshold) —
    per-trigger ONLY, never a persisted setting.
    Errors: tick_already_running (concurrent cron/manual calls — the first
    caller wins the advisory lock).
    """
    # Per-board scope auth: when board_id provided, validate access.
    if board_id:
        ctx = await _get_agent_ctx(board_id)
        if ctx is None:
            return _auth_error()
        triggered_by = ctx.agent.id if hasattr(ctx, "agent") else "agent-mcp"
    else:
        # Global scope — allow any authenticated agent (no per-board check).
        triggered_by = "agent-mcp-global"

    from okto_pulse.core.ports.coordination import (
        CoordinationProviderMissing,
        get_lease_provider,
    )

    try:
        lease_provider = get_lease_provider()
    except CoordinationProviderMissing as exc:
        return json.dumps(
            {
                "error": exc.code,
                "provider": exc.provider_key,
                "message": "Tick lease provider is not configured",
            }
        )

    lease = await lease_provider.try_acquire("kg_daily_tick", ttl_seconds=300)
    if lease is None:
        return json.dumps(
            {
                "error": "tick_already_running",
                "message": "Tick already running, retry shortly",
            }
        )

    # F17 admission gate (gemelar): refuse a degraded concrete board with the
    # SAME structured graph_recovery_needed refusal as the REST endpoint, via the
    # SAME shared _refuse_tick_if_degraded gate (one predicate, no MCP-side
    # duplication). The MCP path owns no request session, so probe under a
    # short-lived one. Runs after the lease check, before tick_id allocation.
    if board_id:
        from okto_pulse.core.application.kg_tick import (
            refuse_tick_if_degraded as _refuse_tick_if_degraded,
        )

        async with get_unit_of_work_factory_for_mcp()() as uow:
            refusal = await _refuse_tick_if_degraded(
                board_id,
                uow,
                scheduler_control=get_scheduler_control_for_mcp(),
            )
        if refusal is not None:
            return json.dumps(refusal)

    import uuid as _uuid
    from datetime import datetime, timezone

    tick_id = str(_uuid.uuid4())
    scheduled_at = datetime.now(timezone.utc).isoformat()

    _tick_logger.info(
        "kg.tick.manual_triggered tick_id=%s user=%s board=%s force=%s source=mcp",
        tick_id,
        triggered_by,
        board_id or None,
        force_full_rebuild,
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
        try:
            async with get_unit_of_work_factory_for_mcp()() as uow:
                await uow.services.kg.dispatch_manual_tick(
                    tick_id=tick_id,
                    board_id=board_id or None,
                    force_full_rebuild=force_full_rebuild,
                )
                await uow.commit()
        except Exception as exc:
            _tick_logger.error(
                "kg.tick.manual_schedule_failed tick_id=%s err=%s source=mcp",
                tick_id,
                exc,
                extra={
                    "event": "kg.tick.manual_schedule_failed",
                    "tick_id": tick_id,
                    "board_id": board_id or None,
                    "force_full_rebuild": force_full_rebuild,
                    "source": "mcp",
                    "error": str(exc),
                },
            )
            return json.dumps(
                {
                    "error": "tick_schedule_failed",
                    "message": (
                        "Failed to persist the KG tick event. "
                        "No background tick was scheduled."
                    ),
                    "detail": str(exc),
                }
            )
    finally:
        await lease_provider.release(lease)

    return json.dumps(
        {
            "tick_id": tick_id,
            "status": "running",
            "scheduled_at": scheduled_at,
        }
    )


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
#   * resolves rebuild artifacts through the edition-composed artifact store
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
    """Run the KG rebuild preflight for a board. REST twin: POST
    /api/v1/kg/rebuild/preflight. Read-only check (TR13): enumerates real
    sources via BoardSourceReader, classifies the KG health state and persists
    the immutable manifest required by /confirm. Admission gate (FR8): refuses
    when graph_state == 'quarantined'; recovery_needed IS admitted — rebuild
    is the prescribed exit from that state. Returns outcome, action_required,
    base_state, eligible_source_count, preflight_hash, manifest_ref,
    source_set_hash; pass manifest_ref + preflight_hash to
    okto_pulse_kg_rebuild_confirm. Errors: rebuild_refused_quarantined.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.application.kg_rebuild import (
        build_source_store as _build_source_store,
        refuse_rebuild_if_quarantined as _refuse_rebuild_if_quarantined,
    )
    from okto_pulse.core.kg.rebuild_audit import (
        require_rebuild_audit_artifact_store,
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
                scheduler_control=get_scheduler_control_for_mcp(),
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
        source_set = await asyncio.to_thread(enumerator.enumerate, board_id=board_id)
    except Exception as exc:
        _rebuild_logger.error(
            "kg.rebuild.preflight.enumerate_failed board=%s err=%s", board_id, exc
        )
        return json.dumps({"error": "preflight_enumerate_failed", "detail": str(exc)})

    def source_probe(_bid: str) -> RebuildSourceSummary:
        return RebuildSourceSummary(
            eligible_count=source_set.eligible_count,
            skipped_cancelled_count=source_set.skipped_cancelled_count,
            has_non_deterministic_inputs=source_set.has_non_deterministic_inputs,
            canonical_source_count=source_set.canonical_source_count,
            working_source_count=source_set.working_source_count,
            skipped_by_maturity_count=source_set.skipped_by_maturity_count,
            skipped_expired_working_count=(source_set.skipped_expired_working_count),
            legacy_unknown_count=source_set.legacy_unknown_count,
            layer_counts=source_set.layer_counts,
            source_partition_counts=source_set.source_partition_counts,
        )

    service = RebuildPreflightService(
        source_probe=source_probe,
        health_probe=health_probe,
    )
    try:
        result = await asyncio.to_thread(service.run, board_id=board_id)
    except Exception as exc:
        _rebuild_logger.error(
            "kg.rebuild.preflight.service_failed board=%s err=%s", board_id, exc
        )
        return json.dumps({"error": "preflight_service_failed", "detail": str(exc)})

    try:
        manifest_store = KGRebuildSourceManifest(
            artifact_store=require_rebuild_audit_artifact_store()
        )
        manifest = await asyncio.to_thread(
            manifest_store.build,
            source_set=source_set,
            preflight_hash=result.preflight_hash,
        )
    except Exception as exc:
        _rebuild_logger.error(
            "kg.rebuild.preflight.manifest_failed board=%s err=%s", board_id, exc
        )
        return json.dumps({"error": "preflight_manifest_failed", "detail": str(exc)})

    payload = result.to_dict()
    payload["manifest_ref"] = manifest.manifest_ref
    payload["source_set_hash"] = manifest.source_set_hash

    _rebuild_logger.info(
        "kg.rebuild.preflight.done board=%s outcome=%s manifest_ref=%s",
        board_id,
        result.outcome,
        manifest.manifest_ref,
    )
    return json.dumps(payload, default=str)


@mcp.tool()
async def okto_pulse_kg_rebuild_confirm(
    board_id: str,
    operation: str,
    preflight_hash: str,
    manifest_ref: str,
) -> str:
    """Issue the single-use confirmation token for a KG rebuild. REST twin:
    POST /api/v1/kg/rebuild/confirm. Loads the manifest persisted by
    /preflight via manifest_ref (NEVER re-enumerates), verifies that
    preflight_hash matches (SHA-256 hex from /preflight), and issues the
    confirmation token — pass it to okto_pulse_kg_rebuild_run. board_id and
    operation must match the /preflight call; a hash/manifest mismatch fails
    closed.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    actor_id = ctx.agent.id if hasattr(ctx, "agent") else "agent-mcp"

    from okto_pulse.core.kg.rebuild_audit import (
        require_rebuild_audit_artifact_store,
    )
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
        return json.dumps(
            {
                "error": "unsupported_operation",
                "reason": "operation not in canonical set",
            }
        )

    if operation not in SUPPORTED_REBUILD_OPERATIONS:
        return json.dumps(
            {
                "error": "operation_pending_implementation",
                "reason": (
                    f"operation={operation!r} not implemented yet; "
                    f"only {sorted(SUPPORTED_REBUILD_OPERATIONS)} supported"
                ),
            }
        )

    try:
        validate_preflight_hash(preflight_hash)
    except ValueError as exc:
        return json.dumps({"error": "invalid_preflight_hash", "reason": str(exc)})

    def _load_and_issue():
        artifact_store = require_rebuild_audit_artifact_store()
        manifest_store = KGRebuildSourceManifest(artifact_store=artifact_store)
        manifest = manifest_store.load(manifest_ref)
        if manifest is None:
            return {
                "error": "manifest_not_found",
                "reason": "manifest_ref does not exist",
            }
        if manifest.board_id != board_id:
            return {
                "error": "manifest_board_mismatch",
                "reason": "manifest_ref belongs to a different board",
            }
        if manifest.preflight_hash != preflight_hash:
            return {
                "error": "preflight_hash_mismatch",
                "reason": "preflight_hash does not match manifest binding",
            }

        store = RebuildConfirmationStore(artifact_store=artifact_store)
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
        result = await asyncio.to_thread(_load_and_issue)
    except Exception as exc:
        _rebuild_logger.error(
            "kg.rebuild.confirm.failed board=%s err=%s", board_id, exc
        )
        return json.dumps({"error": "confirm_failed", "detail": str(exc)})

    _rebuild_logger.info(
        "kg.rebuild.confirm.done board=%s confirmation_id=%s",
        board_id,
        result.get("confirmation_id"),
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
    """Execute the KG rebuild. REST twin: POST /api/v1/kg/rebuild/run. Consumes
    the single-use token from okto_pulse_kg_rebuild_confirm and runs the
    rebuild under the KG-01 admin lane; NEVER mutates the graph if the token
    is invalid, the manifest changed or the exclusive lock fails.
    confirmation_id/operation/preflight_hash/manifest_ref must match
    /confirm; reason is audit-only (max 512 chars). Admission gate (FR8):
    refuses before consuming the token when graph_state == 'quarantined';
    recovery_needed is admitted (rebuild is the prescribed exit).
    Errors: rebuild_refused_quarantined.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    actor_id = ctx.agent.id if hasattr(ctx, "agent") else "agent-mcp"

    from okto_pulse.core.application.kg_rebuild import (
        build_rebuild_step_adapter as _build_rebuild_step_adapter,
        build_source_store as _build_source_store,
        provider_missing_payload as _provider_missing_payload,
        refuse_rebuild_if_quarantined as _refuse_rebuild_if_quarantined,
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
                    board_id,
                    refuse_fn=_refuse_rebuild_if_quarantined,
                    scheduler_control=get_scheduler_control_for_mcp(),
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
        RebuildAuditKGGenerationRepository,
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

    lock = KGSingleWriterLock()

    def _always_owner(bid: str, owner_token: str) -> bool:
        m = lock.inspect(board_id=bid)
        return m is not None and m.owner_token == owner_token

    safe_lifecycle = KGSafeWriteLifecycle(
        step_adapter=get_kg_registry().graph_lifecycle.apply_step,
        owner_probe=LockOwnerProbe(is_active_owner=_always_owner),
        health_probe=HealthProbe(classify=lambda b, g, status, step: "at_risk"),
    )

    source_store_fetch = _build_source_store()
    enumerator = RebuildSourceEnumerator(source_store=source_store_fetch)
    try:
        artifact_store = get_kg_registry().require_rebuild_audit_artifact_store()
    except Exception as exc:
        from okto_pulse.core.composition import RuntimeProviderMissing

        if isinstance(exc, RuntimeProviderMissing):
            return json.dumps(_provider_missing_payload(exc))
        raise

    manifest_store_obj = KGRebuildSourceManifest(artifact_store=artifact_store)

    try:
        _step_adapter_with_sources = _build_rebuild_step_adapter(
            manifest_store_obj=manifest_store_obj,
        )
    except Exception as exc:
        from okto_pulse.core.composition import RuntimeProviderMissing

        if isinstance(exc, RuntimeProviderMissing):
            return json.dumps(_provider_missing_payload(exc))
        raise
    audit_recorder = ConfirmationConsumptionAuditRecorder(
        artifact_store=artifact_store,
    )
    event_publisher = KGRebuiltEventPublisher(
        artifact_store=artifact_store,
    )
    cognitive_marker = CognitivePendingMarker(
        artifact_store=artifact_store,
    )

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
        base_dir=None,
        single_writer_lock=lock,
        safe_write_lifecycle=safe_lifecycle,
        quarantine_service=None,
        confirmation_store=RebuildConfirmationStore(
            audit_recorder=audit_recorder,
            artifact_store=artifact_store,
        ),
        manifest_store=manifest_store_obj,
        source_enumerator=enumerator,
        rebuild_step_adapter=_step_adapter_with_sources,
        generation_repository=RebuildAuditKGGenerationRepository(
            artifact_store=artifact_store
        ),
        promotion_guard=KGGenerationPromotionGuard,
        report_store=RebuildReportStore(artifact_store=artifact_store),
        terminal_state_guard=RebuildReportTerminalStateGuard,
        event_emitter=event_handler,
        orphan_scan_provider=lambda board_id, generation_id: orphan_scanner.scan(
            board_id=board_id,
            generation_id=generation_id,
        ),
        artifact_store=artifact_store,
    )

    try:
        result = await asyncio.to_thread(
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
        board_id,
        result.outcome,
        result.run_id,
    )
    return json.dumps(
        {
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
        },
        default=str,
    )


@mcp.tool()
async def okto_pulse_kg_quarantine_restore(
    quarantine_id: str,
    apply: bool = False,
) -> str:
    """Restore a board's KG from quarantine — dry-run/apply with backup-swap
    (KGD-01 FR4/BR4). apply=false (default) returns the auditable plan (files,
    destinations, conflicts, sizes) with NO mutation. apply=true moves the
    board's live files to a NEW quarantine with manifest (backup_quarantine_id
    in the result), copies the snapshot back, validates the board open and
    emits kg.quarantine.restore_dry_run / kg.quarantine.restored. Response:
    {plan, applied, backup_quarantine_id?}.
    Errors: quarantine_not_found, board_locked (requires a maintenance
    window), partial_restore (manifest records the exact state for rollback —
    never a silent half-restore).
    """
    agent = await _get_authenticated_agent()
    if agent is None:
        return _auth_error()

    from okto_pulse.core.composition import RuntimeProviderMissing
    from okto_pulse.core.kg.interfaces import get_kg_registry
    from okto_pulse.core.kg.interfaces.quarantine_restore import (
        QuarantineRestoreError,
    )

    try:
        restore = get_kg_registry().require_quarantine_restore()
    except RuntimeProviderMissing as exc:
        return json.dumps(
            {
                "error": "runtime_provider_missing",
                "detail": str(exc),
            }
        )

    # KGD-01: o handler no core fala só com a PORTA QuarantineRestore; o
    # adapter concreto (filesystem/Ladybug) é injetado pela composition do
    # Community (hexagonal — TR1).
    try:
        plan = await asyncio.to_thread(restore.plan, quarantine_id)
    except QuarantineRestoreError as exc:
        return json.dumps(exc.to_payload(), default=str)

    plan_payload = plan.to_payload()
    if not apply:
        return json.dumps(
            {
                "plan": plan_payload,
                "applied": False,
                "quarantine_id": plan.quarantine_id,
                "board_id": plan.board_id,
                "board_dir": plan.board_dir,
                "conflicts": list(plan.conflicts),
                "total_bytes": plan.total_bytes,
            },
            default=str,
        )

    # apply MUTA o board de destino — exige acesso do agente ao board
    # resolvido pelo manifest da quarentena.
    ctx = await _get_agent_ctx(plan.board_id)
    if ctx is None:
        return _auth_error()

    try:
        report = await asyncio.to_thread(restore.apply, quarantine_id)
    except QuarantineRestoreError as exc:
        return json.dumps(exc.to_payload(), default=str)

    return json.dumps(
        {
            "plan": plan_payload,
            "applied": report.applied,
            "backup_quarantine_id": report.backup_quarantine_id,
            "quarantine_id": report.quarantine_id,
            "board_id": report.board_id,
            "restored_files": list(report.restored_files),
            "open_validated": report.open_validated,
            "errors": list(report.errors),
        },
        default=str,
    )


# ============================================================================
# CONSOLIDATED POLYMORPHIC LIST HANDLERS (spec P0.B — TR-B1)
#
# These 4 tools are the supported list surface. The 15 entity-specific
# okto_pulse_list_* tools are intentionally not registered.
# ============================================================================


@mcp.tool()
async def okto_pulse_list_by_board(
    board_id: str,
    entity_type: Annotated[str, Field(description="spec | ideation | refinement | sprint | story | topic")],
    filters: Annotated[dict[str, Any] | str | None, Field(description="Per-type filter dict; refinement REQUIRES ideation_id, sprint REQUIRES spec_id")] = None,
    limit: int = 100,
    offset: int = 0,
) -> str:
    """List top-level entities of a board by type (replaces the entity-specific
    list_* tools: specs, ideations, refinements, sprints, stories, topics).
    Returns FULL entity bodies — prefer a low limit for spec listings.

    filters by entity_type: spec: status, labels, assignee_id; ideation:
    status, labels, derivation_pending; refinement: ideation_id (required),
    status, labels, derivation_pending; sprint: spec_id (required), status;
    story: status, topic_id, linked, converted, include_archived; topic:
    include_archived. derivation_pending (bool) — triage for done
    ideations/refinements still lacking a derived child; follow with
    derive_spec_from_ideation / derive_spec_from_refinement.
    Errors: invalid_filter (unknown keys; returns allowed keys),
    missing_required_filter. Docs: okto-pulse://reference/list_tools
    """
    from okto_pulse.core.mcp.filters import (
        invalid_filter_keys,
        supported_filter_keys,
        validate_filters,
    )

    # Auto-deserialize string JSON (MCP transport convention — other tools use coerce_to_list_str)
    if isinstance(filters, str):
        if not filters.strip():
            filters = None
        else:
            try:
                filters = json.loads(filters)
            except json.JSONDecodeError as e:
                return _structured_error(
                    "invalid_filter", [], None, f"Invalid JSON in filters: {e}"
                )

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
        return _structured_error(
            "invalid_filter",
            supported_filter_keys(entity_type, scope="by_board"),
            None,
            err,
            invalid_keys=invalid_filter_keys(
                entity_type, filters or {}, scope="by_board"
            ),
        )

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
            paginated = items[offset : offset + limit]
            return json.dumps(
                {
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
                },
                default=str,
            )

        elif entity_type == "ideation":
            total = len(items)
            paginated = items[offset : offset + limit]
            return json.dumps(
                {
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
                            "active_refinement_count": getattr(
                                i, "active_refinement_count", 0
                            ),
                            "active_spec_count": getattr(i, "active_spec_count", 0),
                            "derivation_pending": is_derivation_pending_ideation(i),
                            "version": i.version,
                            "assignee_id": i.assignee_id,
                            "labels": i.labels,
                            "created_at": i.created_at.isoformat(),
                            "updated_at": i.updated_at.isoformat(),
                        }
                        for i in paginated
                    ],
                },
                default=str,
            )

        elif entity_type == "refinement":
            ideation_id = filters.get("ideation_id", "")
            total = len(items)
            paginated = items[offset : offset + limit]
            return json.dumps(
                {
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
                },
                default=str,
            )

        elif entity_type == "sprint":
            spec_id = filters.get("spec_id", "")
            total = len(items)
            paginated = items[offset : offset + limit]
            return json.dumps(
                {
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
                },
                default=str,
            )

        elif entity_type == "story":
            total = len(items)
            paginated = items[offset : offset + limit]
            return json.dumps(
                {
                    "board_id": board_id,
                    "entity_type": entity_type,
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "items": [_story_payload(s) for s in paginated],
                },
                default=str,
            )

        else:  # topic
            total = len(items)
            paginated = items[offset : offset + limit]
            return json.dumps(
                {
                    "board_id": board_id,
                    "entity_type": entity_type,
                    "total": total,
                    "offset": offset,
                    "limit": limit,
                    "items": [_topic_payload(t) for t in paginated],
                },
                default=str,
            )


@mcp.tool()
async def okto_pulse_list_qa(
    board_id: str,
    entity_type: str,
    entity_id: str,
    filters: dict[str, Any] | str | None = None,
) -> str:
    """List Q&A items for a spec, ideation, or refinement.

    Consolidates: list_spec_qa, list_ideation_qa, list_refinement_qa."""
    from okto_pulse.core.mcp.filters import (
        invalid_filter_keys,
        supported_filter_keys,
        validate_filters,
    )

    # Auto-deserialize string JSON (MCP transport convention)
    if isinstance(filters, str):
        if not filters.strip():
            filters = None
        else:
            try:
                filters = json.loads(filters)
            except json.JSONDecodeError as e:
                return _structured_error(
                    "invalid_filter", [], None, f"Invalid JSON in filters: {e}"
                )

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
        return _structured_error(
            "invalid_filter",
            supported_filter_keys(entity_type, scope="qa"),
            None,
            err,
            invalid_keys=invalid_filter_keys(entity_type, filters or {}, scope="qa"),
        )

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpListQaCommand,
        McpListQaUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListQaUseCase().execute(
            McpListQaCommand(entity_type, entity_id, filters or {}),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


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
    from okto_pulse.core.mcp.filters import (
        invalid_filter_keys,
        supported_filter_keys,
        validate_filters,
    )

    # Auto-deserialize string JSON (MCP transport convention)
    if isinstance(filters, str):
        if not filters.strip():
            filters = None
        else:
            try:
                filters = json.loads(filters)
            except json.JSONDecodeError as e:
                return _structured_error(
                    "invalid_filter", [], None, f"Invalid JSON in filters: {e}"
                )

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
        return _structured_error(
            "invalid_filter",
            supported_filter_keys(entity_type, scope="knowledge"),
            None,
            err,
            invalid_keys=invalid_filter_keys(
                entity_type, filters or {}, scope="knowledge"
            ),
        )

    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.application.use_cases.base import EntityNotFoundError
    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpListKnowledgeCommand,
        McpListKnowledgeUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    try:
        async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
            result = await McpListKnowledgeUseCase().execute(
                McpListKnowledgeCommand(
                    board_id, entity_type, entity_id, filters or {}
                ),
                actor=actor,
                uow=uow,
            )
    except EntityNotFoundError as exc:
        if exc.entity_type == "ideation":
            return json.dumps({"error": "Ideation not found"})
        if exc.entity_type == "card":
            return json.dumps({"error": "Card not found"})
        return _mcp_entity_not_found_error(exc)
    return json.dumps(result.payload, default=str)


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

    from okto_pulse.core.application.use_cases.mcp_mockups_copy_lists import (
        McpListSnapshotsCommand,
        McpListSnapshotsUseCase,
    )
    from okto_pulse.core.inbound.mcp_adapter import MCPAdapterContract

    actor = MCPAdapterContract.actor(ctx, board_id=board_id)
    async with get_unit_of_work_factory_for_mcp()(actor=actor) as uow:
        result = await McpListSnapshotsUseCase().execute(
            McpListSnapshotsCommand(entity_type, entity_id),
            actor=actor,
            uow=uow,
        )
    return json.dumps(result.payload, default=str)


# ============================================================================
# SERVER STARTUP
# ============================================================================


def build_mcp_asgi_app(trace_sink: McpTraceSink | None = None):
    """Compatibility facade to the edition-owned MCP host provider.

    Core retains the command catalog and tool rules only.  The ASGI stack,
    credential middleware and concrete listener are selected by an edition
    through :class:`~okto_pulse.core.ports.mcp_host.McpHostProvider`.
    """

    return get_mcp_host_provider().build_asgi_app(mcp, trace_sink=trace_sink)


def mount_mcp(
    app,
    *,
    mount_path: str = "/mcp",
    trace_sink: McpTraceSink | None = None,
) -> None:
    """Compatibility facade to the edition-owned MCP mount operation."""

    get_mcp_host_provider().mount(
        app,
        mcp,
        mount_path=mount_path,
        trace_sink=trace_sink,
    )


def run_mcp_server():
    """Reject legacy Core-owned listener startup.

    Starting a concrete MCP listener is an edition concern. Community serves
    the catalog through its Local First host in ``community.main.serve``.
    """
    warnings.warn(
        "okto_pulse.core.mcp.server.run_mcp_server is retired. Use the "
        "edition-owned MCP host (Community: okto_pulse.community.main.serve).",
        DeprecationWarning,
        stacklevel=2,
    )
    raise RuntimeError(
        "Core cannot start an MCP listener. Compose an edition MCP host instead."
    )


if __name__ == "__main__":
    run_mcp_server()

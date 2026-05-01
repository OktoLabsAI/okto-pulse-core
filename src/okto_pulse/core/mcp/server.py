"""MCP Server for Okto Pulse Core - enables AI agents to interact with the board."""

import functools
import inspect
import json
import logging
import os
import re
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import uvicorn
from fastmcp import FastMCP
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from okto_pulse.core.infra.config import get_mcp_settings, get_settings
from okto_pulse.core.infra.permissions import Permissions, check_permission
from okto_pulse.core.mcp.helpers import coerce_to_list_str, parse_multi_value
from okto_pulse.core.mcp.trace_middleware import install_if_enabled as _install_trace
from okto_pulse.core.services.main import (
    AgentService,
    AttachmentService,
    BoardService,
    CardService,
    CommentService,
    GuidelineService,
    IdeationQAService,
    IdeationService,
    QAService,
    RefinementKnowledgeService,
    RefinementQAService,
    RefinementService,
    SpecKnowledgeService,
    SpecQAService,
    SpecService,
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


def _auth_error() -> str:
    return json.dumps({"error": "Authentication failed or board access denied"})


def _perm_error(msg: str) -> str:
    return json.dumps({"error": msg})


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
from okto_pulse.core.services.analytics_service import (
    decisions_stats as _decisions_stats,  # noqa: F401
    filter_decisions_by_status as _filter_decisions_by_status,  # noqa: F401
    render_decisions_markdown as _render_decisions_markdown,  # noqa: F401
)


# D-7: spec_coverage agora canônico em services/analytics_service.py — re-export
# preserva callers existentes em mcp/server.py + tests.
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
    No parameters needed — the agent is identified by the API key in the MCP connection.

    Returns:
        JSON with agent profile details
    """
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
    No board_id needed — this updates the global agent profile.

    Args:
        description: New description (optional, empty = no change)
        objective: New objective (optional, empty = no change)

    Returns:
        JSON with updated profile
    """
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
    No parameters needed — the agent is identified by the API key in the MCP connection.

    Returns:
        JSON with agent identity and list of boards
    """
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
    By default only returns UNSEEN mentions. Use include_seen="true" to get all.

    Args:
        board_id: Board ID to search within
        include_seen: "true" to include already-seen mentions (default "false")

    Returns:
        JSON with unseen mentions, each with an item_id you can pass to mark_as_seen
    """
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
    Use this after processing mentions to avoid seeing them again.

    Args:
        board_id: Board ID (for access verification)
        item_ids: Multi-value item IDs to mark as seen (from list_my_mentions item_id
            field). Preferred native list (e.g. ``["c_a", "qa_b"]``); legacy string
            accepted as JSON array or pipe-separated. Comma-only string is REJECTED.
            See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

    Returns:
        JSON with count of newly marked items
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    from sqlalchemy import select
    from sqlalchemy.dialects.postgresql import insert as pg_insert

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
    Use this to check if there's anything new without fetching full details.

    Args:
        board_id: Board ID

    Returns:
        JSON with counts of unseen mentions and recent activity
    """
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


@mcp.tool()
async def okto_pulse_get_board(board_id: str) -> str:
    """
    Get board details with all cards and agents.

    Args:
        board_id: Board ID to retrieve

    Returns:
        JSON string with board details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = BoardService(db)
        board = await service.get_board(board_id)
        await db.commit()

        if not board:
            return json.dumps({"error": "Board not found"})

        agent_service = AgentService(db)
        board_agents = await agent_service.list_agents_for_board(board_id)

        spec_service = SpecService(db)
        board_specs = await spec_service.list_specs(board_id)

        ideation_service = IdeationService(db)
        board_ideations = await ideation_service.list_ideations(board_id)

        return json.dumps(
            {
                "id": board.id,
                "name": board.name,
                "description": board.description,
                "owner_id": board.owner_id,
                "created_at": board.created_at.isoformat(),
                "updated_at": board.updated_at.isoformat(),
                "ideations": [
                    {
                        "id": i.id,
                        "title": i.title,
                        "status": i.status.value,
                        "complexity": i.complexity.value if i.complexity else None,
                        "version": i.version,
                        "labels": i.labels,
                    }
                    for i in board_ideations
                ],
                "specs": [
                    {
                        "id": s.id,
                        "title": s.title,
                        "status": s.status.value,
                        "version": s.version,
                        "labels": s.labels,
                    }
                    for s in board_specs
                ],
                "cards": [
                    {
                        "id": c.id,
                        "title": c.title,
                        "description": c.description,
                        "status": c.status.value,
                        "position": c.position,
                        "assignee_id": c.assignee_id,
                        "spec_id": c.spec_id,
                        "due_date": (
                            c.due_date.isoformat() if c.due_date else None
                        ),
                        "labels": c.labels,
                    }
                    for c in board.cards
                ],
                "agents": [
                    {
                        "id": a.id,
                        "name": a.name,
                        "description": a.description,
                        "is_active": a.is_active,
                    }
                    for a in board_agents
                ],
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_list_agents(board_id: str) -> str:
    """
    List all agents registered on the board.

    Args:
        board_id: Board ID

    Returns:
        JSON array of agents
    """
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
    List all members of the board (owner + agents).

    Args:
        board_id: Board ID

    Returns:
        JSON with owner info and agents list
    """
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


@mcp.tool()
async def okto_pulse_get_activity_log(
    board_id: str, limit: int = 50, offset: int = 0, action: str = "", card_id: str = ""
) -> str:
    """
    Get the activity log (history) for the board with optional filtering and pagination.

    Args:
        board_id: Board ID
        limit: Maximum number of entries to return (default 50, max 200)
        offset: Skip first N entries (default 0)
        action: Filter by action type (optional) — e.g. card_created, card_moved, spec_updated
        card_id: Filter by card ID (optional) — only activities for this card

    Returns:
        JSON array of activity log entries
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    from sqlalchemy import select

    from okto_pulse.core.models.db import ActivityLog

    async with get_db_for_mcp() as db:
        query = select(ActivityLog).where(ActivityLog.board_id == board_id)
        if action:
            query = query.where(ActivityLog.action == action)
        if card_id:
            query = query.where(ActivityLog.card_id == card_id)
        query = query.order_by(ActivityLog.created_at.desc()).offset(offset).limit(limit)
        result = await db.execute(query)
        logs = list(result.scalars().all())
        await db.commit()

        return json.dumps(
            [
                {
                    "id": log.id,
                    "action": log.action,
                    "actor_type": log.actor_type,
                    "actor_id": log.actor_id,
                    "actor_name": log.actor_name,
                    "card_id": log.card_id,
                    "details": log.details,
                    "created_at": log.created_at.isoformat(),
                }
                for log in logs
            ],
            default=str,
        )


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

    Args:
        board_id: Board ID
        title: Card title
        spec_id: REQUIRED — Spec ID to link this card to. The spec must be in 'done' status.
            For bug cards, this is auto-resolved from the origin task if not provided.
        description: Card description (optional). Supports Markdown and Mermaid diagrams (```mermaid code blocks).
        details: Card details/rich text (optional). Supports Markdown and Mermaid diagrams.
        status: Card status - one of: not_started, started, in_progress, validation, on_hold, done, cancelled
        priority: Card priority - one of: none, low, medium, high, very_high, critical (default: none)
        assignee_id: User ID to assign (optional)
        labels: Multi-value labels — preferred native list (e.g. ``["bug", "frontend"]``);
            legacy string accepted as JSON array ``'["bug", "frontend"]'`` or
            pipe-separated ``"bug|frontend"``. Comma-only string is REJECTED.
            See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.
        test_scenario_ids: Multi-value test scenario IDs (e.g. ``["ts_abc", "ts_def"]``)
            — same input shapes as ``labels`` above. For test cards, this is MANDATORY.
            When provided, automatically creates bidirectional links between the
            card and the scenarios.
        card_type: Card type - "normal" (default) or "bug". Bug cards require origin_task_id, severity, expected_behavior, observed_behavior.
        origin_task_id: REQUIRED for bug cards — ID of the task that originated the bug. The spec is auto-resolved from this task.
        severity: REQUIRED for bug cards — one of: critical, major, minor
        expected_behavior: REQUIRED for bug cards — what should happen
        observed_behavior: REQUIRED for bug cards — what actually happens
        steps_to_reproduce: Steps to reproduce the bug (optional)
        action_plan: Plan for fixing the bug (optional)

    Returns:
        JSON with created card details
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
                    from okto_pulse.core.models.schemas import SpecUpdate as SU
                    _, _err = await _safe_spec_update(spec_service, spec_id, ctx.agent_id, SU(test_scenarios=scenarios))
                    if _err:
                        return _err

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
    """
    Get detailed card information including attachments, Q&A, and comments.

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON with full card details
    """
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_task_context(
    board_id: str,
    card_id: str,
    include_knowledge: str = "true",
    include_mockups: str = "true",
    include_qa: str = "true",
    include_comments: str = "true",
    include_superseded: str = "false",
) -> str:
    """
    Get the FULL execution context for a task card. Aggregates the card data with
    all relevant spec information: functional requirements, technical requirements,
    acceptance criteria, test scenarios, business rules, API contracts, knowledge
    base entries, screen mockups, Q&A, and comments.

    **Always call this before starting work on a task** — it provides everything
    an agent needs to understand what to build, how to test it, and what rules apply.

    Args:
        board_id: Board ID
        card_id: Card ID
        include_knowledge: Include spec knowledge base entries (default "true")
        include_mockups: Include screen mockups from card and spec (default "true")
        include_qa: Include Q&A items from card and spec (default "true")
        include_comments: Include card comments (default "true")

    Returns:
        JSON with complete task context: card details + spec requirements + linked artifacts
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_kb = include_knowledge.lower() in ("true", "1", "yes")
    _inc_mockups = include_mockups.lower() in ("true", "1", "yes")
    _inc_qa = include_qa.lower() in ("true", "1", "yes")
    _inc_comments = include_comments.lower() in ("true", "1", "yes")
    _inc_superseded = include_superseded.lower() in ("true", "1", "yes")

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

                if _inc_mockups and spec.screen_mockups:
                    spec_data["screen_mockups"] = spec.screen_mockups

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
                        {
                            "id": kb.id,
                            "title": kb.title,
                            "content": kb.content,
                            "source_type": kb.source_type,
                        }
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

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_get_task_conclusions(board_id: str, card_id: str) -> str:
    """
    Get the conclusions of a completed task card. Conclusions describe what was done,
    the root cause (for bugs), decisions made, and any relevant notes.

    Useful for:
    - Understanding what was done in a previous task before starting related work
    - Bug triage — understanding root cause and fix approach
    - Knowledge transfer between agents or team members

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON with card title, status, conclusions, and bug details if applicable
    """
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
    """
    Update card details.

    Args:
        board_id: Board ID
        card_id: Card ID
        title: New title (optional, empty = no change)
        description: New description (optional)
        details: New details (optional)
        priority: New priority - one of: none, low, medium, high, very_high, critical (optional, empty = no change)
        assignee_id: New assignee (optional)
        labels: Multi-value labels — preferred native list (e.g. ``["bug", "frontend"]``);
            legacy string accepted as JSON array or pipe-separated. Comma-only
            string is REJECTED. See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.
        test_scenario_ids: Multi-value test scenario IDs — same input shapes as
            ``labels``. Use ``okto_pulse_link_task_to_scenario`` for
            bidirectional linking.
        severity: Bug severity - one of: critical, major, minor (optional, bug cards only)
        expected_behavior: Expected behavior description (optional, bug cards only)
        observed_behavior: Observed behavior description (optional, bug cards only)
        steps_to_reproduce: Steps to reproduce the bug (optional, bug cards only)
        action_plan: Plan for fixing the bug (optional, bug cards only)
        linked_test_task_ids: Multi-value test task card IDs linked to this bug
            (optional, bug cards only) — same input shapes as ``labels``.

    Returns:
        JSON with updated card details
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
    """
    Move a card to a different column/position on the board.
    Moving to 'done' REQUIRES a conclusion, completeness, and drift — all with justifications.

    Args:
        board_id: Board ID
        card_id: Card ID
        status: New status - one of: not_started, started, in_progress, validation, on_hold, done, cancelled
        position: New position in column (-1 = end of column)
        conclusion: REQUIRED when status='done'. Detailed summary of changes, files modified, decisions, test results, and follow-ups. Supports Markdown and Mermaid diagrams (```mermaid code blocks).
        completeness: REQUIRED when status='done'. 0-100, how much of the planned work was actually implemented. 100 = fully complete, 0 = nothing delivered. Use -1 when not moving to done.
        completeness_justification: REQUIRED when status='done'. Explains why the completeness score is what it is. E.g. "All planned endpoints implemented and tested" or "Deferred pagination to follow-up card".
        drift: REQUIRED when status='done'. 0-100, how much the implementation deviated from the original plan. 0 = exactly as planned, 100 = completely different approach. Use -1 when not moving to done.
        drift_justification: REQUIRED when status='done'. Explains what caused deviation from the original plan. E.g. "No deviation" or "Had to switch from REST to WebSocket due to real-time requirements discovered during implementation".

    Returns:
        JSON with updated card details
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
        except ValueError as e:
            return json.dumps({"error": str(e), "blocked_by_dependencies": True})

        if not updated:
            return json.dumps({"error": "Failed to move card"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id,
            card_id=card_id,
            action="card_moved",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={"status": status, "position": position},
        )
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
    """
    Delete a card from the board.

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON with success status
    """
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
    Circular dependencies are blocked automatically.

    Args:
        board_id: Board ID
        card_id: The card that will be blocked
        depends_on_id: The card it depends on

    Returns:
        JSON with success or error
    """
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
    Remove a dependency between two cards.

    Args:
        board_id: Board ID
        card_id: The card that has the dependency
        depends_on_id: The card it depended on

    Returns:
        JSON with success status
    """
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
    List cards that this card depends on and cards that depend on it.

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON with depends_on (blockers) and dependents (blocked by this card)
    """
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
    """
    List cards on the board with optional filters and pagination.

    Args:
        board_id: Board ID
        status: Filter by status (optional, empty = all). One of: not_started, started, in_progress, validation, on_hold, done, cancelled. Use "open" for cards NOT in done/cancelled.
        spec_id: Filter by spec ID (optional) — only cards linked to this spec
        priority: Filter by priority (optional) — one of: none, low, medium, high, very_high, critical
        assignee_id: Filter by assignee (optional)
        offset: Skip first N cards (default 0)
        limit: Max cards to return (default 50, max 200)

    Returns:
        JSON with filtered/paginated cards and summary counts
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

        return json.dumps(
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
            default=str,
        )


# ============================================================================
# Q&A TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_ask_question(board_id: str, card_id: str, question: str) -> str:
    """
    Add a question to a card's Q&A board.

    Args:
        board_id: Board ID
        card_id: Card ID
        question: Question text

    Returns:
        JSON with Q&A item details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import QACreate

    async with get_db_for_mcp() as db:
        service = QAService(db)
        qa = await service.create_question(
            card_id, ctx.agent_id, QACreate(question=question)
        )
        if not qa:
            return json.dumps(
                {"error": "Failed to create question (card not found)"}
            )
        await _log_card_activity(
            db, board_id, card_id, "question_added", ctx,
            {"question": question[:100]},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_answer_question(
    board_id: str, qa_id: str, answer: str
) -> str:
    """
    Answer a question on a card's Q&A board.

    Args:
        board_id: Board ID
        qa_id: Q&A item ID
        answer: Answer text

    Returns:
        JSON with updated Q&A details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import QAAnswer

    async with get_db_for_mcp() as db:
        service = QAService(db)
        qa = await service.answer_question(
            qa_id, ctx.agent_id, QAAnswer(answer=answer)
        )
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
    Delete a Q&A item from a card.

    Args:
        board_id: Board ID
        qa_id: Q&A item ID

    Returns:
        JSON with success status
    """
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
    Add a comment to a card.

    Args:
        board_id: Board ID
        card_id: Card ID
        content: Comment text. Supports Markdown and Mermaid diagrams (```mermaid code blocks).

    Returns:
        JSON with comment details
    """
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
    options: str,
    comment_type: str = "choice",
    allow_free_text: str = "false",
) -> str:
    """
    Add a choice board (poll) to a card. Responders can select from the options.

    Args:
        board_id: Board ID
        card_id: Card ID
        question: The question or prompt text displayed above the options
        options: Option labels in any of three formats:
            - JSON array (preferred when labels contain commas):
              ``'["Option A (with, commas)", "Option B"]'``
            - Pipe-separated (when labels contain commas but not pipes):
              ``"Option A|Option B|Option C"``
            - Comma-separated (legacy, fragile if a label contains a comma):
              ``"Option A,Option B,Option C"``
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.
        comment_type: "choice" for single-select (default) or "multi_choice" for multi-select
        allow_free_text: "true" to allow a free-text response in addition to selections

    Returns:
        JSON with the created choice comment
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.COMMENTS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    from okto_pulse.core.models.schemas import ChoiceOption, CommentCreate

    try:
        option_labels = parse_multi_value(options)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options: {e}"})
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
    selected: str,
    free_text: str = "",
) -> str:
    """
    Respond to a choice board comment by selecting one or more options.

    Args:
        board_id: Board ID
        comment_id: Comment ID of the choice board
        selected: Option IDs to select, accepted in three formats:
            ``'["opt_0", "opt_2"]'`` (JSON array, preferred), ``"opt_0|opt_2"``
            (pipe-separated), or ``"opt_0,opt_2"`` (legacy comma-separated).
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.
        free_text: Optional free-text response (only if allow_free_text is enabled)

    Returns:
        JSON with the updated comment including all responses
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    try:
        selected_ids = parse_multi_value(selected)
    except ValueError as e:
        return json.dumps({"error": f"Invalid selected: {e}"})
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
    Get all responses for a choice board comment.

    Args:
        board_id: Board ID
        comment_id: Comment ID of the choice board

    Returns:
        JSON with the choice options and all responses
    """
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
    List all comments on a card.

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON array of comments
    """
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
        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_update_comment(
    board_id: str, comment_id: str, content: str
) -> str:
    """
    Update the agent's own comment.

    Args:
        board_id: Board ID
        comment_id: Comment ID
        content: New comment text

    Returns:
        JSON with updated comment
    """
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
    Delete the agent's own comment.

    Args:
        board_id: Board ID
        comment_id: Comment ID

    Returns:
        JSON with success status
    """
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
    and never pass through the LLM context, saving tokens.

    Args:
        board_id: Board ID
        card_id: Card ID
        filename: Original filename
        content_base64: File content encoded as base64 (use for small files only)
        mime_type: MIME type of the file
        file_path: Absolute path to a local file on the MCP server host
        file_url: HTTP(S) URL of a file to fetch

    Returns:
        JSON with attachment details
    """
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
    List all attachments on a card.

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON array of attachments
    """
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
    Delete an attachment.

    Args:
        board_id: Board ID
        attachment_id: Attachment ID

    Returns:
        JSON with success status
    """
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
    evaluated, refined into refinements, and eventually derived into specs.

    Args:
        board_id: Board ID
        title: Ideation title
        description: High-level description of the idea (optional)
        problem_statement: What problem does this idea solve? (optional)
        proposed_approach: How might this be implemented? (optional)
        assignee_id: User/agent ID to assign (optional)
        labels: Comma-separated labels (optional)

    Returns:
        JSON with created ideation details
    """
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
    Get full details of an ideation including its refinements, specs, and Q&A items.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID

    Returns:
        JSON with ideation details and linked entities
    """
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
) -> str:
    """
    Get the FULL consolidated context of an ideation. Returns all data needed
    to evaluate, review, or derive refinements/specs from this ideation.

    **Always call this before evaluating, moving, or deriving from an ideation.**

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        include_knowledge: Include knowledge base entries (default "true")
        include_mockups: Include screen mockups (default "true")
        include_qa: Include Q&A items (default "true")

    Returns:
        JSON with complete ideation context: details + Q&A + mockups + KBs + refinements + specs + evaluation
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_kb = include_knowledge.lower() in ("true", "1", "yes")
    _inc_mockups = include_mockups.lower() in ("true", "1", "yes")
    _inc_qa = include_qa.lower() in ("true", "1", "yes")

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

        if _inc_kb and hasattr(ideation, "knowledge_bases"):
            result["knowledge_bases"] = [
                {
                    "id": kb.id,
                    "title": kb.title,
                    "description": kb.description,
                    "content": kb.content,
                    "mime_type": kb.mime_type,
                }
                for kb in (ideation.knowledge_bases or [])
            ]

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_list_ideations(
    board_id: str, status: str = "", offset: int = 0, limit: int = 50
) -> str:
    """
    List ideations for a board with optional filtering and pagination.

    Args:
        board_id: Board ID
        status: Filter by status (optional) — one of: draft, review, approved, evaluating, done, cancelled
        offset: Skip first N ideations (default 0)
        limit: Max ideations to return (default 50, max 200)

    Returns:
        JSON with filtered/paginated ideations
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        ideations = await service.list_ideations(board_id, status or None)
        await db.commit()

        total = len(ideations)
        paginated = ideations[offset:offset + limit]

        return json.dumps(
            {
                "board_id": board_id,
                "total": total,
                "offset": offset,
                "limit": limit,
                "ideations": [
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
            },
            default=str,
        )


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
    Update an ideation's fields. Content changes bump the version. Only non-empty fields are updated.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        title: New title (optional, empty = no change)
        description: New description (optional, empty = no change)
        problem_statement: New problem statement (optional, empty = no change)
        proposed_approach: New proposed approach (optional, empty = no change)
        assignee_id: New assignee (optional, empty = no change)
        labels: Comma-separated labels (optional, empty = no change)

    Returns:
        JSON with updated ideation details
    """
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
    - done → draft (new version)

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        status: New status — one of: draft, review, approved, evaluating, done, cancelled

    Returns:
        JSON with updated ideation status
    """
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
        ideation = await service.move_ideation(
            ideation_id, ctx.agent_id, IdeationMove(status=ideation_status), actor_name=ctx.agent_name
        )
        await db.commit()

        if not ideation:
            return json.dumps({"error": "Ideation not found"})

        return json.dumps(
            {
                "success": True,
                "ideation_id": ideation.id,
                "from_status": ideation.status.value,
                "to_status": status,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_ideation(board_id: str, ideation_id: str) -> str:
    """
    Delete an ideation. Linked refinements and Q&A are also deleted (cascade).

    Args:
        board_id: Board ID
        ideation_id: Ideation ID

    Returns:
        JSON with success status
    """
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
    """
    Evaluate an ideation's scope and compute its complexity (small/medium/large).
    Set scope assessment scores (1-5) for each dimension WITH justification, then the system computes complexity.
    - Any score >= 3 -> large (needs refinements before spec)
    - Any score >= 2 -> medium
    - All scores 1 -> small (can derive spec directly)

    Each score MUST include a justification explaining why that score was given.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        domains: Number of domains/systems affected, 1-5
        domains_justification: Why this score — which systems are impacted
        ambiguity: Level of requirement ambiguity, 1-5
        ambiguity_justification: Why this score — what is unclear or well-defined
        dependencies: External dependencies/coordination needed, 1-5
        dependencies_justification: Why this score — what dependencies exist

    Returns:
        JSON with the computed complexity and scope assessment
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationUpdate

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
) -> str:
    """
    Create a spec draft from a DONE ideation. The ideation must be in 'done' status
    (meaning it has been fully reviewed and snapshotted). The spec will have rich context
    compiled from the ideation but structured fields (requirements, criteria) left empty
    for deliberate analysis.

    Artifacts (mockups, KBs) from the ideation are automatically propagated to the spec.
    Use mockup_ids/kb_ids to select specific ones (default: all).

    Args:
        board_id: Board ID
        ideation_id: Ideation ID (must be in 'done' status)
        mockup_ids: Pipe-separated mockup IDs to propagate (optional, empty = all)
        kb_ids: Pipe-separated KB IDs to propagate (optional, empty = all)

    Returns:
        JSON with the created spec details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    _mockup_ids = parse_multi_value(mockup_ids) or None
    _kb_ids = parse_multi_value(kb_ids) or None

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        try:
            spec = await service.derive_spec(
                ideation_id, ctx.agent_id, skip_ownership_check=True,
                mockup_ids=_mockup_ids, kb_ids=_kb_ids,
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
async def okto_pulse_list_ideation_snapshots(board_id: str, ideation_id: str) -> str:
    """
    List all version snapshots of an ideation. Each snapshot is an immutable copy
    of the ideation's state at the moment it was marked as 'done'.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID

    Returns:
        JSON with list of snapshot summaries (version, title, complexity, created_at)
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationService(db)
        snapshots = await service.list_snapshots(ideation_id)
        await db.commit()

        return json.dumps(
            {
                "ideation_id": ideation_id,
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_ideation_snapshot(board_id: str, ideation_id: str, version: str) -> str:
    """
    Get the full immutable snapshot of an ideation at a specific version.
    Includes all fields as they were when the ideation was marked 'done',
    plus a snapshot of all Q&A at that point.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        version: Version number to retrieve

    Returns:
        JSON with complete snapshot including Q&A history
    """
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
    who made the change, and when.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        limit: Maximum number of history entries to return (default 30)

    Returns:
        JSON with list of history entries, newest first
    """
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
# IDEATION Q&A TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_ask_ideation_question(board_id: str, ideation_id: str, question: str) -> str:
    """
    Ask a question on an ideation's Q&A board. Use @Name to direct the question.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        question: Question text (use @Name to mention someone)

    Returns:
        JSON with Q&A item details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationQACreate

    async with get_db_for_mcp() as db:
        service = IdeationQAService(db)
        qa = await service.create_question(ideation_id, ctx.agent_id, IdeationQACreate(question=question))
        if not qa:
            return json.dumps({"error": "Ideation not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="ideation_question_added",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"ideation_id": ideation_id, "question": question[:100]},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_ask_ideation_choice_question(
    board_id: str,
    ideation_id: str,
    question: str,
    options: str,
    question_type: str = "choice",
    allow_free_text: str = "false",
) -> str:
    """
    Ask a choice question (poll/form) on an ideation's Q&A board.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        question: The question text
        options: Option labels in any of three formats:
            - JSON array (preferred when labels contain commas):
              ``'["Mermaid (text-based, lightweight)", "ExcaliDraw (heavy)"]'``
            - Pipe-separated (when labels contain commas but not pipes):
              ``"Option A|Option B|Option C"``
            - Comma-separated (legacy, fragile if a label contains a comma):
              ``"Option A,Option B,Option C"``
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.
        question_type: "choice" for single-select (default) or "multi_choice" for multi-select
        allow_free_text: "true" to also allow a free-text response alongside selections

    Returns:
        JSON with Q&A item including choices
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationQAChoiceOption, IdeationQACreate

    try:
        option_labels = parse_multi_value(options)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options: {e}"})
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
async def okto_pulse_answer_ideation_question(board_id: str, ideation_id: str, qa_id: str, answer: str = "", selected: str = "") -> str:
    """
    Answer a question on an ideation's Q&A board.
    For text questions, provide answer. For choice questions, provide selected option IDs.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID (for context/validation)
        qa_id: Q&A item ID to answer
        answer: Free-text answer (for text questions, or additional text on choice questions with allow_free_text)
        selected: Option IDs for choice questions, accepted in three formats:
            ``'["opt_0", "opt_2"]'`` (JSON array, preferred), ``"opt_0|opt_2"``
            (pipe-separated), or ``"opt_0,opt_2"`` (legacy comma-separated).
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.

    Returns:
        JSON with updated Q&A item
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import IdeationQAAnswer

    try:
        selected_list = parse_multi_value(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": f"Invalid selected: {e}"})

    async with get_db_for_mcp() as db:
        service = IdeationQAService(db)
        qa = await service.answer_question(
            qa_id, ctx.agent_id,
            IdeationQAAnswer(answer=answer or None, selected=selected_list),
        )
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
async def okto_pulse_list_ideation_qa(board_id: str, ideation_id: str) -> str:
    """
    List all Q&A items on an ideation. Check this to understand open questions or clarifications.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID

    Returns:
        JSON with list of Q&A items
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = IdeationQAService(db)
        items = await service.list_qa(ideation_id)
        await db.commit()

        return json.dumps(
            {
                "ideation_id": ideation_id,
                "count": len(items),
                "qa_items": [
                    {
                        "id": qa.id,
                        "question": qa.question,
                        "question_type": qa.question_type,
                        "choices": qa.choices,
                        "allow_free_text": qa.allow_free_text,
                        "answer": qa.answer,
                        "selected": qa.selected,
                        "asked_by": qa.asked_by,
                        "answered_by": qa.answered_by,
                        "created_at": qa.created_at.isoformat(),
                        "answered_at": qa.answered_at.isoformat() if qa.answered_at else None,
                    }
                    for qa in items
                ],
            },
            default=str,
        )


# ============================================================================
# REFINEMENT TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_create_refinement(
    board_id: str,
    ideation_id: str,
    title: str,
    description: str = "",
    in_scope: str = "",
    out_of_scope: str = "",
    analysis: str = "",
    decisions: str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
    mockup_ids: str = "",
    kb_ids: str = "",
) -> str:
    """
    Create a new refinement for a DONE ideation. The ideation must be in 'done' status
    (snapshotted) before refinements can be created. If description is not provided,
    context is compiled from the ideation's problem statement, approach, and Q&A.

    Artifacts (mockups, KBs) from the ideation are automatically propagated.
    Use mockup_ids/kb_ids to select specific ones (default: all).

    Args:
        board_id: Board ID
        ideation_id: Ideation ID (must be in 'done' status)
        title: Refinement title
        description: Description of this refinement aspect (optional — auto-compiled from ideation if empty)
        in_scope: Pipe-separated list of what IS in scope (e.g. "Auth flow|Token refresh|Session management")
        out_of_scope: Pipe-separated list of what is NOT in scope (e.g. "UI changes|Email notifications")
        analysis: Detailed analysis text (optional)
        decisions: Pipe-separated list of decisions made (e.g. "Use REST API|Cache with Redis") (optional)
        assignee_id: User/agent ID to assign (optional)
        labels: Comma-separated labels (optional)
        mockup_ids: Pipe-separated mockup IDs to propagate from ideation (optional, empty = all)
        kb_ids: Pipe-separated KB IDs to propagate from ideation (optional, empty = all)

    Returns:
        JSON with created refinement details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementCreate

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        refinement_data = RefinementCreate(
            ideation_id=ideation_id,
            title=title,
            description=description.replace("\\n", "\n") if description else None,
            in_scope=parse_multi_value(in_scope) or None,
            out_of_scope=parse_multi_value(out_of_scope) or None,
            analysis=analysis.replace("\\n", "\n") if analysis else None,
            decisions=parse_multi_value(decisions) or None,
            assignee_id=assignee_id or None,
            labels=coerce_to_list_str(labels) or None,
            mockup_ids=parse_multi_value(mockup_ids) or None,
            kb_ids=parse_multi_value(kb_ids) or None,
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
    Get full details of a refinement including its specs and Q&A items.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID

    Returns:
        JSON with refinement details and linked entities
    """
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
) -> str:
    """
    Get the FULL consolidated context of a refinement. Returns all data needed
    to review, derive specs, or evaluate this refinement.

    **Always call this before moving, evaluating, or deriving a spec from a refinement.**

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        include_knowledge: Include knowledge base entries (default "true")
        include_mockups: Include screen mockups (default "true")
        include_qa: Include Q&A items (default "true")

    Returns:
        JSON with complete refinement context: details + scope + Q&A + mockups + KBs + derived specs
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_kb = include_knowledge.lower() in ("true", "1", "yes")
    _inc_mockups = include_mockups.lower() in ("true", "1", "yes")
    _inc_qa = include_qa.lower() in ("true", "1", "yes")

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

        if _inc_kb and hasattr(refinement, "knowledge_bases"):
            result["knowledge_bases"] = [
                {
                    "id": kb.id,
                    "title": kb.title,
                    "description": kb.description,
                    "content": kb.content,
                    "mime_type": kb.mime_type,
                }
                for kb in (refinement.knowledge_bases or [])
            ]

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_list_refinements(
    board_id: str, ideation_id: str, status: str = "", offset: int = 0, limit: int = 50
) -> str:
    """
    List refinements for an ideation with optional filtering and pagination.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID
        status: Filter by status (optional) — one of: draft, in_progress, done, cancelled
        offset: Skip first N refinements (default 0)
        limit: Max refinements to return (default 50, max 200)

    Returns:
        JSON with filtered/paginated refinements
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        refinements = await service.list_refinements(ideation_id)
        await db.commit()

        if status:
            refinements = [r for r in refinements if r.status.value == status]

        total = len(refinements)
        paginated = refinements[offset:offset + limit]

        return json.dumps(
            {
                "ideation_id": ideation_id,
                "total": total,
                "offset": offset,
                "limit": limit,
                "refinements": [
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_update_refinement(
    board_id: str,
    refinement_id: str,
    title: str = "",
    description: str = "",
    in_scope: str = "",
    out_of_scope: str = "",
    analysis: str = "",
    decisions: str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Update a refinement's fields. Content changes bump the version. Only non-empty fields are updated.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        title: New title (optional, empty = no change)
        description: New description (optional, empty = no change)
        in_scope: Pipe-separated list of in-scope items (optional, empty = no change)
        out_of_scope: Pipe-separated list of out-of-scope items (optional, empty = no change)
        analysis: New analysis (optional, empty = no change)
        decisions: Pipe-separated list of decisions (optional, empty = no change)
        assignee_id: New assignee (optional, empty = no change)
        labels: Comma-separated labels (optional, empty = no change)

    Returns:
        JSON with updated refinement details
    """
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
        update_kwargs["in_scope"] = parse_multi_value(in_scope)
    if out_of_scope:
        update_kwargs["out_of_scope"] = parse_multi_value(out_of_scope)
    if analysis:
        update_kwargs["analysis"] = analysis.replace("\\n", "\n")
    if decisions:
        update_kwargs["decisions"] = parse_multi_value(decisions)
    if assignee_id:
        update_kwargs["assignee_id"] = assignee_id
    if labels:
        try:
            update_kwargs["labels"] = coerce_to_list_str(labels)
        except ValueError as e:
            return json.dumps({"error": f"Invalid labels: {e}"})

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
    - done → draft (new version)

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        status: New status — one of: draft, review, approved, done, cancelled

    Returns:
        JSON with updated refinement status
    """
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
                "from_status": refinement.status.value,
                "to_status": status,
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_delete_refinement(board_id: str, refinement_id: str) -> str:
    """
    Delete a refinement. Linked Q&A items are also deleted (cascade).

    Args:
        board_id: Board ID
        refinement_id: Refinement ID

    Returns:
        JSON with success status
    """
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
) -> str:
    """
    Create a spec draft from a DONE refinement. The refinement must be in 'done' status.
    Context is compiled from the refinement's scope, analysis, decisions, and Q&A.

    Artifacts (mockups, KBs) from the refinement are automatically propagated to the spec.
    Use mockup_ids/kb_ids to select specific ones (default: all).

    Args:
        board_id: Board ID
        refinement_id: Refinement ID (must be in 'done' status)
        mockup_ids: Pipe-separated mockup IDs to propagate (optional, empty = all)
        kb_ids: Pipe-separated KB IDs to propagate (optional, empty = all)

    Returns:
        JSON with the created spec details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    _mockup_ids = parse_multi_value(mockup_ids) or None
    _kb_ids = parse_multi_value(kb_ids) or None

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        try:
            spec = await service.derive_spec(
                refinement_id, ctx.agent_id, skip_ownership_check=True,
                mockup_ids=_mockup_ids, kb_ids=_kb_ids,
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
    who made the change, and when.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        limit: Maximum number of history entries to return (default 30)

    Returns:
        JSON with list of history entries, newest first
    """
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
    Ask a question on a refinement's Q&A board. Use @Name to direct the question.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        question: Question text (use @Name to mention someone)

    Returns:
        JSON with Q&A item details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementQACreate

    async with get_db_for_mcp() as db:
        service = RefinementQAService(db)
        qa = await service.create_question(refinement_id, ctx.agent_id, RefinementQACreate(question=question))
        if not qa:
            return json.dumps({"error": "Refinement not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="refinement_question_added",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"refinement_id": refinement_id, "question": question[:100]},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_ask_refinement_choice_question(
    board_id: str,
    refinement_id: str,
    question: str,
    options: str,
    question_type: str = "choice",
    allow_free_text: str = "false",
) -> str:
    """
    Ask a choice question (poll/form) on a refinement's Q&A board.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        question: The question text
        options: Option labels in any of three formats:
            - JSON array (preferred when labels contain commas):
              ``'["Mermaid (text-based, lightweight)", "ExcaliDraw (heavy)"]'``
            - Pipe-separated (when labels contain commas but not pipes):
              ``"Option A|Option B|Option C"``
            - Comma-separated (legacy, fragile if a label contains a comma):
              ``"Option A,Option B,Option C"``
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.
        question_type: "choice" for single-select (default) or "multi_choice" for multi-select
        allow_free_text: "true" to also allow a free-text response alongside selections

    Returns:
        JSON with Q&A item including choices
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementQAChoiceOption, RefinementQACreate

    try:
        option_labels = parse_multi_value(options)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options: {e}"})
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
async def okto_pulse_answer_refinement_question(board_id: str, refinement_id: str, qa_id: str, answer: str = "", selected: str = "") -> str:
    """
    Answer a question on a refinement's Q&A board.
    For text questions, provide answer. For choice questions, provide selected option IDs.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID (for context/validation)
        qa_id: Q&A item ID to answer
        answer: Free-text answer (for text questions, or additional text on choice questions with allow_free_text)
        selected: Option IDs for choice questions, accepted in three formats:
            ``'["opt_0", "opt_2"]'`` (JSON array, preferred), ``"opt_0|opt_2"``
            (pipe-separated), or ``"opt_0,opt_2"`` (legacy comma-separated).
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.

    Returns:
        JSON with updated Q&A item
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import RefinementQAAnswer

    try:
        selected_list = parse_multi_value(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": f"Invalid selected: {e}"})

    async with get_db_for_mcp() as db:
        service = RefinementQAService(db)
        qa = await service.answer_question(
            qa_id, ctx.agent_id,
            RefinementQAAnswer(answer=answer or None, selected=selected_list),
        )
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
async def okto_pulse_list_refinement_qa(board_id: str, refinement_id: str) -> str:
    """
    List all Q&A items on a refinement. Check this to understand open questions or clarifications.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID

    Returns:
        JSON with list of Q&A items
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementQAService(db)
        items = await service.list_qa(refinement_id)
        await db.commit()

        return json.dumps(
            {
                "refinement_id": refinement_id,
                "count": len(items),
                "qa_items": [
                    {
                        "id": qa.id,
                        "question": qa.question,
                        "question_type": qa.question_type,
                        "choices": qa.choices,
                        "allow_free_text": qa.allow_free_text,
                        "answer": qa.answer,
                        "selected": qa.selected,
                        "asked_by": qa.asked_by,
                        "answered_by": qa.answered_by,
                        "created_at": qa.created_at.isoformat(),
                        "answered_at": qa.answered_at.isoformat() if qa.answered_at else None,
                    }
                    for qa in items
                ],
            },
            default=str,
        )


# ============================================================================
# SPEC TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_create_spec(
    board_id: str,
    title: str,
    description: str = "",
    context: str = "",
    functional_requirements: str = "",
    technical_requirements: str = "",
    acceptance_criteria: str = "",
    status: str = "draft",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Create a new spec (specification) on the board. Specs define requirements that drive card/task creation.
    AI agents can create specs to propose work, which can then be reviewed, approved, and derived into cards.

    Args:
        board_id: Board ID
        title: Spec title
        description: High-level summary of what needs to be built (optional). Supports Markdown and Mermaid diagrams.
        context: Business context — why this spec exists, how it connects to the bigger picture (optional). Supports Markdown and Mermaid diagrams.
        functional_requirements: Pipe-separated list of functional requirements (e.g. "User can login|User can reset password")
        technical_requirements: Pipe-separated list of technical constraints (e.g. "Must use OAuth2|Response time < 200ms")
        acceptance_criteria: Pipe-separated list of acceptance criteria (e.g. "All tests pass|No console errors")
        status: Spec status — one of: draft, review, approved, in_progress, done, cancelled (default: draft)
        assignee_id: User/agent ID to assign (optional)
        labels: Multi-value labels — preferred native list (e.g. ``["backend", "api"]``);
            legacy string accepted as JSON array or pipe-separated. Comma-only string
            is REJECTED. See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

    Returns:
        JSON with created spec details
    """
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

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec_data = SpecCreate(
            title=title,
            description=description.replace("\\n", "\n") if description else None,
            context=context.replace("\\n", "\n") if context else None,
            functional_requirements=parse_multi_value(functional_requirements) or None,
            technical_requirements=_trs_to_objects(parse_multi_value(technical_requirements) or None),
            acceptance_criteria=parse_multi_value(acceptance_criteria) or None,
            status=spec_status,
            assignee_id=assignee_id or None,
            labels=coerce_to_list_str(labels) or None,
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
                },
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_spec(board_id: str, spec_id: str) -> str:
    """
    Get full details of a spec including its derived cards.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with spec details and linked cards
    """
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

        return json.dumps(
            {
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_spec_context(
    board_id: str,
    spec_id: str,
    include_knowledge: str = "true",
    include_mockups: str = "true",
    include_qa: str = "true",
    include_superseded: str = "false",
) -> str:
    """
    Get the FULL consolidated context of a spec. Returns ALL structured data
    needed to evaluate, validate, or review this spec before advancing it.

    Includes: requirements, test scenarios, business rules, API contracts,
    screen mockups, knowledge bases, Q&A, evaluations, cards, and sprints.

    **Always call this before evaluating, moving, or creating cards from a spec.**

    Args:
        board_id: Board ID
        spec_id: Spec ID
        include_knowledge: Include knowledge base entries (default "true")
        include_mockups: Include screen mockups (default "true")
        include_qa: Include Q&A items (default "true")
        include_superseded: When "false" (default), the `decisions` array
            returns only entries with status="active" — noise reduction for
            the common "what rules today?" path. Set to "true" to get the
            full history (active + superseded + revoked). A `decisions_stats`
            summary is always included so you can see what was filtered.

    Returns:
        JSON with complete spec context: all requirements + structured sections + artifacts + cards + sprints
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    _inc_kb = include_knowledge.lower() in ("true", "1", "yes")
    _inc_mockups = include_mockups.lower() in ("true", "1", "yes")
    _inc_qa = include_qa.lower() in ("true", "1", "yes")
    _inc_superseded = include_superseded.lower() in ("true", "1", "yes")

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
            # Structured sections — ALWAYS included
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

        if _inc_mockups and spec.screen_mockups:
            result["screen_mockups"] = spec.screen_mockups

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
                {
                    "id": kb.id,
                    "title": kb.title,
                    "description": kb.description,
                    "content": kb.content,
                    "mime_type": kb.mime_type,
                }
                for kb in (spec.knowledge_bases or [])
            ]

        # Coverage summary
        ac_count = len(spec.acceptance_criteria or [])
        ts_list = spec.test_scenarios or []
        covered_indices = set()
        for ts in ts_list:
            for idx in (ts.get("linked_criteria") or []):
                if isinstance(idx, int):
                    covered_indices.add(idx)
        result["coverage_summary"] = {
            "acceptance_criteria_total": ac_count,
            "acceptance_criteria_covered": len(covered_indices),
            "uncovered_indices": sorted(set(range(ac_count)) - covered_indices),
            "test_scenarios_total": len(ts_list),
            "business_rules_total": len(spec.business_rules or []),
            "api_contracts_total": len(spec.api_contracts or []),
            "cards_total": len(spec.cards),
            "cards_done": sum(1 for c in spec.cards if c.status.value == "done"),
        }

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
                }
                for s in sprints
            ]
        except Exception:
            pass

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_list_specs(
    board_id: str, status: str = "", offset: int = 0, limit: int = 50
) -> str:
    """
    List specs for a board with optional filtering and pagination.

    Args:
        board_id: Board ID
        status: Filter by status (optional) — one of: draft, review, approved, in_progress, done, cancelled
        offset: Skip first N specs (default 0)
        limit: Max specs to return (default 50, max 200)

    Returns:
        JSON with filtered/paginated specs
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    limit = min(limit, 200)

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        specs = await service.list_specs(board_id, status or None)
        await db.commit()

        total = len(specs)
        paginated = specs[offset:offset + limit]

        return json.dumps(
            {
                "board_id": board_id,
                "total": total,
                "offset": offset,
                "limit": limit,
                "specs": [
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


@mcp.tool()
async def okto_pulse_update_spec(
    board_id: str,
    spec_id: str,
    title: str = "",
    description: str = "",
    context: str = "",
    functional_requirements: str = "",
    technical_requirements: str = "",
    acceptance_criteria: str = "",
    assignee_id: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Update a spec's fields. Content changes (description, context, requirements, criteria) bump the version.
    Only non-empty fields are updated.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        title: New title (optional, empty = no change)
        description: New description (optional, empty = no change)
        context: New context (optional, empty = no change)
        functional_requirements: Pipe-separated list of functional requirements (optional, empty = no change)
        technical_requirements: Pipe-separated list of technical constraints (optional, empty = no change)
        acceptance_criteria: Pipe-separated list of acceptance criteria (optional, empty = no change)
        assignee_id: New assignee (optional, empty = no change)
        labels: Comma-separated labels (optional, empty = no change)

    Returns:
        JSON with updated spec details
    """
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
        update_kwargs["functional_requirements"] = parse_multi_value(functional_requirements)
    if technical_requirements:
        update_kwargs["technical_requirements"] = _trs_to_objects(parse_multi_value(technical_requirements))
    if acceptance_criteria:
        update_kwargs["acceptance_criteria"] = parse_multi_value(acceptance_criteria)
    if assignee_id:
        update_kwargs["assignee_id"] = assignee_id
    if labels:
        try:
            update_kwargs["labels"] = coerce_to_list_str(labels)
        except ValueError as e:
            return json.dumps({"error": f"Invalid labels: {e}"})

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
    Change a spec's status (e.g. draft → review → approved → validated → in_progress → done).

    Args:
        board_id: Board ID
        spec_id: Spec ID
        status: New status — one of: draft, review, approved, validated, in_progress, done, cancelled

    Returns:
        JSON with updated spec status
    """
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
                "from_status": spec.status.value,
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
    concrete Given/When/Then test plans.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        title: Scenario title (e.g. "Valid OAuth2 token grants access")
        given: Precondition (e.g. "User has a valid JWT token")
        when: Action (e.g. "GET /api/v1/boards with Bearer token")
        then: Expected result (e.g. "Returns 200 with board list")
        scenario_type: unit | integration | e2e | manual (default: integration)
        linked_criteria: Pipe-separated INDICES (0-based) of acceptance criteria this scenario validates.
            Example: "0|2|5" links to the 1st, 3rd, and 6th acceptance criteria.
            Use okto_pulse_get_spec to see the acceptance_criteria list and their indices.
        notes: Additional notes or edge cases (optional)

    Returns:
        JSON with the created scenario including resolved criteria text
    """
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

        # Resolve indices to full acceptance criteria text
        criteria_list = None
        if linked_criteria:
            criteria_list = []
            for token in parse_multi_value(linked_criteria):
                try:
                    idx = int(token)
                    if 0 <= idx < len(criteria):
                        criteria_list.append(criteria[idx])
                    else:
                        return json.dumps({"error": f"Criteria index {idx} out of range (0-{len(criteria)-1})"})
                except ValueError:
                    # Fallback: treat as full text match
                    if token in criteria:
                        criteria_list.append(token)
                    else:
                        return json.dumps({"error": f"Criteria '{token[:50]}...' not found. Use indices (0-{len(criteria)-1}) from acceptance_criteria list."})

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
        return json.dumps({"success": True, "scenario": scenario, "coverage": cov}, default=str)


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
    List test scenarios for a spec with coverage information. Supports filtering and pagination.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        status: Filter by scenario status (optional) — one of: draft, ready, automated, passed, failed
        scenario_type: Filter by type (optional) — one of: unit, integration, e2e, manual
        linked: Filter by task linkage (optional) — "linked" = only scenarios with tasks, "unlinked" = only scenarios without tasks
        offset: Skip first N scenarios (default 0)
        limit: Max scenarios to return (default 50, max 200)

    Returns:
        JSON with filtered/paginated scenarios and acceptance criteria coverage status
    """
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
        coverage: dict[str, list[str]] = {}
        for c in criteria:
            covering = [s["id"] for s in all_scenarios if c in (s.get("linked_criteria") or [])]
            coverage[c] = covering

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
                    "uncovered_indices": [i for i, c in enumerate(criteria) if not coverage.get(c)],
                    "uncovered": [c for c, v in coverage.items() if not v],
                    "details": {str(i): coverage.get(c, []) for i, c in enumerate(criteria)},
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
_EVIDENCE_REQUIRED_KEYS: dict[str, tuple[tuple[str, ...], ...]] = {
    # automated: ambas as keys são obrigatórias (duas rules AND)
    "automated": (
        ("test_file_path",),
        ("test_function",),
    ),
    # passed: last_run_at obrigatório + (output_snippet OR test_run_id)
    "passed": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of
    ),
    "failed": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of
    ),
}

import logging as _nc9_logging  # noqa: E402 — local import isolated to NC-9 gate
_evidence_logger = _nc9_logging.getLogger("okto_pulse.spec.test_scenario")


def _validate_evidence(
    status: str, evidence: dict | None
) -> tuple[bool, list[str]]:
    """Return (ok, missing_keys). Empty missing_keys means valid.

    For each rule group, ALL keys in the first non-empty group must be
    present (AND logic). When a group has multiple keys (one-of), at
    least one must be present.
    """
    rules = _EVIDENCE_REQUIRED_KEYS.get(status)
    if not rules:
        return True, []
    if not evidence:
        # Flatten all required keys for the error message.
        flat: list[str] = []
        for group in rules:
            if len(group) == 1:
                flat.extend(group)
            else:
                flat.append(" or ".join(group))
        return False, flat
    missing: list[str] = []
    for group in rules:
        if len(group) == 1:
            key = group[0]
            if not evidence.get(key):
                missing.append(key)
        else:
            # one-of group — at least one key must be present
            if not any(evidence.get(k) for k in group):
                missing.append(" or ".join(group))
    return (len(missing) == 0, missing)


@mcp.tool()
async def okto_pulse_update_test_scenario_status(
    board_id: str,
    spec_id: str,
    scenario_id: str,
    status: str,
    evidence: str = "",
) -> str:
    """
    Update the status of a test scenario, optionally attaching structured
    evidence that the test really exists/ran.

    **Test theater prevention gate (NC-9, spec 873e98cc):**

    When the board's `skip_test_evidence_global` setting is False (default),
    setting status to one of `automated`, `passed`, or `failed` REQUIRES
    structured evidence:
      - `automated`: evidence.test_file_path AND evidence.test_function
      - `passed`/`failed`: evidence.last_run_at AND
        (evidence.output_snippet OR evidence.test_run_id)
      - `draft`/`ready`: evidence opcional (intent declarado)

    When `skip_test_evidence_global=True`, the gate is bypassed — every
    status update is accepted without evidence, but a structured audit log
    `test_scenario.evidence_gate_skipped` is emitted for forensics.

    Evidence is persisted inline within the scenario dict (no DB migration).
    Audit log `test_scenario.status_changed` is emitted on every successful
    update with `evidence_provided`, `evidence_gate_skipped`, and
    `changed_by_agent_id`.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        scenario_id: Test scenario ID (e.g. "ts_abc123")
        status: New status — one of: draft, ready, automated, passed, failed
        evidence: Optional JSON string with keys test_file_path, test_function,
            last_run_at, test_run_id, output_snippet. Empty string = no evidence.

    Returns:
        JSON. On success: {success, scenario_id, old_status, new_status,
        evidence_provided, evidence_gate_skipped}. On gate failure:
        {error: "evidence_required", required: [...], message: "..."}.
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    valid = ("draft", "ready", "automated", "passed", "failed")
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
        from sqlalchemy import update as sql_update
        from okto_pulse.core.models.db import ActivityLog, Spec as SpecModel

        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        # Resolve board settings to know if gate is active.
        from okto_pulse.core.models.db import Board
        board_row = await db.get(Board, board_id)
        board_settings = (board_row.settings if board_row else {}) or {}
        skip_evidence_gate = bool(
            board_settings.get("skip_test_evidence_global", False)
        )

        # Apply gate when skip is OFF.
        if not skip_evidence_gate:
            ok, missing = _validate_evidence(status, evidence_dict)
            if not ok:
                return json.dumps({
                    "error": "evidence_required",
                    "required": missing,
                    "message": (
                        f"Cannot mark scenario as {status} without structured "
                        f"evidence ({', '.join(missing)}). This prevents the "
                        "test theater anti-pattern. To bypass, enable "
                        "skip_test_evidence_global on the board."
                    ),
                })

        scenarios = list(spec.test_scenarios or [])
        old_status = None
        found = False
        for s in scenarios:
            if s.get("id") == scenario_id:
                old_status = s.get("status")
                s["status"] = status
                # Persist evidence inline (only if provided).
                if evidence_dict is not None:
                    s["evidence"] = evidence_dict
                found = True
                break

        if not found:
            return json.dumps({"error": f"Scenario '{scenario_id}' not found"})

        # Direct update — no version bump, just persist + log activity
        await db.execute(
            sql_update(SpecModel).where(SpecModel.id == spec_id).values(test_scenarios=scenarios)
        )

        # Log activity (not version change)
        scenario_title = next((s["title"] for s in scenarios if s["id"] == scenario_id), scenario_id)
        log = ActivityLog(
            board_id=spec.board_id,
            action="test_scenario_status_changed",
            actor_type="agent",
            actor_id=ctx.agent_id,
            actor_name=ctx.agent_name,
            details={
                "spec_id": spec_id,
                "scenario_id": scenario_id,
                "scenario_title": scenario_title,
                "from_status": old_status,
                "to_status": status,
                "evidence_provided": evidence_dict is not None,
                "evidence_gate_skipped": skip_evidence_gate,
            },
        )
        db.add(log)
        await db.commit()

        # Structured log SEMPRE emitido (NC-9 BR audit log).
        _evidence_logger.info(
            "test_scenario.status_changed scenario=%s board=%s from=%s to=%s "
            "evidence=%s skip=%s",
            scenario_id, spec.board_id, old_status, status,
            evidence_dict is not None, skip_evidence_gate,
            extra={
                "event": "test_scenario.status_changed",
                "scenario_id": scenario_id,
                "board_id": spec.board_id,
                "spec_id": spec_id,
                "from_status": old_status,
                "to_status": status,
                "evidence_provided": evidence_dict is not None,
                "evidence_gate_skipped": skip_evidence_gate,
                "changed_by_agent_id": ctx.agent_id,
            },
        )
        # Quando skip está ON, log dedicado para forensics.
        if skip_evidence_gate and status in _EVIDENCE_REQUIRED_KEYS:
            _evidence_logger.info(
                "test_scenario.evidence_gate_skipped scenario=%s board=%s "
                "status=%s evidence=%s",
                scenario_id, spec.board_id, status,
                evidence_dict is not None,
                extra={
                    "event": "test_scenario.evidence_gate_skipped",
                    "scenario_id": scenario_id,
                    "board_id": spec.board_id,
                    "spec_id": spec_id,
                    "status": status,
                    "evidence_provided": evidence_dict is not None,
                    "skip": True,
                    "agent_id": ctx.agent_id,
                },
            )

        return json.dumps({
            "success": True,
            "scenario_id": scenario_id,
            "old_status": old_status,
            "new_status": status,
            "evidence_provided": evidence_dict is not None,
            "evidence_gate_skipped": skip_evidence_gate,
        })


@mcp.tool()
async def okto_pulse_link_task_to_scenario(
    board_id: str, spec_id: str, scenario_id: str, card_id: str
) -> str:
    """
    Link a card (task) to a test scenario, creating bidirectional traceability.
    Updates both the scenario's linked_task_ids and the card's test_scenario_ids.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        scenario_id: Test scenario ID
        card_id: Card ID to link

    Returns:
        JSON with success status
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

        from okto_pulse.core.models.schemas import SpecUpdate, CardUpdate
        _, err = await _safe_spec_update(spec_service, spec_id, ctx.agent_id, SpecUpdate(test_scenarios=scenarios))
        if err:
            return err

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
        return json.dumps({"success": True, "scenario_id": scenario_id, "card_id": card_id, "coverage": cov})


@mcp.tool()
async def okto_pulse_link_task_to_rule(
    board_id: str, spec_id: str, rule_id: str, card_id: str
) -> str:
    """
    Link a card (task) to a business rule, creating traceability.
    Updates the rule's linked_task_ids in the spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        rule_id: Business rule ID (e.g. "br_abc123")
        card_id: Card ID to link

    Returns:
        JSON with success status
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
        return json.dumps({"success": True, "rule_id": rule_id, "card_id": card_id, "coverage": cov})


@mcp.tool()
async def okto_pulse_link_task_to_contract(
    board_id: str, spec_id: str, contract_id: str, card_id: str
) -> str:
    """
    Link a card (task) to an API contract, creating traceability.
    Updates the contract's linked_task_ids in the spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        contract_id: API contract ID (e.g. "api_abc123")
        card_id: Card ID to link

    Returns:
        JSON with success status
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
        return json.dumps({"success": True, "contract_id": contract_id, "card_id": card_id, "coverage": cov})


@mcp.tool()
async def okto_pulse_link_task_to_tr(
    board_id: str, spec_id: str, tr_id: str, card_id: str
) -> str:
    """
    Link a card (task) to a technical requirement, creating traceability.
    Updates the TR's linked_task_ids in the spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        tr_id: Technical requirement ID (e.g. "tr_abc123")
        card_id: Card ID to link

    Returns:
        JSON with success status
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
        return json.dumps({"success": True, "tr_id": tr_id, "card_id": card_id, "coverage": cov})


# ==================== ARCHIVE & RESTORE ====================


@mcp.tool()
async def okto_pulse_archive_tree(
    board_id: str, entity_type: str, entity_id: str
) -> str:
    """
    Archive an entity and all its descendants in cascade.
    Saves pre_archive_status before setting archived=true.

    Args:
        board_id: Board ID
        entity_type: Type of entity — one of: ideation, refinement, spec
        entity_id: Entity ID to archive

    Returns:
        JSON with archived_count per entity type
    """
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
    Returns each entity to its pre_archive_status.

    Args:
        board_id: Board ID
        entity_type: Type of entity — one of: ideation, refinement, spec
        entity_id: Entity ID to restore

    Returns:
        JSON with restored_count per entity type
    """
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
async def okto_pulse_copy_mockups_to_card(
    board_id: str, spec_id: str, card_id: str, screen_ids: list[str] | str = ""
) -> str:
    """
    Copy screen mockups from a spec to a card. Use this when creating implementation
    cards to carry the relevant mockups into the card for the implementer's context.

    Args:
        board_id: Board ID
        spec_id: Source spec ID
        card_id: Target card ID
        screen_ids: Multi-value screen IDs to copy (empty = copy ALL mockups from the
            spec). Preferred native list (e.g. ``["scr_a", "scr_b"]``); legacy string
            accepted as JSON array or pipe-separated. Comma-only string is REJECTED.
            See ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

    Returns:
        JSON with count of mockups copied
    """
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
    Copy knowledge base entries from a spec to a card as attachments/comments.
    Each knowledge base entry is added as a comment on the card with the full content.

    Args:
        board_id: Board ID
        spec_id: Source spec ID
        card_id: Target card ID
        knowledge_ids: Multi-value knowledge base IDs to copy (empty = copy ALL).
            Preferred native list (e.g. ``["kb_a", "kb_b"]``); legacy string accepted
            as JSON array or pipe-separated. Comma-only string is REJECTED. See
            ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

    Returns:
        JSON with count of knowledge entries copied
    """
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

        from okto_pulse.core.models.db import Comment
        copied = 0
        for kb in kbs:
            comment = Comment(
                card_id=card_id,
                author_id=ctx.agent_id,
                content=f"## KB: {kb.title}\n\n{kb.content}",
            )
            db.add(comment)
            copied += 1

        await db.commit()

    return json.dumps({"success": True, "copied": copied})


# ============================================================================
# Card.knowledge_bases — inline JSONB lifecycle (symmetric to spec_knowledge)
# ============================================================================


def _new_card_kb_id() -> str:
    import hashlib, time
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
    to a single task.

    Args:
        board_id: Board ID
        card_id: Card ID
        title: KE title
        content: KE content (Markdown by default)
        description: Short summary (optional)
        mime_type: Content MIME type (default text/markdown)
        source: Free-form provenance hint (e.g. "manual", "copied_from_spec:<spec_id>:<kb_id>")

    Returns:
        JSON with the created KE including its generated id
    """
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
async def okto_pulse_list_card_knowledge(board_id: str, card_id: str) -> str:
    """
    List all knowledge base entries attached to a card.
    Returns titles + descriptions + ids; full content is included as well
    since the rows live inline (no separate fetch path).
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = CardService(db)
        card = await service.get_card(card_id)
        if not card or card.board_id != board_id:
            return json.dumps({"error": "Card not found"})

    return json.dumps({"success": True, "card_id": card_id, "knowledge": list(card.knowledge_bases or [])}, default=str)


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
    Only copies Q&As that have been answered — unanswered questions are skipped.

    Args:
        board_id: Board ID
        spec_id: Source spec ID
        card_id: Target card ID

    Returns:
        JSON with count of Q&A entries copied
    """
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
    Get analytics data for a board. Supports multiple metric types.

    Args:
        board_id: Board ID
        metric_type: Type of analytics — one of: overview, funnel, quality, velocity, coverage, agents
        from_date: Start date filter (ISO format, optional)
        to_date: End date filter (ISO format, optional)

    Returns:
        JSON with analytics data for the requested metric type
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.db import (
        Board, Card, CardStatus, Ideation, Refinement, Spec,
    )
    from sqlalchemy import func, select

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
            from okto_pulse.core.models.db import Sprint, SprintStatus

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
      test-coverage gate will fail.

    Args:
        board_id: Board ID
        stale_hours: Cards unchanged longer than this while active are flagged
            as stale (default 72, ≥1).
        filter_type: Optional — return only blockers of this type. Empty returns all.

    Returns:
        JSON ``{summary: {<type>: count}, total: int, blockers: [...]}``
    """
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
    using When/Then format.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        title: Rule title (e.g. "Discount cap for non-premium users")
        rule: The business rule statement
        when: Condition that triggers the rule
        then: Expected behavior / outcome
        linked_requirements: Pipe-separated INDICES (0-based) of functional requirements this rule relates to.
            Example: "0|2|5" links to the 1st, 3rd, and 6th functional requirement.
        notes: Additional notes (optional)

    Returns:
        JSON with the created business rule including resolved requirement text
    """
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

        req_list = None
        if linked_requirements:
            req_list = []
            for token in parse_multi_value(linked_requirements):
                try:
                    idx = int(token)
                    if 0 <= idx < len(frs):
                        req_list.append(frs[idx])
                    else:
                        return json.dumps({"error": f"Requirement index {idx} out of range (0-{len(frs)-1})"})
                except ValueError:
                    if token in frs:
                        req_list.append(token)
                    else:
                        return json.dumps({"error": f"Requirement '{token[:50]}...' not found. Use indices (0-{len(frs)-1}) from functional_requirements list."})

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
        return json.dumps({"success": True, "business_rule": br, "coverage": cov}, default=str)


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
    Update an existing business rule on a spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        rule_id: Business rule ID (e.g. "br_abc12345")
        title: New title (optional, empty = no change)
        rule: New rule statement (optional)
        when: New condition (optional)
        then: New outcome (optional)
        linked_requirements: Pipe-separated INDICES (0-based) of functional requirements.
            Pass "CLEAR" to remove all links. Empty = no change.
        notes: New notes (optional, "CLEAR" to remove)

    Returns:
        JSON with the updated business rule
    """
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
            req_list = []
            for token in parse_multi_value(linked_requirements):
                try:
                    idx = int(token)
                    if 0 <= idx < len(frs):
                        req_list.append(frs[idx])
                    else:
                        return json.dumps({"error": f"Requirement index {idx} out of range (0-{len(frs)-1})"})
                except ValueError:
                    if token in frs:
                        req_list.append(token)
                    else:
                        return json.dumps({"error": f"Requirement '{token[:50]}...' not found."})
            target["linked_requirements"] = req_list

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(business_rules=rules))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, rules=rules)
        return json.dumps({"success": True, "business_rule": target, "coverage": cov}, default=str)


@mcp.tool()
async def okto_pulse_remove_business_rule(
    board_id: str,
    spec_id: str,
    rule_id: str,
) -> str:
    """
    Remove a business rule from a spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        rule_id: Business rule ID to remove

    Returns:
        JSON confirmation
    """
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
        new_rules = [r for r in rules if r.get("id") != rule_id]
        if len(new_rules) == len(rules):
            return json.dumps({"error": f"Business rule '{rule_id}' not found"})

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(business_rules=new_rules))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, rules=new_rules)
        return json.dumps({"success": True, "removed": rule_id, "remaining": len(new_rules), "coverage": cov})


# ============================================================================
# Decisions — formalized design choices on a spec (spec b66d2562)
#
# Decision vs BusinessRule: a Decision records *why* a choice was made
# ("We chose Kùzu over Neo4j because..."); a BusinessRule is a prescriptive
# norm ("The system MUST clamp scores at 1.5"). They're distinct entities
# with distinct semantics — don't mix them.
# ============================================================================


def _parse_linked_requirements(raw: str, frs: list) -> tuple[list[str] | None, str | None]:
    """Parse a pipe-separated "0|2|5" into resolved FR references.

    Returns (list_or_None, error_str). CLEAR empties the list explicitly.
    """
    if not raw:
        return None, None
    if raw.strip().upper() == "CLEAR":
        return [], None
    out: list[str] = []
    for token in parse_multi_value(raw):
        try:
            idx = int(token)
        except ValueError:
            # Accept resolved text as-is
            out.append(token)
            continue
        if 0 <= idx < len(frs):
            # Keep the index as a string — aligned with BR/TR convention
            out.append(str(idx))
        else:
            return None, f"Requirement index {idx} out of range (0-{len(frs)-1})"
    return out, None


@mcp.tool()
async def okto_pulse_add_decision(
    board_id: str,
    spec_id: str,
    title: str,
    rationale: str,
    context: str = "",
    alternatives_considered: str = "",
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
    Decision to have ≥1 linked task.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        title: Decision title (e.g. "Use Kùzu embedded over Neo4j")
        rationale: Why this choice was made
        context: When/where this applies (optional)
        alternatives_considered: Pipe-separated list of alternatives (e.g. "Neo4j|DuckDB")
        supersedes_decision_id: id of another Decision this one replaces; it auto-moves to status=superseded
        linked_requirements: Pipe-separated FR indices (e.g. "0|2")
        notes: Additional notes

    Returns:
        JSON with created decision and spec coverage snapshot
    """
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

        frs = spec.functional_requirements or []
        req_list, err = _parse_linked_requirements(linked_requirements, frs)
        if err:
            return json.dumps({"error": err})

        alts = None
        if alternatives_considered:
            alts = parse_multi_value(alternatives_considered)

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
    alternatives_considered: str = "",
    supersedes_decision_id: str = "",
    linked_requirements: str = "",
    notes: str = "",
    status: str = "",
) -> str:
    """
    Update an existing Decision. Only non-empty fields are changed; pass "CLEAR"
    to wipe optional string/list fields.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        decision_id: Decision ID ("dec_...")
        title: New title (optional)
        rationale: New rationale (optional)
        context: New context (optional, "CLEAR" to remove)
        alternatives_considered: Pipe-separated list (optional, "CLEAR" to remove)
        supersedes_decision_id: New target Decision id, or "CLEAR" to unset
        linked_requirements: Pipe-separated FR indices ("CLEAR" to empty)
        notes: Notes (optional, "CLEAR" to remove)
        status: One of "active", "superseded", "revoked" (optional)

    Returns:
        JSON with updated decision
    """
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
            if alternatives_considered.strip().upper() == "CLEAR":
                target["alternatives_considered"] = None
            else:
                target["alternatives_considered"] = parse_multi_value(
                    alternatives_considered
                )
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
            req_list, err = _parse_linked_requirements(linked_requirements, frs)
            if err:
                return json.dumps({"error": err})
            target["linked_requirements"] = req_list or None
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

        return json.dumps({"success": True, "decision": target}, default=str)


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
    restore.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        decision_id: Decision ID ("dec_...")

    Returns:
        JSON confirmation
    """
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

        target["status"] = "revoked"

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(
            service, spec_id, ctx.agent_id,
            SpecUpdate(decisions=decisions),
        )
        if _err:
            return _err
        await db.commit()

        return json.dumps({"success": True, "revoked": decision_id, "decision": target})


@mcp.tool()
async def okto_pulse_link_task_to_decision(
    board_id: str,
    spec_id: str,
    decision_id: str,
    card_id: str,
) -> str:
    """
    Link a task card to a Decision, creating traceability.

    Idempotent — re-linking the same card is a no-op. Symmetric with
    okto_pulse_link_task_to_rule; populates decision.linked_task_ids so the
    opt-in coverage gate (skip_decisions_coverage=False) can verify each
    active Decision has at least one linked task.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        decision_id: Decision ID
        card_id: Card ID to link

    Returns:
        JSON with success status
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

        return json.dumps({
            "success": True,
            "decision_id": decision_id,
            "card_id": card_id,
            "linked_tasks": task_ids,
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
    duplicates (same title) are skipped.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with migration summary (decisions_added, context_modified)
    """
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
) -> str:
    """
    List all business rules for a spec with linked functional requirements resolved as text.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON array of business rules with resolved linked requirements
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        rules = spec.business_rules or []
        frs = spec.functional_requirements or []

        result = []
        for r in rules:
            entry = dict(r)
            linked = r.get("linked_requirements") or []
            resolved = []
            for req_text in linked:
                if req_text in frs:
                    idx = frs.index(req_text)
                    resolved.append(f"[FR-{idx}] {req_text}")
                else:
                    resolved.append(req_text)
            entry["resolved_requirements"] = resolved
            result.append(entry)

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
    request_body_json: str = "",
    response_success_json: str = "",
    response_errors_json: str = "",
    linked_requirements: str = "",
    linked_rules: str = "",
    notes: str = "",
) -> str:
    """
    Add an API contract to a spec. API contracts define endpoints, request/response
    shapes, and link to requirements and business rules.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        method: HTTP method or interaction type (GET, POST, PUT, DELETE, PATCH, TOOL, COMPONENT, EVENT)
        path: Endpoint path or identifier (e.g. "/api/v1/users", "UserProfile component")
        description: What this endpoint does (optional)
        request_body_json: JSON string for request body schema (optional). Example: '{"name": "string", "email": "string"}'
        response_success_json: JSON string for success response schema (optional)
        response_errors_json: JSON string for error responses array (optional). Example: '[{"status": 400, "detail": "..."}]'
        linked_requirements: Pipe-separated INDICES (0-based) of functional requirements.
            Example: "0|2|5"
        linked_rules: Pipe-separated business rule IDs. Example: "br_abc123|br_def456"
        notes: Additional notes (optional)

    Returns:
        JSON with the created API contract
    """
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

        # Parse JSON strings
        request_body = None
        if request_body_json:
            try:
                request_body = json.loads(request_body_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid request_body_json: {e}"})

        response_success = None
        if response_success_json:
            try:
                response_success = json.loads(response_success_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid response_success_json: {e}"})

        response_errors = None
        if response_errors_json:
            try:
                response_errors = json.loads(response_errors_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid response_errors_json: {e}"})

        # Resolve linked requirements
        req_list = None
        if linked_requirements:
            req_list = []
            for token in parse_multi_value(linked_requirements):
                try:
                    idx = int(token)
                    if 0 <= idx < len(frs):
                        req_list.append(frs[idx])
                    else:
                        return json.dumps({"error": f"Requirement index {idx} out of range (0-{len(frs)-1})"})
                except ValueError:
                    if token in frs:
                        req_list.append(token)
                    else:
                        return json.dumps({"error": f"Requirement '{token[:50]}...' not found."})

        # Resolve linked rules
        rules_list = None
        if linked_rules:
            rule_ids = {r.get("id") for r in existing_rules}
            rules_list = []
            for token in parse_multi_value(linked_rules):
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

        contracts = list(spec.api_contracts or [])
        contracts.append(contract)

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(api_contracts=contracts))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, contracts=contracts)
        return json.dumps({"success": True, "api_contract": contract, "coverage": cov}, default=str)


@mcp.tool()
async def okto_pulse_update_api_contract(
    board_id: str,
    spec_id: str,
    contract_id: str,
    method: str = "",
    path: str = "",
    description: str = "",
    request_body_json: str = "",
    response_success_json: str = "",
    response_errors_json: str = "",
    linked_requirements: str = "",
    linked_rules: str = "",
    notes: str = "",
) -> str:
    """
    Update an existing API contract on a spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        contract_id: API contract ID (e.g. "api_abc12345")
        method: New HTTP method (optional, empty = no change)
        path: New path (optional)
        description: New description (optional, "CLEAR" to remove)
        request_body_json: New request body JSON (optional, "CLEAR" to remove)
        response_success_json: New success response JSON (optional, "CLEAR" to remove)
        response_errors_json: New error responses JSON (optional, "CLEAR" to remove)
        linked_requirements: Pipe-separated INDICES. "CLEAR" to remove all. Empty = no change.
        linked_rules: Pipe-separated rule IDs. "CLEAR" to remove all. Empty = no change.
        notes: New notes (optional, "CLEAR" to remove)

    Returns:
        JSON with the updated API contract
    """
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

        if request_body_json == "CLEAR":
            target["request_body"] = None
        elif request_body_json:
            try:
                target["request_body"] = json.loads(request_body_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid request_body_json: {e}"})

        if response_success_json == "CLEAR":
            target["response_success"] = None
        elif response_success_json:
            try:
                target["response_success"] = json.loads(response_success_json)
            except json.JSONDecodeError as e:
                return json.dumps({"error": f"Invalid response_success_json: {e}"})

        if response_errors_json == "CLEAR":
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
            req_list = []
            for token in parse_multi_value(linked_requirements):
                try:
                    idx = int(token)
                    if 0 <= idx < len(frs):
                        req_list.append(frs[idx])
                    else:
                        return json.dumps({"error": f"Requirement index {idx} out of range (0-{len(frs)-1})"})
                except ValueError:
                    if token in frs:
                        req_list.append(token)
                    else:
                        return json.dumps({"error": f"Requirement '{token[:50]}...' not found."})
            target["linked_requirements"] = req_list

        existing_rules = spec.business_rules or []
        if linked_rules == "CLEAR":
            target["linked_rules"] = None
        elif linked_rules:
            rule_ids = {r.get("id") for r in existing_rules}
            rules_list = []
            for token in parse_multi_value(linked_rules):
                if token in rule_ids:
                    rules_list.append(token)
                else:
                    return json.dumps({"error": f"Business rule '{token}' not found in spec"})
            target["linked_rules"] = rules_list

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(api_contracts=contracts))
        if _err:
            return _err
        await db.commit()

        return json.dumps({"success": True, "api_contract": target}, default=str)


@mcp.tool()
async def okto_pulse_remove_api_contract(
    board_id: str,
    spec_id: str,
    contract_id: str,
) -> str:
    """
    Remove an API contract from a spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        contract_id: API contract ID to remove

    Returns:
        JSON confirmation
    """
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
        new_contracts = [c for c in contracts if c.get("id") != contract_id]
        if len(new_contracts) == len(contracts):
            return json.dumps({"error": f"API contract '{contract_id}' not found"})

        from okto_pulse.core.models.schemas import SpecUpdate
        _, _err = await _safe_spec_update(service, spec_id, ctx.agent_id, SpecUpdate(api_contracts=new_contracts))
        if _err:
            return _err
        await db.commit()

        cov = _spec_coverage(spec, contracts=new_contracts)
        return json.dumps({"success": True, "removed": contract_id, "remaining": len(new_contracts), "coverage": cov})


@mcp.tool()
async def okto_pulse_list_api_contracts(
    board_id: str,
    spec_id: str,
) -> str:
    """
    List all API contracts for a spec with linked business rules resolved.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON array of API contracts with resolved linked rules and requirements
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        contracts = spec.api_contracts or []
        existing_rules = {r.get("id"): r for r in (spec.business_rules or [])}
        frs = spec.functional_requirements or []

        result = []
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

            # Resolve linked requirements
            linked_reqs = c.get("linked_requirements") or []
            resolved_reqs = []
            for req_text in linked_reqs:
                if req_text in frs:
                    idx = frs.index(req_text)
                    resolved_reqs.append(f"[FR-{idx}] {req_text}")
                else:
                    resolved_reqs.append(req_text)
            entry["resolved_requirements"] = resolved_reqs

            result.append(entry)

        return json.dumps({
            "spec_id": spec_id,
            "total": len(result),
            "api_contracts": result,
        }, default=str)


# ==================== SCREEN MOCKUP TOOLS ====================


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
    Add a screen mockup to any entity (spec, ideation, refinement, or card).
    Screens contain HTML+Tailwind content that renders as visual mockups in the dashboard.

    Args:
        board_id: Board ID
        entity_id: Entity ID (spec, ideation, refinement, or card)
        title: Screen title (e.g. "Login Page", "Dashboard", "Settings Modal")
        entity_type: Type of entity — one of: spec, ideation, refinement, card (default: spec)
        description: What this screen does and when it appears (optional). Supports Markdown.
        screen_type: Type of screen — one of: page, modal, drawer, popover, panel (default: page)
        html_content: HTML+Tailwind markup for the screen mockup. Script tags and on* event attributes are stripped for safety.

    Returns:
        JSON with created screen including its generated ID
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    if entity_type not in ("spec", "ideation", "refinement", "card"):
        return json.dumps({"error": f"Invalid entity_type '{entity_type}'. Must be one of: spec, ideation, refinement, card"})

    import hashlib, time
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
    Update an existing screen mockup's fields on any entity.

    Args:
        board_id: Board ID
        entity_id: Entity ID (spec, ideation, refinement, or card)
        screen_id: Screen mockup ID to update
        entity_type: Type of entity — one of: spec, ideation, refinement, card (default: spec)
        title: New title (empty = no change)
        description: New description (empty = no change)
        html_content: New HTML+Tailwind content (empty = no change). Script tags and on* event attributes are stripped.
        screen_type: New screen type (empty = no change) — one of: page, modal, drawer, popover, panel

    Returns:
        JSON with updated screen
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

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
    Add a design annotation/note to a screen mockup on any entity.

    Args:
        board_id: Board ID
        entity_id: Entity ID (spec, ideation, refinement, or card)
        screen_id: Screen mockup ID
        text: Annotation text (design note, requirement, constraint)
        entity_type: Type of entity — one of: spec, ideation, refinement, card (default: spec)

    Returns:
        JSON with created annotation
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

    import hashlib, time
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
    List screen mockups for any entity with optional filtering and pagination.

    Args:
        board_id: Board ID
        entity_id: Entity ID (spec, ideation, refinement, or card)
        entity_type: Type of entity — one of: spec, ideation, refinement, card (default: spec)
        screen_type: Filter by screen type (optional) — one of: page, modal, drawer, popover, panel
        offset: Skip first N screens (default 0)
        limit: Max screens to return (default 50, max 200)

    Returns:
        JSON with filtered/paginated screens
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

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
    Delete a screen mockup from any entity.

    Args:
        board_id: Board ID
        entity_id: Entity ID (spec, ideation, refinement, or card)
        screen_id: Screen mockup ID to delete
        entity_type: Type of entity — one of: spec, ideation, refinement, card (default: spec)

    Returns:
        JSON with success status
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()
    perm_err = check_permission(ctx.permissions, Permissions.SPECS_UPDATE)
    if perm_err:
        return _perm_error(perm_err)

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

    Returns linked global guidelines and inline board guidelines merged and sorted.

    Args:
        board_id: Board ID

    Returns:
        JSON with list of guidelines sorted by priority (highest first)
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = GuidelineService(db)
        items = await service.get_board_guidelines(board_id)
        await db.commit()

        return json.dumps({"board_id": board_id, "count": len(items), "guidelines": items}, default=str)


@mcp.tool()
async def okto_pulse_list_guidelines(
    board_id: str, offset: str = "0", limit: str = "50", tag: str = "",
) -> str:
    """
    List global guidelines from the catalog. Use this to browse available guidelines
    that can be linked to boards.

    Args:
        board_id: Board ID (used for authentication)
        offset: Pagination offset (default 0)
        limit: Max results (default 50)
        tag: Optional tag filter (empty = all)

    Returns:
        JSON with list of global guidelines
    """
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
    board_id: str, title: str, content: str, tags: str = "", scope: str = "global",
) -> str:
    """
    Create a new guideline. If scope is "global", it goes into the catalog and can be
    linked to any board. If scope is "inline", set a board_id to make it board-specific.

    Args:
        board_id: Board ID (used for authentication; also used as guideline board_id if scope is "inline")
        title: Guideline title
        content: Guideline content (Markdown supported)
        tags: Pipe-separated tags (e.g. "coding|architecture") — empty = no tags
        scope: "global" (catalog) or "inline" (board-specific)

    Returns:
        JSON with created guideline
    """
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

        from okto_pulse.core.models.schemas import GuidelineCreate
        tag_list = parse_multi_value(tags) or None
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
    board_id: str, guideline_id: str, title: str = "", content: str = "", tags: str = "",
) -> str:
    """
    Update a guideline's title, content, or tags.

    Args:
        board_id: Board ID (used for authentication)
        guideline_id: Guideline ID to update
        title: New title (empty = no change)
        content: New content (empty = no change)
        tags: New pipe-separated tags (empty = no change)

    Returns:
        JSON with updated guideline
    """
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

        from okto_pulse.core.models.schemas import GuidelineUpdate
        data = GuidelineUpdate(
            title=title or None,
            content=content or None,
            tags=parse_multi_value(tags) or None,
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
    Delete a guideline. Also removes all board links.

    Args:
        board_id: Board ID (used for authentication)
        guideline_id: Guideline ID to delete

    Returns:
        JSON with success status
    """
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
    Link a global guideline to a board so agents see it when loading board guidelines.

    Args:
        board_id: Board ID
        guideline_id: Guideline ID to link
        priority: Priority order (higher = more important, default 0)

    Returns:
        JSON with link details
    """
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
    Unlink a guideline from a board. The guideline itself is not deleted.

    Args:
        board_id: Board ID
        guideline_id: Guideline ID to unlink

    Returns:
        JSON with success status
    """
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
    Delete a spec. Derived cards are unlinked but not deleted.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with success status
    """
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


@mcp.tool()
async def okto_pulse_link_card_to_spec(board_id: str, spec_id: str, card_id: str) -> str:
    """
    Link an existing card to a spec. The card and spec must belong to the same board.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        card_id: Card ID to link

    Returns:
        JSON with success status
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
    Multiple evaluators can submit independent evaluations.

    Args:
        board_id: Board ID
        spec_id: Spec ID (must be in 'validated' status)
        breakdown_completeness: Score 0-100 — do tasks cover the spec scope?
        breakdown_justification: Why this score
        granularity: Score 0-100 — are tasks properly sized?
        granularity_justification: Why this score
        dependency_coherence: Score 0-100 — do task dependencies make sense?
        dependency_justification: Why this score
        test_coverage_quality: Score 0-100 — do tests cover happy path and edge cases?
        test_coverage_justification: Why this score
        overall_score: Overall score 0-100
        overall_justification: Overall assessment summary
        recommendation: approve | request_changes | reject

    Returns:
        JSON with created evaluation details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.SPECS_EVALUATE)
    if perm_err:
        return _perm_error(perm_err)

    # Validate recommendation
    if recommendation not in ("approve", "request_changes", "reject"):
        return json.dumps({"error": "Recommendation must be one of: approve, request_changes, reject"})

    # Validate scores
    for name, score in [
        ("breakdown_completeness", breakdown_completeness),
        ("granularity", granularity),
        ("dependency_coherence", dependency_coherence),
        ("test_coverage_quality", test_coverage_quality),
        ("overall_score", overall_score),
    ]:
        if not (0 <= score <= 100):
            return json.dumps({"error": f"{name} must be between 0 and 100"})

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SpecService
        service = SpecService(db)
        spec = await service.get_spec(spec_id)
        if not spec:
            return json.dumps({"error": "Spec not found"})

        from okto_pulse.core.models.db import SpecStatus
        if spec.status != SpecStatus.VALIDATED:
            return json.dumps({
                "error": f"Spec must be in 'validated' status to submit evaluations "
                         f"(currently '{spec.status.value}')"
            })

        import uuid as _uuid
        evaluation = {
            "id": f"eval_{_uuid.uuid4().hex[:8]}",
            "spec_id": spec_id,
            "evaluator_id": ctx.agent_id,
            "evaluator_name": ctx.agent_name,
            "evaluator_type": "agent",
            "dimensions": {
                "breakdown_completeness": {"score": breakdown_completeness, "justification": breakdown_justification},
                "granularity": {"score": granularity, "justification": granularity_justification},
                "dependency_coherence": {"score": dependency_coherence, "justification": dependency_justification},
                "test_coverage_quality": {"score": test_coverage_quality, "justification": test_coverage_justification},
            },
            "overall_score": overall_score,
            "overall_justification": overall_justification,
            "recommendation": recommendation,
            "stale": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        evaluations = list(spec.evaluations or [])
        evaluations.append(evaluation)
        spec.evaluations = evaluations
        from sqlalchemy.orm.attributes import flag_modified
        flag_modified(spec, "evaluations")
        await db.commit()

    return json.dumps({"success": True, "evaluation": evaluation}, default=str)


@mcp.tool()
async def okto_pulse_list_spec_evaluations(board_id: str, spec_id: str) -> str:
    """
    List all qualitative evaluations for a spec, with stale indication.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with evaluations list and summary
    """
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
    Get full details of a specific evaluation including all dimensions and justifications.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        evaluation_id: Evaluation ID (e.g. eval_abc12345)

    Returns:
        JSON with full evaluation details
    """
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
    Delete your own evaluation. Only the author can delete their evaluation.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        evaluation_id: Evaluation ID to delete

    Returns:
        JSON with success or error
    """
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
    evolved over time and what exactly was modified at each step.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        limit: Maximum number of history entries to return (default 30)

    Returns:
        JSON with list of history entries, newest first. Each entry includes:
        - action: what happened (created, updated, status_changed, cards_derived, etc.)
        - actor_name: who did it
        - changes: list of {field, old, new} diffs
        - summary: human-readable summary
        - version: spec version at that point
        - created_at: when it happened
    """
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
    BEFORE work begins on tasks.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        question: Question text (use @Name to mention someone)

    Returns:
        JSON with Q&A item details
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import SpecQACreate

    async with get_db_for_mcp() as db:
        service = SpecQAService(db)
        qa = await service.create_question(spec_id, ctx.agent_id, SpecQACreate(question=question))
        if not qa:
            return json.dumps({"error": "Spec not found"})

        board_service = BoardService(db)
        await board_service._log_activity(
            board_id=board_id, action="spec_question_added",
            actor_type="agent", actor_id=ctx.agent_id, actor_name=ctx.agent_name,
            details={"spec_id": spec_id, "question": question[:100]},
        )
        await db.commit()

        return json.dumps(
            {
                "success": True,
                "qa": {
                    "id": qa.id,
                    "question": qa.question,
                    "asked_by": qa.asked_by,
                },
            }
        )


@mcp.tool()
async def okto_pulse_ask_spec_choice_question(
    board_id: str,
    spec_id: str,
    question: str,
    options: str,
    question_type: str = "choice",
    allow_free_text: str = "false",
) -> str:
    """
    Ask a choice question (poll/form) on a spec's Q&A board. The respondent picks from predefined options.
    Use this when you need a structured answer — e.g. "Which auth approach?" with options.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        question: The question text
        options: Option labels in any of three formats:
            - JSON array (preferred when labels contain commas):
              ``'["OAuth2 (RFC 6749, recommended)", "API Keys", "Both"]'``
            - Pipe-separated (when labels contain commas but not pipes):
              ``"OAuth2|API Keys|Both"``
            - Comma-separated (legacy, fragile if a label contains a comma):
              ``"OAuth2,API Keys,Both"``
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.
        question_type: "choice" for single-select (default) or "multi_choice" for multi-select
        allow_free_text: "true" to also allow a free-text response alongside selections

    Returns:
        JSON with Q&A item including choices
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_CREATE)
    if perm_err:
        return _perm_error(perm_err)

    import uuid as _uuid

    from okto_pulse.core.models.schemas import SpecQAChoiceOption, SpecQACreate

    try:
        option_labels = parse_multi_value(options)
    except ValueError as e:
        return json.dumps({"error": f"Invalid options: {e}"})
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
async def okto_pulse_answer_spec_question(board_id: str, spec_id: str, qa_id: str, answer: str = "", selected: str = "") -> str:
    """
    Answer a question on a spec's Q&A board.
    For text questions, provide answer. For choice questions, provide selected option IDs.

    Args:
        board_id: Board ID
        spec_id: Spec ID (for context/validation)
        qa_id: Q&A item ID to answer
        answer: Free-text answer (for text questions, or additional text on choice questions with allow_free_text)
        selected: Option IDs for choice questions, accepted in three formats:
            ``'["opt_0", "opt_2"]'`` (JSON array, preferred), ``"opt_0|opt_2"``
            (pipe-separated), or ``"opt_0,opt_2"`` (legacy comma-separated).
            See ``okto_pulse.core.mcp.helpers.parse_multi_value``.

    Returns:
        JSON with updated Q&A item
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.QA_ANSWER)
    if perm_err:
        return _perm_error(perm_err)

    from okto_pulse.core.models.schemas import SpecQAAnswer

    try:
        selected_list = parse_multi_value(selected) if selected else None
    except ValueError as e:
        return json.dumps({"error": f"Invalid selected: {e}"})

    async with get_db_for_mcp() as db:
        service = SpecQAService(db)
        qa = await service.answer_question(
            qa_id, ctx.agent_id,
            SpecQAAnswer(answer=answer or None, selected=selected_list),
        )
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
async def okto_pulse_list_spec_qa(board_id: str, spec_id: str) -> str:
    """
    List all Q&A items on a spec. Check this before starting work to understand
    any open questions or clarifications about the spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with list of Q&A items
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecQAService(db)
        items = await service.list_qa(spec_id)
        await db.commit()

        return json.dumps(
            {
                "spec_id": spec_id,
                "count": len(items),
                "qa_items": [
                    {
                        "id": qa.id,
                        "question": qa.question,
                        "question_type": qa.question_type,
                        "choices": qa.choices,
                        "allow_free_text": qa.allow_free_text,
                        "answer": qa.answer,
                        "selected": qa.selected,
                        "asked_by": qa.asked_by,
                        "answered_by": qa.answered_by,
                        "created_at": qa.created_at.isoformat(),
                        "answered_at": qa.answered_at.isoformat() if qa.answered_at else None,
                    }
                    for qa in items
                ],
            },
            default=str,
        )


# ============================================================================
# SPEC KNOWLEDGE BASE TOOLS
# ============================================================================


@mcp.tool()
async def okto_pulse_list_spec_knowledge(board_id: str, spec_id: str) -> str:
    """
    List all knowledge base items attached to a spec (titles and descriptions, without content).

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with list of knowledge base items
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = SpecKnowledgeService(db)
        items = await service.list_knowledge(spec_id)
        await db.commit()

        return json.dumps(
            {
                "spec_id": spec_id,
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_spec_knowledge(board_id: str, spec_id: str, knowledge_id: str) -> str:
    """
    Get the full content of a knowledge base item.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        knowledge_id: Knowledge base item ID

    Returns:
        JSON with full knowledge base content
    """
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
    passes through the LLM context, saving tokens.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        title: Title of the knowledge base item
        content: Inline text content (use for small snippets)
        description: Short description of what this document contains (optional)
        mime_type: Content type, default "text/markdown"
        file_path: Absolute path to a local UTF-8 text file on the MCP server host
        file_url: HTTP(S) URL of a UTF-8 text document to fetch

    Returns:
        JSON with created knowledge base item
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
    Delete a knowledge base item from a spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        knowledge_id: Knowledge base item ID

    Returns:
        JSON with success status
    """
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
async def okto_pulse_list_refinement_snapshots(board_id: str, refinement_id: str) -> str:
    """
    List all version snapshots of a refinement. Each snapshot is an immutable copy
    of the refinement's state at the moment it was marked as 'done'.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID

    Returns:
        JSON with list of snapshot summaries (version, title, created_at)
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementService(db)
        snapshots = await service.list_snapshots(refinement_id)
        await db.commit()

        return json.dumps(
            {
                "refinement_id": refinement_id,
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_refinement_snapshot(board_id: str, refinement_id: str, version: str) -> str:
    """
    Get the full immutable snapshot of a refinement at a specific version.
    Includes all fields as they were when the refinement was marked 'done',
    plus a snapshot of all Q&A at that point.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        version: Version number to retrieve

    Returns:
        JSON with complete snapshot including Q&A history
    """
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
async def okto_pulse_list_refinement_knowledge(board_id: str, refinement_id: str) -> str:
    """
    List all knowledge base items attached to a refinement (titles and descriptions, without content).

    Args:
        board_id: Board ID
        refinement_id: Refinement ID

    Returns:
        JSON with list of knowledge base items
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    perm_err = check_permission(ctx.permissions, Permissions.BOARD_READ)
    if perm_err:
        return _perm_error(perm_err)

    async with get_db_for_mcp() as db:
        service = RefinementKnowledgeService(db)
        items = await service.list_knowledge(refinement_id)
        await db.commit()

        return json.dumps(
            {
                "refinement_id": refinement_id,
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
            },
            default=str,
        )


@mcp.tool()
async def okto_pulse_get_refinement_knowledge(board_id: str, refinement_id: str, knowledge_id: str) -> str:
    """
    Get the full content of a refinement knowledge base item.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        knowledge_id: Knowledge base item ID

    Returns:
        JSON with full knowledge base content
    """
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
    passes through the LLM context, saving tokens.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        title: Title of the knowledge base item
        content: Inline text content (use for small snippets)
        description: Short description of what this document contains (optional)
        mime_type: Content type, default "text/markdown"
        file_path: Absolute path to a local UTF-8 text file on the MCP server host
        file_url: HTTP(S) URL of a UTF-8 text document to fetch

    Returns:
        JSON with created knowledge base item
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
    Delete a knowledge base item from a refinement.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID
        knowledge_id: Knowledge base item ID

    Returns:
        JSON with success status
    """
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
    test_scenario_ids: list[str] | str = "",
    business_rule_ids: list[str] | str = "",
    start_date: str = "",
    end_date: str = "",
    labels: list[str] | str = "",
) -> str:
    """
    Create a new sprint for a spec. Sprints break specs into incremental deliverables.

    Args:
        board_id: Board ID
        spec_id: Spec ID this sprint belongs to
        title: Sprint title
        description: Sprint description with scope and deliverables (optional)
        objective: What this sprint aims to achieve (optional but recommended)
        expected_outcome: What success looks like when this sprint is done (optional but recommended)
        test_scenario_ids: Comma-separated spec test scenario IDs scoped to this sprint (optional)
        business_rule_ids: Comma-separated spec business rule IDs scoped to this sprint (optional)
        start_date: ISO date string (optional)
        end_date: ISO date string (optional)
        labels: Comma-separated labels (optional)

    Returns:
        JSON with created sprint details
    """
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
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        try:
            data = SprintCreate(
                title=title, description=description or None, spec_id=spec_id,
                objective=objective or None,
                expected_outcome=expected_outcome or None,
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
                "sprint": {"id": sprint.id, "title": sprint.title, "status": sprint.status.value, "spec_id": sprint.spec_id},
            })
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
    skip_test_coverage: str = "",
    skip_rules_coverage: str = "",
    skip_qualitative_validation: str = "",
) -> str:
    """
    Update sprint fields.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        title: New title (optional, empty = no change)
        description: New description (optional)
        test_scenario_ids: Comma-separated scoped test scenario IDs (optional)
        business_rule_ids: Comma-separated scoped business rule IDs (optional)
        labels: Comma-separated labels (optional)
        skip_test_coverage: "true" or "false" (optional)
        skip_rules_coverage: "true" or "false" (optional)
        skip_qualitative_validation: "true" or "false" (optional)

    Returns:
        JSON with updated sprint details
    """
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
                "sprint": {"id": sprint.id, "title": sprint.title, "version": sprint.version},
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
    Gates: draft→active requires cards, active→review requires scoped test coverage, review→closed requires evaluation.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        status: New status — one of: draft, active, review, closed, cancelled

    Returns:
        JSON with updated sprint details
    """
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
    Get full sprint details including cards, evaluations, and Q&A.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID

    Returns:
        JSON with full sprint details
    """
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

    **Always call this before evaluating, moving, or reviewing a sprint.**

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        include_spec: Include parent spec context with all structured data (default "true")

    Returns:
        JSON with complete sprint context: details + cards + evaluations + Q&A + parent spec + scope
    """
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
                }

                result["scoped"] = {
                    "test_scenarios": scoped_ts,
                    "business_rules": scoped_brs,
                    "technical_requirements": scoped_trs,
                    "api_contracts": scoped_contracts,
                }

        return json.dumps(result, default=str)


@mcp.tool()
async def okto_pulse_list_sprints(board_id: str, spec_id: str) -> str:
    """
    List all sprints for a spec.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with list of sprint summaries
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        sprints = await service.list_sprints(spec_id)
        return json.dumps({
            "spec_id": spec_id,
            "count": len(sprints),
            "sprints": [
                {
                    "id": s.id, "title": s.title, "status": s.status.value,
                    "spec_version": s.spec_version,
                    "test_scenario_ids": s.test_scenario_ids,
                    "business_rule_ids": s.business_rule_ids,
                    "labels": s.labels,
                }
                for s in sprints
            ],
        })


@mcp.tool()
async def okto_pulse_assign_tasks_to_sprint(
    board_id: str,
    sprint_id: str,
    card_ids: list[str] | str,
) -> str:
    """
    Assign cards to a sprint. Cards must belong to the same spec as the sprint.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        card_ids: Multi-value card IDs to assign. Preferred native list (e.g.
            ``["uuid_a", "uuid_b"]``); legacy string accepted as JSON array or
            pipe-separated. Comma-only string is REJECTED. See
            ``okto_pulse.core.mcp.helpers.coerce_to_list_str``.

    Returns:
        JSON with number of cards assigned
    """
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
        from okto_pulse.core.services.main import SprintService
        service = SprintService(db)
        try:
            count = await service.assign_tasks(sprint_id, ids, ctx.agent_id)
            await db.commit()
            return json.dumps({"success": True, "assigned": count})
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
    Submit a qualitative evaluation for a sprint in 'review' status.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID (must be in 'review' status)
        breakdown_completeness: Score 0-100 — do tasks cover the sprint scope?
        breakdown_justification: Why this score
        granularity: Score 0-100 — are tasks properly sized?
        granularity_justification: Why this score
        dependency_coherence: Score 0-100 — do task dependencies make sense?
        dependency_justification: Why this score
        test_coverage_quality: Score 0-100 — do tests cover happy path and edge cases?
        test_coverage_justification: Why this score
        overall_score: Overall score 0-100
        overall_justification: Overall assessment
        recommendation: One of: approve, request_changes, reject

    Returns:
        JSON with evaluation ID and sprint summary
    """
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
    List all evaluations for a sprint.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID

    Returns:
        JSON with evaluations list and summary
    """
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
    Get full details of a specific sprint evaluation.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        evaluation_id: Evaluation ID

    Returns:
        JSON with full evaluation details
    """
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
    Delete your own sprint evaluation.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        evaluation_id: Evaluation ID to delete

    Returns:
        JSON with success or error
    """
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
    Ask a question on a sprint.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        question: Question text

    Returns:
        JSON with created Q&A item
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintQAService
        service = SprintQAService(db)
        qa = await service.create_question(sprint_id, ctx.agent_id, question)
        await db.commit()
        if not qa:
            return json.dumps({"error": "Sprint not found"})
        return json.dumps({
            "success": True,
            "qa": {"id": qa.id, "question": qa.question, "asked_by": qa.asked_by},
        })


@mcp.tool()
async def okto_pulse_answer_sprint_question(
    board_id: str,
    sprint_id: str,
    qa_id: str,
    answer: str,
) -> str:
    """
    Answer a question on a sprint.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID
        qa_id: Q&A item ID
        answer: Answer text

    Returns:
        JSON with updated Q&A item
    """
    ctx = await _get_agent_ctx(board_id)
    if not ctx:
        return _auth_error()

    async with get_db_for_mcp() as db:
        from okto_pulse.core.services.main import SprintQAService
        service = SprintQAService(db)
        qa = await service.answer_question(qa_id, ctx.agent_id, answer)
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
    or remove resolved clarifications that no longer apply.

    Args:
        board_id: Board ID
        spec_id: Spec ID (for context/logging)
        qa_id: Q&A item ID to delete

    Returns:
        JSON with success status
    """
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
    or remove resolved clarifications that no longer apply.

    Args:
        board_id: Board ID
        ideation_id: Ideation ID (for context/logging)
        qa_id: Q&A item ID to delete

    Returns:
        JSON with success status
    """
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
    or remove resolved clarifications that no longer apply.

    Args:
        board_id: Board ID
        refinement_id: Refinement ID (for context/logging)
        qa_id: Q&A item ID to delete

    Returns:
        JSON with success status
    """
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
    or remove resolved clarifications that no longer apply.

    Args:
        board_id: Board ID
        sprint_id: Sprint ID (for context/logging)
        qa_id: Q&A item ID to delete

    Returns:
        JSON with success status
    """
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
    Does NOT create sprints — returns suggestions for review.

    Args:
        board_id: Board ID
        spec_id: Spec ID
        threshold: Max tasks per sprint (default 8)

    Returns:
        JSON with list of suggested sprints (title, card_ids, test_scenario_ids, business_rule_ids)
    """
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
    automatically routes the card: success → done, failed → not_started.

    Args:
        board_id: Board ID
        card_id: Card ID (must be in 'validation' status)
        confidence: Score 0-100 — how confident is the reviewer that the task was implemented correctly?
        confidence_justification: Why this confidence score
        estimated_completeness: Score 0-100 — how complete is the implementation relative to the spec?
        completeness_justification: Why this completeness score
        estimated_drift: Score 0-100 — how much did the implementation deviate from the spec? (lower is better)
        drift_justification: Why this drift score
        general_justification: Overall assessment of the task implementation
        recommendation: One of: approve, reject

    Returns:
        JSON with validation result, outcome, threshold violations, and card routing
    """
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
        except ValueError as e:
            return json.dumps({"error": str(e)})


@mcp.tool()
async def okto_pulse_list_task_validations(board_id: str, card_id: str) -> str:
    """
    List all validations for a task card in reverse chronological order.

    Useful for understanding the validation history of a card, especially
    cards that have been through multiple validation cycles (failed → reworked → resubmitted).

    Args:
        board_id: Board ID
        card_id: Card ID

    Returns:
        JSON with list of validation entries
    """
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
    Get full details of a specific task validation entry.

    Args:
        board_id: Board ID
        card_id: Card ID
        validation_id: Validation ID (e.g. "val_abc12345")

    Returns:
        JSON with full validation details including scores, justifications, outcome, and threshold violations
    """
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
    """
    Submit a Spec Validation Gate record for a spec in 'approved' status.

    This is the entry point for the Spec Validation Gate — a semantic quality
    gate that runs AFTER the existing deterministic coverage gates (AC/FR/TR/Contract).
    Use this AFTER you have confidence the spec is saturated on detail (see
    agent_instructions.md section 2.3a "Detail Saturation").

    The system runs coverage gates first; if any fails the submit is rejected
    with the specific coverage violation. If coverage passes, it computes outcome:
    - FAILED if any threshold violated OR recommendation=reject
    - SUCCESS only if ALL thresholds pass AND recommendation=approve

    On SUCCESS, the spec is atomically promoted from 'approved' to 'validated'
    and enters the content lock (update_spec and related tools will raise
    SpecLockedError). To edit after a success, move the spec back to draft or
    approved — the validation will be cleared but the full history is preserved.

    ANTI-PATTERN WARNING: inflating scores to make the gate pass is a grave
    violation of the detail saturation principle. If outcome=failed, iterate
    on content (add scenarios, refine BRs, specify TRs) rather than just
    raising the numbers.

    Args:
        board_id: Board ID
        spec_id: Spec ID (must be in 'approved' status)
        completeness: Score 0-100 — how complete is the spec detail (ACs, BRs, TRs, scenarios, contracts)?
        completeness_justification: Why this completeness score (min 10 chars)
        assertiveness: Score 0-100 — how measurable/testable is the text (no weasel words)?
        assertiveness_justification: Why this assertiveness score (min 10 chars)
        ambiguity: Score 0-100 — how many sentences admit multiple interpretations? (LOWER IS BETTER)
        ambiguity_justification: Why this ambiguity score (min 10 chars)
        general_justification: Overall assessment (min 20 chars)
        recommendation: One of: approve, reject

    Returns:
        JSON with validation result, outcome, threshold violations, and resolved thresholds.
        On success, spec_status becomes "validated".
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
    The record currently pointed to by current_validation_id has active=true.

    Args:
        board_id: Board ID
        spec_id: Spec ID

    Returns:
        JSON with current_validation_id and validations list (reverse chronological)
    """
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

from okto_pulse.core.mcp.kg_tools import register_kg_tools as _register_kg_tools
from okto_pulse.core.mcp.kg_query_tools import register_kg_query_tools as _register_kg_query_tools

_register_kg_tools(mcp, get_agent=_get_authenticated_agent, get_db=get_db_for_mcp)
_register_kg_query_tools(mcp, get_agent=_get_authenticated_agent, get_db=get_db_for_mcp)

from okto_pulse.core.mcp.kg_power_tools import register_kg_power_tools as _register_kg_power_tools
_register_kg_power_tools(mcp, get_agent=_get_authenticated_agent, get_db=get_db_for_mcp)


# ============================================================================
# KG HEALTH (spec 20f67c2a — Ideação #5, FR2)
# ============================================================================


@mcp.tool()
async def okto_pulse_kg_health(board_id: str) -> str:
    """
    Snapshot of the KG health for one board — gemelar do REST GET /api/v1/kg/health.

    Returns 10 fields aggregating consolidation queue depth, dead-letter
    backlog, total nodes, default-score skew, average relevance, top
    most-disconnected nodes, schema version, and the running count of
    contradict_penalty cap events. Computed in-process; cheap to poll.

    Use it before kicking off long consolidations (high queue_depth means
    your enqueue may sit pending), after flagging contradictions (spike
    in contradict_warn_count = curator should reconcile), or to debug
    flat ranking (default_score_ratio > 0.7 = scoring not differentiating).

    Args:
        board_id: Board ID (uuid)

    Returns:
        JSON with the 10-field KG health snapshot, or {"error": "..."} on auth/not-found.
    """
    ctx = await _get_agent_ctx(board_id)
    if ctx is None:
        return _auth_error()

    from okto_pulse.core.services.kg_health_service import (
        BoardNotFoundError,
        get_kg_health,
    )

    try:
        async with get_db_for_mcp() as db:
            data = await get_kg_health(board_id, db)
    except BoardNotFoundError as exc:
        return json.dumps({"error": str(exc)})
    return json.dumps(data, default=str)


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
    List dead-lettered consolidation rows — gemelar do REST GET /api/v1/kg/queue/dead-letter.

    Use quando você (agente operador) detecta `dead_letter_count > 0` via
    okto_pulse_kg_health e precisa investigar quais rows falharam, com que
    erro, e em quantas tentativas. Cada row inclui o array `errors`
    completo do schema TR16 — uma entrada por tentativa com error_type,
    message, occurred_at, traceback (opcional).

    READ-only no MVP — não há ação de reprocess via MCP (deferred v2).

    Args:
        board_id: Board UUID
        limit: Max rows to return (1-200, default 50)
        offset: Skip first N rows (>=0, default 0)

    Returns:
        JSON `{rows, total, limit, offset}` em sucesso. `{error: "..."}`
        em auth fail.
    """
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

    NUNCA delete `graph.kuzu` para "consertar" — destruiria todo o KG
    do board. Use esta tool em vez disso.

    Args:
        board_id: Board UUID específico (mutuamente exclusivo com all_boards)
        all_boards: Se True, migra todos os boards conhecidos do server.
            Default False — exige board_id.

    Returns:
        Single board: JSON `{board_id, migrated, columns_added, errors,
        duration_ms}`. All-boards: `{results: [<single>, ...]}`.
        Erro de input: `{error: "missing_board_or_all_boards"}`.
    """
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
    erro `tick_already_running` — primeiro a chegar ganha o advisory lock.

    Args:
        board_id: Optional board UUID. Empty string = global tick (all boards).
        force_full_rebuild: When true, resets last_recomputed_at to NULL
            for all nodes in scope before the tick — ignores staleness.

    Returns:
        JSON with `{tick_id, status: "running", scheduled_at}` on 202 success.
        On 409 (lock held), `{error: "tick_already_running", message: "..."}`.
        On auth failure, `{error: "..."}`.
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

    from okto_pulse.core.kg.workers.advisory_lock import get_async_lock

    lock = get_async_lock("kg_daily_tick", "global")
    if lock.locked():
        return json.dumps({
            "error": "tick_already_running",
            "message": "Tick already running, retry shortly",
        })

    import asyncio
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

    asyncio.create_task(
        _dispatch_manual_tick(
            tick_id=tick_id,
            board_id=board_id or None,
            force_full_rebuild=force_full_rebuild,
        )
    )

    return json.dumps({
        "tick_id": tick_id,
        "status": "running",
        "scheduled_at": scheduled_at,
    })


# ============================================================================
# SERVER STARTUP
# ============================================================================


def build_mcp_asgi_app():
    """Build the MCP ASGI application wrapped with the API-key middleware.

    Returns the ASGI app that should be served by uvicorn (or mounted
    elsewhere). Single-process callers (``okto_pulse.community.main.serve``)
    use this to bind the MCP transport to its own port while sharing the
    same Python process as the API server, so the Kùzu lock is held by a
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

    # Read port and host from environment (set by CLI / Docker / compose)
    # or fall back to safe defaults. Default host is loopback so a stray
    # `python -m okto_pulse.core.mcp.server` doesn't accidentally expose
    # the MCP server beyond the local box; container deployments override
    # via MCP_HOST=0.0.0.0 in docker-compose.yml.
    port = int(os.environ.get("MCP_PORT", str(settings.mcp_port)))
    host = os.environ.get("MCP_HOST", "127.0.0.1")
    uvicorn.run(build_mcp_asgi_app(), host=host, port=port, ws="wsproto")


if __name__ == "__main__":
    run_mcp_server()

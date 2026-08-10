"""Transport-neutral catalog for Okto Pulse MCP commands.

The Core owns command names, handler functions, descriptions, JSON-schema
metadata and resource definitions.  A concrete MCP framework is deliberately
not involved here: an edition adapter materializes this catalog into its chosen
transport runtime.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Annotated, Any, Callable, get_args, get_origin, get_type_hints

from pydantic import TypeAdapter


@dataclass(frozen=True)
class CoreMcpTool:
    """A command exposed by the Core MCP catalog."""

    name: str
    fn: Callable[..., Any]
    description: str
    parameters: dict[str, Any]
    title: str | None = None
    enabled: bool = True


@dataclass(frozen=True)
class CoreMcpResource:
    """A lazily-read resource exposed by the Core MCP catalog."""

    uri: str
    fn: Callable[..., Any]
    description: str
    name: str | None = None
    title: str | None = None
    mime_type: str | None = None
    enabled: bool = True


class CoreMcpToolManager:
    """Compatibility projection for consumers that inspect registered tools."""

    def __init__(self) -> None:
        self._tools: dict[str, CoreMcpTool] = {}


class CoreMcpResourceManager:
    """Compatibility projection for consumers that inspect registered resources."""

    def __init__(self) -> None:
        self._resources: dict[str, CoreMcpResource] = {}


class DuplicateMcpToolNameError(ValueError):
    """Raised instead of silently replacing an existing command registration."""

    def __init__(self, tool_name: str) -> None:
        self.tool_name = tool_name
        super().__init__(f"duplicate MCP tool registration: {tool_name}")


def _json_schema_for(annotation: Any) -> dict[str, Any]:
    """Return the client-facing JSON schema for one function parameter.

    ``TypeAdapter`` gives the Core the same Pydantic-derived union/list/object
    fidelity used by the Community MCP transport, while the fallback keeps
    catalog registration fail-safe for an annotation that cannot be resolved.
    """

    if annotation is inspect.Signature.empty:
        return {}
    try:
        schema = TypeAdapter(annotation).json_schema()
    except Exception:
        if annotation is str:
            return {"type": "string"}
        if annotation is int:
            return {"type": "integer"}
        if annotation is float:
            return {"type": "number"}
        if annotation is bool:
            return {"type": "boolean"}
        if annotation in (list, tuple, set):
            return {"type": "array"}
        if annotation is dict:
            return {"type": "object"}
        return {}
    return _without_nonsemantic_titles(dict(schema)) if isinstance(schema, dict) else {}


def _without_nonsemantic_titles(value: Any) -> Any:
    """Drop generated JSON-Schema titles while preserving validation metadata.

    Pydantic repeats class/field names as ``title`` throughout every tool
    schema.  MCP already carries the command and property names, so those
    labels consume the always-loaded metadata budget without adding type,
    bounds, enum, required, union, or closed-object semantics.
    """

    if isinstance(value, dict):
        return {
            key: _without_nonsemantic_titles(item)
            for key, item in value.items()
            if key != "title"
        }
    if isinstance(value, list):
        return [_without_nonsemantic_titles(item) for item in value]
    return value


def _parameters_for(fn: Callable[..., Any]) -> dict[str, Any]:
    """Build an object schema from the public signature of ``fn``."""

    signature = inspect.signature(fn)
    try:
        # include_extras keeps Annotated[...] metadata so parameter
        # descriptions declared via pydantic Field(description=...) reach the
        # MCP schema (auditoria MCP 2026-07-12, achado #69).
        annotations = get_type_hints(fn, include_extras=True)
    except Exception:
        annotations = getattr(fn, "__annotations__", {}) or {}

    properties: dict[str, Any] = {}
    required: list[str] = []
    for parameter in signature.parameters.values():
        if parameter.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue
        annotation = annotations.get(parameter.name, parameter.annotation)
        description = None
        if get_origin(annotation) is Annotated:
            args = get_args(annotation)
            for meta in args[1:]:
                meta_description = getattr(meta, "description", None)
                if meta_description:
                    description = meta_description
                    break
        # Preserve the complete Annotated type when building the schema.  The
        # metadata contains numeric/list constraints as well as descriptions;
        # stripping it made server-side bounds invisible to MCP clients.
        schema = _json_schema_for(annotation)
        if description:
            schema["description"] = description
        if parameter.default is inspect.Signature.empty:
            required.append(parameter.name)
        else:
            try:
                schema.setdefault("default", parameter.default)
            except TypeError:
                # A non-serialisable default is still valid Python API shape;
                # omit it from MCP metadata rather than rejecting the catalog.
                pass
        properties[parameter.name] = schema

    result: dict[str, Any] = {"type": "object", "properties": properties}
    if getattr(fn, "__mcp_closed_schema__", False):
        # New governed surfaces opt in to an explicitly closed root object.
        # Python would reject an unknown keyword at invocation time anyway,
        # but publishing ``additionalProperties: false`` makes that contract
        # discoverable before a client sends a request.
        result["additionalProperties"] = False
    if required:
        result["required"] = required
    return result


class CoreMcpCatalog:
    """In-memory catalog with the small compatibility surface tools use today."""

    def __init__(self, *, name: str, version: str, instructions: str = "") -> None:
        self.name = name
        self.version = version
        self.instructions = instructions
        self._tool_manager = CoreMcpToolManager()
        self._resource_manager = CoreMcpResourceManager()
        self._runtime_clone_hooks: list[Callable[[CoreMcpCatalog], None]] = []

    def register_runtime_clone_hook(
        self,
        hook: Callable[[CoreMcpCatalog], None],
    ) -> None:
        """Reapply instance-bound behavior whenever this catalog is cloned."""

        if hook not in self._runtime_clone_hooks:
            self._runtime_clone_hooks.append(hook)

    def clone_for_runtime(self) -> "CoreMcpCatalog":
        """Clone catalog indexes so compositions cannot share mutations."""

        clone = CoreMcpCatalog(
            name=self.name,
            version=self.version,
            instructions=self.instructions,
        )
        clone._tool_manager._tools.update(self._tool_manager._tools)
        clone._resource_manager._resources.update(self._resource_manager._resources)
        for hook in self._runtime_clone_hooks:
            hook(clone)
        return clone

    def _register_tool(
        self,
        fn: Callable[..., Any],
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        enabled: bool | None = None,
        **_metadata: Any,
    ) -> CoreMcpTool:
        tool_name = name or fn.__name__
        if tool_name in self._tool_manager._tools:
            raise DuplicateMcpToolNameError(tool_name)
        tool = CoreMcpTool(
            name=tool_name,
            fn=fn,
            description=description or inspect.getdoc(fn) or "",
            parameters=_parameters_for(fn),
            title=title,
            enabled=True if enabled is None else enabled,
        )
        self._tool_manager._tools[tool_name] = tool
        return tool

    def tool(
        self,
        name_or_fn: str | Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        enabled: bool | None = None,
        **metadata: Any,
    ) -> Callable[[Callable[..., Any]], CoreMcpTool] | CoreMcpTool:
        """Register a handler using the familiar ``@mcp.tool`` forms."""

        if inspect.isroutine(name_or_fn):
            return self._register_tool(
                name_or_fn,
                name=name,
                title=title,
                description=description,
                enabled=enabled,
                **metadata,
            )

        if isinstance(name_or_fn, str):
            if name is not None:
                raise TypeError("Specify a tool name either positionally or by keyword.")
            resolved_name = name_or_fn
        elif name_or_fn is None:
            resolved_name = name
        else:
            raise TypeError(
                "The first argument to tool() must be a function, a name, or None."
            )

        def decorator(fn: Callable[..., Any]) -> CoreMcpTool:
            return self._register_tool(
                fn,
                name=resolved_name,
                title=title,
                description=description,
                enabled=enabled,
                **metadata,
            )

        return decorator

    async def get_tool(self, name: str) -> CoreMcpTool:
        return self._tool_manager._tools[name]

    async def get_tools(self) -> dict[str, CoreMcpTool]:
        return dict(self._tool_manager._tools)

    def resource(
        self,
        uri: str,
        *,
        name: str | None = None,
        title: str | None = None,
        description: str | None = None,
        mime_type: str | None = None,
        enabled: bool | None = None,
        **_metadata: Any,
    ) -> Callable[[Callable[..., Any]], CoreMcpResource]:
        """Register a lazy resource using the familiar decorator form."""

        def decorator(fn: Callable[..., Any]) -> CoreMcpResource:
            resource = CoreMcpResource(
                uri=uri,
                fn=fn,
                description=description or inspect.getdoc(fn) or "",
                name=name,
                title=title,
                mime_type=mime_type,
                enabled=True if enabled is None else enabled,
            )
            self._resource_manager._resources[uri] = resource
            return resource

        return decorator

    def iter_tools(self) -> tuple[CoreMcpTool, ...]:
        return tuple(self._tool_manager._tools.values())

    def iter_resources(self) -> tuple[CoreMcpResource, ...]:
        return tuple(self._resource_manager._resources.values())


__all__ = [
    "CoreMcpCatalog",
    "CoreMcpResource",
    "CoreMcpTool",
    "DuplicateMcpToolNameError",
]

"""Explicit, testable HTTP telemetry classification policy (spec R5A, card R5A-C).

Replaces the implicit ``path.startswith('/api/')`` filter in the ASGI middleware
with a policy that is a pure function (so it is unit-testable on its own) and a
route-template sanitizer that never lets a raw path / id / query / value reach the
``http`` telemetry event.

Policy (decided by the FIRST path segment, matched EXACTLY — never a substring or
naive prefix, so ``/apiary``, ``/api-keys``, ``/mcping``, ``/healthz``,
``/health-internal``, ``/docs-admin`` do NOT inherit the ``/api`` / ``/health`` /
``/docs`` rules):
- COUNT ``/api`` + ``/api/*`` (product API) and ``/mcp`` + ``/mcp/*`` (the agent
  operational surface);
- EXCLUDE ``/health``, ``/docs``, ``/openapi.json``, ``/redoc`` (and their
  subpaths): probes / docs / schema, which are noise, not product-usage signal;
- everything else is not counted.
"""

from __future__ import annotations

# First path segments counted as product / agent usage telemetry.
_COUNTED_SEGMENTS = frozenset({"api", "mcp"})
# First path segments explicitly excluded — probes, docs and schema endpoints.
# Excluded takes precedence; matched EXACTLY on the first segment.
_EXCLUDED_SEGMENTS = frozenset({"health", "docs", "redoc", "openapi", "openapi.json"})


def _first_segment(path: str) -> str:
    return path.split("?", 1)[0].strip("/").split("/", 1)[0].lower()


def should_count_http(path: str) -> bool:
    """True iff an HTTP path is product/agent-usage telemetry worth counting.

    Exact first-segment classification: ``/api`` and ``/mcp`` (with subpaths) are
    counted; ``/health`` / ``/docs`` / ``/openapi.json`` / ``/redoc`` are excluded;
    a lookalike (``/apiary``, ``/api-keys``, ``/mcping``, ``/healthz``) is NOT
    counted because it is a DIFFERENT first segment."""
    segment = _first_segment(path)
    if segment in _EXCLUDED_SEGMENTS:
        return False
    return segment in _COUNTED_SEGMENTS


def safe_route_template(route: object | None, raw_path: str) -> str:
    """Return a values-free route template for the ``http`` event.

    Prefers the Starlette/FastAPI-resolved ``route.path``, which is the registered
    route PATTERN (e.g. ``/api/v1/cards/{card_id}``) — placeholders, never concrete
    values. When the route did NOT resolve (a 404 / unmatched path), the concrete
    path is NEVER emitted: it collapses to a bounded controlled template for the
    counted surface (``/api/{unresolved}`` or ``/mcp/{unresolved}``), so the
    aggregate key can never carry a raw segment, id, slug, query or PII — even for a
    malicious path like ``/api/cards/SECRET?token=xxx``."""
    template = getattr(route, "path", None)
    if isinstance(template, str) and template:
        return template
    segment = _first_segment(raw_path)
    base = segment if segment in _COUNTED_SEGMENTS else "other"
    return f"/{base}/{{unresolved}}"

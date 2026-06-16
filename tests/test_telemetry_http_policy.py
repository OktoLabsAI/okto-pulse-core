"""Pure-policy unit tests for the HTTP telemetry classification (spec R5A, card
R5A-C). The middleware-level behaviour is in test_telemetry_asgi_middleware.py;
this file pins the policy function + the route-template sanitizer in isolation,
including the false-positive-by-segment and malicious-path cases.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from okto_pulse.core.telemetry.http_policy import safe_route_template, should_count_http  # noqa: E402


class _Route:
    def __init__(self, path: str) -> None:
        self.path = path


def test_policy_counts_api_and_mcp_surfaces() -> None:
    for path in ("/api", "/api/", "/api/v1/cards", "/mcp", "/mcp/tools/call"):
        assert should_count_http(path) is True, path


def test_policy_excludes_probes_docs_and_schema() -> None:
    for path in ("/health", "/health/live", "/docs", "/docs/", "/openapi.json", "/redoc", "/redoc/"):
        assert should_count_http(path) is False, path


def test_policy_rejects_lookalike_segments_not_substrings() -> None:
    # exact first-segment match: a lookalike must NOT inherit /api, /health or /docs.
    for path in (
        "/apiary",
        "/api-keys",
        "/mcping",
        "/healthz",
        "/health-internal",
        "/docs-admin",
        "/",
        "",
        "/kg-health",
    ):
        assert should_count_http(path) is False, path


def test_safe_route_template_prefers_resolved_pattern() -> None:
    # a resolved route -> the registered PATTERN (placeholder), never the value.
    assert safe_route_template(_Route("/api/v1/cards/{card_id}"), "/api/v1/cards/abc") == "/api/v1/cards/{card_id}"


def test_safe_route_template_fallback_is_bounded_with_no_concrete_path() -> None:
    # unresolved route + a concrete id / secret / query: the key is a bounded
    # controlled template, never the concrete path/query/value.
    out = safe_route_template(None, "/api/cards/SECRET?token=xxx")
    assert out == "/api/{unresolved}"
    assert "SECRET" not in out and "token" not in out and "xxx" not in out
    assert safe_route_template(None, "/mcp/foo/bar") == "/mcp/{unresolved}"
    # a non-counted surface that somehow reaches here collapses to a neutral base.
    assert safe_route_template(None, "/weird/x") == "/other/{unresolved}"

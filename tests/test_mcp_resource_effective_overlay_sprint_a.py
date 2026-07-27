"""Sprint A acceptance tests for the frozen effective MCP resource catalog.

Pulse scenarios:
* c3e81cc8 — collision resolution is independent of declaration order;
* 43bca216 — freeze snapshots bytes and rejects every later registration;
* 22358245 — the enabled-resource manifest is canonical and dynamic.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import pytest

from okto_pulse.core.mcp import server as srv
from okto_pulse.core.mcp.manifest import build_resource_manifest
from okto_pulse.core.ports.mcp_resources import (
    CompositeMcpResourceCatalog,
    McpResourceCatalogError,
    McpResourceReplacementTarget,
    McpResourceSpec,
    StaticMcpResourceCatalog,
    freeze_mcp_resource_catalog,
)


URI = "okto-pulse://reference/collision"


def _spec(
    *,
    edition: str,
    body: str,
    uri: str = URI,
    enabled: bool = True,
    replace_target: McpResourceReplacementTarget | None = None,
    source_identity: str | None = None,
    name: str | None = None,
    mime_type: str = "text/plain",
    same_uri_overlay: bool = False,
) -> McpResourceSpec:
    return McpResourceSpec(
        uri=uri,
        description=f"{edition} declaration",
        category="reference",
        edition=edition,
        content=body,
        enabled=enabled,
        replace_target=replace_target,
        source_identity=source_identity,
        name=name,
        mime_type=mime_type,
        same_uri_overlay=same_uri_overlay,
    )


@dataclass(frozen=True)
class _LooseCatalog:
    """Test double that lets the resolver receive malformed declarations."""

    edition: str
    precedence: int
    declarations: tuple[McpResourceSpec, ...]

    def specs(self) -> tuple[McpResourceSpec, ...]:
        return self.declarations


@pytest.fixture(autouse=True)
def _isolated_server_catalog():
    srv.reset_resource_catalog_for_tests()
    yield
    srv.reset_resource_catalog_for_tests()


def test_c3e81cc8_collision_resolution_is_order_independent_and_transactional():
    base = StaticMcpResourceCatalog(
        "base",
        (_spec(edition="base", body="BASE", source_identity="base:source"),),
        precedence=10,
    )
    replacement = StaticMcpResourceCatalog(
        "extension",
        (
            _spec(
                edition="extension",
                body="REPLACEMENT",
                source_identity="extension:source",
                replace_target=McpResourceReplacementTarget(edition="base"),
            ),
        ),
        precedence=20,
    )

    forward = CompositeMcpResourceCatalog((base, replacement)).specs()
    reverse = CompositeMcpResourceCatalog((replacement, base)).specs()

    assert forward == reverse
    assert forward[0].read() == "REPLACEMENT"
    assert forward[0].edition == "extension"
    assert forward[0].source_identity == "extension:source"
    assert forward[0].replaced_source_identity == "base:source"

    undeclared = StaticMcpResourceCatalog(
        "extension",
        (
            _spec(
                edition="extension",
                body="UNDECLARED",
                same_uri_overlay=True,
            ),
        ),
        precedence=20,
    )
    for catalogs in ((base, undeclared), (undeclared, base)):
        with pytest.raises(McpResourceCatalogError) as exc_info:
            CompositeMcpResourceCatalog(catalogs)
        assert exc_info.value.code == "undeclared_replace"

    lower_precedence = StaticMcpResourceCatalog(
        "extension",
        (
            _spec(
                edition="extension",
                body="LOWER",
                replace_target=McpResourceReplacementTarget(edition="base"),
            ),
        ),
        precedence=5,
    )
    with pytest.raises(McpResourceCatalogError) as exc_info:
        CompositeMcpResourceCatalog((base, lower_precedence))
    assert exc_info.value.code == "invalid_replace_precedence"

    duplicate = _LooseCatalog(
        edition="duplicate-edition",
        precedence=30,
        declarations=(
            _spec(edition="duplicate-edition", body="ONE", uri="okto-pulse://x"),
            _spec(edition="duplicate-edition", body="TWO", uri="okto-pulse://x"),
        ),
    )
    before_catalog = srv.effective_resource_catalog()
    before_projection = srv.resource_registry_projection()
    before_handlers = tuple(resource.uri for resource in srv.mcp.iter_resources())

    with pytest.raises(McpResourceCatalogError) as exc_info:
        srv.register_resource_catalog(duplicate)

    assert exc_info.value.code == "same_edition_collision"
    assert srv.effective_resource_catalog() is before_catalog
    assert srv.resource_registry_projection() == before_projection
    assert tuple(resource.uri for resource in srv.mcp.iter_resources()) == before_handlers


def test_43bca216_freeze_snapshots_bytes_and_rejects_later_registration():
    mutable = {"body": "BEFORE"}
    catalog = StaticMcpResourceCatalog(
        "extension",
        (
            McpResourceSpec(
                uri="okto-pulse://extension/mutable",
                description="mutable loader",
                category="extension",
                edition="extension",
                loader=lambda: mutable["body"],
                source_identity="extension:mutable",
            ),
        ),
        precedence=10,
    )
    frozen = freeze_mcp_resource_catalog(CompositeMcpResourceCatalog((catalog,)))
    before_manifest = frozen.manifest.as_dict()
    before_identity = frozen.identity

    mutable["body"] = "AFTER"

    assert frozen.specs()[0].read() == "BEFORE"
    assert frozen.identity == before_identity
    assert frozen.manifest.as_dict() == before_manifest

    srv.register_resource_catalog(catalog)
    with pytest.raises(McpResourceCatalogError) as exc_info:
        srv.effective_resource_manifest()
    assert exc_info.value.code == "registry_unfrozen"
    assert "okto-pulse://extension/mutable" not in {
        resource.uri for resource in srv.mcp.iter_resources()
    }
    published = srv.freeze_resource_catalog()
    assert "okto-pulse://extension/mutable" in {
        resource.uri for resource in srv.mcp.iter_resources()
    }
    assert srv.effective_resource_manifest() is published.manifest
    published_before = (
        published.identity,
        published.manifest_hash,
        published.manifest.as_dict(),
        tuple((spec.uri, spec.read()) for spec in published.specs()),
    )
    late = StaticMcpResourceCatalog(
        "later",
        (_spec(edition="later", body="LATE", uri="okto-pulse://later/item"),),
        precedence=20,
    )

    with pytest.raises(McpResourceCatalogError) as exc_info:
        srv.register_resource_catalog(late)

    assert exc_info.value.code == "registry_frozen"
    current = srv.effective_resource_catalog()
    assert current is published
    assert (
        current.identity,
        current.manifest_hash,
        current.manifest.as_dict(),
        tuple((spec.uri, spec.read()) for spec in current.specs()),
    ) == published_before


def test_43bca216_freeze_evaluates_each_external_loader_exactly_once():
    calls = 0

    def _counting_loader() -> str:
        nonlocal calls
        calls += 1
        return "SNAPSHOT"

    catalog = StaticMcpResourceCatalog(
        "extension",
        (
            McpResourceSpec(
                uri="okto-pulse://extension/counting-loader",
                description="counting loader",
                category="extension",
                edition="extension",
                loader=_counting_loader,
            ),
        ),
        precedence=10,
    )

    srv.register_resource_catalog(catalog)
    assert calls == 0

    frozen = srv.freeze_resource_catalog()

    assert calls == 1
    assert next(
        spec.read()
        for spec in frozen.specs()
        if spec.uri == "okto-pulse://extension/counting-loader"
    ) == "SNAPSHOT"
    assert srv.effective_resource_manifest() is frozen.manifest
    assert calls == 1


def test_43bca216_freeze_publishes_handlers_before_frozen_catalog(monkeypatch):
    uri = "okto-pulse://extension/publication-order"
    catalog = StaticMcpResourceCatalog(
        "extension",
        (_spec(edition="extension", body="ORDERED", uri=uri),),
        precedence=10,
    )
    srv.register_resource_catalog(catalog)
    original_register = srv.register_runtime_value
    observed = False

    def _observe_publication(key: str, value: object) -> None:
        nonlocal observed
        if key == srv._RESOURCE_CATALOG_KEY and getattr(value, "is_frozen", False):
            observed = True
            assert uri in {
                resource.uri for resource in srv.mcp.iter_resources()
            }
        original_register(key, value)

    monkeypatch.setattr(srv, "register_runtime_value", _observe_publication)

    frozen = srv.freeze_resource_catalog()

    assert observed is True
    assert srv.effective_resource_catalog() is frozen


def test_resource_registration_fails_closed_for_invalid_registry_state(monkeypatch):
    monkeypatch.setattr(srv, "_resource_catalog", lambda: object())
    catalog = StaticMcpResourceCatalog(
        "extension",
        (_spec(edition="extension", body="LATE", uri="okto-pulse://later/item"),),
        precedence=10,
    )

    with pytest.raises(McpResourceCatalogError) as exc_info:
        srv.register_resource_catalog(catalog)

    assert exc_info.value.code == "registry_invalid_state"


def test_43bca216_failed_freeze_publishes_no_partial_state():
    def _broken_loader() -> str:
        raise OSError("injected loader failure")

    broken = StaticMcpResourceCatalog(
        "extension",
        (
            McpResourceSpec(
                uri="okto-pulse://extension/broken",
                description="broken loader",
                category="extension",
                edition="extension",
                loader=_broken_loader,
            ),
        ),
        precedence=10,
    )
    srv.register_resource_catalog(broken)
    before_catalog = srv.effective_resource_catalog()
    before_handlers = tuple(resource.uri for resource in srv.mcp.iter_resources())

    with pytest.raises(OSError, match="injected loader failure"):
        srv.freeze_resource_catalog()

    assert srv.effective_resource_catalog() is before_catalog
    assert srv.resource_catalog_is_frozen() is False
    assert tuple(resource.uri for resource in srv.mcp.iter_resources()) == before_handlers


def test_22358245_canonical_manifest_contains_only_enabled_frozen_resources():
    declarations = (
        _spec(
            edition="base",
            uri="okto-pulse://zeta/unicode",
            body="Olá, Καλημέρα, こんにちは 👋",
            name="unicode",
            mime_type="text/markdown",
            source_identity="base:zeta",
        ),
        _spec(
            edition="base",
            uri="okto-pulse://alpha/disabled",
            body="MUST NOT BE SERVED",
            enabled=False,
            source_identity="base:disabled",
        ),
        _spec(
            edition="base",
            uri="okto-pulse://alpha/enabled",
            body="alpha",
            name="alpha",
            mime_type="text/plain",
            source_identity="base:alpha",
        ),
    )

    base_catalog = StaticMcpResourceCatalog("base", declarations, precedence=10)
    disabled_collision = StaticMcpResourceCatalog(
        "extension",
        (
            _spec(
                edition="extension",
                uri="okto-pulse://alpha/enabled",
                body="DISABLED COLLISION",
                enabled=False,
            ),
        ),
        precedence=20,
    )
    effective_first = CompositeMcpResourceCatalog((base_catalog, disabled_collision))
    first = freeze_mcp_resource_catalog(effective_first)
    second = freeze_mcp_resource_catalog(
        CompositeMcpResourceCatalog(
            (
                disabled_collision,
                StaticMcpResourceCatalog(
                    "base", tuple(reversed(declarations)), precedence=10
                ),
            )
        )
    )

    assert {spec.uri for spec in effective_first.specs()} == {
        "okto-pulse://alpha/enabled",
        "okto-pulse://zeta/unicode",
    }
    document = build_resource_manifest(first)
    assert document == build_resource_manifest(second)
    assert document["count"] == 2
    assert [entry["uri"] for entry in document["resources"]] == [
        "okto-pulse://alpha/enabled",
        "okto-pulse://zeta/unicode",
    ]
    assert {entry["enabled"] for entry in document["resources"]} == {True}
    assert all(
        set(entry)
        == {
            "uri",
            "name",
            "mime_type",
            "content_sha256",
            "owning_edition",
            "source_identity",
            "enabled",
        }
        for entry in document["resources"]
    )
    by_uri = {entry["uri"]: entry for entry in document["resources"]}
    unicode_bytes = "Olá, Καλημέρα, こんにちは 👋".encode("utf-8")
    assert by_uri["okto-pulse://zeta/unicode"]["content_sha256"] == hashlib.sha256(
        unicode_bytes
    ).hexdigest()
    assert by_uri["okto-pulse://zeta/unicode"]["mime_type"] == "text/markdown"

    canonical_payload = {
        "count": document["count"],
        "resources": document["resources"],
    }
    expected_hash = hashlib.sha256(
        json.dumps(
            canonical_payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    assert document["manifest_hash"] == expected_hash
    assert document["projection_identity"] == expected_hash
    assert first.identity == first.manifest_hash == expected_hash

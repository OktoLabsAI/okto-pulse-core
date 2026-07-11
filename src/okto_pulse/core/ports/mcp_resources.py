"""MCP Resource catalog contracts (spec R11-A, IMP1) — PURE, edition-agnostic
contracts so the EFFECTIVE okto-pulse:// resource catalog is composed per runtime
(core built-ins + Community-injected operational resources) WITHOUT the core
importing ``okto_pulse.community``.

Contracts:
  - :class:`McpResourceSpec`        — one resource (uri/description/category/
    edition + a deterministic path- OR content-loader). ``read()`` never exposes
    ``file://`` or an arbitrary path to the agent (path-loaders are confined to
    their base dir).
  - :class:`McpResourceCatalog`     — ``@runtime_checkable`` Protocol: a catalog
    that yields its specs.
  - :class:`StaticMcpResourceCatalog` — a concrete immutable catalog.
  - :class:`CompositeMcpResourceCatalog` — merges catalogs in STABLE order and
    reports URI conflicts (it satisfies ``McpResourceCatalog``).

Plus catalog-aware gate helpers (R11-A IMP4):
  - :func:`catalog_uri_conflicts`   — duplicate URIs across editions.
  - :func:`catalog_link_integrity`  — every okto-pulse:// link referenced inside a
    spec's content/description resolves to a catalogued URI.
  - :func:`scan_forbidden_terms`    — SELECTIVE common-runtime leak scan
    (normalized alias/case): .lbug/Kuzu/Ladybug/sentence-transformers/SQLite/
    local-paths fail in the ``core``/``common`` edition but are allowed in
    ``community`` (operational catalogs may name their backend).

Pure: stdlib only; no Community, listener or framework import.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Iterable, Protocol, runtime_checkable

RESOURCE_URI_SCHEME = "okto-pulse://"
_URI_RE = re.compile(r"okto-pulse://[a-zA-Z0-9_/\-]+")


#: A resource is COMMON (edition-agnostic workflow/reference doc — an agent reads
#: the workflow, never the backend; the forbidden-term scan applies) or
#: OPERATIONAL (a backend-specific operational doc the Community edition may
#: supply, which MAY name its concrete backend and is therefore EXEMPT).
RESOURCE_KIND_COMMON = "common"
RESOURCE_KIND_OPERATIONAL = "operational"
_RESOURCE_KINDS = frozenset({RESOURCE_KIND_COMMON, RESOURCE_KIND_OPERATIONAL})


@dataclass(frozen=True)
class McpResourceSpec:
    """A single MCP resource. Exactly one of ``path`` (loaded from ``base_dir``)
    or ``content`` (inline) provides the body; ``read()`` returns it
    deterministically and NEVER leaks a filesystem path to the agent.

    ``kind`` (common/operational) is the discriminator the forbidden-term scan
    uses: COMMON specs must stay backend-free, OPERATIONAL specs are exempt.
    ``provider`` (the runtime/adapter that supplies it) and ``capability`` (what
    it documents) describe the source for the catalog inventory."""

    uri: str
    description: str
    category: str
    edition: str
    kind: str = RESOURCE_KIND_COMMON
    provider: str | None = None
    capability: str | None = None
    #: (R11-B) marks an AUTHORIZED same-URI overlay: an operational spec whose
    #: content overrides a common BASE spec of the SAME uri in the effective
    #: catalog (it is NOT a raw URI conflict). Used so the Community runtime serves
    #: the full backend-aware content while the core common spec stays backend-free.
    same_uri_overlay: bool = False
    # Stable package-relative identifier retained for catalog projections. It is
    # metadata only; loading is always performed by ``loader``.
    path: str | None = None
    content: str | None = None
    loader: Callable[[], str] | None = field(default=None, compare=False, repr=False)

    def __post_init__(self) -> None:
        if not self.uri.startswith(RESOURCE_URI_SCHEME):
            raise ValueError(
                f"resource uri must start with {RESOURCE_URI_SCHEME!r}: {self.uri!r}"
            )
        if (self.loader is None) == (self.content is None):
            raise ValueError(f"{self.uri}: exactly one of loader/content must be set")
        if self.kind not in _RESOURCE_KINDS:
            raise ValueError(
                f"{self.uri}: kind must be one of {sorted(_RESOURCE_KINDS)}"
            )

    @property
    def is_common(self) -> bool:
        return self.kind == RESOURCE_KIND_COMMON

    def read(self) -> str:
        """Return the resource body. Path-loaders are CONFINED to ``base_dir`` (a
        traversal outside it yields '' — never an arbitrary file)."""
        if self.content is not None:
            return self.content
        return self.loader() if self.loader is not None else ""


@runtime_checkable
class McpResourceCatalog(Protocol):
    """A source of :class:`McpResourceSpec` for one edition."""

    @property
    def edition(self) -> str: ...

    def specs(self) -> tuple[McpResourceSpec, ...]:
        """Return this catalog's specs in a STABLE, deterministic order."""
        ...


@dataclass(frozen=True)
class StaticMcpResourceCatalog:
    """A concrete immutable catalog from a fixed spec tuple."""

    edition: str
    _specs: tuple[McpResourceSpec, ...]

    def specs(self) -> tuple[McpResourceSpec, ...]:
        return self._specs


def _apply_overlay(base: McpResourceSpec, overlay: McpResourceSpec) -> McpResourceSpec:
    """(R11-B) The effective spec for a base-common + authorized-overlay pair: the
    operational OVERLAY's content WINS (the Community runtime serves the full
    backend-aware content), while the base's PUBLIC description/category are kept
    (the resources/list surface is unchanged). The effective spec is
    ``kind=operational`` so the merged content (which legitimately names the
    backend) is exempt from the common forbidden-term scan.

    The overlay is a full-content override (not an append): the contaminated docs
    have backend terms scattered INLINE, so an append cannot reproduce today's
    layout — an override of today's verbatim content is byte-equivalent."""
    return McpResourceSpec(
        uri=base.uri,
        description=base.description,
        category=base.category,
        edition=overlay.edition,
        kind=overlay.kind,
        provider=overlay.provider,
        capability=overlay.capability,
        same_uri_overlay=True,
        content=overlay.read(),  # materialize today's verbatim operational content
    )


class CompositeMcpResourceCatalog:
    """Merges catalogs in stable order (the order they were composed).

    R11-B overlay semantics: a same-URI pair of (base common spec, AUTHORIZED
    overlay spec — ``same_uri_overlay=True``) MERGES into a single effective spec
    whose content is the operational overlay (byte-equivalent to the original) and
    whose public description/category come from the base. An UNAUTHORIZED same-URI
    duplicate (no overlay flag) keeps the R11-A behaviour: FIRST-WINS for
    ``specs()`` and a raw CONFLICT in ``conflicts()`` (the drift guard is not
    loosened). Satisfies :class:`McpResourceCatalog`."""

    edition = "composite"

    def __init__(self, catalogs: Iterable[McpResourceCatalog]) -> None:
        self._catalogs: tuple[McpResourceCatalog, ...] = tuple(catalogs)

    @property
    def catalogs(self) -> tuple[McpResourceCatalog, ...]:
        return self._catalogs

    def with_catalog(
        self, catalog: McpResourceCatalog
    ) -> "CompositeMcpResourceCatalog":
        """Return a NEW composite with ``catalog`` appended (immutable compose)."""
        return CompositeMcpResourceCatalog((*self._catalogs, catalog))

    def specs(self) -> tuple[McpResourceSpec, ...]:
        order: list[str] = []
        by_uri: dict[str, McpResourceSpec] = {}
        for cat in self._catalogs:
            for s in cat.specs():
                if s.uri not in by_uri:
                    order.append(s.uri)
                    by_uri[s.uri] = s
                    continue
                existing = by_uri[s.uri]
                # AUTHORIZED overlay (either side marked) -> merge (overlay wins).
                if s.same_uri_overlay and not existing.same_uri_overlay:
                    by_uri[s.uri] = _apply_overlay(existing, s)
                elif existing.same_uri_overlay and not s.same_uri_overlay:
                    by_uri[s.uri] = _apply_overlay(s, existing)
                # else: UNAUTHORIZED duplicate -> first-wins (R11-A), conflict below.
        return tuple(by_uri[u] for u in order)

    def conflicts(self) -> tuple[dict, ...]:
        """URIs declared by more than one catalog as an UNAUTHORIZED duplicate
        (edition drift). An authorized same-URI overlay (``same_uri_overlay=True``
        on either side) is NOT a conflict."""
        owner: dict[str, McpResourceSpec] = {}
        conflicts: list[dict] = []
        for cat in self._catalogs:
            for s in cat.specs():
                prior = owner.get(s.uri)
                if prior is None:
                    owner[s.uri] = s
                    continue
                if prior.edition == cat.edition:
                    continue
                # authorized overlay (either side) is NOT a raw conflict.
                if s.same_uri_overlay or prior.same_uri_overlay:
                    continue
                conflicts.append(
                    {"uri": s.uri, "owner": prior.edition, "duplicate": cat.edition}
                )
        return tuple(conflicts)


# --- catalog-aware gate helpers (IMP4) --------------------------------------


def catalog_uri_conflicts(catalog: McpResourceCatalog) -> tuple[dict, ...]:
    """URI conflicts within a (composite) catalog; () for a flat catalog."""
    if isinstance(catalog, CompositeMcpResourceCatalog):
        return catalog.conflicts()
    return ()


def catalog_link_integrity(catalog: McpResourceCatalog) -> tuple[dict, ...]:
    """Every okto-pulse:// link referenced inside a spec's description/content
    must resolve to a catalogued URI. Returns the broken (source_uri, link)
    pairs; () == holds."""
    known = {s.uri for s in catalog.specs()}
    broken: list[dict] = []
    for s in catalog.specs():
        for blob, where in ((s.description, "description"), (s.read(), "content")):
            for link in _URI_RE.findall(blob or ""):
                if link not in known:
                    broken.append({"source": s.uri, "link": link, "where": where})
    return tuple(broken)


#: Common-runtime implementation leaks that must NOT appear in COMMON resource
#: content (an agent reads the WORKFLOW, never the backend). Normalized
#: (lowercased) substrings; OPERATIONAL Community specs may name their backend.
FORBIDDEN_COMMON_TERMS: tuple[str, ...] = (
    ".lbug",
    "kuzu",
    "ladybug",
    "sentence-transformers",
    "sentence_transformers",
    "sqlite",
    "aiosqlite",
)

#: Local-filesystem path shapes that must NOT leak into a COMMON resource (AC5):
#: Windows drive paths (``C:\...``), the user data dir (``~/.okto-pulse``), and
#: absolute Unix system/data dirs. Case-insensitive.
_LOCAL_PATH_PATTERNS: tuple[str, ...] = (
    r"[A-Za-z]:\\[^\s\"'`]+",  # Windows drive path  C:\Users\...
    r"~[\\/]\.okto[\-_]pulse",  # user data dir  ~/.okto-pulse
    r"(?<![\w.])/(?:home|users|opt|data|root)/[\w./\-]+",  # abs Unix system/data dirs
)
_LOCAL_PATH_RE = re.compile("|".join(_LOCAL_PATH_PATTERNS), re.IGNORECASE)

LOCAL_PATH_TERM = "<local_path>"


def scan_forbidden_terms(
    catalog: McpResourceCatalog,
    *,
    terms: tuple[str, ...] = FORBIDDEN_COMMON_TERMS,
) -> tuple[dict, ...]:
    """SELECTIVE forbidden-term scan: flag a forbidden common-runtime term OR a
    local-filesystem path ONLY in a spec whose ``kind`` is COMMON. OPERATIONAL
    Community specs are EXEMPT (they may name their backend). Matching is
    normalized (lowercased substring + case-insensitive path regex) so
    alias/case variants are caught."""
    normalized = tuple(t.lower() for t in terms)
    findings: list[dict] = []
    for s in catalog.specs():
        if not s.is_common:  # operational specs are exempt
            continue
        raw = (s.description or "") + "\n" + (s.read() or "")
        blob = raw.lower()
        for term in normalized:
            if term in blob:
                findings.append(
                    {"uri": s.uri, "edition": s.edition, "kind": s.kind, "term": term}
                )
        for match in _LOCAL_PATH_RE.findall(raw):
            findings.append(
                {
                    "uri": s.uri,
                    "edition": s.edition,
                    "kind": s.kind,
                    "term": LOCAL_PATH_TERM,
                    "match": match,
                }
            )
    return tuple(findings)


def scan_forbidden_text_surfaces(
    surfaces: dict[str, str],
    *,
    terms: tuple[str, ...] = FORBIDDEN_COMMON_TERMS,
) -> tuple[dict, ...]:
    """Scan already-selected COMMON agent-facing text surfaces.

    Callers must pass the exact surfaces an agent can read, such as static
    instructions/resources or registered MCP tool descriptions. This keeps the
    R11 guard out of comments and internal helper docstrings unless they are
    actually exposed.
    """
    normalized = tuple(t.lower() for t in terms)
    findings: list[dict] = []
    for surface, raw in surfaces.items():
        text = raw or ""
        blob = text.lower()
        for term in normalized:
            if term in blob:
                findings.append({"surface": surface, "term": term})
        for match in _LOCAL_PATH_RE.findall(text):
            findings.append(
                {"surface": surface, "term": LOCAL_PATH_TERM, "match": match}
            )
    return tuple(findings)


__all__ = [
    "RESOURCE_URI_SCHEME",
    "RESOURCE_KIND_COMMON",
    "RESOURCE_KIND_OPERATIONAL",
    "FORBIDDEN_COMMON_TERMS",
    "LOCAL_PATH_TERM",
    "McpResourceSpec",
    "McpResourceCatalog",
    "StaticMcpResourceCatalog",
    "CompositeMcpResourceCatalog",
    "catalog_uri_conflicts",
    "catalog_link_integrity",
    "scan_forbidden_terms",
    "scan_forbidden_text_surfaces",
]

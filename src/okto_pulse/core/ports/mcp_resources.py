"""MCP Resource catalog contracts (spec R11-A, IMP1) — PURE, edition-agnostic
contracts so the effective ``okto-pulse://`` catalog can be composed from built-in
and edition-owned declarations without importing any concrete edition adapter.

Contracts:
  - :class:`McpResourceSpec`        — one resource (uri/description/category/
    edition + a deterministic loader or inline content). ``read()`` returns only
    text; composition-provided path loaders are responsible for confining reads
    to their approved base directory.
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
    operational catalogs (which may name their concrete backend).

Pure: stdlib only; no edition adapter, listener, or framework import.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field, replace as dataclass_replace
from typing import Callable, Iterable, Protocol, runtime_checkable

RESOURCE_URI_SCHEME = "okto-pulse://"
_URI_RE = re.compile(r"okto-pulse://[a-zA-Z0-9_/\-]+")


#: A resource is COMMON (edition-agnostic workflow/reference doc — an agent reads
#: the workflow, never the backend; the forbidden-term scan applies) or
#: OPERATIONAL (a backend-specific operational doc an edition adapter may
#: supply, which MAY name its concrete backend and is therefore EXEMPT).
RESOURCE_KIND_COMMON = "common"
RESOURCE_KIND_OPERATIONAL = "operational"
_RESOURCE_KINDS = frozenset({RESOURCE_KIND_COMMON, RESOURCE_KIND_OPERATIONAL})

RESOURCE_MANIFEST_CANONICALIZATION = "json-utf8-sort-keys-v1"


class McpResourceCatalogError(RuntimeError):
    """Deterministic, machine-readable resource composition failure."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        uri: str | None = None,
        details: dict[str, object] | None = None,
    ) -> None:
        self.code = code
        self.uri = uri
        self.details = dict(details or {})
        suffix = f":{uri}" if uri is not None else ""
        super().__init__(f"{code}{suffix}: {message}")


@dataclass(frozen=True)
class McpResourceReplacementTarget:
    """The edition/source claim an overriding declaration explicitly replaces.

    The URI is implicit: replacement is considered only between declarations for
    the same URI.  ``source_identity`` is optional so an edition can target the
    stable edition boundary without coupling itself to another package's paths.
    When supplied, it must match exactly.
    """

    edition: str
    source_identity: str | None = None

    def __post_init__(self) -> None:
        if not self.edition.strip():
            raise ValueError("replacement target edition must be non-empty")
        if self.source_identity is not None and not self.source_identity.strip():
            raise ValueError("replacement target source_identity must be non-empty")


@dataclass(frozen=True)
class McpResourceSpec:
    """A single MCP resource. Exactly one of ``loader`` or inline ``content``
    provides the body; ``path`` is optional source metadata only.

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
    name: str | None = None
    mime_type: str = "text/plain"
    enabled: bool = True
    source_identity: str | None = None
    replace_target: McpResourceReplacementTarget | None = None
    replaced_source_identity: str | None = None
    #: Compatibility-only declaration marker retained while callers migrate to
    #: ``replace_target``.  It never authorizes a collision by itself.
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
        if not self.edition.strip():
            raise ValueError(f"{self.uri}: edition must be non-empty")
        if not self.mime_type.strip():
            raise ValueError(f"{self.uri}: mime_type must be non-empty")
        if self.replace_target is not None and self.replace_target.edition == self.edition:
            raise ValueError(f"{self.uri}: a declaration cannot replace its own edition")
        if self.name is None:
            object.__setattr__(self, "name", self.uri[len(RESOURCE_URI_SCHEME) :])
        elif not self.name.strip():
            raise ValueError(f"{self.uri}: name must be non-empty")
        if self.source_identity is None:
            source = self.path if self.path is not None else self.uri
            object.__setattr__(self, "source_identity", f"{self.edition}:{source}")
        elif not self.source_identity.strip():
            raise ValueError(f"{self.uri}: source_identity must be non-empty")

    @property
    def is_common(self) -> bool:
        return self.kind == RESOURCE_KIND_COMMON

    def read(self) -> str:
        """Return the resource body supplied by content or the configured loader.

        This port does not resolve filesystem paths.  A composition-provided
        path loader is responsible for enforcing its own base-directory
        confinement before returning text.
        """
        value = self.content if self.content is not None else self.loader()
        if not isinstance(value, str):
            raise TypeError(f"{self.uri}: resource loader must return str")
        return value


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
    precedence: int = 0

    def __post_init__(self) -> None:
        if not self.edition.strip():
            raise ValueError("catalog edition must be non-empty")
        if isinstance(self.precedence, bool) or not isinstance(self.precedence, int):
            raise ValueError("catalog precedence must be an integer")
        seen: set[str] = set()
        for spec in self._specs:
            if spec.edition != self.edition:
                raise McpResourceCatalogError(
                    "edition_mismatch",
                    "declaration edition does not match its catalog edition",
                    uri=spec.uri,
                    details={"catalog": self.edition, "declaration": spec.edition},
                )
            if not spec.enabled:
                continue
            if spec.uri in seen:
                raise McpResourceCatalogError(
                    "same_edition_collision",
                    "one edition declared the same URI more than once",
                    uri=spec.uri,
                    details={"edition": self.edition},
                )
            seen.add(spec.uri)

    def specs(self) -> tuple[McpResourceSpec, ...]:
        return self._specs


class CompositeMcpResourceCatalog:
    """Validated, declaration-order-independent effective resource catalog.

    Same-edition duplicates always fail.  A cross-edition collision has exactly
    one winner only when that declaration names the target edition explicitly and
    its catalog has higher known precedence.  Construction completes validation
    before the catalog can be published by a composition root.
    """

    edition = "composite"

    def __init__(self, catalogs: Iterable[McpResourceCatalog]) -> None:
        self._catalogs: tuple[McpResourceCatalog, ...] = tuple(catalogs)
        self._specs = self._resolve_specs()

    @property
    def is_frozen(self) -> bool:
        return False

    @property
    def catalogs(self) -> tuple[McpResourceCatalog, ...]:
        return self._catalogs

    def with_catalog(
        self, catalog: McpResourceCatalog
    ) -> "CompositeMcpResourceCatalog":
        """Return a NEW composite with ``catalog`` appended (immutable compose)."""
        return CompositeMcpResourceCatalog((*self._catalogs, catalog))

    def specs(self) -> tuple[McpResourceSpec, ...]:
        return self._specs

    def _resolve_specs(self) -> tuple[McpResourceSpec, ...]:
        precedence_by_edition: dict[str, int] = {}
        claims_by_uri: dict[str, list[tuple[McpResourceSpec, int]]] = {}
        for cat in self._catalogs:
            edition = str(cat.edition)
            precedence = getattr(cat, "precedence", 0)
            if isinstance(precedence, bool) or not isinstance(precedence, int):
                raise McpResourceCatalogError(
                    "invalid_precedence",
                    "catalog precedence must be a known integer",
                    details={"edition": edition},
                )
            known = precedence_by_edition.setdefault(edition, precedence)
            if known != precedence:
                raise McpResourceCatalogError(
                    "edition_precedence_conflict",
                    "one edition was registered with multiple precedence values",
                    details={"edition": edition},
                )
            for spec in cat.specs():
                if spec.edition != edition:
                    raise McpResourceCatalogError(
                        "edition_mismatch",
                        "declaration edition does not match its catalog edition",
                        uri=spec.uri,
                        details={"catalog": edition, "declaration": spec.edition},
                    )
                if not spec.enabled:
                    continue
                claims_by_uri.setdefault(spec.uri, []).append((spec, precedence))

        resolved: list[McpResourceSpec] = []
        for uri in sorted(claims_by_uri):
            claims = claims_by_uri[uri]
            editions = [spec.edition for spec, _ in claims]
            if len(editions) != len(set(editions)):
                duplicate = next(
                    edition for edition in editions if editions.count(edition) > 1
                )
                raise McpResourceCatalogError(
                    "same_edition_collision",
                    "one edition declared the same URI more than once",
                    uri=uri,
                    details={"edition": duplicate},
                )
            if len(claims) == 1:
                resolved.append(claims[0][0])
                continue
            if len(claims) != 2:
                raise McpResourceCatalogError(
                    "ambiguous_collision",
                    "a URI has more than two cross-edition claims",
                    uri=uri,
                    details={"editions": sorted(editions)},
                )

            declared = [claim for claim in claims if claim[0].replace_target is not None]
            if len(declared) != 1:
                raise McpResourceCatalogError(
                    "undeclared_replace",
                    "cross-edition collision requires exactly one explicit replacement",
                    uri=uri,
                    details={"editions": sorted(editions)},
                )
            winner, winner_precedence = declared[0]
            loser, loser_precedence = next(claim for claim in claims if claim != declared[0])
            target = winner.replace_target
            assert target is not None
            if target.edition != loser.edition or (
                target.source_identity is not None
                and target.source_identity != loser.source_identity
            ):
                raise McpResourceCatalogError(
                    "invalid_replace_target",
                    "replacement target does not match the competing declaration",
                    uri=uri,
                    details={
                        "winner": winner.edition,
                        "target": target.edition,
                        "actual": loser.edition,
                    },
                )
            if winner_precedence <= loser_precedence:
                raise McpResourceCatalogError(
                    "invalid_replace_precedence",
                    "replacement must have strictly higher known precedence",
                    uri=uri,
                    details={
                        "winner": winner.edition,
                        "winner_precedence": winner_precedence,
                        "target_precedence": loser_precedence,
                    },
                )
            resolved.append(
                dataclass_replace(
                    winner,
                    replaced_source_identity=loser.source_identity,
                )
            )
        return tuple(resolved)

    def conflicts(self) -> tuple[dict, ...]:
        """A constructed composite is fully validated and therefore conflict-free."""

        return ()


@dataclass(frozen=True)
class McpResourceManifestEntry:
    uri: str
    name: str
    mime_type: str
    content_sha256: str
    owning_edition: str
    source_identity: str
    enabled: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "uri": self.uri,
            "name": self.name,
            "mime_type": self.mime_type,
            "content_sha256": self.content_sha256,
            "owning_edition": self.owning_edition,
            "source_identity": self.source_identity,
            "enabled": self.enabled,
        }


def _canonical_resource_manifest_payload(
    entries: tuple[McpResourceManifestEntry, ...],
) -> tuple[dict[str, object], bytes]:
    payload: dict[str, object] = {
        "count": len(entries),
        "resources": [entry.as_dict() for entry in entries],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return payload, encoded


@dataclass(frozen=True)
class McpResourceManifest:
    entries: tuple[McpResourceManifestEntry, ...]
    manifest_hash: str
    projection_identity: str
    canonicalization: str = RESOURCE_MANIFEST_CANONICALIZATION

    @classmethod
    def from_specs(
        cls, specs: tuple[McpResourceSpec, ...]
    ) -> "McpResourceManifest":
        entries = tuple(
            McpResourceManifestEntry(
                uri=spec.uri,
                name=spec.name or spec.uri[len(RESOURCE_URI_SCHEME) :],
                mime_type=spec.mime_type,
                content_sha256=hashlib.sha256(spec.read().encode("utf-8")).hexdigest(),
                owning_edition=spec.edition,
                source_identity=spec.source_identity or f"{spec.edition}:{spec.uri}",
            )
            for spec in sorted(specs, key=lambda item: item.uri)
        )
        _, encoded = _canonical_resource_manifest_payload(entries)
        digest = hashlib.sha256(encoded).hexdigest()
        return cls(
            entries=entries,
            manifest_hash=digest,
            projection_identity=digest,
        )

    @property
    def count(self) -> int:
        return len(self.entries)

    def as_dict(self) -> dict[str, object]:
        payload, _ = _canonical_resource_manifest_payload(self.entries)
        return {
            **payload,
            "manifest_hash": self.manifest_hash,
            "projection_identity": self.projection_identity,
            "canonicalization": self.canonicalization,
        }

    def render(self) -> str:
        return json.dumps(
            self.as_dict(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


@dataclass(frozen=True)
class FrozenMcpResourceCatalog:
    """Enabled, URI-sorted, exact-byte snapshot accepted by transport hosts."""

    _specs: tuple[McpResourceSpec, ...]
    manifest: McpResourceManifest
    source_catalogs: tuple[McpResourceCatalog, ...] = ()
    edition: str = field(default="effective", init=False)
    is_frozen: bool = field(default=True, init=False)

    @property
    def catalogs(self) -> tuple[McpResourceCatalog, ...]:
        return self.source_catalogs

    @property
    def identity(self) -> str:
        return self.manifest.projection_identity

    @property
    def manifest_hash(self) -> str:
        return self.manifest.manifest_hash

    @property
    def count(self) -> int:
        return self.manifest.count

    def specs(self) -> tuple[McpResourceSpec, ...]:
        return self._specs


def freeze_mcp_resource_catalog(
    catalog: McpResourceCatalog,
) -> FrozenMcpResourceCatalog:
    """Materialize one immutable enabled projection without publishing state.

    Every loader is evaluated exactly once into a UTF-8 text snapshot.  Any
    validation/read/hash failure raises before a caller receives a frozen object.
    """

    if isinstance(catalog, FrozenMcpResourceCatalog):
        return catalog
    resolved = (
        catalog
        if isinstance(catalog, CompositeMcpResourceCatalog)
        else CompositeMcpResourceCatalog((catalog,))
    )
    frozen_specs = tuple(
        dataclass_replace(spec, content=spec.read(), loader=None)
        for spec in resolved.specs()
        if spec.enabled
    )
    frozen_specs = tuple(sorted(frozen_specs, key=lambda item: item.uri))
    manifest = McpResourceManifest.from_specs(frozen_specs)
    return FrozenMcpResourceCatalog(
        frozen_specs,
        manifest,
        source_catalogs=resolved.catalogs,
    )


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
#: (lowercased) substrings; OPERATIONAL specs may name their backend.
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
    specs are EXEMPT (they may name their backend). Matching is
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
    "RESOURCE_MANIFEST_CANONICALIZATION",
    "FORBIDDEN_COMMON_TERMS",
    "LOCAL_PATH_TERM",
    "McpResourceCatalogError",
    "McpResourceReplacementTarget",
    "McpResourceSpec",
    "McpResourceCatalog",
    "StaticMcpResourceCatalog",
    "CompositeMcpResourceCatalog",
    "FrozenMcpResourceCatalog",
    "McpResourceManifest",
    "McpResourceManifestEntry",
    "freeze_mcp_resource_catalog",
    "catalog_uri_conflicts",
    "catalog_link_integrity",
    "scan_forbidden_terms",
    "scan_forbidden_text_surfaces",
]

"""Capability descriptor port (spec R11-D, IMP1) — a PURE, edition-agnostic
contract that describes a capability / provider / resource of the ACTIVE runtime
in a SERIALIZABLE, DETERMINISTIC way, so an edition (Community) can publish what
its composition supplies WITHOUT the core knowing any provider-specific detail.

PURITY: importable from ``okto_pulse.core.ports`` with stdlib only — it does NOT
import ``okto_pulse.community``, the concrete MCP server, discovery-intents, or
any provider-specific module. The DTO fields are GENERIC (id/kind/provider/
edition/capability/metadata); the concrete backend name (e.g. an embedded graph
DB) only ever appears inside the bounded ``metadata`` of a Community-built
descriptor, never as a field of the common DTO.

Descriptors are DERIVED from the active ``RuntimeComposition`` by the edition
adapter (R11-D IMP2) — they are NOT hard-coded in the common core.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Protocol, runtime_checkable

#: A descriptor describes a wired PROVIDER, an abstract CAPABILITY, or a catalog
#: RESOURCE classification.
DESCRIPTOR_KIND_PROVIDER = "provider"
DESCRIPTOR_KIND_CAPABILITY = "capability"
DESCRIPTOR_KIND_RESOURCE = "resource"
_DESCRIPTOR_KINDS = frozenset(
    {DESCRIPTOR_KIND_PROVIDER, DESCRIPTOR_KIND_CAPABILITY, DESCRIPTOR_KIND_RESOURCE}
)


@dataclass(frozen=True)
class CapabilityDescriptor:
    """A serializable, DETERMINISTIC descriptor of one runtime capability.

    Generic by construction: ``metadata`` is the ONLY place an edition may record
    provider-specific facts (e.g. a backend name), and it is a bounded string map
    — the common DTO type stays free of provider detail.

    Immutability (R05-A lesson): ``metadata`` is normalized to an IMMUTABLE,
    sorted ``tuple[tuple[str, str], ...]`` (a frozen dataclass only blocks
    reassignment, not mutation of an inner dict). The constructor accepts a
    Mapping OR pairs and copies them, so mutating the input afterwards never
    changes the descriptor. Use :attr:`metadata_dict` for dict-style access."""

    id: str
    kind: str
    provider: str
    edition: str
    capability: str
    #: accepts a Mapping or pairs at construction; STORED as an immutable,
    #: sorted ``tuple[tuple[str, str], ...]`` (deterministic + hashable).
    metadata: Any = ()

    def __post_init__(self) -> None:
        if self.kind not in _DESCRIPTOR_KINDS:
            raise ValueError(f"{self.id}: kind must be one of {sorted(_DESCRIPTOR_KINDS)}")
        if not self.id or not self.provider or not self.capability:
            raise ValueError(f"{self.id!r}: id/provider/capability must be non-empty")
        raw = self.metadata
        pairs = raw.items() if isinstance(raw, Mapping) else tuple(raw)
        normalized = []
        for k, v in pairs:
            if not isinstance(k, str) or not isinstance(v, str):
                raise ValueError(f"{self.id}: metadata must be a string->string map")
            normalized.append((k, v))
        # immutable, sorted, copied (mutating the input cannot reach us).
        object.__setattr__(self, "metadata", tuple(sorted(normalized)))

    @property
    def metadata_dict(self) -> dict[str, str]:
        """A fresh dict view of the immutable metadata (for `[key]`/`.get`)."""
        return dict(self.metadata)

    def to_dict(self) -> dict[str, Any]:
        """Deterministic, JSON-serializable form (metadata keys sorted)."""
        return {
            "id": self.id,
            "kind": self.kind,
            "provider": self.provider,
            "edition": self.edition,
            "capability": self.capability,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "CapabilityDescriptor":
        return cls(
            id=str(d["id"]),
            kind=str(d["kind"]),
            provider=str(d["provider"]),
            edition=str(d["edition"]),
            capability=str(d["capability"]),
            metadata=dict(d.get("metadata") or {}),
        )


@runtime_checkable
class CapabilityDescriptorSource(Protocol):
    """A source of :class:`CapabilityDescriptor` for an edition/runtime."""

    def descriptors(self) -> tuple[CapabilityDescriptor, ...]:
        """Return the descriptors in a STABLE, deterministic order."""
        ...


def descriptors_to_dicts(
    descriptors: tuple[CapabilityDescriptor, ...]
) -> list[dict[str, Any]]:
    """Serialize a descriptor set deterministically (sorted by id)."""
    return [d.to_dict() for d in sorted(descriptors, key=lambda d: d.id)]


def classify_resources(
    resource_specs: tuple[Any, ...],
    descriptors: tuple[CapabilityDescriptor, ...],
) -> tuple[dict[str, Any], ...]:
    """Classify each resource spec (anything with ``uri`` / ``capability`` /
    ``provider`` / ``edition`` — e.g. an :class:`McpResourceSpec`) by the matching
    descriptor, VIA the descriptors (NOT a hard-coded table). A spec matches a
    descriptor when their ``capability`` agrees (and ``provider`` agrees when the
    spec names one). Returns one record per spec."""
    by_capability: dict[str, list[CapabilityDescriptor]] = {}
    for d in descriptors:
        by_capability.setdefault(d.capability, []).append(d)

    out: list[dict[str, Any]] = []
    for spec in resource_specs:
        cap = getattr(spec, "capability", None)
        prov = getattr(spec, "provider", None)
        match: CapabilityDescriptor | None = None
        for d in by_capability.get(cap or "", ()):
            if prov is None or d.provider == prov:
                match = d
                break
        out.append(
            {
                "uri": getattr(spec, "uri", None),
                "capability": cap,
                "provider": prov,
                "edition": getattr(spec, "edition", None),
                "descriptor_id": match.id if match else None,
                "classified": match is not None,
            }
        )
    return tuple(out)


#: Providers/editions that are OUT OF SCOPE for this backend-only axis. R11-D must
#: NOT introduce a SaaS provider; the scope gate flags any descriptor that does.
SAAS_SCOPE_MARKERS: tuple[str, ...] = ("saas", "multi_tenant", "multitenant", "tenant")


def capability_scope_violations(
    descriptors: tuple[CapabilityDescriptor, ...],
) -> tuple[dict[str, Any], ...]:
    """(R11-D IMP4) Scope gate: a descriptor whose provider/edition/capability or
    metadata names a SaaS/multi-tenant concern is a violation (this axis is
    backend-only, no SaaS provider). Normalized (lowercased) substring match."""
    markers = tuple(m.lower() for m in SAAS_SCOPE_MARKERS)
    violations: list[dict[str, Any]] = []
    for d in descriptors:
        blob = " ".join(
            [d.id, d.kind, d.provider, d.edition, d.capability,
             *(f"{k} {v}" for k, v in d.metadata)]
        ).lower()
        for m in markers:
            if m in blob:
                violations.append({"id": d.id, "marker": m})
                break
    return tuple(violations)


__all__ = [
    "DESCRIPTOR_KIND_PROVIDER",
    "DESCRIPTOR_KIND_CAPABILITY",
    "DESCRIPTOR_KIND_RESOURCE",
    "SAAS_SCOPE_MARKERS",
    "CapabilityDescriptor",
    "CapabilityDescriptorSource",
    "descriptors_to_dicts",
    "classify_resources",
    "capability_scope_violations",
]

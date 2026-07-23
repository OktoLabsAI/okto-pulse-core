"""Canonical Resource Gate lineage resolver.

The resolver owns the in-process contract for resource provenance,
deduplication, effective Resource Gate state, and coverage-obligation
projection. Storage access is intentionally inverted through a provider so
ResourceGateService can reuse its existing parent traversal and collection
primitives instead of duplicating them here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter
from typing import Any, Iterable, Literal, Mapping, Protocol

from okto_pulse.core.observability.sample_buffer import runtime_sample_buffer

EntityType = Literal["ideation", "refinement", "spec", "card"]
ResourceType = Literal["architecture", "mockup", "knowledge_base"]
ResourceState = Literal["provided", "not_applicable", "missing"]
CoverageState = Literal["not_required", "covered", "uncovered", "not_applicable"]
AttachmentKind = Literal["direct", "inherited_reference", "not_applicable"]


@dataclass(frozen=True, slots=True)
class ResourceRevisionStamp:
    """Storage-neutral revision evidence for one physical resource snapshot."""

    root_id: str
    immediate_parent_id: str | None = None
    source_revision: str | None = None
    source_content_sha256: str | None = None

    def to_dict(self) -> dict[str, str | None]:
        return {
            "root_id": self.root_id,
            "immediate_parent_id": self.immediate_parent_id,
            "source_revision": self.source_revision,
            "source_content_sha256": self.source_content_sha256,
        }


ENTITY_TYPES: tuple[str, ...] = ("ideation", "refinement", "spec", "card")
RESOURCE_TYPES: tuple[str, ...] = ("architecture", "mockup", "knowledge_base")
ORIGIN_FIELD_ORDER: tuple[str, ...] = (
    "root_source_design_id",
    "root_source_kb_id",
    "root_source_mockup_id",
    "source_ref",
    "origin_ref",
    "source",
    "source_design_id",
    "source_kb_id",
    "source_mockup_id",
    "origin_id",
    "id",
)

METRIC_RESOLVE_TOTAL = "resource_lineage_resolve_total"
METRIC_RESOLUTION_FAILED_TOTAL = "resource_lineage_resolution_failed_total"
METRIC_DEDUP_GROUPS_TOTAL = "resource_lineage_dedup_groups_total"
METRIC_COVERAGE_UNCOVERED_TOTAL = "resource_gate_coverage_uncovered_total"
METRIC_RESOLVE_DURATION_MS = "resource_lineage_resolve_duration_ms"

_SAFE_METRIC_LABELS: frozenset[str] = frozenset(
    {
        "owner_type",
        "outcome",
        "reason",
        "resource_type",
        "coverage_state",
    }
)


@dataclass(frozen=True)
class ResourceLineageMetricSample:
    metric_name: str
    labels: dict[str, str]
    value: int | float = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "labels": dict(self.labels),
            "value": self.value,
        }


_RESOURCE_LINEAGE_METRIC_SAMPLES = runtime_sample_buffer(
    "services.resource_lineage"
)


def reset_resource_lineage_observability_for_tests() -> None:
    _RESOURCE_LINEAGE_METRIC_SAMPLES.clear()


def get_resource_lineage_metric_samples() -> list[dict[str, Any]]:
    return [sample.to_dict() for sample in _RESOURCE_LINEAGE_METRIC_SAMPLES.snapshot()]


def observe_resource_lineage_coverage_uncovered(
    *,
    resource_type: str,
    reason: str,
) -> None:
    _record_resource_lineage_metric(
        METRIC_COVERAGE_UNCOVERED_TOTAL,
        labels={
            "owner_type": "spec",
            "outcome": "uncovered",
            "resource_type": resource_type,
            "reason": reason,
        },
    )


def _observe_resource_lineage_resolved(
    *,
    owner_type: str,
    counts: Mapping[str, Any],
    duration_ms: float,
) -> None:
    _record_resource_lineage_metric(
        METRIC_RESOLVE_TOTAL,
        labels={"owner_type": owner_type, "outcome": "success"},
    )
    _record_resource_lineage_metric(
        METRIC_RESOLVE_DURATION_MS,
        labels={"owner_type": owner_type, "outcome": "success"},
        value=duration_ms,
    )
    dedup_groups = sum(
        1
        for item in counts.get("by_unique_resource", [])
        if int(item.get("attachment_count") or 0) > 1
    )
    if dedup_groups:
        _record_resource_lineage_metric(
            METRIC_DEDUP_GROUPS_TOTAL,
            labels={"owner_type": owner_type, "outcome": "deduped"},
            value=dedup_groups,
        )


def _observe_resource_lineage_failed(
    *,
    owner_type: str,
    reason: str,
    duration_ms: float,
) -> None:
    _record_resource_lineage_metric(
        METRIC_RESOLUTION_FAILED_TOTAL,
        labels={"owner_type": owner_type, "outcome": "failure", "reason": reason},
    )
    _record_resource_lineage_metric(
        METRIC_RESOLVE_DURATION_MS,
        labels={"owner_type": owner_type, "outcome": "failure", "reason": reason},
        value=duration_ms,
    )


def _record_resource_lineage_metric(
    metric_name: str,
    *,
    labels: Mapping[str, Any],
    value: int | float = 1,
) -> None:
    safe_labels = {
        key: str(value)
        for key, value in labels.items()
        if key in _SAFE_METRIC_LABELS and value is not None
    }
    _RESOURCE_LINEAGE_METRIC_SAMPLES.append(
        ResourceLineageMetricSample(
            metric_name=metric_name,
            labels=safe_labels,
            value=value,
        )
    )


class ResourceLineageError(ValueError):
    """Base resolver exception with a stable machine-readable code."""

    def __init__(self, code: str, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = details or {}


class UnsupportedLineageEntityType(ResourceLineageError):
    """Raised when an owner type cannot participate in Resource Gate lineage."""

    def __init__(self, entity_type: str):
        super().__init__(
            "unsupported_entity_type",
            f"Entity type '{entity_type}' cannot own Resource Gate resources.",
            details={"entity_type": entity_type, "supported": list(ENTITY_TYPES)},
        )


class LineageCycleError(ResourceLineageError):
    """Raised when a provider reports a repeated owner in one parent chain."""

    def __init__(self, owner_ref: str):
        super().__init__(
            "lineage_cycle",
            f"Resource lineage cycle detected at '{owner_ref}'.",
            details={"owner_ref": owner_ref},
        )


class AmbiguousResourceOrigin(ResourceLineageError):
    """Raised when conflicting origin evidence prevents deterministic grouping."""

    def __init__(
        self,
        *,
        resource_type: str,
        resource_id: str | None,
        evidence: Mapping[str, Any],
    ):
        super().__init__(
            "ambiguous_origin",
            "A resource origin could not be resolved deterministically.",
            details={
                "resource_type": resource_type,
                "resource_id": resource_id,
                "evidence": {k: v for k, v in evidence.items() if v},
            },
        )


@dataclass(frozen=True)
class LineageEntityRef:
    entity_type: str
    entity_id: str
    title: str | None = None
    entity: Any | None = None

    @property
    def ref(self) -> str:
        return f"{self.entity_type}:{self.entity_id}"


@dataclass(frozen=True)
class ResourceAttachment:
    resource_type: str
    resource_id: str | None
    title: str | None
    unique_resource_id: str
    attachment_kind: AttachmentKind
    source_entity_type: str | None
    source_entity_id: str | None
    source_entity_title: str | None
    coverage_state: CoverageState
    origin_evidence: dict[str, Any] = field(default_factory=dict)
    effective: bool = True
    inherited: bool = False
    raw: dict[str, Any] = field(default_factory=dict)
    revision_stamp: ResourceRevisionStamp | None = None

    def to_dict(self) -> dict[str, Any]:
        stamp = self.revision_stamp.to_dict() if self.revision_stamp else None
        return {
            "contract_version": 2,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "id": self.resource_id,
            "title": self.title,
            "unique_resource_id": self.unique_resource_id,
            "attachment_kind": self.attachment_kind,
            "source_entity_type": self.source_entity_type,
            "source_entity_id": self.source_entity_id,
            "source_entity_title": self.source_entity_title,
            "coverage_state": self.coverage_state,
            "origin_evidence": dict(self.origin_evidence),
            "effective": self.effective,
            "inherited": self.inherited,
            "raw": dict(self.raw),
            "root_id": stamp["root_id"] if stamp else None,
            "immediate_parent_id": stamp["immediate_parent_id"] if stamp else None,
            "source_revision": stamp["source_revision"] if stamp else None,
            "source_content_sha256": (
                stamp["source_content_sha256"] if stamp else None
            ),
            "revision_stamp": stamp,
        }


@dataclass(frozen=True)
class UniqueResource:
    resource_type: str
    unique_resource_id: str
    representative_resource_id: str | None
    title: str | None
    origin_evidence: dict[str, Any]
    attachment_count: int
    attachment_kinds: tuple[str, ...]
    revision_stamps: tuple[ResourceRevisionStamp, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        stamps = [stamp.to_dict() for stamp in self.revision_stamps]
        return {
            "contract_version": 2,
            "resource_type": self.resource_type,
            "unique_resource_id": self.unique_resource_id,
            "representative_resource_id": self.representative_resource_id,
            "title": self.title,
            "origin_evidence": dict(self.origin_evidence),
            "attachment_count": self.attachment_count,
            "attachment_kinds": list(self.attachment_kinds),
            "root_id": _common_stamp_field(stamps, "root_id"),
            "immediate_parent_id": _common_stamp_field(
                stamps, "immediate_parent_id"
            ),
            "source_revision": _common_stamp_field(stamps, "source_revision"),
            "source_content_sha256": _common_stamp_field(
                stamps, "source_content_sha256"
            ),
            "revision_stamps": stamps,
        }


@dataclass(frozen=True)
class ResourceStateEnvelope:
    resource_type: str
    state: ResourceState
    direct_count: int
    inherited_count: int
    total_count: int
    direct_refs: tuple[dict[str, Any], ...]
    inherited_refs: tuple[dict[str, Any], ...]
    na_mark: dict[str, Any] | None
    blocking: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.resource_type,
            "state": self.state,
            "direct_count": self.direct_count,
            "inherited_count": self.inherited_count,
            "total_count": self.total_count,
            "direct_refs": [dict(item) for item in self.direct_refs],
            "inherited_refs": [dict(item) for item in self.inherited_refs],
            "na_mark": dict(self.na_mark) if self.na_mark else None,
            "blocking": self.blocking,
        }


@dataclass(frozen=True)
class CoverageObligation:
    resource_type: str
    resource_id: str | None
    unique_resource_id: str
    title: str | None
    source_entity_type: str | None
    source_entity_id: str | None
    source_entity_title: str | None
    coverage_state: CoverageState = "uncovered"
    reason: str | None = None
    remediation: str | None = None
    origin_evidence: dict[str, Any] = field(default_factory=dict)
    revision_stamp: ResourceRevisionStamp | None = None

    def to_dict(self) -> dict[str, Any]:
        stamp = self.revision_stamp.to_dict() if self.revision_stamp else None
        return {
            "contract_version": 2,
            "resource_type": self.resource_type,
            "resource_id": self.resource_id,
            "id": self.resource_id,
            "unique_resource_id": self.unique_resource_id,
            "title": self.title,
            "source_entity_type": self.source_entity_type,
            "source_entity_id": self.source_entity_id,
            "source_entity_title": self.source_entity_title,
            "coverage_state": self.coverage_state,
            "reason": self.reason,
            "remediation": self.remediation,
            "origin_evidence": dict(self.origin_evidence),
            "root_id": stamp["root_id"] if stamp else None,
            "immediate_parent_id": stamp["immediate_parent_id"] if stamp else None,
            "source_revision": stamp["source_revision"] if stamp else None,
            "source_content_sha256": (
                stamp["source_content_sha256"] if stamp else None
            ),
            "revision_stamp": stamp,
        }


@dataclass(frozen=True)
class ResolvedResourceLineage:
    owner: LineageEntityRef
    unique_resources: tuple[UniqueResource, ...]
    attachments: tuple[ResourceAttachment, ...]
    counts: dict[str, Any]
    resource_states: tuple[ResourceStateEnvelope, ...]
    coverage_obligations: tuple[CoverageObligation, ...] = ()
    uncovered_required_resources: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": 2,
            "owner": {
                "entity_type": self.owner.entity_type,
                "entity_id": self.owner.entity_id,
                "title": self.owner.title,
            },
            "unique_resources": [item.to_dict() for item in self.unique_resources],
            "attachments": [item.to_dict() for item in self.attachments],
            "counts": dict(self.counts),
            "resource_states": [item.to_dict() for item in self.resource_states],
            "coverage_obligations": [
                item.to_dict() for item in self.coverage_obligations
            ],
            "uncovered_required_resources": [
                dict(item) for item in self.uncovered_required_resources
            ],
        }


class ResolvedResourceLineageProjection:
    """Backend projection contract for downstream Resource Gate consumers."""

    @staticmethod
    def project(
        lineage: ResolvedResourceLineage,
        *,
        profile: Literal["summary", "full"] = "full",
    ) -> dict[str, Any]:
        del profile  # Reserved for downstream compaction profiles.
        attachments = [
            ResolvedResourceLineageProjection._attachment_projection(item)
            for item in lineage.attachments
        ]
        return {
            "contract_version": 2,
            "owner": {
                "entity_type": lineage.owner.entity_type,
                "entity_id": lineage.owner.entity_id,
                "title": lineage.owner.title,
            },
            "unique_resources": [
                item.to_dict() for item in lineage.unique_resources
            ],
            "attachments": attachments,
            "counts": dict(lineage.counts),
            "provenance_labels": [
                ResolvedResourceLineageProjection._provenance_label(item)
                for item in lineage.attachments
            ],
            "coverage_obligations": [
                item.to_dict() for item in lineage.coverage_obligations
            ],
            "uncovered_required_resources": [
                dict(item) for item in lineage.uncovered_required_resources
            ],
        }

    @staticmethod
    def _attachment_projection(attachment: ResourceAttachment) -> dict[str, Any]:
        payload = attachment.to_dict()
        payload.pop("raw", None)
        return payload

    @staticmethod
    def _provenance_label(attachment: ResourceAttachment) -> dict[str, Any]:
        return {
            "resource_type": attachment.resource_type,
            "attachment_kind": attachment.attachment_kind,
            "source_entity_type": attachment.source_entity_type,
            "coverage_state": attachment.coverage_state,
            "effective": attachment.effective,
            "inherited": attachment.inherited,
        }


class ResourceLineageProvider(Protocol):
    """Adapter boundary implemented by ResourceGateService in the wiring cards."""

    async def load_entity_ref(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> Any:
        ...

    async def load_parent_refs(self, board_id: str, root: Any) -> list[Any]:
        ...

    async def collect_refs(self, ref: Any) -> dict[str, list[dict[str, Any]]]:
        ...

    async def load_active_marks(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
    ) -> dict[str, Any]:
        ...

    def serialize_na_mark(
        self,
        mark: Any | None,
        *,
        effective: bool,
        source: Any | None = None,
    ) -> dict[str, Any] | None:
        ...


class ResolvedResourceLineageService:
    """Resolve Resource Gate resource lineage through an inverted provider."""

    def __init__(self, provider: ResourceLineageProvider):
        self._provider = provider

    async def resolve(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        include_coverage: bool = True,
        projection_profile: Literal["legacy", "summary", "full"] = "full",
    ) -> ResolvedResourceLineage:
        started_at = perf_counter()
        try:
            resolved = await self._resolve_uninstrumented(
                board_id,
                entity_type,
                entity_id,
                include_coverage=include_coverage,
                projection_profile=projection_profile,
            )
        except ResourceLineageError as exc:
            _observe_resource_lineage_failed(
                owner_type=str(entity_type),
                reason=exc.code,
                duration_ms=(perf_counter() - started_at) * 1000,
            )
            raise
        except Exception:
            _observe_resource_lineage_failed(
                owner_type=str(entity_type),
                reason="unexpected",
                duration_ms=(perf_counter() - started_at) * 1000,
            )
            raise
        _observe_resource_lineage_resolved(
            owner_type=str(entity_type),
            counts=resolved.counts,
            duration_ms=(perf_counter() - started_at) * 1000,
        )
        return resolved

    async def _resolve_uninstrumented(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        include_coverage: bool,
        projection_profile: Literal["legacy", "summary", "full"],
    ) -> ResolvedResourceLineage:
        del projection_profile  # Projection formatting is handled by RG-01.4.
        self._validate_entity_type(entity_type)
        root = await self._provider.load_entity_ref(board_id, str(entity_type), entity_id)
        owner = self._coerce_entity_ref(root)
        parents = await self._load_checked_parents(board_id, root, owner)
        direct_refs = await self._provider.collect_refs(root)

        inherited_refs: dict[str, list[dict[str, Any]]] = {
            resource_type: [] for resource_type in RESOURCE_TYPES
        }
        inherited_na_marks: dict[str, Any] = {}
        inherited_na_sources: dict[str, Any] = {}
        for parent in parents:
            parent_ref = self._coerce_entity_ref(parent)
            parent_refs = await self._provider.collect_refs(parent)
            for resource_type, refs in parent_refs.items():
                inherited_refs.setdefault(resource_type, []).extend(refs)
            parent_marks = await self._provider.load_active_marks(
                board_id,
                parent_ref.entity_type,
                parent_ref.entity_id,
            )
            for resource_type, mark in parent_marks.items():
                if resource_type not in inherited_na_marks:
                    inherited_na_marks[resource_type] = mark
                    inherited_na_sources[resource_type] = parent

        active_marks = await self._provider.load_active_marks(
            board_id,
            str(entity_type),
            entity_id,
        )

        attachments: list[ResourceAttachment] = []
        resource_states: list[ResourceStateEnvelope] = []
        coverage_obligations: list[CoverageObligation] = []

        for resource_type in RESOURCE_TYPES:
            direct = list(direct_refs.get(resource_type) or [])
            inherited = list(inherited_refs.get(resource_type) or [])
            own_mark = active_marks.get(resource_type)
            na_mark = own_mark or inherited_na_marks.get(resource_type)
            na_source = (
                None if own_mark is not None else inherited_na_sources.get(resource_type)
            )
            state = self._resolve_state(direct, inherited, na_mark)
            direct_attachments = [
                self._attachment_from_ref(
                    resource_type=resource_type,
                    ref=item,
                    attachment_kind="direct",
                    coverage_state="not_required",
                )
                for item in direct
            ]
            inherited_attachments = [
                self._attachment_from_ref(
                    resource_type=resource_type,
                    ref=item,
                    attachment_kind="inherited_reference",
                    coverage_state="not_required",
                    inherited=True,
                )
                for item in inherited
            ]
            attachments.extend(direct_attachments)
            attachments.extend(inherited_attachments)

            na_payload = self._provider.serialize_na_mark(
                na_mark,
                effective=state == "not_applicable",
                source=na_source,
            )
            if na_payload is not None:
                attachments.append(
                    self._attachment_from_na_mark(
                        resource_type=resource_type,
                        na_payload=na_payload,
                        effective=state == "not_applicable",
                    )
                )

            resource_states.append(
                ResourceStateEnvelope(
                    resource_type=resource_type,
                    state=state,
                    direct_count=len(direct),
                    inherited_count=len(inherited),
                    total_count=len(direct) + len(inherited),
                    direct_refs=tuple(direct),
                    inherited_refs=tuple(inherited),
                    na_mark=na_payload,
                    blocking=state == "missing",
                )
            )

            if include_coverage and str(entity_type) == "spec" and state == "provided":
                coverage_source = direct_attachments or inherited_attachments
                coverage_obligations.extend(
                    self._coverage_obligation_from_attachment(item)
                    for item in coverage_source
                )

        unique_resources = self._unique_resources(attachments)
        counts = self._counts(
            attachments=attachments,
            unique_resources=unique_resources,
            resource_states=resource_states,
            coverage_obligations=coverage_obligations,
        )
        return ResolvedResourceLineage(
            owner=owner,
            unique_resources=tuple(unique_resources),
            attachments=tuple(attachments),
            counts=counts,
            resource_states=tuple(resource_states),
            coverage_obligations=tuple(coverage_obligations),
        )

    async def _load_checked_parents(
        self,
        board_id: str,
        root: Any,
        owner: LineageEntityRef,
    ) -> list[Any]:
        parents = await self._provider.load_parent_refs(board_id, root)
        seen = {owner.ref}
        for parent in parents:
            parent_ref = self._coerce_entity_ref(parent)
            if parent_ref.ref in seen:
                raise LineageCycleError(parent_ref.ref)
            seen.add(parent_ref.ref)
        return parents

    @staticmethod
    def _coerce_entity_ref(ref: Any) -> LineageEntityRef:
        return LineageEntityRef(
            entity_type=str(getattr(ref, "entity_type")),
            entity_id=str(getattr(ref, "entity_id")),
            title=getattr(ref, "title", None),
            entity=getattr(ref, "entity", None),
        )

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type not in ENTITY_TYPES:
            raise UnsupportedLineageEntityType(entity_type)

    @staticmethod
    def _resolve_state(
        direct: Iterable[dict[str, Any]],
        inherited: Iterable[dict[str, Any]],
        na_mark: Any | None,
    ) -> ResourceState:
        if list(direct) or list(inherited):
            return "provided"
        if na_mark is not None:
            return "not_applicable"
        return "missing"

    def _attachment_from_ref(
        self,
        *,
        resource_type: str,
        ref: Mapping[str, Any],
        attachment_kind: AttachmentKind,
        coverage_state: CoverageState,
        inherited: bool = False,
    ) -> ResourceAttachment:
        origin_evidence = self._origin_evidence(ref)
        unique_resource_id = self._unique_resource_id(
            resource_type=resource_type,
            resource_id=_optional_str(ref.get("id")),
            origin_evidence=origin_evidence,
            fallback_ref=ref,
        )
        return ResourceAttachment(
            resource_type=resource_type,
            resource_id=_optional_str(ref.get("id")),
            title=_optional_str(ref.get("title")),
            unique_resource_id=unique_resource_id,
            attachment_kind=attachment_kind,
            source_entity_type=_optional_str(ref.get("source_entity_type")),
            source_entity_id=_optional_str(ref.get("source_entity_id")),
            source_entity_title=_optional_str(ref.get("source_entity_title")),
            coverage_state=coverage_state,
            origin_evidence=origin_evidence,
            effective=True,
            inherited=inherited,
            raw=dict(ref),
            revision_stamp=self._revision_stamp_from_ref(
                resource_type=resource_type,
                ref=ref,
                unique_resource_id=unique_resource_id,
            ),
        )

    def _attachment_from_na_mark(
        self,
        *,
        resource_type: str,
        na_payload: Mapping[str, Any],
        effective: bool,
    ) -> ResourceAttachment:
        resource_id = _optional_str(na_payload.get("id"))
        origin_evidence = self._origin_evidence(na_payload)
        unique_resource_id = self._unique_resource_id(
            resource_type=resource_type,
            resource_id=resource_id,
            origin_evidence=origin_evidence,
            fallback_ref=na_payload,
            suffix="not_applicable",
        )
        return ResourceAttachment(
            resource_type=resource_type,
            resource_id=resource_id,
            title=None,
            unique_resource_id=unique_resource_id,
            attachment_kind="not_applicable",
            source_entity_type=_optional_str(na_payload.get("source_entity_type")),
            source_entity_id=_optional_str(na_payload.get("source_entity_id")),
            source_entity_title=None,
            coverage_state="not_applicable",
            origin_evidence=origin_evidence,
            effective=effective,
            inherited=bool(na_payload.get("inherited")),
            raw=dict(na_payload),
        )

    @staticmethod
    def _coverage_obligation_from_attachment(
        attachment: ResourceAttachment,
    ) -> CoverageObligation:
        return CoverageObligation(
            resource_type=attachment.resource_type,
            resource_id=attachment.resource_id,
            unique_resource_id=attachment.unique_resource_id,
            title=attachment.title,
            source_entity_type=attachment.source_entity_type,
            source_entity_id=attachment.source_entity_id,
            source_entity_title=attachment.source_entity_title,
            origin_evidence=dict(attachment.origin_evidence),
            revision_stamp=attachment.revision_stamp,
        )

    @staticmethod
    def _revision_stamp_from_ref(
        *,
        resource_type: str,
        ref: Mapping[str, Any],
        unique_resource_id: str,
    ) -> ResourceRevisionStamp | None:
        # The revision stamp and dedup projection must name the same logical
        # root, including legacy/source_ref fallbacks where no typed root id is
        # available.  Derive it from the already-validated unique identity
        # instead of independently selecting a physical attachment id.
        prefix = f"{resource_type}:"
        root_id = (
            _optional_str(unique_resource_id[len(prefix) :])
            if unique_resource_id.startswith(prefix)
            else None
        )
        if root_id is None:
            return None

        immediate_fields: tuple[str, ...]
        if resource_type == "knowledge_base":
            immediate_fields = ("immediate_parent_kb_id", "source_kb_id")
        elif resource_type == "mockup":
            immediate_fields = ("immediate_parent_mockup_id", "source_mockup_id")
        else:
            immediate_fields = ("immediate_parent_design_id",)

        return ResourceRevisionStamp(
            root_id=root_id,
            immediate_parent_id=_first_present(ref, immediate_fields),
            source_revision=_first_present(
                ref, ("source_revision", "source_version")
            ),
            source_content_sha256=_first_present(
                ref, ("source_content_sha256", "content_hash")
            ),
        )

    @staticmethod
    def _origin_evidence(ref: Mapping[str, Any]) -> dict[str, Any]:
        evidence: dict[str, Any] = {}
        for key in ORIGIN_FIELD_ORDER:
            value = ref.get(key)
            if value:
                evidence[key] = str(value)
        return evidence

    @staticmethod
    def _unique_resource_id(
        *,
        resource_type: str,
        resource_id: str | None,
        origin_evidence: Mapping[str, Any],
        fallback_ref: Mapping[str, Any],
        suffix: str | None = None,
    ) -> str:
        source_ref = _first_present(
            origin_evidence,
            ("source_ref", "origin_ref", "source"),
        )
        canonical_origin_ids = _canonical_origin_ids(
            origin_evidence, resource_type=resource_type
        )
        if len(canonical_origin_ids) > 1:
            raise AmbiguousResourceOrigin(
                resource_type=resource_type,
                resource_id=resource_id,
                evidence=origin_evidence,
            )
        source_id = canonical_origin_ids[0] if canonical_origin_ids else None
        identity = source_id or source_ref or resource_id
        if not identity:
            source_entity_type = fallback_ref.get("source_entity_type") or "unknown"
            source_entity_id = fallback_ref.get("source_entity_id") or "unknown"
            title = fallback_ref.get("title") or "untitled"
            identity = f"{source_entity_type}:{source_entity_id}:{title}"
        prefix = f"{resource_type}:"
        if suffix:
            prefix = f"{prefix}{suffix}:"
        return f"{prefix}{identity}"

    @staticmethod
    def _unique_resources(
        attachments: Iterable[ResourceAttachment],
    ) -> list[UniqueResource]:
        grouped: dict[str, list[ResourceAttachment]] = {}
        for attachment in attachments:
            if attachment.attachment_kind == "not_applicable":
                continue
            grouped.setdefault(attachment.unique_resource_id, []).append(attachment)

        unique: list[UniqueResource] = []
        for unique_id, items in grouped.items():
            representative = items[0]
            kinds = tuple(sorted({item.attachment_kind for item in items}))
            evidence: dict[str, Any] = {}
            stamps: list[ResourceRevisionStamp] = []
            for item in items:
                evidence.update(item.origin_evidence)
                if item.revision_stamp is not None and item.revision_stamp not in stamps:
                    stamps.append(item.revision_stamp)
            stamps.sort(key=_revision_stamp_sort_key)
            unique.append(
                UniqueResource(
                    resource_type=representative.resource_type,
                    unique_resource_id=unique_id,
                    representative_resource_id=representative.resource_id,
                    title=representative.title,
                    origin_evidence=evidence,
                    attachment_count=len(items),
                    attachment_kinds=kinds,
                    revision_stamps=tuple(stamps),
                )
            )
        return unique

    @staticmethod
    def _counts(
        *,
        attachments: list[ResourceAttachment],
        unique_resources: list[UniqueResource],
        resource_states: list[ResourceStateEnvelope],
        coverage_obligations: list[CoverageObligation],
    ) -> dict[str, Any]:
        return {
            "unique_resources_count": len(unique_resources),
            "attachment_count": len(attachments),
            # R6-IMP4 (FR4/AC4): unambiguous raw-vs-effective labels so a raw total
            # is never read as effective coverage. `unique_effective_count` is the
            # deduped effective resource count (coverage basis); `raw_attachment_count`
            # is the raw attachment total (NOT coverage). Additive aliases — the
            # legacy keys above are preserved for back-compat.
            "unique_effective_count": len(unique_resources),
            "raw_attachment_count": len(attachments),
            "coverage_basis": "unique_effective",
            "direct_resources_count": sum(
                1 for item in attachments if item.attachment_kind == "direct"
            ),
            "inherited_references_count": sum(
                1
                for item in attachments
                if item.attachment_kind == "inherited_reference"
            ),
            "not_applicable_count": sum(
                1 for item in attachments if item.attachment_kind == "not_applicable"
            ),
            "covered_required_resources_count": sum(
                1 for item in coverage_obligations if item.coverage_state == "covered"
            ),
            "uncovered_required_resources_count": sum(
                1 for item in coverage_obligations if item.coverage_state == "uncovered"
            ),
            "dedup_groups_count": sum(
                1 for item in unique_resources if item.attachment_count > 1
            ),
            "by_unique_resource": [
                {
                    "resource_type": item.resource_type,
                    "unique_resource_id": item.unique_resource_id,
                    "attachment_count": item.attachment_count,
                }
                for item in unique_resources
            ],
            "by_resource_type": {
                item.resource_type: {
                    "state": item.state,
                    "direct_count": item.direct_count,
                    "inherited_count": item.inherited_count,
                    "total_count": item.total_count,
                }
                for item in resource_states
            },
        }


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _common_stamp_field(
    stamps: Iterable[Mapping[str, str | None]], field_name: str
) -> str | None:
    """Return one common value without selecting between divergent stamps."""

    values = {stamp.get(field_name) for stamp in stamps}
    return next(iter(values)) if len(values) == 1 else None


def _revision_stamp_sort_key(stamp: ResourceRevisionStamp) -> tuple[str, ...]:
    return (
        stamp.root_id,
        stamp.immediate_parent_id or "",
        stamp.source_revision or "",
        stamp.source_content_sha256 or "",
    )


def _first_present(values: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = values.get(key)
        if value:
            return str(value)
    return None


def _canonical_origin_ids(
    values: Mapping[str, Any], *, resource_type: str | None = None
) -> list[str]:
    root_ids: list[str] = []
    for key in (
        "root_source_design_id",
        "root_source_kb_id",
        "root_source_mockup_id",
    ):
        value = values.get(key)
        if not value:
            continue
        text = str(value)
        if text not in root_ids:
            root_ids.append(text)
    if root_ids:
        return root_ids

    # Mockup snapshots intentionally carry both the canonical ``origin_id`` and
    # their immediate ``source_mockup_id``.  Across multiple hops those values
    # differ by design; the former is the logical Resource Gate identity.
    if resource_type == "mockup":
        canonical = values.get("origin_id") or values.get("source_mockup_id")
        return [str(canonical)] if canonical else []

    ids: list[str] = []
    for key in ("source_design_id", "source_kb_id", "source_mockup_id", "origin_id"):
        value = values.get(key)
        if not value:
            continue
        text = str(value)
        if text not in ids:
            ids.append(text)
    return ids


def _source_ref_contains(source_ref: str, source_id: str) -> bool:
    if source_ref == source_id:
        return True
    return any(part == source_id for part in source_ref.replace("\\", "/").split("/")) or (
        source_ref.rsplit(":", 1)[-1] == source_id
    )

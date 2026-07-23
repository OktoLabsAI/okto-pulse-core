"""Resource Gate application policy over an edition-owned relational adapter."""

from __future__ import annotations

from typing import Any

from okto_pulse.core.ports.relational_services import (
    resolve_resource_gate_adapter_factory,
)
from okto_pulse.core.services.architecture import (
    ArchitectureFindingGate,
    ArchitecturePropagationEligibilityPolicy,
)
from okto_pulse.core.services.architecture_observability import (
    observe_architecture_done_blocker,
)
from okto_pulse.core.services.resource_gate_contracts import (
    ENTITY_TYPES,
    RESOURCE_TYPES,
    SOURCE_CHANNELS,
    EntityType,
    ResourceGateError,
    ResourceGateJustificationRequired,
    ResourceGateNotFound,
    ResourceGateViolation,
    ResourceState,
    ResourceType,
    SourceChannel,
)
from okto_pulse.core.services.resource_lineage import (
    ResolvedResourceLineage,
    ResolvedResourceLineageService,
    ResourceLineageError,
    observe_resource_lineage_coverage_uncovered,
)


class ResourceGateService:
    """Own invariant gate decisions while delegating persistence and mapping."""

    warning_message = (
        "WARNING: marking this resource as N/A may lead to partial or incorrect "
        "solutions if the resource is actually needed."
    )

    def __init__(
        self,
        relational_context: object | None = None,
        *,
        db: object | None = None,
    ) -> None:
        if relational_context is None:
            relational_context = db
        self.db = relational_context
        self._adapter = resolve_resource_gate_adapter_factory()(
            relational_context
        )

    @staticmethod
    def is_spec_resource_task_coverage_required(board: Any | None) -> bool:
        settings = (getattr(board, "settings", None) or {}) if board else {}
        return bool(settings.get("require_spec_resource_task_coverage", True))

    @staticmethod
    def is_spec_architecture_required_for_validation(board: Any | None) -> bool:
        settings = (getattr(board, "settings", None) or {}) if board else {}
        if not bool(settings.get("auto_derive_spec_resources_enabled", False)):
            return False
        configured = settings.get("auto_derive_spec_resource_types") or []
        return "architecture" in {
            str(getattr(resource_type, "value", resource_type))
            for resource_type in configured
        }

    async def get_summary(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
    ) -> dict[str, Any]:
        lineage = await self._resolve_resource_lineage(
            board_id,
            str(entity_type),
            entity_id,
            include_coverage=False,
            projection_profile="summary",
        )
        return await self._summary_from_lineage(
            board_id=board_id,
            entity_type=str(entity_type),
            entity_id=entity_id,
            lineage=lineage,
        )

    async def _summary_from_lineage(
        self,
        *,
        board_id: str,
        entity_type: str,
        entity_id: str,
        lineage: ResolvedResourceLineage,
    ) -> dict[str, Any]:
        resources = [
            self._legacy_resource_state_from_lineage_state(item.to_dict())
            for item in lineage.resource_states
        ]
        missing_resources = [
            item for item in resources if item["state"] == "missing"
        ]
        architecture_resource = next(
            (
                item
                for item in resources
                if item["resource_type"] == "architecture"
            ),
            None,
        )
        findings_result = await ArchitectureFindingGate(self.db).evaluate(
            board_id=board_id,
            owner_type=entity_type,
            owner_id=entity_id,
            architecture_refs=self._effective_architecture_refs(
                architecture_resource
            ),
        )
        architecture_findings = findings_result["architecture_findings"]
        architecture_propagation = await self._architecture_propagation_block(
            architecture_resource=architecture_resource,
        )
        warnings: list[dict[str, Any]] = []
        if architecture_findings["active_count"]:
            warnings.append(
                {
                    "code": "architecture_findings_active",
                    "message": (
                        "Active Architecture Design findings block Done until "
                        "the backend architecture critic resolves them."
                    ),
                    "active_count": architecture_findings["active_count"],
                    "top_remediation": architecture_findings["top_remediation"],
                }
            )
        if architecture_propagation["blocking"]:
            warnings.append(
                {
                    "code": "architecture_propagation_blocked_on_inherited",
                    "message": architecture_propagation["remediation"],
                    "ineligible_source_count": len(
                        architecture_propagation["ineligible_sources"]
                    ),
                }
            )
        return {
            "board_id": board_id,
            "entity_type": entity_type,
            "entity_id": entity_id,
            "resources": resources,
            "blocking": bool(missing_resources),
            "missing_resources": missing_resources,
            "warnings": warnings,
            "architecture_findings": architecture_findings,
            "architecture_findings_blocking": bool(
                architecture_findings["active_count"]
            ),
            "architecture_propagation": architecture_propagation,
            "architecture_propagation_blocking": architecture_propagation[
                "blocking"
            ],
            "resource_lineage": lineage.to_dict(),
            "lineage_counts": lineage.counts,
        }

    async def _architecture_propagation_block(
        self,
        *,
        architecture_resource: dict[str, Any] | None,
    ) -> dict[str, Any]:
        empty: dict[str, Any] = {
            "blocking": False,
            "ineligible_sources": [],
            "remediation": None,
        }
        if not architecture_resource:
            return empty
        inherited = list(architecture_resource.get("inherited_refs") or [])
        if not inherited:
            return empty
        policy = ArchitecturePropagationEligibilityPolicy(self.db)
        ineligible: list[dict[str, Any]] = []
        seen: set[str] = set()
        for ref in inherited:
            design_id = str(
                ref.get("id") or ref.get("source_design_id") or ""
            ).strip()
            if not design_id or design_id in seen:
                continue
            seen.add(design_id)
            eligibility = await policy.evaluate(design_id)
            if not eligibility.eligible:
                ineligible.append(eligibility.to_dict())
        if not ineligible:
            return empty
        return {
            "blocking": True,
            "ineligible_sources": ineligible,
            "remediation": (
                "An inherited Architecture Design source is ineligible for "
                "propagation. Fix the SOURCE design (resolve the active critic "
                "findings or restore its verdict) and re-run the architecture "
                "critic, then retry the copy. Acknowledgement is audit-only and "
                "does NOT authorize propagation; do not mark architecture N/A "
                "to bypass this."
            ),
        }

    async def get_effective_resources(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
    ) -> dict[str, Any]:
        lineage = await self._resolve_resource_lineage(
            board_id,
            str(entity_type),
            entity_id,
            include_coverage=False,
            projection_profile="full",
        )
        resources: dict[str, list[dict[str, Any]]] = {
            resource_type: [] for resource_type in RESOURCE_TYPES
        }
        representatives: dict[str, Any] = {}
        for attachment in lineage.attachments:
            if (
                not attachment.effective
                or attachment.attachment_kind == "not_applicable"
            ):
                continue
            current = representatives.get(attachment.unique_resource_id)
            if current is None or (
                current.attachment_kind != "direct"
                and attachment.attachment_kind == "direct"
            ):
                representatives[attachment.unique_resource_id] = attachment

        for attachment in representatives.values():
            resources[attachment.resource_type].append(
                await self._effective_resource_item(
                    board_id=board_id,
                    resource_type=attachment.resource_type,
                    ref=dict(attachment.raw),
                    attachment_kind=attachment.attachment_kind,
                    inherited=attachment.inherited,
                )
            )
        return {
            "board_id": board_id,
            "entity_type": str(entity_type),
            "entity_id": entity_id,
            "resources": resources,
            "lineage_counts": lineage.counts,
            "resource_lineage": lineage.to_dict(),
        }

    async def _resolve_resource_lineage(
        self,
        board_id: str,
        entity_type: str,
        entity_id: str,
        *,
        include_coverage: bool,
        projection_profile: str,
    ) -> ResolvedResourceLineage:
        try:
            return await ResolvedResourceLineageService(self).resolve(
                board_id,
                entity_type,
                entity_id,
                include_coverage=include_coverage,
                projection_profile=projection_profile,
            )
        except ResourceLineageError as exc:
            raise ResourceGateViolation(
                "resource_lineage_resolution_failed",
                (
                    "Resource Gate could not resolve resource lineage for "
                    f"{entity_type} '{entity_id}': {exc}"
                ),
                details={
                    "board_id": board_id,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "lineage_error_code": exc.code,
                    "lineage_error_details": exc.details,
                },
            ) from exc

    async def _effective_resource_item(self, **request: Any) -> dict[str, Any]:
        ref = dict(request["ref"])
        inherited = bool(request["inherited"])
        metadata = {
            "resource_type": request["resource_type"],
            "resource_id": ref.get("id"),
            "id": ref.get("id"),
            "title": ref.get("title"),
            "attachment_kind": request["attachment_kind"],
            "inherited": inherited,
            "read_only": inherited or ref.get("source_entity_type") == "card",
            "source_entity_type": ref.get("source_entity_type"),
            "source_entity_id": ref.get("source_entity_id"),
            "source_entity_title": ref.get("source_entity_title"),
            "provenance": {
                "source_entity_type": ref.get("source_entity_type"),
                "source_entity_id": ref.get("source_entity_id"),
                "source_entity_title": ref.get("source_entity_title"),
                "resource_id": ref.get("id"),
            },
            "ref": ref,
            "hydrated": False,
        }
        try:
            resource = await self._hydrate_effective_resource(
                board_id=request["board_id"],
                resource_type=request["resource_type"],
                ref=ref,
            )
        except Exception as exc:
            return {**metadata, "hydration_error": str(exc)}
        if not resource:
            return {
                **metadata,
                "hydration_error": (
                    "Resource payload not found for effective ref."
                ),
            }
        return {
            **resource,
            **metadata,
            "resource": resource,
            "hydrated": True,
        }

    async def _hydrate_effective_resource(
        self, **request: Any
    ) -> dict[str, Any] | None:
        return await self._adapter.hydrate_effective_resource(**request)

    async def mark_not_applicable(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        resource_type: ResourceType | str,
        actor_id: str,
        *,
        justification: str | None = None,
        source_channel: SourceChannel | str = "ui",
    ) -> dict[str, Any]:
        self._validate_entity_type(str(entity_type))
        self._validate_resource_type(str(resource_type))
        self._validate_source_channel(str(source_channel))
        if source_channel in {"api", "mcp"} and not (justification or "").strip():
            raise ResourceGateJustificationRequired(str(source_channel))
        mark_id = await self._adapter.save_not_applicable(
            board_id,
            str(entity_type),
            entity_id,
            str(resource_type),
            actor_id,
            justification=justification,
            source_channel=str(source_channel),
        )
        summary = await self.get_summary(board_id, entity_type, entity_id)
        warning = self.warning_message if source_channel in {"api", "mcp"} else None
        if warning:
            summary["warnings"].append(
                {
                    "code": "resource_not_applicable_risk",
                    "message": warning,
                    "resource_type": str(resource_type),
                }
            )
        return {
            "success": True,
            "mark_id": mark_id,
            "summary": summary,
            "warning": warning,
        }

    async def clear_not_applicable(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        resource_type: ResourceType | str,
        actor_id: str,
        *,
        reason: str | None = None,
    ) -> dict[str, Any]:
        self._validate_entity_type(str(entity_type))
        self._validate_resource_type(str(resource_type))
        affected = await self._adapter.clear_not_applicable(
            board_id,
            str(entity_type),
            entity_id,
            str(resource_type),
            actor_id,
            reason=reason or "cleared",
        )
        return {
            "success": True,
            "cleared": affected,
            "summary": await self.get_summary(board_id, entity_type, entity_id),
        }

    async def validate_entity_completion(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
    ) -> dict[str, Any]:
        """Apply the Core Level 1 completion policy to adapter evidence."""

        summary = await self.get_summary(board_id, entity_type, entity_id)
        blocking_resources = [
            resource
            for resource in summary["resources"]
            if resource["state"] == "missing"
        ]
        architecture_findings = summary.get("architecture_findings") or {}
        blocking_findings = list(
            architecture_findings.get("top_remediation") or []
        )
        architecture_propagation = summary.get("architecture_propagation") or {}
        blocking_propagation = (
            architecture_propagation
            if summary.get("architecture_propagation_blocking")
            else {}
        )
        return {
            "allowed": not (
                blocking_resources or blocking_findings or blocking_propagation
            ),
            "blocking_resources": blocking_resources,
            "blocking_architecture_findings": blocking_findings,
            "blocking_architecture_propagation": blocking_propagation,
            "architecture_findings": architecture_findings,
            "summary": summary,
        }

    def _raise_architecture_propagation_block(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        phase: str,
        architecture_propagation: dict[str, Any],
        summary: dict[str, Any],
    ) -> None:
        observe_architecture_done_blocker(
            board_id=board_id,
            owner_type=str(entity_type),
            active_count=0,
            design_count=len(
                architecture_propagation.get("ineligible_sources") or []
            ),
            phase=phase,
        )
        raise ResourceGateViolation(
            "architecture_propagation_blocked",
            (
                f"Cannot complete {entity_type} '{entity_id}': an inherited "
                "Architecture Design source is ineligible for propagation. "
                f"{architecture_propagation.get('remediation') or ''}".strip()
            ),
            details={
                "board_id": board_id,
                "entity_type": str(entity_type),
                "entity_id": entity_id,
                "phase": phase,
                "architecture_propagation": architecture_propagation,
                "summary": summary,
            },
        )

    async def validate_or_raise_entity_completion(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        phase: str = "completion",
    ) -> dict[str, Any]:
        """Enforce the Core completion transition and emit stable violations."""

        result = await self.validate_entity_completion(
            board_id,
            entity_type,
            entity_id,
        )
        if result["allowed"]:
            return result
        if result["blocking_resources"]:
            labels = ", ".join(
                self._resource_label(item["resource_type"])
                for item in result["blocking_resources"]
            )
            raise ResourceGateViolation(
                "resource_gate_missing_resources",
                (
                    f"Cannot complete {entity_type} '{entity_id}': missing "
                    f"mandatory resource(s): {labels}. Attach the resource(s) "
                    "or mark each one as N/A before completing."
                ),
                details={
                    "board_id": board_id,
                    "entity_type": str(entity_type),
                    "entity_id": entity_id,
                    "phase": phase,
                    "blocking_resources": result["blocking_resources"],
                    "summary": result["summary"],
                },
            )
        blocking = result["blocking_architecture_findings"]
        if not blocking and result["blocking_architecture_propagation"]:
            self._raise_architecture_propagation_block(
                board_id,
                entity_type,
                entity_id,
                phase=phase,
                architecture_propagation=result[
                    "blocking_architecture_propagation"
                ],
                summary=result["summary"],
            )
        labels = ", ".join(
            f"{item.get('code', 'architecture_warning')} "
            f"({item.get('target_ref') or item.get('path') or 'unknown target'})"
            for item in blocking[:5]
        )
        extra = f" and {len(blocking) - 5} more" if len(blocking) > 5 else ""
        findings = result["architecture_findings"]
        observe_architecture_done_blocker(
            board_id=board_id,
            owner_type=str(entity_type),
            active_count=int(findings.get("active_count") or 0),
            design_count=int(findings.get("design_count") or 0),
            phase=phase,
        )
        raise ResourceGateViolation(
            "architecture_findings_block_done",
            (
                f"Cannot complete {entity_type} '{entity_id}': active "
                f"Architecture Design finding(s) remain: {labels}{extra}. "
                "Resolve the findings by updating the architecture design; "
                "warning acknowledgement is audit-only and does not bypass Done."
            ),
            details={
                "board_id": board_id,
                "entity_type": str(entity_type),
                "entity_id": entity_id,
                "phase": phase,
                "architecture_findings": findings,
                "blocking_architecture_findings": blocking,
                "summary": result["summary"],
            },
        )

    async def validate_or_raise_spec_architecture_validation_resource(
        self,
        board_id: str,
        spec_id: str,
        *,
        board: Any | None,
        phase: str,
    ) -> dict[str, Any]:
        if not self.is_spec_architecture_required_for_validation(board):
            return {
                "allowed": True,
                "enabled": False,
                "board_id": board_id,
                "spec_id": spec_id,
                "blocking_resources": [],
            }
        result = await self.validate_entity_completion(board_id, "spec", spec_id)
        missing = [
            resource
            for resource in result["blocking_resources"]
            if resource["resource_type"] == "architecture"
        ]
        if not missing:
            return result
        raise ResourceGateViolation(
            "resource_gate_spec_missing_architecture",
            (
                "Cannot validate spec: Architecture is required by the board's "
                "Spec resource propagation policy and is still missing. Attach "
                "an Architecture Design to the spec or mark Architecture as N/A "
                "with justification before validation."
            ),
            details={
                "board_id": board_id,
                "spec_id": spec_id,
                "phase": phase,
                "blocking_resources": missing,
                "summary": result["summary"],
                "policy": {
                    "auto_derive_spec_resources_enabled": True,
                    "required_resource_type": "architecture",
                },
            },
        )

    async def validate_or_raise_architecture_findings(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        phase: str = "completion",
    ) -> dict[str, Any]:
        summary = await self.get_summary(board_id, entity_type, entity_id)
        findings = summary.get("architecture_findings") or {}
        blocking = list(findings.get("top_remediation") or [])
        if not blocking:
            propagation = summary.get("architecture_propagation") or {}
            if summary.get("architecture_propagation_blocking"):
                self._raise_architecture_propagation_block(
                    board_id,
                    entity_type,
                    entity_id,
                    phase=phase,
                    architecture_propagation=propagation,
                    summary=summary,
                )
            return summary
        self._raise_architecture_findings_block(
            board_id,
            entity_type,
            entity_id,
            phase=phase,
            findings=findings,
            blocking=blocking,
            summary=summary,
        )
        raise AssertionError("unreachable")

    async def validate_spec_resource_task_coverage(
        self,
        board_id: str,
        spec_id: str,
        *,
        enabled: bool = True,
    ) -> dict[str, Any]:
        """Apply Level 2 coverage policy to lineage and task evidence."""

        lineage = await self._resolve_resource_lineage(
            board_id,
            "spec",
            spec_id,
            include_coverage=True,
            projection_profile="full",
        )
        summary = await self._summary_from_lineage(
            board_id=board_id,
            entity_type="spec",
            entity_id=spec_id,
            lineage=lineage,
        )
        provided_refs = [
            obligation.to_dict() for obligation in lineage.coverage_obligations
        ]
        if not enabled or not provided_refs:
            return {
                "allowed": True,
                "enabled": enabled,
                "board_id": board_id,
                "spec_id": spec_id,
                "required_resources": provided_refs,
                "uncovered_resources": [],
                "architecture_findings": summary.get("architecture_findings"),
                "summary": summary,
            }

        task_cards = await self._adapter.load_spec_task_cards(spec_id)
        coverage_ids = await self._adapter.collect_task_resource_id_coverage(
            task_cards
        )
        uncovered: list[dict[str, Any]] = []
        for ref in provided_refs:
            resource_type = ref["resource_type"]
            identities = self._resource_identity_values(ref)
            eligible_ids = coverage_ids[resource_type]["eligible"]
            cancelled_ids = coverage_ids[resource_type]["cancelled"]
            if identities and identities.intersection(eligible_ids):
                continue
            if identities and identities.intersection(cancelled_ids):
                reason = "covered_only_by_cancelled_task"
                remediation = (
                    "Attach or copy this resource to at least one non-cancelled "
                    "task. Cancelled tasks do not count as coverage."
                )
            else:
                reason = "uncovered"
                remediation = (
                    "Attach or copy this resource directly to at least one "
                    "non-cancelled task."
                )
            observe_resource_lineage_coverage_uncovered(
                resource_type=resource_type,
                reason=reason,
            )
            uncovered.append({
                "resource_type": resource_type,
                "resource_id": ref.get("id"),
                "unique_resource_id": ref.get("unique_resource_id"),
                "resource_title": ref.get("title"),
                "source_entity_type": ref.get("source_entity_type"),
                "source_entity_id": ref.get("source_entity_id"),
                "source_entity_title": ref.get("source_entity_title"),
                "origin_evidence": dict(ref.get("origin_evidence") or {}),
                "reason": reason,
                "remediation": remediation,
            })
        return {
            "allowed": not uncovered,
            "enabled": enabled,
            "board_id": board_id,
            "spec_id": spec_id,
            "required_resources": provided_refs,
            "uncovered_resources": uncovered,
            "architecture_findings": summary.get("architecture_findings"),
            "summary": summary,
        }

    async def validate_or_raise_spec_resource_task_coverage(
        self,
        board_id: str,
        spec_id: str,
        *,
        phase: str,
        enabled: bool = True,
    ) -> dict[str, Any]:
        result = await self.validate_spec_resource_task_coverage(
            board_id,
            spec_id,
            enabled=enabled,
        )
        if result["allowed"]:
            findings = result.get("architecture_findings") or {}
            if phase == "spec_done" and findings.get("active_count"):
                self._raise_architecture_findings_block(
                    board_id,
                    "spec",
                    spec_id,
                    phase=phase,
                    findings=findings,
                    blocking=list(findings.get("top_remediation") or []),
                    summary=result["summary"],
                )
            return result
        labels = ", ".join(
            (
                f"{self._resource_label(item['resource_type'])} "
                f"({item['resource_title']})"
                if item.get("resource_title")
                else self._resource_label(item["resource_type"])
            )
            for item in result["uncovered_resources"][:5]
        )
        extra_count = len(result["uncovered_resources"]) - 5
        extra = f" and {extra_count} more" if extra_count > 0 else ""
        raise ResourceGateViolation(
            "resource_gate_spec_task_coverage",
            (
                "Cannot advance spec: mandatory spec resource(s) are not "
                "covered by non-cancelled task cards: "
                f"{labels}{extra}. Copy or attach every effective spec "
                "Architecture, Mockup and Knowledge Base resource to at least "
                "one task, or disable the board setting "
                "'require_spec_resource_task_coverage'."
            ),
            details={
                "board_id": board_id,
                "spec_id": spec_id,
                "phase": phase,
                "uncovered_resources": result["uncovered_resources"],
                "required_resources": result["required_resources"],
                "summary": result["summary"],
            },
        )

    def _raise_architecture_findings_block(
        self,
        board_id: str,
        entity_type: EntityType | str,
        entity_id: str,
        *,
        phase: str,
        findings: dict[str, Any],
        blocking: list[dict[str, Any]],
        summary: dict[str, Any],
    ) -> None:
        labels = ", ".join(
            f"{item.get('code', 'architecture_warning')} "
            f"({item.get('target_ref') or item.get('path') or 'unknown target'})"
            for item in blocking[:5]
        )
        extra = f" and {len(blocking) - 5} more" if len(blocking) > 5 else ""
        observe_architecture_done_blocker(
            board_id=board_id,
            owner_type=str(entity_type),
            active_count=int(findings.get("active_count") or 0),
            design_count=int(findings.get("design_count") or 0),
            phase=phase,
        )
        raise ResourceGateViolation(
            "architecture_findings_block_done",
            (
                f"Cannot complete {entity_type} '{entity_id}': active "
                f"Architecture Design finding(s) remain: {labels}{extra}. "
                "Resolve the findings by updating the architecture design; "
                "warning acknowledgement is audit-only and does not bypass Done."
            ),
            details={
                "board_id": board_id,
                "entity_type": str(entity_type),
                "entity_id": entity_id,
                "phase": phase,
                "architecture_findings": findings,
                "blocking_architecture_findings": blocking,
                "summary": summary,
            },
        )

    async def load_entity_ref(self, *args: Any, **kwargs: Any) -> Any:
        return await self._adapter.load_entity_ref(*args, **kwargs)

    async def load_parent_refs(self, *args: Any, **kwargs: Any) -> list[Any]:
        return await self._adapter.load_parent_refs(*args, **kwargs)

    async def collect_refs(self, *args: Any, **kwargs: Any) -> dict[str, list[dict]]:
        return await self._adapter.collect_refs(*args, **kwargs)

    async def filter_inherited_refs(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, list[dict]]:
        filter_refs = getattr(self._adapter, "filter_inherited_refs", None)
        if not callable(filter_refs):
            return args[2] if len(args) > 2 else kwargs["refs"]
        return await filter_refs(*args, **kwargs)

    async def load_active_marks(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return await self._adapter.load_active_marks(*args, **kwargs)

    def serialize_na_mark(self, *args: Any, **kwargs: Any) -> dict | None:
        return self._adapter.serialize_na_mark(*args, **kwargs)

    @staticmethod
    def _coverage_obligation_refs(resource: dict[str, Any]) -> list[dict[str, Any]]:
        direct = list(resource.get("direct_refs") or [])
        return direct or list(resource.get("inherited_refs") or [])

    @staticmethod
    def _source_ref_values(source_ref: Any) -> set[str]:
        if not source_ref:
            return set()
        text = str(source_ref)
        values = {text}
        for separator in (":", "/", "\\"):
            if separator in text:
                values.add(text.rsplit(separator, 1)[-1])
        return values

    @staticmethod
    def _resource_identity_values(item: Any) -> set[str]:
        if not isinstance(item, dict):
            return {str(item)} if item else set()
        values: set[str] = set()
        for key in (
            "id",
            "origin_id",
            "source_id",
            "source_mockup_id",
            "source_kb_id",
            "source_design_id",
        ):
            value = item.get(key)
            if value:
                text = str(value)
                values.add(text)
                if text.startswith("cardkb_"):
                    values.add(text[len("cardkb_"):])
        for key in ("source_ref", "origin_ref", "source"):
            values.update(ResourceGateService._source_ref_values(item.get(key)))
        return values

    @staticmethod
    def _resolve_state(direct: Any, inherited: Any, na_mark: Any) -> ResourceState:
        if list(direct) or list(inherited):
            return "provided"
        if na_mark is not None:
            return "not_applicable"
        return "missing"

    @staticmethod
    def _resource_label(resource_type: str) -> str:
        return {
            "architecture": "Architecture",
            "mockup": "Mockup",
            "knowledge_base": "Knowledge Base",
        }.get(resource_type, resource_type)

    @staticmethod
    def _effective_architecture_refs(
        resource: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        if not resource or resource.get("resource_type") != "architecture":
            return []
        direct = list(resource.get("direct_refs") or [])
        return direct or list(resource.get("inherited_refs") or [])

    @classmethod
    def _legacy_resource_state_from_lineage_state(
        cls,
        state: dict[str, Any],
    ) -> dict[str, Any]:
        resource_type = str(state["resource_type"])
        resource_state = state["state"]
        return {
            **state,
            "reason": None if resource_state != "missing" else "missing",
            "remediation": cls._remediation(resource_type, resource_state),
        }

    @staticmethod
    def _remediation(
        resource_type: str,
        state: ResourceState,
    ) -> str | None:
        if state != "missing":
            return None
        return (
            f"Attach a {ResourceGateService._resource_label(resource_type)} "
            "resource or mark it as N/A."
        )

    @staticmethod
    def _validate_entity_type(entity_type: str) -> None:
        if entity_type not in ENTITY_TYPES:
            raise ResourceGateError(
                "invalid_entity_type",
                f"Invalid entity_type '{entity_type}'. Expected one of: "
                f"{', '.join(ENTITY_TYPES)}.",
            )

    @staticmethod
    def _validate_resource_type(resource_type: str) -> None:
        if resource_type not in RESOURCE_TYPES:
            raise ResourceGateError(
                "invalid_resource_type",
                f"Invalid resource_type '{resource_type}'. Expected one of: "
                f"{', '.join(RESOURCE_TYPES)}.",
            )

    @staticmethod
    def _validate_source_channel(source_channel: str) -> None:
        if source_channel not in SOURCE_CHANNELS:
            raise ResourceGateError(
                "invalid_source_channel",
                f"Invalid source_channel '{source_channel}'. Expected one of: "
                f"{', '.join(SOURCE_CHANNELS)}.",
            )


__all__ = [
    "ENTITY_TYPES",
    "RESOURCE_TYPES",
    "SOURCE_CHANNELS",
    "EntityType",
    "ResourceGateError",
    "ResourceGateJustificationRequired",
    "ResourceGateNotFound",
    "ResourceGateService",
    "ResourceGateViolation",
    "ResolvedResourceLineageService",
    "ResourceState",
    "ResourceType",
    "SourceChannel",
]

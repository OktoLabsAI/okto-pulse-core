"""MCP-scoped spec CRUD use cases (SaaS Refactor spec R01A MCP-FU6, family: spec).

PURITY GUARDRAIL (Codex-mandated, enforced by test_r01a_mcp_spec_uow.py): this
module is transport-free. It MUST NOT import the MCP server/transport package nor
any server-side transport helper. The MCP adapter (server.py) keeps JSON parsing,
fail-closed token resolution, ``_canonical_api_contract_error`` /
``_structured_error`` envelopes, ``REGISTRY.validate_target_type`` and the
``emit_alias_usage`` / ``emit_registry_violation`` telemetry (coordinated by the
returned outcome). These use cases receive an already-validated/coerced command,
run the mutation/query via the service over the UoW, and return typed
transport-free data / result / error.

Spec family traits (from the inventory): NO inline activity log; every entity-id
read/write fails closed outside the authenticated board. The test_scenario
lifecycle + structured-entity paths rely on SERVICE-OWNED commits (these use
cases must NOT add a UoW commit there).
"""

from __future__ import annotations

from typing import Any

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    commit,
)
from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_authorization,
)
from okto_pulse.core.application.use_cases.mutation_permissions import (
    transition_permission_requirement,
)
from okto_pulse.core.domain.test_scenarios import ScenarioType
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork
from okto_pulse.core.services.application_schemas import (
    PersistedTestScenarioSpecUpdate,
)


async def _require_actor_board_spec(
    service: Any,
    spec_id: str,
    actor: ActorContext,
    *,
    board_id: str | None = None,
) -> Any:
    """Load a Spec without disclosing entities outside the authenticated board."""
    scoped_board_id = board_id or actor.board_id
    if scoped_board_id is None or (
        actor.board_id is not None and scoped_board_id != actor.board_id
    ):
        raise EntityNotFoundError("spec", spec_id)
    spec = await service.get_spec(spec_id)
    if spec is None or spec.board_id != scoped_board_id:
        raise EntityNotFoundError("spec", spec_id)
    return spec


async def _require_actor_board_scenario_spec(
    service: Any, spec_id: str, actor: ActorContext
) -> Any:
    """Use the scenario adapters' governed not-found error contract."""
    try:
        return await _require_actor_board_spec(service, spec_id, actor)
    except EntityNotFoundError as exc:
        raise ValueError("scenario_not_found: spec not found") from exc


# --- move (board-scope + old_status capture + state machine) ----------------


class McpMoveSpecCommand:
    __slots__ = ("spec_id", "board_id", "data")

    def __init__(self, spec_id: str, board_id: str, data: Any) -> None:
        self.spec_id = spec_id
        self.board_id = board_id
        self.data = data


class McpMoveSpecResult:
    __slots__ = ("spec", "old_status")

    def __init__(self, spec: Any, old_status: str) -> None:
        self.spec = spec
        self.old_status = old_status


class McpMoveSpecUseCase:
    """Move a board-scoped spec, capturing ``old_status`` BEFORE the move (the
    legacy MCP envelope returns ``from_status``/``to_status``). A missing or
    cross-board spec — and a ``None`` move result — both map to the legacy
    ``"Spec not found"`` via ``EntityNotFoundError``. ``GateContractError`` /
    ``ValueError`` from ``SpecService.move_spec`` propagate uncaught for the
    adapter to map in that exact order."""

    async def execute(
        self, command: McpMoveSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpMoveSpecResult:
        service = uow.services.specs
        existing = await _require_actor_board_spec(
            service,
            command.spec_id,
            actor,
            board_id=command.board_id,
        )
        old_status = existing.status.value
        await require_authorization(
            actor,
            transition_permission_requirement(
                "spec",
                existing.status,
                command.data.status,
                legacy_operation="specs:move",
            ),
            uow=uow,
            board_id=existing.board_id,
        )
        spec = await service.move_spec(
            command.spec_id, actor.actor_id, command.data, actor_name=actor.actor_name
        )
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        # Resolve persistence-managed revisions before the commit while this
        # transaction still owns the row. A post-commit refresh could observe
        # a later writer or fail after the mutation was already durable.
        await uow.synchronize()
        await uow.reload(spec, fields=("status", "edition", "version"))
        await commit(uow)
        return McpMoveSpecResult(spec, old_status)


# --- update (thin variant; adapter keeps SPECS_UPDATE flat perm + envelope) --


class McpUpdateSpecCommand:
    __slots__ = ("spec_id", "payload")

    def __init__(self, spec_id: str, payload: Any) -> None:
        self.spec_id = spec_id
        self.payload = payload


class McpUpdateSpecResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class McpUpdateSpecUseCase:
    """Update a spec (write) — the mutation half of the legacy ``_safe_spec_update``
    helper, transport-free. This is a deliberate VARIANT, NOT a reuse of the REST
    ``UpdateSpecUseCase`` (which adds field-level permission enforcement the legacy
    MCP tool never had; reusing it would newly reject calls). The flat ``SPECS_UPDATE``
    check + the ``{"error": str(exc)}`` ValueError envelope stay in the adapter:
    ``ValueError`` from ``_validate_spec_linked_refs`` propagates UNCAUGHT (no commit),
    and a ``None`` result (missing spec) is ``EntityNotFoundError`` →
    adapter ``"Spec not found"``. Reused by the JSON-list sub-entity tools, which
    build the ``SpecUpdate`` payload (resolved in the adapter) before calling this."""

    async def execute(
        self, command: McpUpdateSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateSpecResult:
        await _require_actor_board_spec(uow.services.specs, command.spec_id, actor)
        spec = await uow.services.specs.update_spec(
            command.spec_id, actor.actor_id, command.payload
        )
        if not spec:
            raise EntityNotFoundError("spec", command.spec_id)
        await commit(uow)
        return McpUpdateSpecResult(spec)


# --- derive spec from ideation/refinement (shared variant) ------------------


class McpDeriveSpecCommand:
    __slots__ = (
        "source", "source_id", "mockup_ids", "kb_ids",
        "architecture_design_ids", "architecture_propagation_mode",
        "knowledge_propagation",
    )

    def __init__(
        self,
        source: str,
        source_id: str,
        *,
        mockup_ids: Any = None,
        kb_ids: Any = None,
        architecture_design_ids: Any = None,
        architecture_propagation_mode: str = "copy",
        knowledge_propagation: Any = None,
    ) -> None:
        self.source = source
        self.source_id = source_id
        self.mockup_ids = mockup_ids
        self.kb_ids = kb_ids
        self.architecture_design_ids = architecture_design_ids
        self.architecture_propagation_mode = architecture_propagation_mode
        self.knowledge_propagation = knowledge_propagation


class McpDeriveSpecResult:
    __slots__ = ("spec", "resource_propagation", "knowledge_mutation")

    def __init__(
        self,
        spec: Any,
        resource_propagation: Any = None,
        knowledge_mutation: Any = None,
    ) -> None:
        self.spec = spec
        self.resource_propagation = (
            resource_propagation
            if resource_propagation is not None
            else getattr(spec, "resource_propagation", None)
        )
        self.knowledge_mutation = knowledge_mutation


class McpDeriveSpecUseCase:
    """Derive a spec draft from a DONE ideation OR refinement (write), with artifact
    propagation. ``source`` selects the service (both expose the same
    ``derive_spec(id, actor, skip_ownership_check=True, mockup_ids, kb_ids,
    architecture_design_ids, architecture_propagation_mode)``). Before that
    ownership bypass, the parent must exist on the actor's authenticated board;
    missing and cross-board parents both fail closed as ``EntityNotFoundError``.
    ``ValueError`` (status/propagation) propagates for the adapter's
    ``{"error": str}``. Single commit; the spec is returned WITHOUT a re-fetch
    (matching the legacy MCP tool, unlike the REST DeriveSpecUseCase)."""

    async def execute(
        self, command: McpDeriveSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpDeriveSpecResult:

        if command.knowledge_propagation is not None:
            if command.source != "refinement":
                raise ValueError(
                    "knowledge_propagation v2 is not supported for ideation derive"
                )
            if command.kb_ids is not None:
                from okto_pulse.core.services.knowledge_propagation import (
                    KnowledgePropagationServiceError,
                )

                raise KnowledgePropagationServiceError(
                    "conflicting_propagation_parameters",
                    "legacy kb_ids and knowledge_propagation v2 are mutually exclusive",
                )
            from okto_pulse.core.application.use_cases.knowledge_propagation import (
                DeriveSpecKnowledgeV2Command,
                DeriveSpecKnowledgeV2UseCase,
            )

            mutation = await DeriveSpecKnowledgeV2UseCase().execute(
                DeriveSpecKnowledgeV2Command(
                    command.source_id,
                    command.knowledge_propagation,
                    mockup_ids=command.mockup_ids,
                    architecture_design_ids=command.architecture_design_ids,
                    architecture_propagation_mode=(
                        command.architecture_propagation_mode
                    ),
                ),
                actor=actor,
                uow=uow,
            )
            return McpDeriveSpecResult(
                None,
                knowledge_mutation=mutation,
            )

        service = (
            uow.services.ideations
            if command.source == "ideation"
            else uow.services.refinements
        )
        parent = (
            await service.get_ideation(command.source_id)
            if command.source == "ideation"
            else await service.get_refinement(command.source_id)
        )
        if parent is None or actor.board_id is None or parent.board_id != actor.board_id:
            raise EntityNotFoundError(command.source, command.source_id)
        spec = await service.derive_spec(
            command.source_id,
            actor.actor_id,
            skip_ownership_check=True,
            mockup_ids=command.mockup_ids,
            kb_ids=command.kb_ids,
            architecture_design_ids=command.architecture_design_ids,
            architecture_propagation_mode=command.architecture_propagation_mode,
        )
        if not spec:
            raise EntityNotFoundError(command.source, command.source_id)
        if command.source == "refinement":
            from okto_pulse.core.application.use_cases.research_decision_ledger import (
                bind_research_decisions_to_spec,
            )

            await bind_research_decisions_to_spec(
                refinement=parent,
                spec=spec,
                uow=uow,
            )
        await commit(uow)
        return McpDeriveSpecResult(spec)


# --- create (skip_ownership + R3-IMP1 pre-commit resource propagation) -------


class McpCreateSpecCommand:
    __slots__ = ("board_id", "spec_data", "refinement_id")

    def __init__(self, board_id: str, spec_data: Any, refinement_id: str) -> None:
        self.board_id = board_id
        self.spec_data = spec_data
        self.refinement_id = refinement_id


class McpCreateSpecResult:
    """``spec`` None → adapter "Failed to create spec". ``lineage_error`` /
    ``propagation_error`` carry the (domain) exception for the adapter to render via
    ``to_error_dict()`` — on those paths the use case did NOT commit, so the UoW
    rolls the just-flushed spec back on context exit (decision #4: no half-resourced
    spec). A preflight lineage failure has ``spec`` None and is rendered before the
    adapter's generic create failure. ``resource_propagation`` is the success summary
    (or None)."""

    __slots__ = ("spec", "resource_propagation", "lineage_error", "propagation_error")

    def __init__(
        self,
        spec: Any,
        resource_propagation: Any = None,
        *,
        lineage_error: Any = None,
        propagation_error: Any = None,
    ) -> None:
        self.spec = spec
        self.resource_propagation = resource_propagation
        self.lineage_error = lineage_error
        self.propagation_error = propagation_error


class McpCreateSpecUseCase:
    """Create a spec (``skip_ownership_check=True``) and, when ``refinement_id`` is
    set, resolve its effective lineage before the dependent Spec FK write, then
    propagate resources INSIDE the same UoW BEFORE the single commit (R3-IMP1).
    A lineage/propagation failure is returned in the result (NOT committed), so no
    orphan or half-resourced spec persists. The adapter renders the legacy JSON
    (``to_error_dict``) and keeps the ``invalid status`` / multi-value coercion."""

    async def execute(
        self, command: McpCreateSpecCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpCreateSpecResult:
        from okto_pulse.core.services.effective_resource_propagation import (
            ResourceLineageResolutionError,
            ResourcePropagationError,
        )
        from okto_pulse.core.services.main import SpecLineagePreflightError

        resolved_lineage = None
        ideation_id = getattr(command.spec_data, "ideation_id", None)
        if ideation_id or command.refinement_id:
            try:
                resolved_lineage = (
                    await uow.services.resolve_effective_spec_parent_lineage(
                        board_id=command.board_id,
                        ideation_id=ideation_id,
                        refinement_id=command.refinement_id or None,
                    )
                )
            except ResourceLineageResolutionError as exc:
                return McpCreateSpecResult(None, lineage_error=exc)

        try:
            spec = await uow.services.specs.create_spec(
                command.board_id,
                actor.actor_id,
                command.spec_data,
                skip_ownership_check=True,
            )
        except SpecLineagePreflightError as exc:
            return McpCreateSpecResult(None, lineage_error=exc)
        if not spec:
            return McpCreateSpecResult(None)

        resource_propagation = None
        if command.refinement_id:
            try:
                resource_propagation = (
                    await uow.services.propagate_effective_resources_to_spec(
                    board_id=command.board_id,
                    spec=spec,
                    refinement_id=command.refinement_id,
                    user_id=actor.actor_id,
                    resolved_lineage=resolved_lineage,
                    )
                )
            except ResourceLineageResolutionError as exc:
                return McpCreateSpecResult(spec, lineage_error=exc)
            except ResourcePropagationError as exc:
                return McpCreateSpecResult(spec, propagation_error=exc)

        await commit(uow)
        return McpCreateSpecResult(spec, resource_propagation)


# --- get_spec_context (board-scope only; adapter owns presentation projection) -


class McpGetSpecContextCommand:
    __slots__ = ("spec_id", "board_id")

    def __init__(self, spec_id: str, board_id: str) -> None:
        self.spec_id = spec_id
        self.board_id = board_id


class McpGetSpecContextResult:
    __slots__ = ("spec",)

    def __init__(self, spec: Any) -> None:
        self.spec = spec


class McpGetSpecContextUseCase:
    """Board-scoped spec fetch for the consolidated-context tool (read, no commit).
    A missing OR cross-board spec is ``EntityNotFoundError`` → the adapter's
    ``{"error": "Spec not found"}`` (BEFORE the aggregation). Transport-free: the
    heavy presentation aggregation (5+ services, the sprint swallow, projection /
    gate_readiness, ``_mcp_architecture_for_parent`` / ``_serialize_knowledge_base``)
    stays in the inbound presentation adapter and outside the use case."""

    async def execute(
        self, command: McpGetSpecContextCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpGetSpecContextResult:
        spec = await uow.services.specs.get_spec(command.spec_id)
        if not spec or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        return McpGetSpecContextResult(spec)


# --- spec sub-entity list-mutations (domain logic lives HERE, per Codex) -----
# Pattern: the use case fetches the spec, applies the not-found/board-scope domain
# check, resolves tokens via the CORE resolvers (analytics_service), builds the new
# JSON list and persists via SpecService.update_spec. The adapter keeps only parse/
# coercion (parse_multi_value), the MCP envelopes, _saturation_or_coverage / the
# unresolved-token message text (server helpers), and telemetry. Unresolved tokens
# are returned in the result (no persist) so the adapter renders the exact string.


class McpAddBusinessRuleCommand:
    __slots__ = (
        "spec_id", "rule_id", "title", "rule", "when", "then", "notes",
        "linked_requirement_tokens",
    )

    def __init__(
        self,
        spec_id: str,
        rule_id: str,
        title: str,
        rule: str,
        when: str,
        then: str,
        notes: str | None,
        linked_requirement_tokens: list | None,
    ) -> None:
        self.spec_id = spec_id
        self.rule_id = rule_id
        self.title = title
        self.rule = rule
        self.when = when
        self.then = then
        self.notes = notes
        self.linked_requirement_tokens = linked_requirement_tokens


class McpAddBusinessRuleResult:
    """``unresolved_tokens`` set (with ``frs`` for the adapter's available-id list) →
    fail-closed, NOT persisted. Otherwise ``business_rule`` + ``coverage`` (core
    ``_spec_coverage``) for the adapter to wrap with ``_saturation_or_coverage``."""

    __slots__ = ("business_rule", "coverage", "unresolved_tokens", "frs")

    def __init__(
        self,
        *,
        business_rule: Any = None,
        coverage: Any = None,
        unresolved_tokens: Any = None,
        frs: Any = None,
    ) -> None:
        self.business_rule = business_rule
        self.coverage = coverage
        self.unresolved_tokens = unresolved_tokens
        self.frs = frs


class McpAddBusinessRuleUseCase:
    """Append a business rule to a spec's JSON list (write). Fetch → not-found →
    STRICT fail-closed FR token resolution (core ``resolve_linked_requirements_to_ids``)
    → build the rule + new list → ``SpecService.update_spec`` (ValueError propagates)
    → commit → core coverage. Missing and cross-board specs fail closed."""

    async def execute(
        self, command: McpAddBusinessRuleCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddBusinessRuleResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            resolve_linked_requirements_to_ids,
        )
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        frs = spec.functional_requirements or []
        req_list = None
        if command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirements_to_ids(
                command.linked_requirement_tokens, frs
            )
            if unresolved:
                return McpAddBusinessRuleResult(unresolved_tokens=unresolved, frs=frs)
            req_list = resolved or None

        br = {
            "id": command.rule_id,
            "title": command.title,
            "rule": command.rule,
            "when": command.when,
            "then": command.then,
            "linked_requirements": req_list,
            "notes": command.notes,
        }
        rules = list(spec.business_rules or [])
        rules.append(br)
        await service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(business_rules=rules)
        )
        await commit(uow)
        return McpAddBusinessRuleResult(
            business_rule=br, coverage=spec_coverage_summary(spec, rules=rules)
        )


class McpUpdateBusinessRuleCommand:
    __slots__ = (
        "spec_id", "rule_id", "title", "rule", "when", "then",
        "notes", "notes_clear", "linked_requirement_tokens", "linked_clear",
    )

    def __init__(
        self,
        spec_id: str,
        rule_id: str,
        *,
        title: str,
        rule: str,
        when: str,
        then: str,
        notes: str,
        notes_clear: bool,
        linked_requirement_tokens: list | None,
        linked_clear: bool,
    ) -> None:
        self.spec_id = spec_id
        self.rule_id = rule_id
        self.title = title
        self.rule = rule
        self.when = when
        self.then = then
        self.notes = notes
        self.notes_clear = notes_clear
        self.linked_requirement_tokens = linked_requirement_tokens
        self.linked_clear = linked_clear


class McpUpdateBusinessRuleResult:
    __slots__ = ("business_rule", "coverage", "unresolved_tokens", "frs", "not_found")

    def __init__(
        self,
        *,
        business_rule: Any = None,
        coverage: Any = None,
        unresolved_tokens: Any = None,
        frs: Any = None,
        not_found: bool = False,
    ) -> None:
        self.business_rule = business_rule
        self.coverage = coverage
        self.unresolved_tokens = unresolved_tokens
        self.frs = frs
        self.not_found = not_found


class McpUpdateBusinessRuleUseCase:
    """Update a business rule in place (write). Fetch → not-found → locate by id
    (``not_found`` flag → adapter "Business rule '<id>' not found") → apply the
    non-empty edits + CLEAR sentinels → STRICT fail-closed FR token resolution on
    linked_requirements → persist → core coverage. The adapter pre-coerces (\n,
    parse_multi_value) + detects CLEAR; the unresolved-token envelope text stays
    in the adapter."""

    async def execute(
        self, command: McpUpdateBusinessRuleCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateBusinessRuleResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            resolve_linked_requirements_to_ids,
        )
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        rules = list(spec.business_rules or [])
        target = next((r for r in rules if r.get("id") == command.rule_id), None)
        if not target:
            return McpUpdateBusinessRuleResult(not_found=True)

        if command.title:
            target["title"] = command.title
        if command.rule:
            target["rule"] = command.rule
        if command.when:
            target["when"] = command.when
        if command.then:
            target["then"] = command.then
        if command.notes_clear:
            target["notes"] = None
        elif command.notes:
            target["notes"] = command.notes

        frs = spec.functional_requirements or []
        if command.linked_clear:
            target["linked_requirements"] = None
        elif command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirements_to_ids(
                command.linked_requirement_tokens, frs
            )
            if unresolved:
                return McpUpdateBusinessRuleResult(
                    unresolved_tokens=unresolved, frs=frs
                )
            target["linked_requirements"] = resolved or None

        await service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(business_rules=rules)
        )
        await commit(uow)
        return McpUpdateBusinessRuleResult(
            business_rule=target, coverage=spec_coverage_summary(spec, rules=rules)
        )


# --- shared remove (business_rule/api_contract HARD; decision SOFT) ----------


class McpRemoveSpecEntityCommand:
    __slots__ = ("spec_id", "target_type", "entity_id")

    def __init__(self, spec_id: str, target_type: str, entity_id: str) -> None:
        self.spec_id = spec_id
        self.target_type = target_type
        self.entity_id = entity_id


class McpRemoveSpecEntityResult:
    """``not_found`` → adapter renders the per-type "<Type> '<id>' not found".
    ``revoked_decision`` set for the decision SOFT-delete; otherwise
    ``removed``/``remaining``/``coverage`` for the HARD-remove envelope. The
    adapter owns REGISTRY validation, the ``_telemetry`` emit and the envelopes."""

    __slots__ = ("removed", "remaining", "coverage", "revoked_decision", "not_found")

    def __init__(
        self,
        *,
        removed: Any = None,
        remaining: Any = None,
        coverage: Any = None,
        revoked_decision: Any = None,
        not_found: bool = False,
    ) -> None:
        self.removed = removed
        self.remaining = remaining
        self.coverage = coverage
        self.revoked_decision = revoked_decision
        self.not_found = not_found


class McpRemoveSpecEntityUseCase:
    """Remove a spec JSON sub-entity (write). ``business_rule``/``api_contract`` HARD
    (filter out + core coverage on the survivors); ``decision`` SOFT (status=revoked,
    restorable). Missing entity → ``not_found`` (no persist). ``ValueError`` from the
    linked-ref validation propagates for the adapter (which emits error telemetry).
    The adapter keeps REGISTRY.validate_target_type + emit_alias_usage/registry +
    envelopes (the target_type is validated BEFORE this call)."""

    async def execute(
        self, command: McpRemoveSpecEntityCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpRemoveSpecEntityResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        tt = command.target_type
        if tt == "business_rule":
            rules = list(spec.business_rules or [])
            new_rules = [r for r in rules if r.get("id") != command.entity_id]
            if len(new_rules) == len(rules):
                return McpRemoveSpecEntityResult(not_found=True)
            await service.update_spec(
                command.spec_id, actor.actor_id, SpecUpdate(business_rules=new_rules)
            )
            await commit(uow)
            return McpRemoveSpecEntityResult(
                removed=command.entity_id,
                remaining=len(new_rules),
                coverage=spec_coverage_summary(spec, rules=new_rules),
            )

        if tt == "api_contract":
            contracts = list(spec.api_contracts or [])
            new_contracts = [c for c in contracts if c.get("id") != command.entity_id]
            if len(new_contracts) == len(contracts):
                return McpRemoveSpecEntityResult(not_found=True)
            await service.update_spec(
                command.spec_id, actor.actor_id, SpecUpdate(api_contracts=new_contracts)
            )
            await commit(uow)
            return McpRemoveSpecEntityResult(
                removed=command.entity_id,
                remaining=len(new_contracts),
                coverage=spec_coverage_summary(spec, contracts=new_contracts),
            )

        # decision — SOFT-delete (status=revoked, restorable via update_decision).
        decisions = list(spec.decisions or [])
        target = next(
            (d for d in decisions if d.get("id") == command.entity_id), None
        )
        if target is None:
            return McpRemoveSpecEntityResult(not_found=True)
        target["status"] = "revoked"
        await service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(decisions=decisions)
        )
        await commit(uow)
        return McpRemoveSpecEntityResult(revoked_decision=target)


# --- list business_rules (fetch + active filter; adapter projects) -----------


class McpListBusinessRulesCommand:
    __slots__ = ("spec_id", "include_all")

    def __init__(self, spec_id: str, include_all: bool) -> None:
        self.spec_id = spec_id
        self.include_all = include_all


class McpListBusinessRulesResult:
    __slots__ = ("rules", "frs")

    def __init__(self, rules: list, frs: list) -> None:
        self.rules = rules
        self.frs = frs


class McpListBusinessRulesUseCase:
    """Fetch a spec's business rules + its FRs (read, no commit), applying the
    active-only domain filter unless ``include_all``. Missing spec →
    ``EntityNotFoundError`` (adapter ``"Spec not found"``). The FR-resolution
    projection + ``emit_compaction_metric`` stay in the adapter (transport)."""

    async def execute(
        self, command: McpListBusinessRulesCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpListBusinessRulesResult:
        spec = await _require_actor_board_spec(
            uow.services.specs, command.spec_id, actor
        )
        rules = list(spec.business_rules or [])
        if not command.include_all:
            rules = [
                item
                for item in rules
                if not isinstance(item, dict)
                or item.get("status", "active") == "active"
            ]
        return McpListBusinessRulesResult(rules, spec.functional_requirements or [])


# --- api_contract (Codex opt-C: resolver moved to core; F9 validate in use case) -


class McpAddApiContractCommand:
    __slots__ = (
        "spec_id", "contract_id", "method", "path", "description",
        "request_body", "response_success", "response_errors",
        "linked_requirement_tokens", "linked_rule_tokens", "notes",
    )

    def __init__(
        self,
        spec_id: str,
        contract_id: str,
        method: str,
        path: str,
        description: str,
        *,
        request_body: Any,
        response_success: Any,
        response_errors: Any,
        linked_requirement_tokens: list | None,
        linked_rule_tokens: list | None,
        notes: str | None,
    ) -> None:
        self.spec_id = spec_id
        self.contract_id = contract_id
        self.method = method
        self.path = path
        self.description = description
        self.request_body = request_body
        self.response_success = response_success
        self.response_errors = response_errors
        self.linked_requirement_tokens = linked_requirement_tokens
        self.linked_rule_tokens = linked_rule_tokens
        self.notes = notes


class McpAddApiContractResult:
    """Fail-closed signals (NOT persisted) for the adapter to render the legacy
    envelopes WITHOUT re-reading the spec: ``unresolved_tokens`` (+ ``available_fr_ids``/
    ``available_tr_ids``/``fr_count``), ``bad_rule_token``, or ``invalid_contract_exc``
    (the F9/F10 ``ValidationError`` for ``_canonical_api_contract_error``). On success:
    ``contract`` + core ``coverage``."""

    __slots__ = (
        "contract", "coverage", "unresolved_tokens", "available_fr_ids",
        "available_tr_ids", "fr_count", "bad_rule_token", "invalid_contract_exc",
    )

    def __init__(
        self,
        *,
        contract: Any = None,
        coverage: Any = None,
        unresolved_tokens: Any = None,
        available_fr_ids: Any = None,
        available_tr_ids: Any = None,
        fr_count: int = 0,
        bad_rule_token: Any = None,
        invalid_contract_exc: Any = None,
    ) -> None:
        self.contract = contract
        self.coverage = coverage
        self.unresolved_tokens = unresolved_tokens
        self.available_fr_ids = available_fr_ids
        self.available_tr_ids = available_tr_ids
        self.fr_count = fr_count
        self.bad_rule_token = bad_rule_token
        self.invalid_contract_exc = invalid_contract_exc


class McpAddApiContractUseCase:
    """Append an API contract (write). Fetch → not-found → FR/TR token resolution
    (core ``resolve_linked_requirement_tokens_to_fr_or_tr_ids``, fail-closed) →
    linked_rules existence (against the spec's business rules) → build → F9
    on-write validation (``ApiContract.model_validate(on_write=True)``) → F10 bulk
    ``SpecUpdate`` validation → persist → core coverage. The adapter parses JSON,
    canonicalizes the ValidationError and renders the envelopes."""

    async def execute(
        self, command: McpAddApiContractCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddApiContractResult:
        from pydantic import ValidationError

        from okto_pulse.core.services.application_schemas import ApiContract, SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_requirement_tokens_to_fr_or_tr_ids,
        )
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        frs = spec.functional_requirements or []
        trs = spec.technical_requirements or []
        existing_rules = spec.business_rules or []

        req_list = None
        if command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
                command.linked_requirement_tokens, frs, trs
            )
            if unresolved:
                return McpAddApiContractResult(
                    unresolved_tokens=unresolved,
                    available_fr_ids=available_structured_ids(frs),
                    available_tr_ids=available_structured_ids(trs),
                    fr_count=len(frs),
                )
            req_list = resolved or None

        rules_list = None
        if command.linked_rule_tokens:
            rule_ids = {r.get("id") for r in existing_rules}
            rules_list = []
            for token in command.linked_rule_tokens:
                if token in rule_ids:
                    rules_list.append(token)
                else:
                    return McpAddApiContractResult(bad_rule_token=token)

        contract = {
            "id": command.contract_id,
            "method": command.method.upper(),
            "path": command.path,
            "description": command.description,
            "request_body": command.request_body,
            "response_success": command.response_success,
            "response_errors": command.response_errors,
            "linked_requirements": req_list,
            "linked_rules": rules_list,
            "notes": command.notes,
        }

        # F9: on-write strictness (non-verb method rejected at the boundary).
        try:
            ApiContract.model_validate(contract, context={"on_write": True})
        except ValidationError as exc:
            return McpAddApiContractResult(invalid_contract_exc=exc)

        contracts = list(spec.api_contracts or [])
        contracts.append(contract)
        # F10: the bulk SpecUpdate re-validates tolerantly (read-back, no on_write);
        # a leak here would surface the raw pydantic URL, so canonicalize it too.
        try:
            contract_update = SpecUpdate(api_contracts=contracts)
        except ValidationError as exc:
            return McpAddApiContractResult(invalid_contract_exc=exc)

        await service.update_spec(command.spec_id, actor.actor_id, contract_update)
        await commit(uow)
        return McpAddApiContractResult(
            contract=contract,
            coverage=spec_coverage_summary(spec, contracts=contracts),
        )


class McpUpdateApiContractCommand:
    """``field_updates`` carries the simple-field changes the adapter already
    resolved (description / request_body / response_success / response_errors /
    notes — CLEAR encoded as ""/None, JSON pre-parsed; absent key = no change).
    method/path apply when truthy (use case upper-cases method). linked_* use the
    clear flag + the pre-coerced token list."""

    __slots__ = (
        "spec_id", "contract_id", "method", "path", "field_updates",
        "linked_req_clear", "linked_requirement_tokens",
        "linked_rule_clear", "linked_rule_tokens",
    )

    def __init__(
        self,
        spec_id: str,
        contract_id: str,
        *,
        method: str,
        path: str,
        field_updates: dict,
        linked_req_clear: bool,
        linked_requirement_tokens: list | None,
        linked_rule_clear: bool,
        linked_rule_tokens: list | None,
    ) -> None:
        self.spec_id = spec_id
        self.contract_id = contract_id
        self.method = method
        self.path = path
        self.field_updates = field_updates
        self.linked_req_clear = linked_req_clear
        self.linked_requirement_tokens = linked_requirement_tokens
        self.linked_rule_clear = linked_rule_clear
        self.linked_rule_tokens = linked_rule_tokens


class McpUpdateApiContractResult:
    __slots__ = (
        "contract", "not_found", "unresolved_tokens", "available_fr_ids",
        "available_tr_ids", "fr_count", "bad_rule_token", "invalid_contract_exc",
    )

    def __init__(
        self,
        *,
        contract: Any = None,
        not_found: bool = False,
        unresolved_tokens: Any = None,
        available_fr_ids: Any = None,
        available_tr_ids: Any = None,
        fr_count: int = 0,
        bad_rule_token: Any = None,
        invalid_contract_exc: Any = None,
    ) -> None:
        self.contract = contract
        self.not_found = not_found
        self.unresolved_tokens = unresolved_tokens
        self.available_fr_ids = available_fr_ids
        self.available_tr_ids = available_tr_ids
        self.fr_count = fr_count
        self.bad_rule_token = bad_rule_token
        self.invalid_contract_exc = invalid_contract_exc


class McpUpdateApiContractUseCase:
    """Update an API contract in place (write). Fetch → not-found → locate by id →
    apply simple-field changes + method/path → FR/TR resolution (CLEAR or core
    fail-closed) → linked_rules existence (CLEAR or against the spec's rules) → F9
    on-write validation → F10 bulk SpecUpdate → persist. No coverage in the legacy
    envelope (returns the contract + the adapter's deprecation_warning)."""

    async def execute(
        self, command: McpUpdateApiContractCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateApiContractResult:
        from pydantic import ValidationError

        from okto_pulse.core.services.application_schemas import ApiContract, SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_requirement_tokens_to_fr_or_tr_ids,
        )

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        contracts = list(spec.api_contracts or [])
        target = next(
            (c for c in contracts if c.get("id") == command.contract_id), None
        )
        if not target:
            return McpUpdateApiContractResult(not_found=True)

        if command.method:
            target["method"] = command.method.upper()
        if command.path:
            target["path"] = command.path
        target.update(command.field_updates)

        frs = spec.functional_requirements or []
        trs = spec.technical_requirements or []
        if command.linked_req_clear:
            target["linked_requirements"] = None
        elif command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
                command.linked_requirement_tokens, frs, trs
            )
            if unresolved:
                return McpUpdateApiContractResult(
                    unresolved_tokens=unresolved,
                    available_fr_ids=available_structured_ids(frs),
                    available_tr_ids=available_structured_ids(trs),
                    fr_count=len(frs),
                )
            target["linked_requirements"] = resolved or None

        if command.linked_rule_clear:
            target["linked_rules"] = None
        elif command.linked_rule_tokens:
            rule_ids = {r.get("id") for r in (spec.business_rules or [])}
            rules_list = []
            for token in command.linked_rule_tokens:
                if token in rule_ids:
                    rules_list.append(token)
                else:
                    return McpUpdateApiContractResult(bad_rule_token=token)
            target["linked_rules"] = rules_list

        try:
            ApiContract.model_validate(target, context={"on_write": True})
        except ValidationError as exc:
            return McpUpdateApiContractResult(invalid_contract_exc=exc)
        try:
            contract_update = SpecUpdate(api_contracts=contracts)
        except ValidationError as exc:
            return McpUpdateApiContractResult(invalid_contract_exc=exc)

        await service.update_spec(command.spec_id, actor.actor_id, contract_update)
        await commit(uow)
        return McpUpdateApiContractResult(contract=target)


class McpListApiContractsCommand:
    __slots__ = ("spec_id", "include_all")

    def __init__(self, spec_id: str, include_all: bool) -> None:
        self.spec_id = spec_id
        self.include_all = include_all


class McpListApiContractsResult:
    __slots__ = ("contracts", "existing_rules", "frs", "trs")

    def __init__(self, contracts: list, existing_rules: dict, frs: list, trs: list) -> None:
        self.contracts = contracts
        self.existing_rules = existing_rules
        self.frs = frs
        self.trs = trs


class McpListApiContractsUseCase:
    """Fetch a spec's API contracts (+ the business-rule map, FRs, TRs) for the
    adapter's linked-rule/FR-TR-resolution projection (read, no commit). Applies the
    active-only domain filter unless ``include_all``. Missing spec →
    ``EntityNotFoundError``. The heavy projection + ``emit_compaction_metric`` stay
    in the adapter (transport)."""

    async def execute(
        self, command: McpListApiContractsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpListApiContractsResult:
        spec = await _require_actor_board_spec(
            uow.services.specs, command.spec_id, actor
        )
        contracts = list(spec.api_contracts or [])
        if not command.include_all:
            contracts = [
                item
                for item in contracts
                if not isinstance(item, dict)
                or item.get("status", "active") == "active"
            ]
        existing_rules = {r.get("id"): r for r in (spec.business_rules or [])}
        return McpListApiContractsResult(
            contracts,
            existing_rules,
            spec.functional_requirements or [],
            spec.technical_requirements or [],
        )


# --- decision (add + update; soft-delete already via McpRemoveSpecEntity) -----


class McpAddDecisionCommand:
    __slots__ = (
        "spec_id", "dec_id", "title", "rationale", "context", "alternatives",
        "supersedes_decision_id", "linked_requirement_tokens", "notes",
    )

    def __init__(
        self,
        spec_id: str,
        dec_id: str,
        title: str,
        rationale: str,
        *,
        context: str | None,
        alternatives: list | None,
        supersedes_decision_id: str,
        linked_requirement_tokens: list | None,
        notes: str | None,
    ) -> None:
        self.spec_id = spec_id
        self.dec_id = dec_id
        self.title = title
        self.rationale = rationale
        self.context = context
        self.alternatives = alternatives
        self.supersedes_decision_id = supersedes_decision_id
        self.linked_requirement_tokens = linked_requirement_tokens
        self.notes = notes


class McpAddDecisionResult:
    __slots__ = (
        "decision", "decisions_total", "unresolved_tokens", "available_fr_ids",
        "available_tr_ids", "fr_count", "supersede_not_found",
    )

    def __init__(
        self,
        *,
        decision: Any = None,
        decisions_total: int = 0,
        unresolved_tokens: Any = None,
        available_fr_ids: Any = None,
        available_tr_ids: Any = None,
        fr_count: int = 0,
        supersede_not_found: Any = None,
    ) -> None:
        self.decision = decision
        self.decisions_total = decisions_total
        self.unresolved_tokens = unresolved_tokens
        self.available_fr_ids = available_fr_ids
        self.available_tr_ids = available_tr_ids
        self.fr_count = fr_count
        self.supersede_not_found = supersede_not_found


class McpAddDecisionUseCase:
    """Append a formalized Decision (write). Fetch -> not-found -> FR/TR token
    resolution (core, fail-closed) -> optional auto-supersede (flip the referenced
    decision to ``superseded``; missing target -> ``supersede_not_found``) -> build
    -> persist. The adapter coerces ``alternatives``/tokens and renders envelopes."""

    async def execute(
        self, command: McpAddDecisionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddDecisionResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_requirement_tokens_to_fr_or_tr_ids,
        )

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        frs = spec.functional_requirements or []
        trs = spec.technical_requirements or []
        req_list = None
        if command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
                command.linked_requirement_tokens, frs, trs
            )
            if unresolved:
                return McpAddDecisionResult(
                    unresolved_tokens=unresolved,
                    available_fr_ids=available_structured_ids(frs),
                    available_tr_ids=available_structured_ids(trs),
                    fr_count=len(frs),
                )
            req_list = resolved or None

        decisions = list(spec.decisions or [])
        if command.supersedes_decision_id:
            found_target = False
            for d in decisions:
                if d.get("id") == command.supersedes_decision_id:
                    d["status"] = "superseded"
                    found_target = True
                    break
            if not found_target:
                return McpAddDecisionResult(
                    supersede_not_found=command.supersedes_decision_id
                )

        decision = {
            "id": command.dec_id,
            "title": command.title,
            "rationale": command.rationale,
            "context": command.context,
            "alternatives_considered": command.alternatives,
            "supersedes_decision_id": command.supersedes_decision_id or None,
            "linked_requirements": req_list,
            "linked_task_ids": None,
            "status": "active",
            "notes": command.notes,
        }
        decisions.append(decision)
        await service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(decisions=decisions)
        )
        await commit(uow)
        return McpAddDecisionResult(decision=decision, decisions_total=len(decisions))


class McpUpdateDecisionCommand:
    __slots__ = (
        "spec_id", "decision_id", "field_updates", "supersedes_clear",
        "supersedes_value", "status", "linked_requirement_tokens",
    )

    def __init__(
        self,
        spec_id: str,
        decision_id: str,
        *,
        field_updates: dict,
        supersedes_clear: bool,
        supersedes_value: str,
        status: str,
        linked_requirement_tokens: list | None,
    ) -> None:
        self.spec_id = spec_id
        self.decision_id = decision_id
        self.field_updates = field_updates
        self.supersedes_clear = supersedes_clear
        self.supersedes_value = supersedes_value
        self.status = status
        self.linked_requirement_tokens = linked_requirement_tokens


class McpUpdateDecisionResult:
    __slots__ = (
        "decision", "not_found", "unresolved_tokens", "available_fr_ids",
        "available_tr_ids", "fr_count", "invalid_status",
    )

    def __init__(
        self,
        *,
        decision: Any = None,
        not_found: bool = False,
        unresolved_tokens: Any = None,
        available_fr_ids: Any = None,
        available_tr_ids: Any = None,
        fr_count: int = 0,
        invalid_status: Any = None,
    ) -> None:
        self.decision = decision
        self.not_found = not_found
        self.unresolved_tokens = unresolved_tokens
        self.available_fr_ids = available_fr_ids
        self.available_tr_ids = available_tr_ids
        self.fr_count = fr_count
        self.invalid_status = invalid_status


class McpUpdateDecisionUseCase:
    """Update a Decision in place (write). Fetch -> not-found -> locate by id ->
    apply simple-field changes (``field_updates``, CLEAR pre-encoded by the adapter)
    -> supersedes (CLEAR or set + flip the referenced decision) -> FR/TR resolution
    (no CLEAR branch in the legacy) -> status validation -> persist."""

    async def execute(
        self, command: McpUpdateDecisionCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateDecisionResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_requirement_tokens_to_fr_or_tr_ids,
        )

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        decisions = list(spec.decisions or [])
        target = next(
            (d for d in decisions if d.get("id") == command.decision_id), None
        )
        if target is None:
            return McpUpdateDecisionResult(not_found=True)

        target.update(command.field_updates)

        if command.supersedes_clear:
            target["supersedes_decision_id"] = None
        elif command.supersedes_value:
            target["supersedes_decision_id"] = command.supersedes_value
            for d in decisions:
                if d.get("id") == command.supersedes_value:
                    d["status"] = "superseded"
                    break

        if command.linked_requirement_tokens:
            frs = spec.functional_requirements or []
            trs = spec.technical_requirements or []
            resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
                command.linked_requirement_tokens, frs, trs
            )
            if unresolved:
                return McpUpdateDecisionResult(
                    unresolved_tokens=unresolved,
                    available_fr_ids=available_structured_ids(frs),
                    available_tr_ids=available_structured_ids(trs),
                    fr_count=len(frs),
                )
            target["linked_requirements"] = resolved or None

        if command.status:
            if command.status not in ("active", "superseded", "revoked"):
                return McpUpdateDecisionResult(invalid_status=command.status)
            target["status"] = command.status

        await service.update_spec(
            command.spec_id, actor.actor_id, SpecUpdate(decisions=decisions)
        )
        await commit(uow)
        return McpUpdateDecisionResult(decision=target)


# --- integration_requirement (board-scoped: add + list) ----------------------


class McpAddIntegrationRequirementCommand:
    __slots__ = (
        "spec_id", "board_id", "ir_id", "title", "integration_type", "description",
        "provider", "consumer", "contract_ref", "endpoint", "method",
        "data_contract", "linked_requirement_tokens", "linked_api_contracts", "notes",
    )

    def __init__(
        self,
        spec_id: str,
        board_id: str,
        ir_id: str,
        title: str,
        integration_type: str,
        *,
        description: str,
        provider: str,
        consumer: str,
        contract_ref: str,
        endpoint: str,
        method: str,
        data_contract: Any,
        linked_requirement_tokens: list | None,
        linked_api_contracts: list | None,
        notes: str | None,
    ) -> None:
        self.spec_id = spec_id
        self.board_id = board_id
        self.ir_id = ir_id
        self.title = title
        self.integration_type = integration_type
        self.description = description
        self.provider = provider
        self.consumer = consumer
        self.contract_ref = contract_ref
        self.endpoint = endpoint
        self.method = method
        self.data_contract = data_contract
        self.linked_requirement_tokens = linked_requirement_tokens
        self.linked_api_contracts = linked_api_contracts
        self.notes = notes


class McpAddIntegrationRequirementResult:
    __slots__ = (
        "requirement", "coverage", "unresolved_tokens", "available_fr_ids",
        "available_tr_ids", "fr_count",
    )

    def __init__(
        self,
        *,
        requirement: Any = None,
        coverage: Any = None,
        unresolved_tokens: Any = None,
        available_fr_ids: Any = None,
        available_tr_ids: Any = None,
        fr_count: int = 0,
    ) -> None:
        self.requirement = requirement
        self.coverage = coverage
        self.unresolved_tokens = unresolved_tokens
        self.available_fr_ids = available_fr_ids
        self.available_tr_ids = available_tr_ids
        self.fr_count = fr_count


class McpAddIntegrationRequirementUseCase:
    """Append an Integration Requirement (write, BOARD-SCOPED: a missing OR
    cross-board spec is ``EntityNotFoundError`` -> "Spec not found"). FR/TR token
    resolution (core, fail-closed) -> build -> persist -> core coverage. The adapter
    validates integration_type, parses data_contract JSON, coerces linked_api_contracts
    and renders envelopes."""

    async def execute(
        self, command: McpAddIntegrationRequirementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddIntegrationRequirementResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_requirement_tokens_to_fr_or_tr_ids,
        )
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await service.get_spec(command.spec_id)
        if not spec or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)

        frs = spec.functional_requirements or []
        trs = spec.technical_requirements or []
        req_list = None
        if command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
                command.linked_requirement_tokens, frs, trs
            )
            if unresolved:
                return McpAddIntegrationRequirementResult(
                    unresolved_tokens=unresolved,
                    available_fr_ids=available_structured_ids(frs),
                    available_tr_ids=available_structured_ids(trs),
                    fr_count=len(frs),
                )
            req_list = resolved or None

        requirement = {
            "id": command.ir_id,
            "title": command.title,
            "integration_type": command.integration_type,
            "description": command.description,
            "provider": command.provider or None,
            "consumer": command.consumer or None,
            "contract_ref": command.contract_ref or None,
            "endpoint": command.endpoint or None,
            "method": command.method or None,
            "data_contract": command.data_contract,
            "linked_requirements": req_list,
            "linked_api_contracts": command.linked_api_contracts,
            "linked_task_ids": None,
            "status": "active",
            "notes": command.notes,
        }
        requirements = list(getattr(spec, "integration_requirements", None) or [])
        requirements.append(requirement)
        await service.update_spec(
            command.spec_id,
            actor.actor_id,
            SpecUpdate(integration_requirements=requirements),
        )
        await commit(uow)
        return McpAddIntegrationRequirementResult(
            requirement=requirement,
            coverage=spec_coverage_summary(spec, integration_requirements=requirements),
        )


class McpListIntegrationRequirementsCommand:
    __slots__ = ("spec_id", "board_id", "include_all")

    def __init__(self, spec_id: str, board_id: str, include_all: bool) -> None:
        self.spec_id = spec_id
        self.board_id = board_id
        self.include_all = include_all


class McpListIntegrationRequirementsResult:
    __slots__ = ("requirements",)

    def __init__(self, requirements: list) -> None:
        self.requirements = requirements


class McpListIntegrationRequirementsUseCase:
    """Board-scoped Integration Requirements fetch (read, no commit) with the
    active-only domain filter unless ``include_all``. Missing OR cross-board spec ->
    ``EntityNotFoundError`` -> "Spec not found"."""

    async def execute(
        self, command: McpListIntegrationRequirementsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpListIntegrationRequirementsResult:
        spec = await uow.services.specs.get_spec(command.spec_id)
        if not spec or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        requirements = list(getattr(spec, "integration_requirements", None) or [])
        if not command.include_all:
            requirements = [
                item
                for item in requirements
                if not isinstance(item, dict)
                or item.get("status", "active") == "active"
            ]
        return McpListIntegrationRequirementsResult(requirements)


# --- observability_requirement (board-scoped: add + list) --------------------


class McpAddObservabilityRequirementCommand:
    __slots__ = (
        "spec_id", "board_id", "or_id", "title", "signal_type", "description",
        "target", "metric_name", "threshold", "severity", "owner",
        "linked_requirement_tokens", "linked_integration_requirements", "notes",
    )

    def __init__(
        self,
        spec_id: str,
        board_id: str,
        or_id: str,
        title: str,
        signal_type: str,
        *,
        description: str,
        target: str,
        metric_name: str,
        threshold: str,
        severity: str,
        owner: str,
        linked_requirement_tokens: list | None,
        linked_integration_requirements: list | None,
        notes: str | None,
    ) -> None:
        self.spec_id = spec_id
        self.board_id = board_id
        self.or_id = or_id
        self.title = title
        self.signal_type = signal_type
        self.description = description
        self.target = target
        self.metric_name = metric_name
        self.threshold = threshold
        self.severity = severity
        self.owner = owner
        self.linked_requirement_tokens = linked_requirement_tokens
        self.linked_integration_requirements = linked_integration_requirements
        self.notes = notes


class McpAddObservabilityRequirementResult:
    __slots__ = (
        "requirement", "coverage", "unresolved_tokens", "available_fr_ids",
        "available_tr_ids", "fr_count",
    )

    def __init__(
        self,
        *,
        requirement: Any = None,
        coverage: Any = None,
        unresolved_tokens: Any = None,
        available_fr_ids: Any = None,
        available_tr_ids: Any = None,
        fr_count: int = 0,
    ) -> None:
        self.requirement = requirement
        self.coverage = coverage
        self.unresolved_tokens = unresolved_tokens
        self.available_fr_ids = available_fr_ids
        self.available_tr_ids = available_tr_ids
        self.fr_count = fr_count


class McpAddObservabilityRequirementUseCase:
    """Append an Observability Requirement (write, BOARD-SCOPED). Mirrors the IR
    use case: a missing OR cross-board spec is ``EntityNotFoundError`` -> "Spec not
    found"; FR/TR token resolution (core, fail-closed) -> build -> persist -> core
    coverage. The adapter validates signal_type, coerces
    linked_integration_requirements and renders envelopes."""

    async def execute(
        self, command: McpAddObservabilityRequirementCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddObservabilityRequirementResult:
        from okto_pulse.core.services.application_schemas import SpecUpdate
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_requirement_tokens_to_fr_or_tr_ids,
        )
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await service.get_spec(command.spec_id)
        if not spec or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)

        frs = spec.functional_requirements or []
        trs = spec.technical_requirements or []
        req_list = None
        if command.linked_requirement_tokens:
            resolved, unresolved = resolve_linked_requirement_tokens_to_fr_or_tr_ids(
                command.linked_requirement_tokens, frs, trs
            )
            if unresolved:
                return McpAddObservabilityRequirementResult(
                    unresolved_tokens=unresolved,
                    available_fr_ids=available_structured_ids(frs),
                    available_tr_ids=available_structured_ids(trs),
                    fr_count=len(frs),
                )
            req_list = resolved or None

        requirement = {
            "id": command.or_id,
            "title": command.title,
            "signal_type": command.signal_type,
            "description": command.description,
            "target": command.target or None,
            "metric_name": command.metric_name or None,
            "threshold": command.threshold or None,
            "severity": command.severity or None,
            "owner": command.owner or None,
            "linked_requirements": req_list,
            "linked_integration_requirements": command.linked_integration_requirements,
            "linked_task_ids": None,
            "status": "active",
            "notes": command.notes,
        }
        requirements = list(getattr(spec, "observability_requirements", None) or [])
        requirements.append(requirement)
        await service.update_spec(
            command.spec_id,
            actor.actor_id,
            SpecUpdate(observability_requirements=requirements),
        )
        await commit(uow)
        return McpAddObservabilityRequirementResult(
            requirement=requirement,
            coverage=spec_coverage_summary(spec, observability_requirements=requirements),
        )


class McpListObservabilityRequirementsCommand:
    __slots__ = ("spec_id", "board_id", "include_all")

    def __init__(self, spec_id: str, board_id: str, include_all: bool) -> None:
        self.spec_id = spec_id
        self.board_id = board_id
        self.include_all = include_all


class McpListObservabilityRequirementsResult:
    __slots__ = ("requirements",)

    def __init__(self, requirements: list) -> None:
        self.requirements = requirements


class McpListObservabilityRequirementsUseCase:
    """Board-scoped Observability Requirements fetch (read, no commit) with the
    active-only domain filter unless ``include_all``. Missing OR cross-board spec ->
    ``EntityNotFoundError`` -> "Spec not found"."""

    async def execute(
        self, command: McpListObservabilityRequirementsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpListObservabilityRequirementsResult:
        spec = await uow.services.specs.get_spec(command.spec_id)
        if not spec or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        requirements = list(getattr(spec, "observability_requirements", None) or [])
        if not command.include_all:
            requirements = [
                item
                for item in requirements
                if not isinstance(item, dict)
                or item.get("status", "active") == "active"
            ]
        return McpListObservabilityRequirementsResult(requirements)


# --- test_scenario (add commits; update/delete = SERVICE SELF-COMMIT) ---------
# COMMIT MAP (Codex-required, enforced by the oracle):
#   add    -> use-case commit (SpecService.update_spec + commit(uow); legacy
#             _safe_spec_update + db.commit).
#   list   -> read, no commit.
#   update -> SpecService.update_test_scenario SELF-COMMITS -> use case MUST NOT
#             add commit(uow) (double-commit); domain exceptions propagate.
#   delete -> SpecService.delete_test_scenario SELF-COMMITS -> same.


class McpAddTestScenarioCommand:
    __slots__ = (
        "spec_id", "scenario_id", "title", "given", "when", "then",
        "scenario_type", "linked_criteria_tokens", "notes",
    )

    def __init__(
        self,
        spec_id: str,
        scenario_id: str,
        title: str,
        given: str,
        when: str,
        then: str,
        *,
        scenario_type: ScenarioType,
        linked_criteria_tokens: list | None,
        notes: str | None,
    ) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.title = title
        self.given = given
        self.when = when
        self.then = then
        self.scenario_type = scenario_type
        self.linked_criteria_tokens = linked_criteria_tokens
        self.notes = notes


class McpAddTestScenarioResult:
    __slots__ = (
        "scenario", "coverage", "invalid_scenario_type",
        "unresolved_criteria", "available_ac_ids", "criteria_count",
    )

    def __init__(
        self,
        *,
        scenario: Any = None,
        coverage: Any = None,
        invalid_scenario_type: Any = None,
        unresolved_criteria: Any = None,
        available_ac_ids: Any = None,
        criteria_count: int = 0,
    ) -> None:
        self.scenario = scenario
        self.coverage = coverage
        self.invalid_scenario_type = invalid_scenario_type
        self.unresolved_criteria = unresolved_criteria
        self.available_ac_ids = available_ac_ids
        self.criteria_count = criteria_count


class McpAddTestScenarioUseCase:
    """Append a test scenario (write, USE-CASE COMMIT). Fetch -> not-found ->
    fail-closed scenario_type -> STRICT fail-closed AC token resolution (core
    ``resolve_linked_criteria_to_ids``) -> build -> ``SpecService.update_spec`` ->
    commit -> core coverage. The adapter renders the typed envelopes."""

    async def execute(
        self, command: McpAddTestScenarioCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpAddTestScenarioResult:
        from okto_pulse.core.services.analytics_service import (
            available_structured_ids,
            resolve_linked_criteria_to_ids,
        )
        from okto_pulse.core.services.test_scenario_lifecycle import (
            is_valid_scenario_type,
        )
        from okto_pulse.core.services.traceability import spec_coverage_summary

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        if not is_valid_scenario_type(command.scenario_type):
            return McpAddTestScenarioResult(
                invalid_scenario_type=command.scenario_type
            )

        criteria = spec.acceptance_criteria or []
        criteria_list = None
        if command.linked_criteria_tokens:
            resolved_ids, unresolved = resolve_linked_criteria_to_ids(
                command.linked_criteria_tokens, criteria
            )
            if unresolved:
                return McpAddTestScenarioResult(
                    unresolved_criteria=unresolved,
                    available_ac_ids=available_structured_ids(criteria),
                    criteria_count=len(criteria),
                )
            criteria_list = resolved_ids

        scenario = {
            "id": command.scenario_id,
            "title": command.title,
            "linked_criteria": criteria_list,
            "scenario_type": command.scenario_type,
            "given": command.given,
            "when": command.when,
            "then": command.then,
            "notes": command.notes,
            "status": "draft",
            "linked_task_ids": None,
        }
        scenarios = list(spec.test_scenarios or [])
        scenarios.append(scenario)
        await service.update_spec(
            command.spec_id,
            actor.actor_id,
            PersistedTestScenarioSpecUpdate.from_iterable(scenarios),
        )
        await commit(uow)
        return McpAddTestScenarioResult(
            scenario=scenario,
            coverage=spec_coverage_summary(spec, scenarios=scenarios),
        )


class McpListTestScenariosCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class McpListTestScenariosResult:
    __slots__ = ("all_scenarios", "criteria")

    def __init__(self, all_scenarios: list, criteria: list) -> None:
        self.all_scenarios = all_scenarios
        self.criteria = criteria


class McpListTestScenariosUseCase:
    """Fetch a spec's test scenarios + acceptance criteria (read, no commit). Missing
    spec -> ``EntityNotFoundError`` -> "Spec not found". The filter / pagination /
    coverage-map / summary projection stays in the adapter (presentation)."""

    async def execute(
        self, command: McpListTestScenariosCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpListTestScenariosResult:
        spec = await _require_actor_board_spec(
            uow.services.specs, command.spec_id, actor
        )
        return McpListTestScenariosResult(
            spec.test_scenarios or [], spec.acceptance_criteria or []
        )


class McpUpdateTestScenarioCommand:
    __slots__ = (
        "spec_id", "scenario_id", "title", "given", "when", "then",
        "scenario_type", "linked_criteria_tokens", "notes", "clear_fields",
    )

    def __init__(
        self,
        spec_id: str,
        scenario_id: str,
        *,
        title: str,
        given: str,
        when: str,
        then: str,
        scenario_type: ScenarioType | None,
        linked_criteria_tokens: list | None,
        notes: str,
        clear_fields: list | None,
    ) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id
        self.title = title
        self.given = given
        self.when = when
        self.then = then
        self.scenario_type = scenario_type
        self.linked_criteria_tokens = linked_criteria_tokens
        self.notes = notes
        self.clear_fields = clear_fields


class McpUpdateTestScenarioResult:
    __slots__ = ("result",)

    def __init__(self, result: Any) -> None:
        self.result = result


class McpUpdateTestScenarioUseCase:
    """Edit a test scenario body via ``SpecService.update_test_scenario``, which
    SELF-COMMITS — so this use case MUST NOT add ``commit(uow)`` (double-commit).
    ``SpecLockedError`` / ``InvalidScenarioTypeError`` / ``ValueError`` propagate for
    the adapter to map to the legacy envelopes."""

    async def execute(
        self, command: McpUpdateTestScenarioCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpUpdateTestScenarioResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "spec.tests.edit",
                legacy_operation="specs:update",
            ),
            uow=uow,
            board_id=actor.board_id,
        )
        service = uow.services.specs
        await _require_actor_board_scenario_spec(service, command.spec_id, actor)
        result = await service.update_test_scenario(
            command.spec_id,
            actor.actor_id,
            command.scenario_id,
            title=command.title or None,
            given=command.given or None,
            when=command.when or None,
            then=command.then or None,
            scenario_type=command.scenario_type,
            linked_criteria=command.linked_criteria_tokens,
            notes=command.notes or None,
            clear=command.clear_fields,
        )
        return McpUpdateTestScenarioResult(result)


class McpDeleteTestScenarioCommand:
    __slots__ = ("spec_id", "scenario_id")

    def __init__(self, spec_id: str, scenario_id: str) -> None:
        self.spec_id = spec_id
        self.scenario_id = scenario_id


class McpDeleteTestScenarioResult:
    __slots__ = ("result",)

    def __init__(self, result: Any) -> None:
        self.result = result


class McpDeleteTestScenarioUseCase:
    """Delete a test scenario (CASCADE-clean Card.test_scenario_ids) via
    ``SpecService.delete_test_scenario``, which SELF-COMMITS — so this use case MUST
    NOT add ``commit(uow)``. ``SpecLockedError`` / ``ValueError`` propagate for the
    adapter to map."""

    async def execute(
        self, command: McpDeleteTestScenarioCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpDeleteTestScenarioResult:
        await require_authorization(
            actor,
            PermissionRequirement(
                "spec.tests.delete",
                legacy_operation="specs:update",
            ),
            uow=uow,
            board_id=actor.board_id,
        )
        service = uow.services.specs
        await _require_actor_board_scenario_spec(service, command.spec_id, actor)
        result = await service.delete_test_scenario(
            command.spec_id, actor.actor_id, command.scenario_id
        )
        return McpDeleteTestScenarioResult(result)


# --- structured-entity (shared _mcp_apply_structured_spec_entity helper) ------
# CONDITIONAL COMMIT (byte-faithful to the legacy): commit only when the apply
# both succeeded AND changed fields; otherwise roll the session back.


class McpApplyStructuredSpecEntityCommand:
    __slots__ = (
        "board_id", "spec_id", "entity_type", "entity_id", "operation",
        "payload", "expected_spec_version", "task_id", "ack_token", "permission_set",
    )

    def __init__(
        self,
        *,
        board_id: str,
        spec_id: str,
        entity_type: str,
        entity_id: str,
        operation: str,
        payload: dict,
        expected_spec_version: Any,
        task_id: str,
        ack_token: str,
        permission_set: Any,
    ) -> None:
        self.board_id = board_id
        self.spec_id = spec_id
        self.entity_type = entity_type
        self.entity_id = entity_id
        self.operation = operation
        self.payload = payload
        self.expected_spec_version = expected_spec_version
        self.task_id = task_id
        self.ack_token = ack_token
        self.permission_set = permission_set


class McpApplyStructuredSpecEntityResult:
    __slots__ = ("result",)

    def __init__(self, result: Any) -> None:
        self.result = result


class McpApplyStructuredSpecEntityUseCase:
    """Apply a structured spec-entity mutation via ``StructuredSpecEntityService``
    (which owns authorization, impact and event logic). CONDITIONAL persistence,
    byte-faithful to the legacy MCP helper: commit ONLY when ``result.success and
    result.changed_fields``, else roll the session back (a no-op write must not
    commit). The adapter keeps auth, the api_contract gating, entity_type validation,
    payload/expected-version parsing and renders ``result.as_dict()``."""

    async def execute(
        self, command: McpApplyStructuredSpecEntityCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpApplyStructuredSpecEntityResult:
        from okto_pulse.core.services.spec_structured_entities import (
            StructuredSpecEntityCommand,
        )

        service = uow.services.structured_specs
        result = await service.apply(
            StructuredSpecEntityCommand(
                board_id=command.board_id,
                spec_id=command.spec_id,
                actor_id=actor.actor_id,
                entity_type=command.entity_type,
                entity_id=command.entity_id or None,
                operation=command.operation,
                payload=command.payload,
                expected_spec_version=command.expected_spec_version,
                task_id=command.task_id or None,
                ack_token=command.ack_token or None,
                permission_set=command.permission_set,
            )
        )
        if result.success and result.changed_fields:
            await commit(uow)
        else:
            await uow.rollback()
        return McpApplyStructuredSpecEntityResult(result)


# --- migrate_spec_decisions (markdown "## Decisions" -> structured, idempotent) -


class McpMigrateSpecDecisionsCommand:
    __slots__ = ("spec_id",)

    def __init__(self, spec_id: str) -> None:
        self.spec_id = spec_id


class McpMigrateSpecDecisionsResult:
    __slots__ = ("no_block", "decisions_added", "context_modified", "added")

    def __init__(
        self,
        *,
        no_block: bool = False,
        decisions_added: int = 0,
        context_modified: bool = False,
        added: Any = None,
    ) -> None:
        self.no_block = no_block
        self.decisions_added = decisions_added
        self.context_modified = context_modified
        self.added = added or []


class McpMigrateSpecDecisionsUseCase:
    """One-shot, idempotent migrator: extract the ``## Decisions`` markdown bullets
    from ``spec.context`` into structured ``spec.decisions[]`` (dedupe by title) and
    strip the block from context. This is migration DOMAIN logic, so the whole
    extract/build/persist runs in the use case; the adapter is a thin auth + render
    shell. ``no_block`` -> the adapter's "nothing to migrate" envelope."""

    async def execute(
        self, command: McpMigrateSpecDecisionsCommand, *, actor: ActorContext, uow: PulseUnitOfWork
    ) -> McpMigrateSpecDecisionsResult:
        import re
        import uuid as _uuid

        from okto_pulse.core.services.application_schemas import SpecUpdate

        service = uow.services.specs
        spec = await _require_actor_board_spec(service, command.spec_id, actor)

        context_text = spec.context or ""
        pattern = re.compile(
            r"^##\s+Decisions[ \t]*\r?\n"
            r"((?:(?:[ \t]*[-*]\s+.*(?:\r?\n|$))|(?:[ \t]*\r?\n))+)",
            re.MULTILINE | re.IGNORECASE,
        )
        match = pattern.search(context_text)
        if not match:
            return McpMigrateSpecDecisionsResult(no_block=True)

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
                "rationale": raw,
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

        new_context = pattern.sub("", context_text).rstrip() + "\n"
        context_modified = new_context.strip() != (context_text or "").strip()

        await service.update_spec(
            command.spec_id,
            actor.actor_id,
            SpecUpdate(decisions=existing, context=new_context),
        )
        await commit(uow)
        return McpMigrateSpecDecisionsResult(
            decisions_added=len(added),
            context_modified=context_modified,
            added=[{"id": d["id"], "title": d["title"]} for d in added],
        )

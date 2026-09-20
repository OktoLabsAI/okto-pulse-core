"""Bounded, authorized read of qualification paths over one relational snapshot."""

import json
from dataclasses import asdict, dataclass
from okto_pulse.core.ports.delivery_inventory import default_delivery_inventory_policy

from okto_pulse.core.application.use_cases.authorization import (
    PermissionRequirement,
    require_all,
)
from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.domain.verification_plan import (
    MAX_PLAN_NODES,
)
from okto_pulse.core.domain.execution_contract import execution_contract
from okto_pulse.core.ports.test_evidence import supported_test_verification_methods
from okto_pulse.core.application.use_cases.board_access import load_accessible_board
from okto_pulse.core.domain.criterion_verification import (
    VERIFICATION_REQUIREMENT_FIELDS,
)
from okto_pulse.core.domain.requirement_verification import VerificationRequirementRef
from okto_pulse.core.domain.requirement_verification_resolution import (
    resolve_requirement_verification,
)
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationQuery,
)
from okto_pulse.core.repositories.interfaces.unit_of_work import PulseUnitOfWork


class RequirementVerificationReadError(ValueError):
    """Safe public code without payload or provider diagnostics."""


@dataclass(frozen=True, slots=True)
class GetRequirementVerificationCommand:
    board_id: str
    spec_id: str
    offset: int = 0
    limit: int = 25
    requirement_type: str | None = None
    requirement_id: str | None = None
    paths_offset: int = 0

    def __post_init__(self):
        for value in (self.board_id, self.spec_id):
            if not isinstance(value, str) or not value.strip() or len(value) > 255:
                raise RequirementVerificationReadError(
                    "verification_read_scope_invalid"
                )
        for value, minimum, maximum in (
            (self.offset, 0, 2**63 - 1),
            (self.limit, 1, 100),
            (self.paths_offset, 0, 2**63 - 1),
        ):
            if type(value) is not int or not minimum <= value <= maximum:
                raise RequirementVerificationReadError(
                    "verification_read_window_invalid"
                )
        if (self.requirement_type is None) != (self.requirement_id is None):
            raise RequirementVerificationReadError("verification_read_identity_invalid")
        if self.requirement_type is not None:
            try:
                VerificationRequirementRef(
                    requirement_type=self.requirement_type,
                    requirement_id=self.requirement_id,
                )
            except ValueError:
                raise RequirementVerificationReadError(
                    "verification_read_identity_invalid"
                ) from None
        elif self.paths_offset:
            raise RequirementVerificationReadError(
                "verification_read_identity_required"
            )


def _bytes(value):
    return len(
        json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        ).encode()
    )


def project_requirement_verification(resolved, command):
    """Page only presentation; global resolution never depends on this window."""
    all_rows = resolved["requirements"]
    rows = all_rows
    if command.requirement_type is not None:
        rows = [
            row
            for row in rows
            if (row["requirement_type"], row["requirement_id"])
            == (command.requirement_type, command.requirement_id)
        ]
    result = {key: value for key, value in resolved.items() if key != "requirements"}
    result.update(
        {
            "contract_version": "requirement-verification/v1",
            "total": len(rows) if resolved["population_complete"] else None,
            "population_total": len(all_rows)
            if resolved["population_complete"]
            else None,
            "resolved_count": sum(row["qualification_resolved"] for row in all_rows),
            "counts_scope": "complete"
            if resolved["population_complete"]
            else "observed",
            "offset": command.offset,
            "limit": command.limit,
            "items": [],
        }
    )
    for row in rows[command.offset : command.offset + command.limit]:
        item = {
            key: value
            for key, value in row.items()
            if key not in {"criteria_paths", "blockers"}
        }
        item.update(
            {
                "blockers": row["blockers"][:20],
                "blocker_count": len(row["blockers"]),
                "blockers_truncated": len(row["blockers"]) > 20,
                "criteria_paths": [],
                "paths_total": len(row["criteria_paths"]),
                "paths_offset": command.paths_offset,
            }
        )
        if "implementation_contributions" in item:
            contributions = item["implementation_contributions"]
            item["implementation_contributions"] = [
                {
                    **contribution,
                    "criterion_ids": contribution["criterion_ids"][:20],
                    "criterion_count": len(contribution["criterion_ids"]),
                    "criteria_truncated": len(contribution["criterion_ids"]) > 20,
                    "sources": contribution["sources"][:20],
                    "source_count": len(contribution["sources"]),
                    "sources_truncated": len(contribution["sources"]) > 20,
                }
                for contribution in contributions[:20]
            ]
            item["contribution_count"] = len(contributions)
            item["contributions_truncated"] = len(contributions) > 20
        for path in row["criteria_paths"][
            command.paths_offset : command.paths_offset + 100
        ]:
            if "scenario_plans" in path:
                plans = path["scenario_plans"]
                path = {
                    **path,
                    "scenario_plans": [
                        {
                            **plan,
                            "test_card_ids": plan["test_card_ids"][:20],
                            "test_card_count": len(plan["test_card_ids"]),
                            "test_cards_truncated": len(plan["test_card_ids"]) > 20,
                        }
                        for plan in plans[:20]
                    ],
                    "scenario_count": len(plans),
                    "scenarios_truncated": len(plans) > 20,
                }
            if _bytes([*item["criteria_paths"], path]) > 16 * 1024:
                break
            item["criteria_paths"].append(path)
        next_path = command.paths_offset + len(item["criteria_paths"])
        item["paths_has_more"] = next_path < len(row["criteria_paths"])
        item["next_paths_offset"] = (
            next_path
            if item["paths_has_more"] and next_path > command.paths_offset
            else None
        )
        item["paths_unavailable"] = (
            item["paths_has_more"] and not item["criteria_paths"]
        )
        if _bytes({**result, "items": [*result["items"], item]}) > 240 * 1024:
            break
        result["items"].append(item)
    next_offset = command.offset + len(result["items"])
    result["has_more"] = next_offset < len(rows)
    result["next_offset"] = (
        next_offset if result["has_more"] and next_offset > command.offset else None
    )
    return result


class GetRequirementVerificationUseCase:
    async def execute(
        self,
        command: GetRequirementVerificationCommand,
        *,
        actor: ActorContext,
        uow: PulseUnitOfWork,
    ):
        await uow.begin_consistent_read()
        if await load_accessible_board(uow, command.board_id, actor) is None:
            raise EntityNotFoundError("spec", command.spec_id)
        await require_all(
            actor,
            PermissionRequirement("spec.entity.read"),
            PermissionRequirement("spec.integration_requirements.read"),
            PermissionRequirement("spec.observability_requirements.read"),
            uow=uow,
            board_id=command.board_id,
        )
        fields = (*VERIFICATION_REQUIREMENT_FIELDS.values(), "acceptance_criteria")
        # Qualification retains its existing authority. Optional planning facts
        # are never loaded when either scenario or Card read is denied.
        can_read_planning = True
        try:
            await require_all(
                actor,
                PermissionRequirement("spec.tests.read"),
                PermissionRequirement("card.entity.read"),
                uow=uow,
                board_id=command.board_id,
            )
        except PermissionDeniedError:
            can_read_planning = False
        records = await uow.services.list_application_records(
            ApplicationQuery(
                entity="spec",
                filters=(
                    ApplicationFilter("id", "eq", command.spec_id),
                    ApplicationFilter("board_id", "eq", command.board_id),
                ),
                select_fields=(
                    "id",
                    "board_id",
                    "version",
                    "edition",
                    "status",
                    "archived",
                    "execution_contract",
                    *fields,
                    *(("test_scenarios", "api_contracts", "decisions", "title", "description", "context") if can_read_planning else ()),
                ),
                limit=1,
            )
        )
        if not records:
            raise EntityNotFoundError("spec", command.spec_id)
        spec = records[0]
        if spec.id != command.spec_id or spec.board_id != command.board_id:
            raise EntityNotFoundError("spec", command.spec_id)
        if any(
            type(getattr(spec, name, None)) is not int or getattr(spec, name) < 1
            for name in ("edition", "version")
        ):
            raise RequirementVerificationReadError("verification_snapshot_unavailable")
        resolved = resolve_requirement_verification(
            spec_id=spec.id,
            collections={
                field: getattr(spec, field) for field in fields if hasattr(spec, field)
            },
        )
        if can_read_planning:
            card_fields = (
                "id",
                "board_id",
                "spec_id",
                "card_type",
                "status",
                "archived",
                "test_scenario_ids",
                "title", "description", "details",
            )
            cards = await uow.services.list_application_records(
                ApplicationQuery(
                    entity="card",
                    filters=(
                        ApplicationFilter("board_id", "eq", command.board_id),
                        ApplicationFilter("spec_id", "eq", command.spec_id),
                    ),
                    select_fields=card_fields,
                    limit=MAX_PLAN_NODES + 1,
                )
            )
            plan = default_delivery_inventory_policy().execution_plan(
                spec=spec,
                cards=[
                    {field: getattr(card, field, None) for field in card_fields}
                    for card in cards
                ],
                admitted_methods=supported_test_verification_methods(),
            )
            resolved = plan.qualification
            inventory = plan.inventory
            responsibilities = inventory.responsibilities
            families = sorted({row.family for row in inventory.rows})
            resolved["effective_inventory"] = {
                "contract_version": inventory.contract_version,
                "population_complete": inventory.population_complete,
                "plan_complete": inventory.complete,
                "total": len(inventory.rows) if inventory.population_complete else None,
                "pending_count": sum(bool(row.blockers) for row in inventory.rows) if inventory.population_complete else None,
                "unassigned_count": sum(not row.contributions for row in inventory.rows) if inventory.population_complete else None,
                "families": {family: sum(row.family == family for row in inventory.rows) for family in families},
                "snapshot_sha256": inventory.snapshot_sha256,
                "issues": list(inventory.issues),
                "adoption_evaluated": True,
                "contract_adopted": execution_contract(spec) is not None,
                "delivery_evaluated": False,
            }
            by_requirement = {
                (row.requirement_type, row.requirement_id): row
                for row in responsibilities.rows
            }
            for row in resolved["requirements"]:
                responsibility = by_requirement[
                    (row["requirement_type"], row["requirement_id"])
                ]
                row["implementation_contributions"] = [
                    asdict(fact) for fact in responsibility.contributions
                ]
                row["contribution_blockers"] = list(responsibility.blockers)
            resolved.update(
                {
                    "implementation_plan_evaluated": True,
                    "implementation_scope": "qualified_requirements",
                    "implementation_plan_complete": responsibilities.complete,
                    "implementation_population_complete": responsibilities.population_complete,
                    "implementation_issues": list(responsibilities.issues),
                }
            )
        else:
            resolved.update(
                {
                    "verification_work_evaluated": False,
                    "implementation_plan_evaluated": False,
                    "implementation_plan_complete": False,
                    "implementation_population_complete": False,
                    "method_plan_complete": False,
                    "verification_work_complete": False,
                    "planning_population_complete": False,
                    "planning_issues": [
                        {"code": "verification_planning_read_restricted"}
                    ],
                    "planning_issue_count": 1,
                    "planning_issues_truncated": False,
                }
            )
        return {
            **project_requirement_verification(resolved, command),
            "board_id": spec.board_id,
            "spec_id": spec.id,
            "spec_version": spec.version,
            "spec_edition": spec.edition,
            "spec_status": str(getattr(spec.status, "value", spec.status)),
            "archived": spec.archived,
            "execution_contract": execution_contract(spec).model_dump(mode="json") if execution_contract(spec) is not None else None,
        }

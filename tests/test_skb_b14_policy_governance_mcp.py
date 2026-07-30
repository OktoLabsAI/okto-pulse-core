"""B14 MCP inventory, authorization, schema and resource contracts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    CommandValidationError,
    EntityNotFoundError,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.guidelines_crud import (
    UnlinkBoardGuidelineCommand,
    UnlinkBoardGuidelineUseCase,
)
from okto_pulse.core.application.use_cases.mcp_board_crud import (
    McpUnlinkGuidelineFromBoardCommand,
    McpUnlinkGuidelineFromBoardUseCase,
)
from okto_pulse.core.application.use_cases.policy_governance import (
    ADOPTION_MANAGE,
    COMPLIANCE_EVALUATE,
    COMPLIANCE_READ,
    IMPACT_PREVIEW,
    POLICY_GOVERNANCE_CAPABILITIES,
    REVISIONS_CREATE,
    REVISIONS_READ,
    REVISIONS_RETIRE,
    RULES_AUTHOR_BLOCKING,
    WAIVER_READ,
    WAIVER_REQUEST,
    WAIVER_REVALIDATE,
    WAIVER_REVIEW,
    WAIVER_REVOKE,
    GuidelineRevisionUnderBump,
)
from okto_pulse.core.domain.guideline_lifecycle import GuidelineVersionBump
from okto_pulse.core.domain.realm import LOCAL_REALM_ID
from okto_pulse.core.inbound.guideline_policy_error import (
    project_guideline_policy_error,
)
from okto_pulse.core.mcp.catalog import CoreMcpCatalog
from okto_pulse.core.mcp.outcome import McpOutcomeKind, McpToolOutcome
from okto_pulse.core.mcp.policy_governance_tools import (
    POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION,
    GuidelineRevisionPatchInput,
    _native,
    register_policy_governance_tools,
)
from okto_pulse.core.ports.guideline_policy import (
    GuidelinePolicyAdapterMissing,
    GuidelinePolicyCasConflict,
    GuidelinePolicyInvalidCursor,
)
from okto_pulse.core.services.governance_observability import (
    METRIC_POLICY_GOVERNANCE_AUTHORIZATION_DECISION,
    get_governance_metric_samples,
    reset_governance_metric_samples,
)


NEW_TOOL_NAMES = (
    "okto_pulse_list_guideline_revisions",
    "okto_pulse_get_guideline_revision",
    "okto_pulse_create_guideline_revision",
    "okto_pulse_retire_guideline",
    "okto_pulse_preview_guideline_impact",
    "okto_pulse_get_guideline_impact",
    "okto_pulse_list_guideline_impact_items",
    "okto_pulse_adopt_guideline_revision",
    "okto_pulse_evaluate_policy_compliance",
    "okto_pulse_list_policy_compliance_receipts",
    "okto_pulse_get_policy_compliance_receipt",
    "okto_pulse_get_current_policy_compliance_receipt",
    "okto_pulse_list_policy_compliance_findings",
    "okto_pulse_list_policy_waivers",
    "okto_pulse_get_policy_waiver",
    "okto_pulse_list_policy_waiver_events",
    "okto_pulse_request_policy_waiver",
    "okto_pulse_review_policy_waiver",
    "okto_pulse_revoke_policy_waiver",
    "okto_pulse_revalidate_policy_waiver",
)

PAGINATED_TOOLS = (
    "okto_pulse_list_guideline_revisions",
    "okto_pulse_list_guideline_impact_items",
    "okto_pulse_list_policy_compliance_receipts",
    "okto_pulse_list_policy_compliance_findings",
    "okto_pulse_list_policy_waivers",
)

_NOW = datetime(2026, 7, 29, 12, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("bump", "wire_value"),
    (
        (GuidelineVersionBump.PATCH, "patch"),
        (GuidelineVersionBump.MINOR, "minor"),
        (GuidelineVersionBump.MAJOR, "major"),
    ),
)
def test_mcp_projects_version_bump_as_closed_wire_literal(
    bump: GuidelineVersionBump,
    wire_value: str,
) -> None:
    assert _native(bump) == wire_value


def _registered_catalog(
    *,
    denied_capability: str,
) -> tuple[CoreMcpCatalog, "_ForbiddenUowFactory", dict[str, int]]:
    catalog = CoreMcpCatalog(name="policy-governance-test", version="test")
    uow_factory = _ForbiddenUowFactory()
    calls = {"settings": 0, "provider": 0}

    class Permissions:
        def check(self, capability: str) -> str | None:
            if capability == denied_capability:
                return f"Permission denied: requires '{capability}'"
            return None

    async def get_agent(board_id: str) -> object:
        assert board_id == "board-1"
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            realm_id=None,
            permissions=Permissions(),
        )

    def get_uow() -> object:
        calls["provider"] += 1
        return uow_factory

    def get_settings() -> object:
        calls["settings"] += 1
        raise AssertionError("cursor settings touched before capability fence")

    register_policy_governance_tools(
        catalog,
        get_board_agent=get_agent,
        get_uow=get_uow,
        get_settings=get_settings,
    )
    return catalog, uow_factory, calls


class _ForbiddenUowFactory:
    def __init__(self) -> None:
        self.factory_calls = 0
        self.enter_calls = 0

    def __call__(self, *, actor: object) -> "_ForbiddenUowFactory":
        del actor
        self.factory_calls += 1
        return self

    async def __aenter__(self) -> object:
        self.enter_calls += 1
        raise AssertionError("UoW entered before capability fence")

    async def __aexit__(self, *_args: object) -> None:
        return None


def _walk_objects(schema: object) -> None:
    if isinstance(schema, dict):
        if schema.get("type") == "object" or "properties" in schema:
            assert schema.get("additionalProperties") is False, schema
        for value in schema.values():
            _walk_objects(value)
    elif isinstance(schema, list):
        for value in schema:
            _walk_objects(value)


def test_exact_inventory_and_every_nested_input_object_is_closed() -> None:
    catalog, _uow, _calls = _registered_catalog(
        denied_capability=REVISIONS_READ
    )
    assert tuple(catalog._tool_manager._tools) == NEW_TOOL_NAMES

    for name in NEW_TOOL_NAMES:
        schema = catalog._tool_manager._tools[name].parameters
        assert schema["type"] == "object"
        assert schema["additionalProperties"] is False
        _walk_objects(schema)


def test_pagination_projection_and_authoritative_input_contracts_are_closed() -> None:
    catalog, _uow, _calls = _registered_catalog(
        denied_capability=REVISIONS_READ
    )
    schemas = {
        name: catalog._tool_manager._tools[name].parameters
        for name in NEW_TOOL_NAMES
    }
    for name in PAGINATED_TOOLS:
        properties = schemas[name]["properties"]
        assert set(properties["profile"]["enum"]) == {"summary", "detail"}
        assert {"limit", "cursor", "profile"} <= set(properties)
        assert "offset" not in properties

    waiver_event_properties = schemas[
        "okto_pulse_list_policy_waiver_events"
    ]["properties"]
    assert {"limit", "cursor", "profile", "offset"}.isdisjoint(
        waiver_event_properties
    )

    forbidden = {
        "actor_id",
        "actor_type",
        "binding_id",
        "binding_revision",
        "created_at",
        "evaluation_id",
        "event_id",
        "head_revision",
        "occurred_at",
        "policy_set_digest",
        "retirement_id",
        "updated_at",
        "waiver_event_id",
    }
    for schema in schemas.values():
        assert forbidden.isdisjoint(schema["properties"])

    assert "impact_digest" in schemas[
        "okto_pulse_adopt_guideline_revision"
    ]["properties"]
    for name in (
        "okto_pulse_review_policy_waiver",
        "okto_pulse_revoke_policy_waiver",
        "okto_pulse_revalidate_policy_waiver",
    ):
        assert "expected_waiver_revision" in schemas[name]["required"]


def test_operation_capability_matrix_covers_exactly_the_13_closed_leaves() -> None:
    mapped = {
        capability
        for capabilities in POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION.values()
        for capability in capabilities
    } | {RULES_AUTHOR_BLOCKING}
    assert len(POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION) == len(NEW_TOOL_NAMES)
    assert mapped == set(POLICY_GOVERNANCE_CAPABILITIES)


_DENIAL_CASES = (
    (
        "okto_pulse_list_guideline_revisions",
        REVISIONS_READ,
        {
            "board_id": "board-1",
            "guideline_id": "guideline-1",
            "cursor": "intentionally-invalid",
        },
    ),
    (
        "okto_pulse_create_guideline_revision",
        REVISIONS_CREATE,
        {
            "board_id": "board-1",
            "guideline_id": "guideline-1",
            "idempotency_key": "create-1",
            "patch": GuidelineRevisionPatchInput(title="Updated"),
        },
    ),
    (
        "okto_pulse_retire_guideline",
        REVISIONS_RETIRE,
        {
            "board_id": "board-1",
            "guideline_id": "guideline-1",
            "status": "retired",
            "reason": "No longer applicable",
            "idempotency_key": "retire-1",
        },
    ),
    (
        "okto_pulse_preview_guideline_impact",
        IMPACT_PREVIEW,
        {
            "board_id": "board-1",
            "guideline_id": "guideline-1",
            "proposed_priority": 1,
            "proposed_default_enforcement": "advisory",
            "idempotency_key": "impact-1",
        },
    ),
    (
        "okto_pulse_adopt_guideline_revision",
        ADOPTION_MANAGE,
        {
            "board_id": "board-1",
            "guideline_id": "guideline-1",
            "impact_receipt_id": "impact-1",
            "impact_digest": "a" * 64,
            "idempotency_key": "adopt-1",
        },
    ),
    (
        "okto_pulse_evaluate_policy_compliance",
        COMPLIANCE_EVALUATE,
        {
            "board_id": "board-1",
            "entity_type": "spec",
            "subject_id": "spec-1",
            "idempotency_key": "evaluate-1",
        },
    ),
    (
        "okto_pulse_get_policy_compliance_receipt",
        COMPLIANCE_READ,
        {"board_id": "board-1", "receipt_id": "receipt-1"},
    ),
    (
        "okto_pulse_get_policy_waiver",
        WAIVER_READ,
        {"board_id": "board-1", "waiver_id": "waiver-1"},
    ),
    (
        "okto_pulse_request_policy_waiver",
        WAIVER_REQUEST,
        {
            "board_id": "board-1",
            "finding_id": "finding-1",
            "justification": "Temporary exception",
            "evidence_refs": ["evidence-1"],
            "expires_at": _NOW,
            "idempotency_key": "request-1",
        },
    ),
    (
        "okto_pulse_review_policy_waiver",
        WAIVER_REVIEW,
        {
            "board_id": "board-1",
            "waiver_id": "waiver-1",
            "decision": "approve",
            "reason": "Reviewed",
            "evidence_refs": ["evidence-1"],
            "expected_waiver_revision": 1,
            "idempotency_key": "review-1",
        },
    ),
    (
        "okto_pulse_revoke_policy_waiver",
        WAIVER_REVOKE,
        {
            "board_id": "board-1",
            "waiver_id": "waiver-1",
            "reason": "No longer justified",
            "evidence_refs": ["evidence-1"],
            "expected_waiver_revision": 1,
            "idempotency_key": "revoke-1",
        },
    ),
    (
        "okto_pulse_revalidate_policy_waiver",
        WAIVER_REVALIDATE,
        {
            "board_id": "board-1",
            "waiver_id": "waiver-1",
            "reason": "Evidence refreshed",
            "evidence_refs": ["evidence-2"],
            "expected_waiver_revision": 1,
            "new_expires_at": _NOW,
            "idempotency_key": "revalidate-1",
        },
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("tool_name", "capability", "kwargs"), _DENIAL_CASES)
async def test_capability_denial_precedes_cursor_factory_and_uow(
    tool_name: str,
    capability: str,
    kwargs: dict[str, Any],
) -> None:
    reset_governance_metric_samples()
    catalog, uow_factory, calls = _registered_catalog(
        denied_capability=capability
    )

    outcome = await catalog._tool_manager._tools[tool_name].fn(**kwargs)

    assert isinstance(outcome, McpToolOutcome)
    assert outcome.kind is McpOutcomeKind.ERROR
    assert outcome.code == "permission_denied"
    assert calls == {"settings": 0, "provider": 0}
    assert uow_factory.factory_calls == uow_factory.enter_calls == 0
    samples = get_governance_metric_samples()
    assert len(samples) == 1
    assert samples[0]["metric_name"] == (
        METRIC_POLICY_GOVERNANCE_AUTHORIZATION_DECISION
    )
    assert samples[0]["labels"] == {
        "surface": "mcp",
        "operation": samples[0]["labels"]["operation"],
        "capability": capability,
        "outcome": "deny",
    }
    assert capability in POLICY_GOVERNANCE_CAPABILITY_BY_OPERATION[
        samples[0]["labels"]["operation"]
    ]
    assert "board-1" not in repr(samples)
    assert "agent-1" not in repr(samples)


@pytest.mark.asyncio
async def test_rule_authoring_is_the_13th_pre_uow_capability_fence() -> None:
    reset_governance_metric_samples()
    catalog, uow_factory, calls = _registered_catalog(
        denied_capability=RULES_AUTHOR_BLOCKING
    )
    patch = GuidelineRevisionPatchInput.model_validate(
        {
            "rules": [
                {
                    "rule_id": "rule-1",
                    "code": "require_owner",
                    "title": "Owner required",
                    "description": "A spec must declare an owner.",
                    "target_entity_types": ["spec"],
                    "predicates": [
                        {
                            "predicate_code": "exists",
                            "parameters": {"fact": "resource_gate_ready"},
                        }
                    ],
                    "enforcement": "blocking",
                }
            ]
        }
    )

    outcome = await catalog._tool_manager._tools[
        "okto_pulse_create_guideline_revision"
    ].fn(
        board_id="board-1",
        guideline_id="guideline-1",
        idempotency_key="create-rule-1",
        patch=patch,
    )

    assert outcome.code == "permission_denied"
    assert calls == {"settings": 0, "provider": 0}
    assert uow_factory.factory_calls == uow_factory.enter_calls == 0
    samples = get_governance_metric_samples()
    assert [sample["labels"]["outcome"] for sample in samples] == [
        "allow",
        "deny",
    ]
    assert [sample["labels"]["capability"] for sample in samples] == [
        REVISIONS_CREATE,
        RULES_AUTHOR_BLOCKING,
    ]
    assert all(set(sample["labels"]) == {
        "surface",
        "operation",
        "capability",
        "outcome",
    } for sample in samples)


@pytest.mark.parametrize(
    "error",
    (
        CommandValidationError("invalid_input"),
        GuidelinePolicyInvalidCursor(),
        GuidelineRevisionUnderBump(
            minimum_bump=GuidelineVersionBump.MINOR,
            minimum_semantic_version="1.1.0",
            declared_semantic_version="1.0.1",
        ),
        PermissionDeniedError("denied"),
        EntityNotFoundError("guideline", "guideline-1"),
        GuidelinePolicyCasConflict(),
        GuidelinePolicyAdapterMissing(),
    ),
)
def test_mcp_error_outcome_preserves_canonical_projector_semantics(
    error: Exception,
) -> None:
    from okto_pulse.core.mcp.policy_governance_tools import _error_outcome

    projected = project_guideline_policy_error(error)
    outcome = _error_outcome(error)

    assert outcome.code == projected["code"]
    assert outcome.message == projected["message"]
    assert outcome.retryable is projected["retryable"]
    assert outcome.next_action == {"rel": projected["next_action"]}
    assert outcome.details == {
        "category": projected["category"],
        "status_category": projected["status_category"],
        "http_status": projected["http_status"],
        **projected["details"],
    }


def test_policy_resource_contract_and_pointer_cardinality() -> None:
    mcp_root = (
        Path(__file__).parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "mcp"
    )
    resource_root = mcp_root / "resources"
    all_resources = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(resource_root.rglob("*.md"))
    )
    assert all_resources.count("policy-compliance-resource/v1") == 1
    assert "The five policy list tools" not in all_resources

    pointer = "okto-pulse://reference/policy-compliance"
    pointer_files = (
        mcp_root / "agent_instructions.md",
        resource_root / "workflows" / "ideations.md",
        resource_root / "workflows" / "refinements.md",
        resource_root / "workflows" / "specs.md",
        resource_root / "workflows" / "sprints.md",
        resource_root / "workflows" / "cards.md",
        resource_root / "workflows" / "preflight.md",
        resource_root / "reference" / "transitions.md",
        resource_root / "reference" / "quality-assessments.md",
        resource_root / "reference" / "errors.md",
        resource_root / "reference" / "destructive_ops.md",
        resource_root / "reference" / "list_tools.md",
        resource_root / "reference" / "projection_profiles.md",
        resource_root / "reference" / "tool-docs" / "guideline.md",
        resource_root / "reference" / "tool-docs" / "test-scenario.md",
    )
    for path in pointer_files:
        assert path.read_text(encoding="utf-8").count(pointer) == 1, path
    assert not (resource_root / "workflows" / "test-scenario.md").exists()


@pytest.mark.asyncio
async def test_legacy_unlink_denial_precedes_uow_factory_and_enter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server

    reset_governance_metric_samples()
    calls = {"provider": 0, "factory": 0, "enter": 0}

    class Permissions:
        def check(self, capability: str) -> str | None:
            if capability == ADOPTION_MANAGE:
                return "adoption denied"
            return None

    async def get_agent(_board_id: str) -> object:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            realm_id=LOCAL_REALM_ID,
            permissions=Permissions(),
        )

    class ForbiddenFactory:
        def __call__(self, *, actor: object) -> object:
            del actor
            calls["factory"] += 1
            return self

        async def __aenter__(self) -> object:
            calls["enter"] += 1
            raise AssertionError("legacy unlink entered UoW after denial")

        async def __aexit__(self, *_args: object) -> None:
            return None

    factory = ForbiddenFactory()

    def get_uow() -> object:
        calls["provider"] += 1
        return factory

    monkeypatch.setattr(server, "_get_agent_ctx", get_agent)
    monkeypatch.setattr(
        server,
        "check_permission",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("legacy unlink permission fence was called")
        ),
    )
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", get_uow)

    raw = await server.mcp._tool_manager._tools[
        "okto_pulse_unlink_guideline_from_board"
    ].fn(board_id="board-1", guideline_id="guideline-1")

    assert "adoption denied" in json.loads(raw)["error"]
    assert calls == {"provider": 0, "factory": 0, "enter": 0}
    assert get_governance_metric_samples()[-1]["labels"] == {
        "surface": "mcp",
        "operation": "unlink_guideline",
        "capability": ADOPTION_MANAGE,
        "outcome": "deny",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "arguments", "capability", "operation"),
    (
        (
            "okto_pulse_create_guideline",
            {
                "board_id": "board-1",
                "title": "Policy",
                "content": "Content",
            },
            REVISIONS_CREATE,
            "create_guideline",
        ),
        (
            "okto_pulse_update_guideline",
            {"board_id": "board-1", "guideline_id": "guideline-1"},
            REVISIONS_CREATE,
            "update_guideline",
        ),
        (
            "okto_pulse_delete_guideline",
            {"board_id": "board-1", "guideline_id": "guideline-1"},
            REVISIONS_RETIRE,
            "delete_guideline",
        ),
        (
            "okto_pulse_link_guideline_to_board",
            {"board_id": "board-1", "guideline_id": "guideline-1"},
            ADOPTION_MANAGE,
            "link_guideline",
        ),
        (
            "okto_pulse_update_board_guideline_priority",
            {
                "board_id": "board-1",
                "guideline_id": "guideline-1",
                "priority": "3",
            },
            ADOPTION_MANAGE,
            "update_guideline_priority",
        ),
    ),
)
async def test_legacy_guideline_mutation_denial_precedes_uow(
    monkeypatch: pytest.MonkeyPatch,
    tool_name: str,
    arguments: dict[str, str],
    capability: str,
    operation: str,
) -> None:
    from okto_pulse.core.mcp import server

    reset_governance_metric_samples()
    calls = {"provider": 0}

    class Permissions:
        def check(self, required: str) -> str | None:
            return "explicit custom deny" if required == capability else None

    async def get_agent(_board_id: str) -> object:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            realm_id=LOCAL_REALM_ID,
            permissions=Permissions(),
        )

    def forbidden_uow() -> object:
        calls["provider"] += 1
        raise AssertionError("denied legacy mutation resolved a UoW")

    monkeypatch.setattr(server, "_get_agent_ctx", get_agent)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        forbidden_uow,
    )

    raw = await server.mcp._tool_manager._tools[tool_name].fn(**arguments)

    assert "explicit custom deny" in json.loads(raw)["error"]
    assert calls == {"provider": 0}
    assert get_governance_metric_samples()[-1]["labels"] == {
        "surface": "mcp",
        "operation": operation,
        "capability": capability,
        "outcome": "deny",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command", "capability"),
    (
        pytest.param(
            "create",
            SimpleNamespace(data=SimpleNamespace(board_id=None)),
            REVISIONS_CREATE,
            id="create",
        ),
        pytest.param(
            "update",
            SimpleNamespace(guideline_id="guideline-1", data=SimpleNamespace()),
            REVISIONS_CREATE,
            id="update",
        ),
        pytest.param(
            "delete",
            SimpleNamespace(guideline_id="guideline-1"),
            REVISIONS_RETIRE,
            id="retire",
        ),
        pytest.param(
            "link",
            SimpleNamespace(
                board_id="board-1",
                data=SimpleNamespace(guideline_id="guideline-1"),
            ),
            ADOPTION_MANAGE,
            id="link",
        ),
        pytest.param(
            "priority",
            SimpleNamespace(
                board_id="board-1",
                guideline_id="guideline-1",
                priority=3,
            ),
            ADOPTION_MANAGE,
            id="priority",
        ),
    ),
)
async def test_legacy_guideline_use_cases_repeat_capability_before_uow(
    use_case: str,
    command: object,
    capability: str,
) -> None:
    from okto_pulse.core.application.use_cases.guidelines_crud import (
        CreateGuidelineUseCase,
        DeleteGuidelineUseCase,
        LinkOrCreateBoardGuidelineUseCase,
        UpdateBoardGuidelinePriorityUseCase,
        UpdateGuidelineUseCase,
    )

    implementations = {
        "create": CreateGuidelineUseCase(),
        "update": UpdateGuidelineUseCase(),
        "delete": DeleteGuidelineUseCase(),
        "link": LinkOrCreateBoardGuidelineUseCase(),
        "priority": UpdateBoardGuidelinePriorityUseCase(),
    }

    class Permissions:
        def check(self, required: str) -> str | None:
            return "denied" if required == capability else None

    class UntouchableUow:
        @property
        def services(self) -> object:
            raise AssertionError("denied mutation touched UoW services")

    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id="board-1",
        realm_id=LOCAL_REALM_ID,
        permissions=Permissions(),
    )

    with pytest.raises(PermissionDeniedError):
        await implementations[use_case].execute(
            command,
            actor=actor,
            uow=UntouchableUow(),
        )


@pytest.mark.asyncio
async def test_legacy_unlink_allow_preserves_envelope_and_one_uow(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import server

    calls = {
        "provider": 0,
        "factory": 0,
        "enter": 0,
        "write": 0,
        "commit": 0,
    }

    class Permissions:
        def check(self, _capability: str) -> None:
            return None

    async def get_agent(_board_id: str) -> object:
        return SimpleNamespace(
            agent_id="agent-1",
            agent_name="Agent",
            realm_id=LOCAL_REALM_ID,
            permissions=Permissions(),
        )

    class Guidelines:
        async def unlink_guideline_from_board(self, *_args: object, **_kwargs: object) -> bool:
            calls["write"] += 1
            return True

    class Uow:
        services = SimpleNamespace(guidelines=Guidelines())

        async def commit(self) -> None:
            calls["commit"] += 1

        async def __aenter__(self) -> "Uow":
            calls["enter"] += 1
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

    class Factory:
        def __call__(self, *, actor: object) -> Uow:
            del actor
            calls["factory"] += 1
            return Uow()

    factory = Factory()

    def get_uow() -> object:
        calls["provider"] += 1
        return factory

    monkeypatch.setattr(server, "_get_agent_ctx", get_agent)
    monkeypatch.setattr(
        server,
        "check_permission",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("legacy unlink permission fence was called")
        ),
    )
    monkeypatch.setattr(server, "get_unit_of_work_factory_for_mcp", get_uow)

    raw = await server.mcp._tool_manager._tools[
        "okto_pulse_unlink_guideline_from_board"
    ].fn(board_id="board-1", guideline_id="guideline-1")

    assert json.loads(raw) == {"success": True}
    assert calls == {
        "provider": 1,
        "factory": 1,
        "enter": 1,
        "write": 1,
        "commit": 1,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("use_case", "command"),
    (
        (
            McpUnlinkGuidelineFromBoardUseCase(),
            McpUnlinkGuidelineFromBoardCommand("board-1", "guideline-1"),
        ),
        (
            UnlinkBoardGuidelineUseCase(),
            UnlinkBoardGuidelineCommand("board-1", "guideline-1"),
        ),
    ),
)
async def test_unlink_use_cases_repeat_capability_check_before_uow_access(
    use_case: object,
    command: object,
) -> None:
    class Permissions:
        def check(self, capability: str) -> str | None:
            return "denied" if capability == ADOPTION_MANAGE else None

    class UntouchableUow:
        @property
        def services(self) -> object:
            raise AssertionError("denied unlink touched UoW services")

    actor = ActorContext(
        "agent-1",
        "mcp",
        board_id="board-1",
        realm_id=LOCAL_REALM_ID,
        permissions=Permissions(),
    )

    with pytest.raises(PermissionDeniedError):
        await use_case.execute(command, actor=actor, uow=UntouchableUow())

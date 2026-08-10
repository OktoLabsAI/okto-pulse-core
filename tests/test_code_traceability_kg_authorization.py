"""P1 confidentiality coverage for generic Code Traceability KG surfaces."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from okto_pulse.core.application.use_cases.base import (
    ActorContext,
    PermissionDeniedError,
)
from okto_pulse.core.application.use_cases.code_traceability_kg_access import (
    EvaluateCodeTraceabilityKGReadAccessUseCase,
    mask_code_traceability_graph_metrics,
    require_code_traceability_safe_arbitrary_query,
)
from okto_pulse.core.domain.code_traceability_kg import (
    CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH,
    CODE_TRACEABILITY_KG_READ_PERMISSIONS,
    CODE_TRACEABILITY_KG_SUBTYPES,
    CODE_TRACEABILITY_KG_WRITE_FIELDS,
    CodeTraceabilityKGWriteViolation,
    code_traceability_kg_read_decision,
    require_code_traceability_candidate_writer,
)
from okto_pulse.core.domain.permissions import PermissionSet


BOARD_ID = "board-code-traceability-kg"


def _permission_document(
    *,
    ct_read: bool,
    power: bool = True,
) -> dict[str, Any]:
    return {
        "board": {"read": True},
        "kg": {
            "admin": {"settings_read": True},
            "power": {"cypher": power, "natural": power},
            "query": {"related_context": True},
            "operations": {"audit": {"read": True}},
            "session": {
                "get_similar": True,
                "propose": True,
                "commit": True,
            },
        },
        "code_traceability": {
            "investigation": {"read": ct_read},
            "evidence": {"read": ct_read},
            "target": {"read": ct_read},
            "overlap": {"read": ct_read},
        },
    }


def _actor(*, ct_read: bool) -> ActorContext:
    return ActorContext(
        "agent-ct-kg",
        "mcp",
        board_id=BOARD_ID,
        permissions=PermissionSet(_permission_document(ct_read=ct_read)),
    )


@pytest.mark.asyncio
async def test_all_four_code_traceability_leaves_are_required() -> None:
    use_case = EvaluateCodeTraceabilityKGReadAccessUseCase()

    granted = await use_case.execute(actor=_actor(ct_read=True), board_id=BOARD_ID)
    denied = await use_case.execute(actor=_actor(ct_read=False), board_id=BOARD_ID)

    assert granted.allowed is True
    assert granted.missing_permissions == ()
    assert denied.allowed is False
    assert denied.missing_permissions == CODE_TRACEABILITY_KG_READ_PERMISSIONS


@pytest.mark.asyncio
async def test_arbitrary_query_guard_denies_without_complete_ct_authority() -> None:
    decision = await EvaluateCodeTraceabilityKGReadAccessUseCase().execute(
        actor=_actor(ct_read=False),
        board_id=BOARD_ID,
    )

    with pytest.raises(PermissionDeniedError):
        require_code_traceability_safe_arbitrary_query(decision)


def test_health_mask_removes_graph_counts_but_preserves_stop_rule() -> None:
    denied = code_traceability_kg_read_decision(())
    payload = {
        "graph_state": "healthy",
        "discovery_state": "healthy",
        "overall_state": "healthy",
        "metric_status": "ok",
        "total_nodes": 9,
        "default_score_ratio": 0.4,
        "operational_domains": {
            "active_queue": {"count": 2},
            "dead_letter": {"count": 1},
        },
        "root_cause": {"materialized_node_count": 9},
    }

    masked = mask_code_traceability_graph_metrics(payload, denied)

    assert masked["graph_state"] == "healthy"
    assert masked["discovery_state"] == "healthy"
    assert masked["overall_state"] == "healthy"
    assert masked["metric_status"] == "unavailable"
    assert "total_nodes" not in masked
    assert "default_score_ratio" not in masked
    assert "materialized_node_count" not in masked["root_cause"]
    assert masked["operational_domains"]["active_queue"]["count"] == 2
    assert masked["operational_domains"]["dead_letter"]["count"] == 1
    assert (
        masked["code_traceability_metric_visibility"]["reason"]
        == "code_traceability_kg_read_permissions_missing"
    )


def test_health_mask_is_identity_for_complete_ct_authority() -> None:
    granted = code_traceability_kg_read_decision(
        CODE_TRACEABILITY_KG_READ_PERMISSIONS
    )
    payload = {"graph_state": "healthy", "total_nodes": 9}

    assert mask_code_traceability_graph_metrics(payload, granted) is payload


@pytest.mark.parametrize("field", CODE_TRACEABILITY_KG_WRITE_FIELDS)
def test_generic_candidate_cannot_forge_any_ct_metadata_field(field: str) -> None:
    candidate = {
        "candidate_id": "forged",
        "node_type": "Entity",
        "kind_of": None,
        field: "forged-value",
    }

    with pytest.raises(CodeTraceabilityKGWriteViolation):
        require_code_traceability_candidate_writer(
            candidate,
            writer_path="commit_consolidation",
        )


@pytest.mark.parametrize("kind_of", CODE_TRACEABILITY_KG_SUBTYPES)
def test_ct_subtypes_are_reserved_but_internal_worker_is_admitted(
    kind_of: str,
) -> None:
    candidate = {
        "candidate_id": "ct",
        "node_type": "Entity",
        "kind_of": kind_of,
        "source_ref": "provider://source",
    }

    with pytest.raises(CodeTraceabilityKGWriteViolation):
        require_code_traceability_candidate_writer(
            candidate,
            writer_path="commit_consolidation",
        )
    require_code_traceability_candidate_writer(
        candidate,
        writer_path=CODE_TRACEABILITY_DETERMINISTIC_WRITER_PATH,
    )


@pytest.mark.asyncio
async def test_generic_begin_rejects_ct_candidate_before_session_staging() -> None:
    from okto_pulse.core.kg.primitives import KGPrimitiveError, begin_consolidation
    from okto_pulse.core.kg.schemas import BeginConsolidationRequest, NodeCandidate

    request = BeginConsolidationRequest(
        board_id=BOARD_ID,
        artifact_type="spec",
        artifact_id="spec-1",
        raw_content="content",
        deterministic_candidates=[
            NodeCandidate(
                candidate_id="forged-ct",
                node_type="Entity",
                title="forged",
                kind_of="code_evidence",
            )
        ],
    )

    with pytest.raises(KGPrimitiveError) as exc_info:
        await begin_consolidation(request, agent_id="cognitive-agent")

    assert exc_info.value.code == "code_traceability_projection_reserved"


def test_generic_reconciliation_rejects_existing_ct_target_before_mutation() -> None:
    from okto_pulse.core.kg import primitives
    from okto_pulse.core.kg.primitives import KGPrimitiveError
    from okto_pulse.core.kg.schemas import NodeCandidate, ReconciliationHint

    class _Result:
        rows = (("implementation_target",),)

    class _Scope:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement: str, _params: dict) -> _Result:
            self.statements.append(statement)
            assert "SET " not in statement
            return _Result()

    scope = _Scope()
    candidate = NodeCandidate(
        candidate_id="legacy-candidate",
        node_type="Entity",
        title="legacy",
    )
    hint = ReconciliationHint(
        candidate_id="legacy-candidate",
        operation="UPDATE",
        target_node_id="ct-node",
        confidence=1.0,
        reason="manual override",
    )

    with pytest.raises(KGPrimitiveError) as exc_info:
        primitives._require_no_code_traceability_existing_targets(
            scope,
            node_candidates={candidate.candidate_id: candidate},
            effective_hints={candidate.candidate_id: hint},
            agent_id="cognitive-agent",
            session_id="session-ct",
        )

    assert exc_info.value.code == "code_traceability_projection_reserved"
    assert len(scope.statements) == 1


@pytest.mark.asyncio
async def test_generic_boost_denies_ct_before_set_or_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.domain.code_traceability_kg import (
        CodeTraceabilityKGWriteViolation,
    )
    from okto_pulse.core.kg import governance
    from okto_pulse.core.kg.interfaces import registry as registry_module
    from okto_pulse.core.kg.interfaces.graph_transaction import GraphStatementResult

    class _Scope:
        def __init__(self) -> None:
            self.statements: list[str] = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def execute(self, statement: str, _params: dict) -> GraphStatementResult:
            self.statements.append(statement)
            if "SET " in statement:
                raise AssertionError("CT boost reached SET")
            if "MATCH (n:Entity" in statement:
                return GraphStatementResult.from_rows(
                    [[0.5, None, None, "code_evidence"]]
                )
            return GraphStatementResult()

    class _Transaction:
        def __init__(self, scope: _Scope) -> None:
            self.scope = scope

        async def begin(self, _board_id: str) -> _Scope:
            return self.scope

    scope = _Scope()
    monkeypatch.setattr(
        registry_module,
        "get_kg_registry",
        lambda: SimpleNamespace(graph_transaction=_Transaction(scope)),
    )

    with pytest.raises(CodeTraceabilityKGWriteViolation):
        await governance.mutate_boost_node_graph(
            BOARD_ID,
            "ct-node",
            actor_id="cognitive-agent",
        )

    assert scope.statements
    assert all("SET " not in statement for statement in scope.statements)


def test_bounded_query_templates_exclude_ct_before_limit_and_count() -> None:
    from okto_pulse.core.kg import cypher_templates as tpl

    templates = (
        tpl.GET_ALL_NODES,
        tpl.GET_ALL_NODES_BY_TYPE,
        tpl.GET_ALL_NODES_AFTER_CURSOR,
        tpl.GET_ALL_NODES_BY_TYPE_AFTER_CURSOR,
        tpl.COUNT_ALL_NODES,
        tpl.COUNT_ALL_NODES_BY_TYPE,
        tpl.GET_RELATED_CONTEXT,
    )
    for template in templates:
        assert "$include_code_traceability" in template
        for subtype in CODE_TRACEABILITY_KG_SUBTYPES:
            assert subtype in template


class _Catalog:
    def __init__(self) -> None:
        self.tools: dict[str, Any] = {}

    def tool(self):
        def _decorate(func):
            self.tools[func.__name__] = func
            return func

        return _decorate


def _mcp_context(*, ct_read: bool) -> SimpleNamespace:
    return SimpleNamespace(
        agent_id="agent-ct-kg",
        permissions=PermissionSet(_permission_document(ct_read=ct_read)),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "arguments"),
    (
        (
            "okto_pulse_kg_get_similar_nodes",
            {"session_id": "session-ct", "candidate_id": "candidate"},
        ),
        (
            "okto_pulse_kg_propose_reconciliation",
            {"session_id": "session-ct"},
        ),
        (
            "okto_pulse_kg_commit_consolidation",
            {"session_id": "session-ct"},
        ),
    ),
)
async def test_session_graph_tools_deny_before_provider_without_ct_grants(
    tool_name: str,
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_tools

    async def _agent():
        return SimpleNamespace(id="agent-ct-kg")

    async def _board_agent(_board_id: str):
        return _mcp_context(ct_read=False)

    async def _session(*_args, **_kwargs):
        return SimpleNamespace(board_id=BOARD_ID)

    async def _unexpected(*_args, **_kwargs):
        raise AssertionError("session graph provider ran after CT denial")

    def _unexpected_uow():
        raise AssertionError("session UoW opened after CT denial")

    monkeypatch.setattr(kg_tools, "_require_open_session", _session)
    monkeypatch.setattr(kg_tools, "get_similar_nodes", _unexpected)
    catalog = _Catalog()
    kg_tools.register_kg_tools(
        catalog,
        get_agent=_agent,
        get_uow=_unexpected_uow,
        get_board_agent=_board_agent,
    )

    payload = json.loads(await catalog.tools[tool_name](**arguments))

    assert payload["error"]["code"] == "permission_denied"
    assert set(payload["error"]["required_permissions"]) == set(
        CODE_TRACEABILITY_KG_READ_PERMISSIONS
    )


@pytest.mark.asyncio
async def test_session_similarity_runs_with_complete_ct_read_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_tools

    async def _agent():
        return SimpleNamespace(id="agent-ct-kg")

    async def _board_agent(_board_id: str):
        return _mcp_context(ct_read=True)

    async def _session(*_args, **_kwargs):
        return SimpleNamespace(board_id=BOARD_ID)

    class _Response:
        def model_dump_json(self) -> str:
            return json.dumps(
                {
                    "session_id": "session-ct",
                    "candidate_id": "candidate",
                    "similar": [],
                }
            )

    called = False

    async def _similar(*_args, **_kwargs):
        nonlocal called
        called = True
        return _Response()

    monkeypatch.setattr(kg_tools, "_require_open_session", _session)
    monkeypatch.setattr(kg_tools, "get_similar_nodes", _similar)
    catalog = _Catalog()
    kg_tools.register_kg_tools(
        catalog,
        get_agent=_agent,
        get_uow=lambda: None,
        get_board_agent=_board_agent,
    )

    payload = json.loads(
        await catalog.tools["okto_pulse_kg_get_similar_nodes"](
            session_id="session-ct",
            candidate_id="candidate",
        )
    )

    assert called is True
    assert payload["similar"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool_name", "arguments"),
    (
        (
            "okto_pulse_kg_query_cypher",
            {"board_id": BOARD_ID, "cypher": "MATCH (n) RETURN n"},
        ),
        (
            "okto_pulse_kg_query_natural",
            {"board_id": BOARD_ID, "nl_query": "traceability"},
        ),
        (
            "okto_pulse_kg_query_reflective",
            {"board_id": BOARD_ID, "nl_query": "traceability"},
        ),
    ),
)
async def test_arbitrary_mcp_queries_stop_before_provider_without_ct_grants(
    tool_name: str,
    arguments: dict[str, Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_power_tools

    async def _agent():
        return SimpleNamespace(id="agent-ct-kg")

    async def _board_agent(_board_id: str):
        return _mcp_context(ct_read=False)

    def _unexpected(*_args, **_kwargs):
        raise AssertionError("query provider ran after CT authorization denial")

    monkeypatch.setattr(kg_power_tools, "execute_cypher_read_only", _unexpected)
    monkeypatch.setattr(kg_power_tools, "execute_natural_query", _unexpected)
    catalog = _Catalog()
    kg_power_tools.register_kg_power_tools(
        catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    payload = json.loads(await catalog.tools[tool_name](**arguments))

    assert payload["error"]["code"] == "permission_denied"
    assert set(payload["error"]["required_permissions"]) == set(
        CODE_TRACEABILITY_KG_READ_PERMISSIONS
    )


@pytest.mark.asyncio
async def test_arbitrary_mcp_query_runs_with_complete_ct_read_grant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.mcp import kg_power_tools

    async def _agent():
        return SimpleNamespace(id="agent-ct-kg")

    async def _board_agent(_board_id: str):
        return _mcp_context(ct_read=True)

    monkeypatch.setattr(
        kg_power_tools,
        "execute_cypher_read_only",
        lambda *_args, **_kwargs: {
            "columns": ["value"],
            "rows": [["legacy"]],
            "row_count": 1,
        },
    )
    catalog = _Catalog()
    kg_power_tools.register_kg_power_tools(
        catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    payload = json.loads(
        await catalog.tools["okto_pulse_kg_query_cypher"](
            board_id=BOARD_ID,
            cypher="MATCH (n) RETURN n.title AS value",
        )
    )

    assert payload["rows"] == [["legacy"]]


@pytest.mark.asyncio
@pytest.mark.parametrize("ct_read", (False, True))
async def test_provenance_drift_requires_complete_ct_authority_before_provider(
    ct_read: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import provenance_drift
    from okto_pulse.core.mcp import kg_power_tools

    async def _agent():
        return SimpleNamespace(id="agent-ct-kg")

    async def _board_agent(_board_id: str):
        return _mcp_context(ct_read=ct_read)

    called = False

    async def _report(*_args, **_kwargs):
        nonlocal called
        called = True
        return {
            "checked_count": 0,
            "drifted_count": 0,
            "skipped_count": 0,
            "drifted": [],
        }

    monkeypatch.setattr(provenance_drift, "provenance_drift_report", _report)
    catalog = _Catalog()
    kg_power_tools.register_kg_power_tools(
        catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    payload = json.loads(
        await catalog.tools["okto_pulse_kg_provenance_drift"](
            board_id=BOARD_ID,
        )
    )

    assert called is ct_read, payload
    if ct_read:
        assert payload["drifted"] == []
    else:
        assert payload["error"]["code"] == "permission_denied"


@pytest.mark.asyncio
async def test_related_context_and_export_filter_ct_for_explicit_deny(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.kg import graph_export
    from okto_pulse.core.mcp import kg_export_tools, kg_query_tools

    async def _agent():
        return SimpleNamespace(id="agent-ct-kg")

    async def _board_agent(_board_id: str):
        return _mcp_context(ct_read=False)

    async def _user_boards(*_args, **_kwargs):
        return SimpleNamespace(id="agent-ct-kg"), [BOARD_ID]

    observed: dict[str, bool] = {}

    class _Service:
        def check_board_access(self, _boards, _board_id):
            return None

        def get_related_context(self, *_args, **kwargs):
            observed["related_context"] = kwargs["include_code_traceability"]
            return []

    def _export(*_args, **kwargs):
        observed["export"] = kwargs["include_code_traceability"]
        return {"@graph": []}

    monkeypatch.setattr(kg_query_tools, "_get_user_boards", _user_boards)
    monkeypatch.setattr(kg_query_tools, "get_kg_service", lambda: _Service())
    monkeypatch.setattr(graph_export, "export_board_jsonld", _export)

    query_catalog = _Catalog()
    export_catalog = _Catalog()
    kg_query_tools.register_kg_query_tools(
        query_catalog,
        get_agent=_agent,
        get_uow=lambda: None,
        get_board_agent=_board_agent,
    )
    kg_export_tools.register_kg_export_tools(
        export_catalog,
        get_agent=_agent,
        get_board_agent=_board_agent,
    )

    related_payload = json.loads(
        await query_catalog.tools["okto_pulse_kg_get_related_context"](
            board_id=BOARD_ID,
            artifact_id="spec:11111111-1111-1111-1111-111111111111",
        )
    )
    export_payload = json.loads(
        await export_catalog.tools["okto_pulse_kg_export_jsonld"](
            board_id=BOARD_ID,
        )
    )

    assert related_payload["count"] == 0
    assert export_payload["@graph"] == []
    assert observed == {"related_context": False, "export": False}

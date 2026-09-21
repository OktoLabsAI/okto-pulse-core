from types import SimpleNamespace as NS
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.application.use_cases.base import ActorContext, EntityNotFoundError
from okto_pulse.core.application.use_cases.kg_node_source import ResolveKGNodeSourceUseCase
from okto_pulse.core.domain.permissions import PermissionSet


@pytest.fixture
def scope():
    entity = NS(id="owner", board_id="board-a", title="Source title", card_type="test")
    services = NS(**{
        plural: NS(**{method: AsyncMock(return_value=entity)})
        for plural, method in [
            ("specs", "get_spec"), ("refinements", "get_refinement"),
            ("ideations", "get_ideation"), ("sprints", "get_sprint"),
            ("stories", "get_story"), ("cards", "get_card"),
        ]
    })
    services.code_investigations = NS(get_receipt=AsyncMock(return_value=NS(
        board_id="board-a", subject_type="refinement", subject_id="owner", subject_version=7,
    )))
    services.code_traceability = NS(
        get_evidence=AsyncMock(return_value=NS(board_id="board-a", parent_type="spec", parent_id="owner", parent_version=3)),
        get_target=AsyncMock(return_value=NS(board_id="board-a", card_id="owner")),
    )
    services.shares = NS(get_share_permission=AsyncMock(return_value=None))
    return NS(
        boards=NS(get=AsyncMock(return_value=NS(id="board-a", owner_id="reader", realm_id=None))),
        services=services, commit=AsyncMock(), rollback=AsyncMock(), entity=entity,
    )


async def resolve(scope, ref, actor=None):
    return await ResolveKGNodeSourceUseCase().execute(
        board_id="board-a", source_artifact_ref=ref,
        actor=actor or ActorContext("reader", "system", board_id="board-a"), uow=scope,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["spec", "refinement", "ideation", "story", "card", "task", "test", "bug"])
async def test_direct_owners(scope, kind):
    result = await resolve(scope, f"{kind}:owner")
    assert result.status == "resolved"
    assert result.target.entity_id == "owner"
    assert result.target.board_id == "board-a"
    assert result.target.entity_type == ("card" if kind in {"card", "task", "test", "bug"} else kind)
    scope.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize('ref', ['sprint:retired', 'sprint:retired:decision:old'], ids=['direct', 'concept'])
async def test_retired_sprint_source_preserves_opaque_reference_without_live_lookup(scope, ref):
    del scope.services.sprints
    result = await resolve(scope, ref)
    assert result.status == 'unsupported'
    assert result.source_artifact_ref == ref
    assert result.target is None
    scope.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_indirect_evidence_with_retired_sprint_owner_does_not_resolve_live_target(scope):
    del scope.services.sprints
    scope.services.code_traceability.get_evidence.return_value.parent_type = 'sprint'
    result = await resolve(scope, 'code_evidence:e1')
    assert result.status == 'unsupported'
    assert result.source_artifact_ref == 'code_evidence:e1'
    assert result.target is None


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["decision:bounded-native", "fr:fr-1", "requirement:r1", "alternative:hash", "bug:some-other-id"])
async def test_concept_suffix_is_not_the_owner_id(scope, suffix):
    result = await resolve(scope, f"spec:owner:{suffix}")
    assert result.target.entity_id == "owner"
    scope.services.specs.get_spec.assert_awaited_once_with("owner")


@pytest.mark.asyncio
async def test_governed_explicit_bug_alias(scope):
    ident = "9b5105f6-7fac-42d8-912b-8cb398e6987f"
    result = await resolve(scope, f"card:bug:{ident}:learning:hash")
    assert result.target.entity_type == "card"
    assert result.target.entity_id == ident


@pytest.mark.asyncio
@pytest.mark.parametrize(("ref", "kind", "version"), [
    ("code_investigation_receipt:r1", "refinement", 7),
    ("code_evidence:e1", "spec", 3),
    ("implementation_target:t1", "card", None),
])
async def test_indirect_records_resolve_their_declared_owner(scope, ref, kind, version):
    result = await resolve(scope, ref)
    assert result.target.entity_type == kind
    assert result.target.entity_id == "owner"
    assert result.target.source_version == version
    assert result.source_artifact_ref == ref
    scope.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ref", ["opaque", "source:opaque", "final_report:technical", "board:b1", "spec:", " spec:owner", "spec:o\n", "x" * 2049, "implementation_target:t1:extra"], ids=["opaque", "code-source", "report", "board", "empty-id", "whitespace", "control-char", "too-long", "indirect-suffix"])
async def test_unknown_or_infrastructure_refs_are_not_guessed(scope, ref):
    assert (await resolve(scope, ref)).status == "unsupported"
    scope.services.specs.get_spec.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ref", [None, ""])
async def test_missing_ref(scope, ref):
    assert (await resolve(scope, ref)).status == "missing_source"


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_missing_and_cross_board_owners_share_non_enumerable_result(scope, missing):
    scope.entity.board_id = "board-b"
    if missing:
        scope.services.specs.get_spec.return_value = None
    result = await resolve(scope, "spec:owner")
    assert result.status == "unavailable"
    assert result.target is None


@pytest.mark.asyncio
async def test_forbidden_owner_does_not_fetch_or_disclose_title(scope):
    actor = ActorContext("reader", "system", board_id="board-a", permissions=PermissionSet({"spec": {"entity": {"read": False}}}))
    result = await resolve(scope, "spec:owner", actor)
    assert result.status == "unavailable"
    assert result.target is None
    scope.services.specs.get_spec.assert_not_awaited()


@pytest.mark.asyncio
async def test_traceability_permission_denial_precedes_record_read(scope):
    actor = ActorContext("reader", "system", board_id="board-a", permissions=PermissionSet({}))
    assert (await resolve(scope, "code_investigation_receipt:r1", actor)).status == "unavailable"
    scope.services.code_investigations.get_receipt.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("ref,store,method", [
    ("code_investigation_receipt:r1", "code_investigations", "get_receipt"),
    ("code_evidence:e1", "code_traceability", "get_evidence"),
    ("implementation_target:t1", "code_traceability", "get_target"),
])
async def test_cross_board_indirect_record_fails_closed(scope, ref, store, method):
    getattr(getattr(scope.services, store), method).return_value.board_id = "board-b"
    result = await resolve(scope, ref)
    assert result.status == "unavailable"
    assert result.target is None


@pytest.mark.asyncio
async def test_board_authority_and_realm_are_checked(scope):
    for actor in [ActorContext("other", "rest"), ActorContext("reader", "system", board_id="board-b"), ActorContext("reader", "system", realm_id="another-realm")]:
        with pytest.raises(EntityNotFoundError):
            await resolve(scope, "spec:owner", actor)
    scope.services.specs.get_spec.assert_not_awaited()


@pytest.mark.asyncio
async def test_operational_failures_are_not_reported_as_missing_source(scope):
    scope.services.specs.get_spec.side_effect = RuntimeError("read failed")
    with pytest.raises(RuntimeError, match="read failed"):
        await resolve(scope, "spec:owner")

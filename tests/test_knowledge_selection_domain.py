from __future__ import annotations

from dataclasses import FrozenInstanceError
import json

import pytest

from okto_pulse.core.domain.knowledge_selection import (
    KNOWLEDGE_PROPAGATION_CONTRACT_VERSION,
    KnowledgeAssignment,
    KnowledgeAssignmentState,
    KnowledgeOriginClass,
    KnowledgePropagationContractError,
    KnowledgePropagationMode,
    KnowledgeRelevanceEntityType,
    KnowledgeRelevanceLink,
    KnowledgeSelection,
    KnowledgeSelectionState,
    KnowledgeTargetType,
)
from okto_pulse.core.domain.resource_revision import (
    ResourceRevisionStamp as DomainResourceRevisionStamp,
)
from okto_pulse.core.domain.knowledge_fingerprint import knowledge_content_sha256
from okto_pulse.core.services import (
    ResolvedResourceLineageProjection,
    ResourceRevisionStamp as ServicesResourceRevisionStamp,
)
from okto_pulse.core.services.resource_lineage import ResourceRevisionStamp


def _assignment(**overrides: object) -> KnowledgeAssignment:
    values: dict[str, object] = {
        "assignment_id": " assignment-1 ",
        "board_id": " board-1 ",
        "target_type": " card ",
        "target_id": " card-1 ",
        "source_knowledge_id": " kb-1 ",
        "revision_stamp": ResourceRevisionStamp(
            root_id=" kb-root ",
            immediate_parent_id=" kb-parent ",
            source_revision=" 7 ",
            source_content_sha256=f" {'a' * 64} ",
        ),
        "mode": "reference",
        "state": "active",
        "origin_class": "v2",
        "actor_id": " agent-1 ",
        "revision": 1,
        "justification": " relevant to AC ",
        "relevance_links": (
            KnowledgeRelevanceLink("acceptance_criterion", " ac_1 "),
        ),
    }
    values.update(overrides)
    return KnowledgeAssignment(**values)  # type: ignore[arg-type]


def test_selection_enums_expose_the_v2_wire_values() -> None:
    assert [item.value for item in KnowledgeSelectionState] == [
        "omitted",
        "explicit_empty",
        "explicit_ids",
    ]
    assert [item.value for item in KnowledgePropagationMode] == [
        "reference",
        "snapshot",
        "drop",
    ]
    assert [item.value for item in KnowledgeAssignmentState] == [
        "active",
        "stale",
        "source_deleted",
        "dropped",
        "inactive",
    ]
    assert [item.value for item in KnowledgeOriginClass] == [
        "v2",
        "legacy_all",
        "selected_legacy",
        "legacy_unresolved",
    ]
    assert [item.value for item in KnowledgeTargetType] == ["spec", "card"]


def test_selection_preserves_tri_state_and_canonicalizes_ids_as_a_set() -> None:
    omitted = KnowledgeSelection.omitted()
    explicit_empty = KnowledgeSelection.explicit_empty()
    selected = KnowledgeSelection.explicit_ids(
        [" kb-b ", "kb-a", "kb-b", " kb-c "],
        mode="snapshot",
    )

    assert omitted.as_dict() == {
        "contract_version": 2,
        "selection_state": "omitted",
        "knowledge_ids": [],
        "mode": None,
    }
    assert explicit_empty.as_dict() == {
        "contract_version": 2,
        "selection_state": "explicit_empty",
        "knowledge_ids": [],
        "mode": "drop",
    }
    assert selected.knowledge_ids == ("kb-a", "kb-b", "kb-c")
    assert selected.mode is KnowledgePropagationMode.SNAPSHOT
    assert json.loads(json.dumps(selected.as_dict())) == selected.as_dict()


@pytest.mark.parametrize(
    ("state", "ids", "mode", "code"),
    [
        ("omitted", ("kb-1",), None, "omitted_selection_must_be_empty"),
        ("omitted", (), "reference", "omitted_selection_must_be_empty"),
        ("explicit_empty", (), None, "explicit_empty_requires_drop"),
        ("explicit_empty", ("kb-1",), "drop", "explicit_empty_requires_drop"),
        (
            "explicit_ids",
            (),
            "reference",
            "explicit_ids_require_ids_and_mode",
        ),
        ("explicit_ids", ("kb-1",), None, "explicit_ids_require_ids_and_mode"),
    ],
)
def test_selection_rejects_incoherent_state(
    state: str,
    ids: tuple[str, ...],
    mode: str | None,
    code: str,
) -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        KnowledgeSelection(
            selection_state=state,  # type: ignore[arg-type]
            knowledge_ids=ids,
            mode=mode,  # type: ignore[arg-type]
        )

    assert raised.value.code == code


def test_selection_rejects_empty_ids_but_allows_explicit_drop_ids() -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        KnowledgeSelection.explicit_ids(["kb-1", "   "], mode="reference")
    assert raised.value.code == "empty_knowledge_ids"

    selection = KnowledgeSelection.explicit_ids(["kb-1"], mode="drop")
    assert selection.as_dict() == {
        "contract_version": 2,
        "selection_state": "explicit_ids",
        "knowledge_ids": ["kb-1"],
        "mode": "drop",
    }


@pytest.mark.parametrize(
    "entity_type",
    [
        KnowledgeRelevanceEntityType.FUNCTIONAL_REQUIREMENT,
        "acceptance_criterion",
        "test_scenario",
    ],
)
def test_relevance_link_accepts_only_functional_entity_types(
    entity_type: KnowledgeRelevanceEntityType | str,
) -> None:
    link = KnowledgeRelevanceLink(entity_type, " entity-1 ")  # type: ignore[arg-type]
    assert link.entity_id == "entity-1"
    assert link.as_dict()["entity_type"] in {
        "functional_requirement",
        "acceptance_criterion",
        "test_scenario",
    }


def test_relevance_link_rejects_unknown_type_and_empty_id() -> None:
    with pytest.raises(KnowledgePropagationContractError) as unknown:
        KnowledgeRelevanceLink("business_rule", "br-1")  # type: ignore[arg-type]
    assert unknown.value.code == "invalid_relevance_entity_type"

    with pytest.raises(KnowledgePropagationContractError) as empty:
        KnowledgeRelevanceLink("test_scenario", " ")  # type: ignore[arg-type]
    assert empty.value.code == "empty_relevance_entity_id"


def test_assignment_is_frozen_canonical_and_serializable_with_public_stamp() -> None:
    assignment = _assignment()

    assert assignment.assignment_id == "assignment-1"
    assert assignment.mode is KnowledgePropagationMode.REFERENCE
    assert assignment.state is KnowledgeAssignmentState.ACTIVE
    assert assignment.origin_class is KnowledgeOriginClass.V2
    assert type(assignment.revision_stamp) is ResourceRevisionStamp
    assert DomainResourceRevisionStamp is ResourceRevisionStamp
    assert assignment.revision_stamp.to_dict() == {
        "root_id": "kb-root",
        "immediate_parent_id": "kb-parent",
        "source_revision": "7",
        "source_content_sha256": "a" * 64,
    }
    assert assignment.relevance_links == (
        KnowledgeRelevanceLink(
            KnowledgeRelevanceEntityType.ACCEPTANCE_CRITERION, "ac_1"
        ),
    )
    assert json.loads(json.dumps(assignment.as_dict())) == assignment.as_dict()
    assert assignment.as_dict()["contract_version"] == (
        KNOWLEDGE_PROPAGATION_CONTRACT_VERSION
    )
    with pytest.raises(FrozenInstanceError):
        assignment.state = KnowledgeAssignmentState.STALE  # type: ignore[misc]


def test_assignment_v2_wire_shape_is_guarded_by_a_complete_golden_value() -> None:
    assert _assignment().as_dict() == {
        "contract_version": 2,
        "assignment_id": "assignment-1",
        "board_id": "board-1",
        "target_type": "card",
        "target_id": "card-1",
        "source_knowledge_id": "kb-1",
        "revision_stamp": {
            "root_id": "kb-root",
            "immediate_parent_id": "kb-parent",
            "source_revision": "7",
            "source_content_sha256": "a" * 64,
        },
        "mode": "reference",
        "state": "active",
        "origin_class": "v2",
        "actor_id": "agent-1",
        "revision": 1,
        "justification": "relevant to AC",
        "relevance_links": [
            {
                "entity_type": "acceptance_criterion",
                "entity_id": "ac_1",
            }
        ],
    }


def test_sibling_resource_lineage_contracts_are_consumable_without_duplication() -> None:
    assert DomainResourceRevisionStamp is ResourceRevisionStamp
    assert ServicesResourceRevisionStamp is ResourceRevisionStamp
    assert callable(ResolvedResourceLineageProjection.project)
    assert knowledge_content_sha256({"content": "known"}) == (
        "2b6f329cf72d2aeaff74c4244cc9a5c56ea8790916a389b6cf476d581e832371"
    )


@pytest.mark.parametrize(
    ("mode", "state"),
    [
        ("drop", "active"),
        ("drop", "inactive"),
        ("reference", "dropped"),
        ("snapshot", "dropped"),
    ],
)
def test_assignment_requires_drop_mode_and_dropped_state_together(
    mode: str, state: str
) -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(mode=mode, state=state)
    assert raised.value.code == "drop_assignment_state_mismatch"

    dropped = _assignment(
        mode="drop",
        state="dropped",
        justification="no longer relevant",
    )
    assert dropped.mode is KnowledgePropagationMode.DROP
    assert dropped.state is KnowledgeAssignmentState.DROPPED


def test_assignment_rejects_duplicate_or_untyped_relevance_links() -> None:
    link = KnowledgeRelevanceLink("functional_requirement", "fr-1")  # type: ignore[arg-type]
    with pytest.raises(KnowledgePropagationContractError) as duplicate:
        _assignment(relevance_links=(link, link))
    assert duplicate.value.code == "duplicate_relevance_link"

    with pytest.raises(KnowledgePropagationContractError) as untyped:
        _assignment(relevance_links=({"entity_type": "test_scenario"},))
    assert untyped.value.code == "invalid_relevance_link"


@pytest.mark.parametrize(
    "ids",
    [
        {"kb-1", "kb-2"},
        {"first": "kb-1"},
        (item for item in ("kb-1", "kb-2")),
    ],
    ids=["set", "mapping", "generator"],
)
def test_selection_rejects_unordered_or_one_shot_id_collections(ids: object) -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        KnowledgeSelection(
            selection_state=KnowledgeSelectionState.EXPLICIT_IDS,
            knowledge_ids=ids,  # type: ignore[arg-type]
            mode=KnowledgePropagationMode.REFERENCE,
        )
    assert raised.value.code == "invalid_knowledge_ids"


@pytest.mark.parametrize(
    "links",
    [
        {KnowledgeRelevanceLink("test_scenario", "ts-1")},  # type: ignore[arg-type]
        (item for item in (KnowledgeRelevanceLink("test_scenario", "ts-1"),)),
    ],
    ids=["set", "generator"],
)
def test_assignment_rejects_unordered_or_one_shot_link_collections(
    links: object,
) -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(relevance_links=links)
    assert raised.value.code == "invalid_relevance_links"


def test_contract_errors_have_a_stable_cross_surface_envelope() -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        KnowledgeSelection.explicit_ids([" "], mode="reference")

    assert raised.value.as_dict() == {
        "code": "empty_knowledge_ids",
        "detail": "knowledge_ids must not be empty",
    }
    assert raised.value.to_error_dict() == raised.value.as_dict()


@pytest.mark.parametrize("revision", [-1, 1.5, True, "1"])
def test_assignment_revision_is_a_non_negative_integer(revision: object) -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(revision=revision)
    assert raised.value.code == "invalid_assignment_revision"


def test_assignment_requires_the_public_resource_revision_stamp() -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(
            revision_stamp={
                "root_id": "kb-root",
                "source_revision": "7",
                "source_content_sha256": "abc123",
            }
        )
    assert raised.value.code == "invalid_revision_stamp"


def test_assignment_rejects_an_unreachable_target_type() -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(target_type="refinement")
    assert raised.value.code == "invalid_target_type"


def test_v2_assignment_requires_revision_and_canonical_hash_evidence() -> None:
    with pytest.raises(KnowledgePropagationContractError) as missing:
        _assignment(
            revision_stamp=ResourceRevisionStamp(root_id="kb-root"),
        )
    assert missing.value.code == "v2_assignment_revision_evidence_required"

    with pytest.raises(KnowledgePropagationContractError) as malformed:
        _assignment(
            revision_stamp=ResourceRevisionStamp(
                root_id="kb-root",
                source_revision="7",
                source_content_sha256="ABC123",
            ),
        )
    assert malformed.value.code == "invalid_source_content_sha256"

    legacy = _assignment(
        origin_class="legacy_all",
        revision_stamp=ResourceRevisionStamp(root_id="kb-root"),
    )
    assert legacy.revision_stamp.source_revision is None


def test_only_snapshot_assignments_can_be_stale() -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(mode="reference", state="stale")
    assert raised.value.code == "stale_assignment_requires_snapshot"

    stale = _assignment(mode="snapshot", state="stale")
    assert stale.state is KnowledgeAssignmentState.STALE


def test_drop_assignment_requires_justification() -> None:
    for value in (None, "   "):
        with pytest.raises(KnowledgePropagationContractError) as raised:
            _assignment(mode="drop", state="dropped", justification=value)
        assert raised.value.code == "knowledge_drop_justification_required"


def test_every_explicit_v2_assignment_requires_justification() -> None:
    with pytest.raises(KnowledgePropagationContractError) as raised:
        _assignment(mode="reference", state="active", justification=" ")
    assert raised.value.code == "knowledge_assignment_justification_required"

    legacy = _assignment(
        origin_class="legacy_all",
        justification=" ",
        revision_stamp=ResourceRevisionStamp(root_id="kb-root"),
    )
    assert legacy.justification is None


def test_selection_and_relevance_links_are_order_invariant() -> None:
    first = KnowledgeSelection.explicit_ids(
        ["kb-b", "kb-a"], mode="reference"
    )
    second = KnowledgeSelection.explicit_ids(
        ["kb-a", "kb-b"], mode="reference"
    )
    assert first == second
    assert first.as_dict() == second.as_dict()

    links = (
        KnowledgeRelevanceLink("test_scenario", "ts-2"),
        KnowledgeRelevanceLink("functional_requirement", "fr-1"),
    )
    assignment = _assignment(relevance_links=links)
    assert [item.entity_id for item in assignment.relevance_links] == [
        "fr-1",
        "ts-2",
    ]

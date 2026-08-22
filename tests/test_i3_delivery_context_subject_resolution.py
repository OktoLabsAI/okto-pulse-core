from __future__ import annotations

from types import SimpleNamespace

import pytest

from okto_pulse.core.application.use_cases.code_traceability import (
    _record_delivery_context,
    _subject_delivery_context,
)
from okto_pulse.core.domain.code_traceability import (
    CodeDeliveryContextRequired,
    CodeTraceabilitySubjectType,
    DeliveryContext,
    DirectSpecDeliveryContextProvenance,
)
from okto_pulse.core.models.schemas import (
    IdeationSnapshotResponse,
    RefinementSummary,
    RefinementSnapshotResponse,
)


def _inherited_spec(**changes: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "id": "spec-1",
        "board_id": "board-1",
        "version": 7,
        "refinement_id": "refinement-1",
        "source_refinement_snapshot_id": "snapshot-3",
        "source_refinement_version": 3,
        "delivery_context": DeliveryContext.BROWNFIELD,
        "delivery_context_provenance": {
            "value": DeliveryContext.BROWNFIELD.value,
            "inherited_value": DeliveryContext.BROWNFIELD.value,
            "source_refinement_id": "refinement-1",
            "source_refinement_version": 3,
            "override_reason": None,
        },
    }
    values.update(changes)
    return SimpleNamespace(**values)


def test_refinement_context_is_explicit_and_never_inferred() -> None:
    refinement = SimpleNamespace(delivery_context=DeliveryContext.HYBRID)
    assert (
        _record_delivery_context(
            refinement,
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        )
        is DeliveryContext.HYBRID
    )

    with pytest.raises(CodeDeliveryContextRequired) as raised:
        _record_delivery_context(
            SimpleNamespace(),
            subject_type=CodeTraceabilitySubjectType.REFINEMENT,
        )
    assert raised.value.details == {"reason": "persisted_delivery_context_required"}


def test_spec_context_parses_typed_inherited_provenance() -> None:
    assert (
        _record_delivery_context(
            _inherited_spec(),
            subject_type=CodeTraceabilitySubjectType.SPEC,
        )
        is DeliveryContext.BROWNFIELD
    )


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        (
            {"delivery_context_provenance": None},
            "persisted_delivery_context_provenance_required",
        ),
        (
            {"delivery_context": None},
            "persisted_delivery_context_required",
        ),
        (
            {"delivery_context": DeliveryContext.GREENFIELD},
            "persisted_delivery_context_provenance_mismatch",
        ),
        (
            {"source_refinement_version": 4},
            "persisted_delivery_context_provenance_lineage_mismatch",
        ),
        (
            {
                "delivery_context_provenance": {
                    "value": "brownfield",
                    "inherited_value": "brownfield",
                    "source_refinement_id": "refinement-1",
                    "source_refinement_version": 3,
                    "source_spec_id": "spec-1",
                    "source_spec_version": 7,
                }
            },
            "persisted_delivery_context_provenance_invalid",
        ),
    ),
)
def test_spec_context_fails_closed_on_legacy_or_inconsistent_state(
    changes: dict[str, object],
    reason: str,
) -> None:
    with pytest.raises(CodeDeliveryContextRequired) as raised:
        _record_delivery_context(
            _inherited_spec(**changes),
            subject_type=CodeTraceabilitySubjectType.SPEC,
        )
    assert raised.value.details == {"reason": reason}


def test_direct_spec_context_requires_matching_typed_lineage() -> None:
    provenance = DirectSpecDeliveryContextProvenance(
        value=DeliveryContext.GREENFIELD,
        source_spec_id="spec-direct",
        source_spec_version=2,
    )
    direct = SimpleNamespace(
        id="spec-direct",
        version=3,
        refinement_id=None,
        source_refinement_snapshot_id=None,
        source_refinement_version=None,
        delivery_context=DeliveryContext.GREENFIELD,
        delivery_context_provenance=provenance,
    )
    assert (
        _record_delivery_context(
            direct,
            subject_type=CodeTraceabilitySubjectType.SPEC,
        )
        is DeliveryContext.GREENFIELD
    )

    direct.refinement_id = "refinement-legacy"
    with pytest.raises(CodeDeliveryContextRequired) as raised:
        _record_delivery_context(
            direct,
            subject_type=CodeTraceabilitySubjectType.SPEC,
        )
    assert raised.value.details == {
        "reason": "persisted_delivery_context_provenance_lineage_mismatch"
    }


@pytest.mark.asyncio
async def test_card_context_validates_the_owning_spec_exactly() -> None:
    spec = _inherited_spec()

    class Specs:
        async def get_spec(self, spec_id: str) -> object | None:
            return spec if spec_id == "spec-1" else None

    uow = SimpleNamespace(services=SimpleNamespace(specs=Specs()))
    card = SimpleNamespace(id="card-1", board_id="board-1", spec_id="spec-1")
    assert (
        await _subject_delivery_context(
            card,
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.CARD,
            uow=uow,
        )
        is DeliveryContext.BROWNFIELD
    )

    spec.delivery_context = DeliveryContext.GREENFIELD
    with pytest.raises(CodeDeliveryContextRequired) as raised:
        await _subject_delivery_context(
            card,
            board_id="board-1",
            subject_type=CodeTraceabilitySubjectType.CARD,
            uow=uow,
        )
    assert raised.value.details == {
        "reason": "persisted_delivery_context_provenance_mismatch"
    }


def test_context_manifests_belong_only_to_refinement_snapshot_response() -> None:
    contextual_fields = {
        "code_evidence_manifest",
        "source_context_manifest",
        "source_context_sha256",
    }
    assert contextual_fields.isdisjoint(IdeationSnapshotResponse.model_fields)
    assert contextual_fields.issubset(RefinementSnapshotResponse.model_fields)
    assert "source_context_sha256" not in RefinementSummary.model_fields

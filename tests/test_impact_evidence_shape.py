"""SK-B2-S1 I1 — impact_evidence shape v1, declared ConclusionEntry fields,
read tolerance and projections (FR-1..FR-4, FR-9; TR-1/TR-2/TR-7)."""

import pytest
from pydantic import ValidationError

from okto_pulse.core.models.schemas import (
    CardMove,
    ConclusionEntry,
    ConclusionEntrySummary,
    ImpactEvidence,
    ImpactEvidenceFile,
    ImpactEvidenceSurface,
    ImpactEvidenceSymbol,
    ImpactEvidenceTest,
)

_BASE_ENTRY = {
    "text": "done",
    "author_id": "agent-1",
    "created_at": "2026-01-01T00:00:00+00:00",
}

_VALID_BLOCK = {
    "schema_version": 1,
    "files": [
        {
            "repo": "core",
            "path": "src/okto_pulse/core/models/schemas.py",
            "change_kind": "modified",
        }
    ],
    "symbols": [
        {
            "name": "ImpactEvidence",
            "kind": "class",
            "action": "created",
            "repo": "core",
            "file": "src/okto_pulse/core/models/schemas.py",
        }
    ],
    "surfaces": [{"kind": "mcp_tool", "identifier": "okto_pulse_move_card"}],
    "tests": [
        {
            "action": "added",
            "repo": "core",
            "test_file_path": "tests/test_impact_evidence_shape.py",
            "scenario_id": "ts_1",
        }
    ],
    "evidence_refs": ["tests/test_impact_evidence_shape.py::test_round_trip"],
}


def test_round_trip_preserves_submitted_block():
    """TS: AC-2 — the parsed block round-trips byte-equivalent content."""

    block = ImpactEvidence.model_validate(_VALID_BLOCK)
    dumped = block.model_dump(exclude_none=True)
    assert dumped["schema_version"] == 1
    assert dumped["files"][0]["path"] == "src/okto_pulse/core/models/schemas.py"
    assert dumped["symbols"][0]["file"].endswith("schemas.py")
    assert dumped["tests"][0]["scenario_id"] == "ts_1"
    assert block.is_minimally_populated()


@pytest.mark.parametrize(
    "mutation",
    [
        {"path": "src\\okto_pulse\\x.py"},  # backslash
        {"path": "/abs/path.py"},  # leading slash
        {"path": "D:/abs.py"},  # drive letter
        {"path": "a/../escape.py"},  # parent segment
        {"change_kind": "renamed"},  # renamed sem previous_path
        {"previous_path": "old.py"},  # previous_path sem renamed
        {"repo": "vendor"},  # repo fora do enum
        {"change_kind": "added"},  # enum errado (contrato diz created)
        {"unknown_key": 1},  # extra=forbid
    ],
)
def test_file_rejection_matrix(mutation):
    """TS: AC-3/FR-2 — each violation rejects with a field-naming error."""

    payload = {"repo": "core", "path": "src/ok.py", "change_kind": "modified"}
    payload.update(mutation)
    with pytest.raises(ValidationError):
        ImpactEvidenceFile.model_validate(payload)


def test_symbol_requires_file_and_closed_enums():
    with pytest.raises(ValidationError):
        ImpactEvidenceSymbol.model_validate(
            {"name": "x", "kind": "class", "action": "created", "repo": "core"}
        )
    with pytest.raises(ValidationError):
        ImpactEvidenceSymbol.model_validate(
            {
                "name": "x",
                "kind": "lambda",
                "action": "created",
                "repo": "core",
                "file": "a.py",
            }
        )


def test_surface_and_test_enums_closed():
    with pytest.raises(ValidationError):
        ImpactEvidenceSurface.model_validate(
            {"kind": "webhook", "identifier": "x"}
        )
    with pytest.raises(ValidationError):
        ImpactEvidenceTest.model_validate(
            {"action": "removed", "repo": "core", "test_file_path": "t.py"}
        )


def test_caps_enforced():
    """TS: FR-2 — 201 files rejects the whole block."""

    files = [
        {"repo": "core", "path": f"src/f{i}.py", "change_kind": "modified"}
        for i in range(201)
    ]
    with pytest.raises(ValidationError):
        ImpactEvidence.model_validate({"schema_version": 1, "files": files})


def test_evidence_refs_stripped_unique_nonempty():
    with pytest.raises(ValidationError):
        ImpactEvidence.model_validate(
            {"schema_version": 1, "evidence_refs": ["   "]}
        )
    with pytest.raises(ValidationError):
        ImpactEvidence.model_validate(
            {"schema_version": 1, "evidence_refs": ["a", " a "]}
        )
    block = ImpactEvidence.model_validate(
        {"schema_version": 1, "evidence_refs": ["  ts_1  "]}
    )
    assert block.evidence_refs == ["ts_1"]


def test_empty_block_is_not_minimally_populated():
    """FR-6 bar: evidence_refs alone do not satisfy 'require'."""

    assert not ImpactEvidence.model_validate(
        {"schema_version": 1, "evidence_refs": ["ts_1"]}
    ).is_minimally_populated()


def test_unknown_schema_version_rejected_on_write():
    with pytest.raises(ValidationError):
        ImpactEvidence.model_validate({"schema_version": 2})


def test_read_tolerance_malformed_block_normalizes_to_none():
    """TS: AC-4/FR-3 — a corrupted stored block never fails the read."""

    for garbage in (
        {"schema_version": 99},
        {"schema_version": 1, "files": [{"repo": "core"}]},
        "not-a-dict",
        42,
    ):
        entry = ConclusionEntry.model_validate(
            {**_BASE_ENTRY, "impact_evidence": garbage}
        )
        assert entry.impact_evidence is None, garbage


def test_read_keeps_valid_stored_block():
    entry = ConclusionEntry.model_validate(
        {**_BASE_ENTRY, "impact_evidence": _VALID_BLOCK}
    )
    assert entry.impact_evidence is not None
    assert entry.impact_evidence.files[0].repo == "core"


def test_absent_block_preserves_legacy_shape():
    """TS: AC-1 — entries without the field keep today's behavior."""

    entry = ConclusionEntry.model_validate(dict(_BASE_ENTRY))
    assert entry.impact_evidence is None
    assert entry.source is None
    assert entry.validation_id is None


def test_declared_source_and_validation_id_survive():
    """TS: AC-14/FR-9 — legacy provenance fields are declared, not stripped."""

    entry = ConclusionEntry.model_validate(
        {
            **_BASE_ENTRY,
            "source": "task_validation",
            "validation_id": "val_123",
        }
    )
    assert entry.source == "task_validation"
    assert entry.validation_id == "val_123"
    lean = ConclusionEntrySummary.model_validate(
        {
            **_BASE_ENTRY,
            "source": "task_validation",
            "validation_id": "val_123",
        }
    )
    assert lean.source == "task_validation"


def test_summary_projection_never_carries_impact_evidence():
    """TS: AC-5/FR-4 — the lean entry has no impact_evidence field at all."""

    assert "impact_evidence" not in ConclusionEntrySummary.model_fields
    lean = ConclusionEntrySummary.model_validate(
        {**_BASE_ENTRY, "impact_evidence": _VALID_BLOCK}
    )
    assert "impact_evidence" not in lean.model_dump()


def test_card_move_field_outside_untouched_one_of():
    """TS: AC-13/TR-7 — impact_evidence enters CardMove OUTSIDE the placement
    oneOf; the published oneOf keeps its three variants byte-identical."""

    schema = CardMove.model_json_schema()
    assert "impact_evidence" in schema["properties"]
    one_of = schema["oneOf"]
    assert [variant["title"] for variant in one_of] == [
        "positional",
        "relative",
        "global",
    ]
    for variant in one_of:
        assert "impact_evidence" not in str(variant)
    move = CardMove.model_validate(
        {
            "status": "validation",
            "conclusion": "done",
            "impact_evidence": _VALID_BLOCK,
        }
    )
    assert move.impact_evidence is not None
    with pytest.raises(ValidationError):
        CardMove.model_validate(
            {
                "status": "validation",
                "impact_evidence": {"schema_version": 1, "bogus": True},
            }
        )

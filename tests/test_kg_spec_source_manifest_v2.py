"""Current source manifests bind content, IR/OR and current quality/RDL heads."""

from __future__ import annotations

import dataclasses


from okto_pulse.core.kg.board_source_store import (
    QUALITY_CURRENT_HEAD_FINGERPRINT_FIELDS,
    RESEARCH_DECISION_CURRENT_HEAD_FINGERPRINT_FIELDS,
    SPEC_CONTENT_COLUMNS,
    SPEC_SOURCE_MANIFEST_VERSION,
    _canonical_content_hash,
)
from okto_pulse.core.ports.consolidation import (
    CurrentQualityAssessmentSummary,
    CurrentResearchDecisionSummary,
)
from okto_pulse.core.kg.rebuild_sources import (
    KGRebuildSourceManifest,
    RebuildSourceEnumerator,
    SourceSetRevalidation,
)
from okto_pulse.core.kg.source_maturity import classify_source_for_kg


def _spec_row(**overrides) -> dict:
    """A minimal `specs` row dict the hash function reads by column name."""
    row = {col: f"val-{col}" for col in SPEC_CONTENT_COLUMNS}
    row["integration_requirements"] = '[{"id": "ir_base"}]'
    row["observability_requirements"] = '[{"id": "or_base"}]'
    row.update(overrides)
    return row


def _spec_source(
    content_hash: str,
    *,
    id_: str = "s1",
) -> dict:
    """A spec entry of the enumerated source set (done → canonical partition)."""
    return {
        "artifact_type": "spec",
        "id": id_,
        "source_ref": f"spec:{id_}",
        "source_version": "1",
        "content_hash": content_hash,
        "created_at": "2026-06-08T00:00:00+00:00",
        "updated_at": "2026-06-08T00:00:00+00:00",
        "status": "done",
        "source_artifact_status": "done",
        "has_minimal_evidence": True,
    }


def test_projection_fingerprint_fields_match_current_head_dtos() -> None:
    assert QUALITY_CURRENT_HEAD_FINGERPRINT_FIELDS == tuple(
        field.name
        for field in dataclasses.fields(CurrentQualityAssessmentSummary)
        if field.name != "projection_fingerprint"
    )
    assert RESEARCH_DECISION_CURRENT_HEAD_FINGERPRINT_FIELDS == tuple(
        field.name
        for field in dataclasses.fields(CurrentResearchDecisionSummary)
        if field.name != "projection_fingerprint"
    )


# ---------------------------------------------------------------------------
# ts_f04cdc26 (card 4b8011bb) — IR/OR change the source hash
# ---------------------------------------------------------------------------


def test_ir_or_change_alters_spec_source_hash():
    base = _spec_row()
    only_ir_changed = _spec_row(integration_requirements='[{"id": "ir_OTHER"}]')
    only_or_changed = _spec_row(observability_requirements='[{"id": "or_OTHER"}]')

    h_base = _canonical_content_hash(base, SPEC_CONTENT_COLUMNS)
    # IR change → v2 hash differs.
    assert h_base != _canonical_content_hash(only_ir_changed, SPEC_CONTENT_COLUMNS)
    # OR change → v2 hash differs.
    assert h_base != _canonical_content_hash(only_or_changed, SPEC_CONTENT_COLUMNS)
    # Unchanged IR/OR → v2 hash stable.
    assert h_base == _canonical_content_hash(_spec_row(), SPEC_CONTENT_COLUMNS)


def test_current_projection_change_is_manifest_drift(tmp_path):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    baseline = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3_A")]
    ).enumerate(board_id="b-v2-drift")
    manifest_v3 = store.build(
        source_set=baseline,
        preflight_hash="b" * 64,
    )
    assert manifest_v3.manifest_schema_version == SPEC_SOURCE_MANIFEST_VERSION
    projection_only_change = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3_CHANGED")]
    ).enumerate(board_id="b-v2-drift")
    assert (
        store.classify_revalidation(
            manifest=manifest_v3,
            current_source_set=projection_only_change,
        ).outcome
        is SourceSetRevalidation.MANIFEST_DRIFT
    )


def test_v3_manifest_json_never_persists_compatibility_hashes(tmp_path):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3")]
    ).enumerate(board_id="b-json")
    manifest = store.build(
        source_set=source_set,
        preflight_hash="c" * 64,
    )

    payload = manifest.to_dict()
    assert payload["manifest_schema_version"] == 3
    assert all(
        "content_hash_v1" not in row and "content_hash_v2" not in row
        for row in payload["sources"]
    )
    loaded = store.load(manifest.manifest_ref)
    assert loaded is not None
    assert loaded == manifest


def test_revalidation_rejects_unknown_schema_even_when_v3_hash_matches(
    tmp_path,
):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3")]
    ).enumerate(board_id="b-unknown-schema")
    current = store.build(
        source_set=source_set,
        preflight_hash="d" * 64,
    )
    unsupported = dataclasses.replace(
        current,
        manifest_schema_version=4,
    )

    assert (
        store.classify_revalidation(
            manifest=unsupported,
            current_source_set=source_set,
        ).outcome
        is SourceSetRevalidation.MANIFEST_DRIFT
    )


# ---------------------------------------------------------------------------
# ts_2893dc95 (card dab1d3af) — regression guard
# ---------------------------------------------------------------------------


def test_regression_ir_or_and_spec_done_stay_canonical():
    # (a) IR/OR must remain in the v2 spec manifest columns — a removal makes
    # this fail with a message naming the dropped sub-entity.
    for field in ("integration_requirements", "observability_requirements"):
        assert field in SPEC_CONTENT_COLUMNS, (
            f"REGRESSION: spec source manifest must keep sub-entity '{field}' "
            f"in the canonical content hash; it was removed from "
            f"SPEC_CONTENT_COLUMNS."
        )

    # (b) behavioral: changing IR changes the canonical (v2) hash — so a silent
    # removal of IR from the hashed columns would be caught here too.
    row_a = _spec_row(integration_requirements='[{"id": "A"}]')
    row_b = _spec_row(integration_requirements='[{"id": "B"}]')
    assert _canonical_content_hash(
        row_a, SPEC_CONTENT_COLUMNS
    ) != _canonical_content_hash(row_b, SPEC_CONTENT_COLUMNS)

    # (c) a spec-done source is canonical; a non-done spec is not (the spec-done
    # children must not leave the canonical graph).
    done = classify_source_for_kg(
        artifact_type="spec",
        artifact_status="done",
        content_hash="h",
    )
    assert done.graph_layer == "canonical"
    draft = classify_source_for_kg(
        artifact_type="spec",
        artifact_status="draft",
        content_hash="h",
    )
    assert draft.graph_layer != "canonical"

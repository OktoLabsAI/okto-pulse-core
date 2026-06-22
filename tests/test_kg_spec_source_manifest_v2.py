"""Behavioral tests for spec source manifest v2 + legacy rebaseline (spec eaf185c9, card 5ec8c75c).

One test per spec test_scenario / board card, plus the validator/codex-mandated
adversarial safety case:
    * ts_f04cdc26 (card 4b8011bb) — IR/OR change the spec source hash; unchanged IR/OR keep it stable.
    * ts_7593fee5 (card 7bb20805) — a legacy manifest rebaselines (no DLQ/drift) AND the
      adversarial safety case (2): a legacy manifest whose v1 content REALLY changed is
      MANIFEST_DRIFT, never silently rebaselined.
    * ts_2893dc95 (card dab1d3af) — regression guard: fails if IR/OR leave the spec manifest
      or a spec-done source stops being canonical, with a message naming the affected field.

Behavioral: exercises the real hash function / manifest revalidate / persisted audit — no stubs.
"""

from __future__ import annotations

import dataclasses

import pytest

from okto_pulse.core.kg.board_source_store import (
    SPEC_CONTENT_COLUMNS_V1,
    SPEC_CONTENT_COLUMNS_V2,
    _canonical_content_hash,
)
from okto_pulse.core.kg.rebuild_sources import (
    KGRebuildSourceManifest,
    RebuildSourceEnumerator,
    SourceSetRevalidation,
    _compose_source_set_hash_v1,
    get_spec_manifest_rebaseline_count,
    read_spec_manifest_rebaseline_audit,
    reset_spec_manifest_rebaseline_counter,
)
from okto_pulse.core.kg.source_maturity import classify_source_for_kg


@pytest.fixture(autouse=True)
def _reset_rebaseline_counter():
    reset_spec_manifest_rebaseline_counter()
    yield
    reset_spec_manifest_rebaseline_counter()


def _spec_row(**overrides) -> dict:
    """A minimal `specs` row dict the hash function reads by column name."""
    row = {col: f"val-{col}" for col in SPEC_CONTENT_COLUMNS_V2}
    row["integration_requirements"] = '[{"id": "ir_base"}]'
    row["observability_requirements"] = '[{"id": "or_base"}]'
    row.update(overrides)
    return row


def _spec_source(content_hash: str, content_hash_v1: str, *, id_: str = "s1") -> dict:
    """A spec entry of the enumerated source set (done → canonical partition)."""
    return {
        "artifact_type": "spec",
        "id": id_,
        "source_ref": f"spec:{id_}",
        "source_version": "1",
        "content_hash": content_hash,
        "content_hash_v1": content_hash_v1,
        "created_at": "2026-06-08T00:00:00+00:00",
        "updated_at": "2026-06-08T00:00:00+00:00",
        "status": "done",
        "source_artifact_status": "done",
        "has_minimal_evidence": True,
    }


# ---------------------------------------------------------------------------
# ts_f04cdc26 (card 4b8011bb) — IR/OR change the source hash
# ---------------------------------------------------------------------------


def test_ir_or_change_alters_spec_source_hash():
    base = _spec_row()
    only_ir_changed = _spec_row(integration_requirements='[{"id": "ir_OTHER"}]')
    only_or_changed = _spec_row(observability_requirements='[{"id": "or_OTHER"}]')

    h_base = _canonical_content_hash(base, SPEC_CONTENT_COLUMNS_V2)
    # IR change → v2 hash differs.
    assert h_base != _canonical_content_hash(only_ir_changed, SPEC_CONTENT_COLUMNS_V2)
    # OR change → v2 hash differs.
    assert h_base != _canonical_content_hash(only_or_changed, SPEC_CONTENT_COLUMNS_V2)
    # Unchanged IR/OR → v2 hash stable.
    assert h_base == _canonical_content_hash(_spec_row(), SPEC_CONTENT_COLUMNS_V2)

    # The v1 (legacy) hash IGNORES IR/OR — proving they live ONLY in v2, which
    # is what makes a legacy rebaseline provable.
    v1_base = _canonical_content_hash(base, SPEC_CONTENT_COLUMNS_V1)
    assert v1_base == _canonical_content_hash(only_ir_changed, SPEC_CONTENT_COLUMNS_V1)
    assert v1_base == _canonical_content_hash(only_or_changed, SPEC_CONTENT_COLUMNS_V1)


# ---------------------------------------------------------------------------
# ts_7593fee5 (card 7bb20805) — legacy rebaseline (no DLQ) + adversarial case (2)
# ---------------------------------------------------------------------------


def _legacy_manifest_for(store, source_set):
    """Build a real v2 manifest, then forge the legacy (v1) one a pre-upgrade
    board would have on disk: manifest_schema_version=1, source_set_hash = the
    v1-compatible hash of the same source set."""
    manifest_v2 = store.build(source_set=source_set, preflight_hash="a" * 64)
    v1_hash = _compose_source_set_hash_v1(source_set)
    legacy = dataclasses.replace(
        manifest_v2, manifest_schema_version=1, source_set_hash=v1_hash
    )
    return legacy


def test_legacy_board_rebaselines_without_dlq(tmp_path):
    enum = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v2", "hash_v1")]
    )
    source_set = enum.enumerate(board_id="b-legacy")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    legacy = _legacy_manifest_for(store, source_set)

    result = store.revalidate(manifest=legacy, current_source_set=source_set)

    # then: classified as REBASELINE (NOT drift → no DLQ / canonical debt).
    assert result.outcome is SourceSetRevalidation.REBASELINE
    assert not result.is_drift
    assert "spec:s1" in result.rebaselined_source_refs
    assert get_spec_manifest_rebaseline_count("b-legacy") == 1

    # FR7 audit: a formal, queryable persisted record with from/to version,
    # the hash fields considered, and the rebaselined source_refs.
    records = read_spec_manifest_rebaseline_audit(tmp_path, "b-legacy")
    assert len(records) == 1
    rec = records[0]
    assert rec["from_manifest_schema_version"] == 1
    assert rec["to_manifest_schema_version"] == 2
    # The auditable v1->v2 delta: IR/OR are absent from v1, present in v2.
    assert "integration_requirements" not in rec["hash_fields_v1"]
    assert "observability_requirements" not in rec["hash_fields_v1"]
    assert "integration_requirements" in rec["hash_fields_v2"]
    assert "observability_requirements" in rec["hash_fields_v2"]
    assert rec["outcome"] == "rebaseline"
    assert "spec:s1" in rec["rebaselined_source_refs"]


def test_legacy_with_real_v1_content_change_is_drift_not_rebaseline(tmp_path):
    # Adversarial safety case (2): the stored legacy baseline was bound to
    # source set A (v1 content "hash_v1"); the live set A' has DIFFERENT v1
    # content. The v1-compatible hash no longer matches, so this is REAL
    # content drift and MUST block — the rebaseline must NOT mask it.
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    enum_a = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v2_A", "hash_v1_A")]
    )
    source_a = enum_a.enumerate(board_id="b-drift")
    legacy = _legacy_manifest_for(store, source_a)
    reset_spec_manifest_rebaseline_counter()  # ignore the build/setup

    enum_b = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v2_B", "hash_v1_B_CHANGED")]
    )
    source_b = enum_b.enumerate(board_id="b-drift")

    result = store.revalidate(manifest=legacy, current_source_set=source_b)

    assert result.outcome is SourceSetRevalidation.MANIFEST_DRIFT
    assert result.is_drift
    assert result.rebaselined_source_refs == ()
    # Not counted as a rebaseline, and no rebaseline audit record written.
    assert get_spec_manifest_rebaseline_count("b-drift") == 0
    assert read_spec_manifest_rebaseline_audit(tmp_path, "b-drift") == []


def test_already_v2_manifest_treats_ir_or_change_as_normal_drift(tmp_path):
    # After the baseline is v2, IR/OR changes are NORMAL drift, never a second
    # rebaseline (codex case 4).
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    enum = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v2", "hash_v1")]
    )
    source_set = enum.enumerate(board_id="b-v2")
    manifest_v2 = store.build(source_set=source_set, preflight_hash="b" * 64)
    assert manifest_v2.manifest_schema_version == 2

    enum2 = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v2_CHANGED", "hash_v1_CHANGED")]
    )
    changed = enum2.enumerate(board_id="b-v2")
    result = store.revalidate(manifest=manifest_v2, current_source_set=changed)
    assert result.outcome is SourceSetRevalidation.MANIFEST_DRIFT


# ---------------------------------------------------------------------------
# ts_2893dc95 (card dab1d3af) — regression guard
# ---------------------------------------------------------------------------


def test_regression_ir_or_and_spec_done_stay_canonical():
    # (a) IR/OR must remain in the v2 spec manifest columns — a removal makes
    # this fail with a message naming the dropped sub-entity.
    for field in ("integration_requirements", "observability_requirements"):
        assert field in SPEC_CONTENT_COLUMNS_V2, (
            f"REGRESSION: spec source manifest must keep sub-entity '{field}' "
            f"in the canonical content hash; it was removed from "
            f"SPEC_CONTENT_COLUMNS_V2."
        )

    # (b) behavioral: changing IR changes the canonical (v2) hash — so a silent
    # removal of IR from the hashed columns would be caught here too.
    row_a = _spec_row(integration_requirements='[{"id": "A"}]')
    row_b = _spec_row(integration_requirements='[{"id": "B"}]')
    assert _canonical_content_hash(
        row_a, SPEC_CONTENT_COLUMNS_V2
    ) != _canonical_content_hash(row_b, SPEC_CONTENT_COLUMNS_V2)

    # (c) a spec-done source is canonical; a non-done spec is not (the spec-done
    # children must not leave the canonical graph).
    done = classify_source_for_kg(
        artifact_type="spec", artifact_status="done", content_hash="h",
    )
    assert done.graph_layer == "canonical"
    draft = classify_source_for_kg(
        artifact_type="spec", artifact_status="draft", content_hash="h",
    )
    assert draft.graph_layer != "canonical"

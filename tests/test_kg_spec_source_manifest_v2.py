"""Behavioral tests for source manifest v1/v2 compatibility under schema v3.

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
import hashlib
import json

import pytest

from okto_pulse.core.kg.board_source_store import (
    QUALITY_CURRENT_HEAD_FINGERPRINT_FIELDS,
    RESEARCH_DECISION_CURRENT_HEAD_FINGERPRINT_FIELDS,
    SPEC_CONTENT_COLUMNS_V1,
    SPEC_CONTENT_COLUMNS_V2,
    SPEC_SOURCE_MANIFEST_VERSION,
    _canonical_content_hash,
)
from okto_pulse.core.ports.consolidation import (
    CurrentQualityAssessmentSummary,
    CurrentResearchDecisionSummary,
)
from okto_pulse.core.kg.rebuild_sources import (
    KGRebuildSourceManifest,
    RebaselineEvidenceConflictError,
    RebaselineEvidenceFenceLostError,
    RebuildSourceEnumerator,
    SourceSetRevalidation,
    _compose_source_set_hash_v1,
    _compose_source_set_hash_v2,
    get_spec_manifest_rebaseline_count,
    read_spec_manifest_rebaseline_audit,
    reset_spec_manifest_rebaseline_counter,
)
from okto_pulse.core.kg.source_maturity import classify_source_for_kg
from memory_rebuild_audit_storage import InMemoryRebuildAuditArtifactStore


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


def _spec_source(
    content_hash: str,
    content_hash_v2: str,
    content_hash_v1: str,
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
        "content_hash_v1": content_hash_v1,
        "content_hash_v2": content_hash_v2,
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


def _manifest_for_schema(store, source_set, schema_version: int):
    """Forge the exact immutable hash a v1/v2 installation persisted."""

    manifest_v3 = store.build(source_set=source_set, preflight_hash="a" * 64)
    compatibility_hash = (
        _compose_source_set_hash_v1(source_set)
        if schema_version == 1
        else _compose_source_set_hash_v2(source_set)
    )
    legacy = dataclasses.replace(
        manifest_v3,
        manifest_schema_version=schema_version,
        source_set_hash=compatibility_hash,
    )
    return legacy


def _persisted_manifest_for_schema(store, source_set, schema_version: int):
    """Persist the shape a real pre-v3 manifest stored on disk."""

    current = store.build(source_set=source_set, preflight_hash="a" * 64)
    compatibility_hash = (
        _compose_source_set_hash_v1(source_set)
        if schema_version == 1
        else _compose_source_set_hash_v2(source_set)
    )
    compatibility_field = (
        "content_hash_v1" if schema_version == 1 else "content_hash_v2"
    )

    def _legacy_rows(rows):
        return tuple(
            dataclasses.replace(
                row,
                content_hash=(getattr(row, compatibility_field) or row.content_hash),
                content_hash_v1="",
                content_hash_v2="",
            )
            for row in rows
        )

    legacy = dataclasses.replace(
        current,
        manifest_schema_version=schema_version,
        source_set_hash=compatibility_hash,
        sources=_legacy_rows(current.sources),
        working_sources=_legacy_rows(current.working_sources),
        skipped_by_maturity=_legacy_rows(current.skipped_by_maturity),
        skipped_expired_working=_legacy_rows(current.skipped_expired_working),
        legacy_unknown=_legacy_rows(current.legacy_unknown),
        payload_digest="",
    )
    store.artifact_store.write_json_atomic(
        store._manifest_key(legacy.manifest_ref),
        legacy.to_dict(),
    )
    return legacy


def test_legacy_board_rebaselines_without_dlq(tmp_path):
    enum = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    )
    source_set = enum.enumerate(board_id="b-legacy")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    legacy = _manifest_for_schema(store, source_set, 1)

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
    assert rec["to_manifest_schema_version"] == 3
    # The auditable v1->v2 delta: IR/OR are absent from v1, present in v2.
    assert "integration_requirements" not in rec["hash_fields_v1"]
    assert "observability_requirements" not in rec["hash_fields_v1"]
    assert "integration_requirements" in rec["hash_fields_v2"]
    assert "observability_requirements" in rec["hash_fields_v2"]
    assert "quality_head_fingerprints" in rec["hash_fields_v3"]
    assert "research_decision_head_fingerprints" in rec["hash_fields_v3"]
    assert rec["outcome"] == "rebaseline"
    assert "spec:s1" in rec["rebaselined_source_refs"]


def test_pure_revalidation_classifies_legacy_without_artifact_or_counter(
    tmp_path,
):
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    ).enumerate(board_id="b-pure-legacy")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    legacy = _manifest_for_schema(store, source_set, 1)
    reset_spec_manifest_rebaseline_counter()

    result = store.classify_revalidation(
        manifest=legacy,
        current_source_set=source_set,
    )

    assert result.outcome is SourceSetRevalidation.REBASELINE
    assert len(result.to_source_set_hash) == 64
    assert result.to_source_set_hash != legacy.source_set_hash
    assert get_spec_manifest_rebaseline_count("b-pure-legacy") == 0
    assert read_spec_manifest_rebaseline_audit(tmp_path, "b-pure-legacy") == []


@pytest.mark.parametrize("schema_version", [1, 2])
def test_load_verified_accepts_hash_bound_legacy_manifest_for_pure_rebaseline(
    tmp_path,
    schema_version,
):
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    ).enumerate(board_id=f"b-verified-v{schema_version}")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    legacy = _persisted_manifest_for_schema(store, source_set, schema_version)

    verified = store.load_verified(
        legacy.manifest_ref,
        expected_board_id=source_set.board_id,
        expected_preflight_hash=legacy.preflight_hash,
        cognitive_digest=source_set.cognitive_durable_digest,
    )
    result = store.classify_revalidation(
        manifest=verified,
        current_source_set=source_set,
    )

    assert result.outcome is SourceSetRevalidation.REBASELINE
    assert get_spec_manifest_rebaseline_count(source_set.board_id) == 0
    assert read_spec_manifest_rebaseline_audit(tmp_path, source_set.board_id) == []


def test_load_verified_rejects_legacy_partition_hash_tamper(tmp_path):
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    ).enumerate(board_id="b-legacy-integrity")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    legacy = _persisted_manifest_for_schema(store, source_set, 1)
    payload = legacy.to_dict()
    payload["sources"][0]["content_hash"] = "forged-legacy-row"
    store.artifact_store.write_json_atomic(
        store._manifest_key(legacy.manifest_ref),
        payload,
    )

    from okto_pulse.core.kg.rebuild_sources import (
        RebuildSourceManifestIntegrityError,
    )

    with pytest.raises(RebuildSourceManifestIntegrityError):
        store.load_verified(
            legacy.manifest_ref,
            expected_board_id=source_set.board_id,
            expected_preflight_hash=legacy.preflight_hash,
            cognitive_digest=source_set.cognitive_durable_digest,
        )


def test_governed_rebaseline_evidence_is_run_bound_exactly_once(tmp_path):
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    ).enumerate(board_id="b-governed-legacy")
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    legacy = _manifest_for_schema(store, source_set, 2)
    result = store.classify_revalidation(
        manifest=legacy,
        current_source_set=source_set,
    )
    reset_spec_manifest_rebaseline_counter()

    assert store.record_rebaseline(
        manifest=legacy,
        result=result,
        evidence_id="run_legacy:manifest_legacy",
        fence_valid=lambda: True,
        recorded_at="2026-08-15T10:00:00+00:00",
    )
    assert not store.record_rebaseline(
        manifest=legacy,
        result=result,
        evidence_id="run_legacy:manifest_legacy",
        fence_valid=lambda: True,
        recorded_at="2026-08-15T11:00:00+00:00",
    )

    records = read_spec_manifest_rebaseline_audit(
        tmp_path,
        "b-governed-legacy",
    )
    assert len(records) == 1
    assert records[0]["evidence_id"] == "run_legacy:manifest_legacy"
    assert records[0]["recorded_at"] == "2026-08-15T10:00:00+00:00"
    assert records[0]["to_source_set_hash"] == result.to_source_set_hash
    assert get_spec_manifest_rebaseline_count("b-governed-legacy") == 1

    with pytest.raises(RebaselineEvidenceConflictError):
        store.record_rebaseline(
            manifest=legacy,
            result=dataclasses.replace(
                result,
                rebaselined_source_refs=("spec:forged",),
            ),
            evidence_id="run_legacy:manifest_legacy",
            fence_valid=lambda: True,
        )
    assert len(read_spec_manifest_rebaseline_audit(tmp_path, "b-governed-legacy")) == 1
    assert get_spec_manifest_rebaseline_count("b-governed-legacy") == 1


def test_governed_rebaseline_reproves_fence_inside_artifact_transaction():
    authority = {"valid": True}

    class _FenceDroppingStore(InMemoryRebuildAuditArtifactStore):
        def replace_json(self, key, transform):  # noqa: ANN001, ANN201
            authority["valid"] = False
            self.purge_board_artifacts("b-fenced-legacy")
            return super().replace_json(key, transform)

    artifact_store = _FenceDroppingStore()
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    ).enumerate(board_id="b-fenced-legacy")
    store = KGRebuildSourceManifest(artifact_store=artifact_store)
    legacy = _manifest_for_schema(store, source_set, 1)
    result = store.classify_revalidation(
        manifest=legacy,
        current_source_set=source_set,
    )
    reset_spec_manifest_rebaseline_counter()
    # The manifest build itself used the same store before the interleaving
    # hook matters; arm authority immediately before governed evidence.
    authority["valid"] = True

    with pytest.raises(RebaselineEvidenceFenceLostError):
        store.record_rebaseline(
            manifest=legacy,
            result=result,
            evidence_id="run_fenced:manifest_fenced",
            fence_valid=lambda: authority["valid"],
        )

    assert (
        read_spec_manifest_rebaseline_audit(
            None,
            "b-fenced-legacy",
            artifact_store=artifact_store,
        )
        == []
    )
    assert get_spec_manifest_rebaseline_count("b-fenced-legacy") == 0


def test_legacy_with_real_v1_content_change_is_drift_not_rebaseline(tmp_path):
    # Adversarial safety case (2): the stored legacy baseline was bound to
    # source set A (v1 content "hash_v1"); the live set A' has DIFFERENT v1
    # content. The v1-compatible hash no longer matches, so this is REAL
    # content drift and MUST block — the rebaseline must NOT mask it.
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    enum_a = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3_A", "hash_v2_A", "hash_v1_A")]
    )
    source_a = enum_a.enumerate(board_id="b-drift")
    legacy = _manifest_for_schema(store, source_a, 1)
    reset_spec_manifest_rebaseline_counter()  # ignore the build/setup

    enum_b = RebuildSourceEnumerator(
        source_store=lambda _b: [
            _spec_source(
                "hash_v3_B",
                "hash_v2_B",
                "hash_v1_B_CHANGED",
            )
        ]
    )
    source_b = enum_b.enumerate(board_id="b-drift")

    result = store.revalidate(manifest=legacy, current_source_set=source_b)

    assert result.outcome is SourceSetRevalidation.MANIFEST_DRIFT
    assert result.is_drift
    assert result.rebaselined_source_refs == ()
    # Not counted as a rebaseline, and no rebaseline audit record written.
    assert get_spec_manifest_rebaseline_count("b-drift") == 0
    assert read_spec_manifest_rebaseline_audit(tmp_path, "b-drift") == []


def test_v2_manifest_rebaselines_to_v3_from_exact_compatibility_hash(tmp_path):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    enum = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    )
    source_set = enum.enumerate(board_id="b-v2")
    manifest_v2 = _manifest_for_schema(store, source_set, 2)

    result = store.revalidate(
        manifest=manifest_v2,
        current_source_set=source_set,
    )

    assert result.outcome is SourceSetRevalidation.REBASELINE
    assert result.from_manifest_schema_version == 2
    assert result.to_manifest_schema_version == 3
    assert result.rebaselined_source_refs == ("spec:s1",)


def test_v2_and_v3_real_content_drift_are_never_rebaselined(tmp_path):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    baseline = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3_A", "hash_v2_A", "hash_v1_A")]
    ).enumerate(board_id="b-v2-drift")
    manifest_v2 = _manifest_for_schema(store, baseline, 2)
    changed = RebuildSourceEnumerator(
        source_store=lambda _b: [
            _spec_source("hash_v3_B", "hash_v2_CHANGED", "hash_v1_A")
        ]
    ).enumerate(board_id="b-v2-drift")

    result = store.revalidate(
        manifest=manifest_v2,
        current_source_set=changed,
    )
    assert result.outcome is SourceSetRevalidation.MANIFEST_DRIFT

    manifest_v3 = store.build(
        source_set=baseline,
        preflight_hash="b" * 64,
    )
    assert manifest_v3.manifest_schema_version == SPEC_SOURCE_MANIFEST_VERSION
    projection_only_change = RebuildSourceEnumerator(
        source_store=lambda _b: [
            _spec_source("hash_v3_CHANGED", "hash_v2_A", "hash_v1_A")
        ]
    ).enumerate(board_id="b-v2-drift")
    assert (
        store.revalidate(
            manifest=manifest_v3,
            current_source_set=projection_only_change,
        ).outcome
        is SourceSetRevalidation.MANIFEST_DRIFT
    )


def test_v3_manifest_json_never_persists_compatibility_hashes(tmp_path):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
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
    assert loaded.sources[0].content_hash_v1 == ""
    assert loaded.sources[0].content_hash_v2 == ""


def test_v1_and_v2_source_set_hashes_keep_the_legacy_json_composition():
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
    ).enumerate(board_id="b-byte-compat")
    row = source_set.sources[0]

    def independently_compose(content_hash: str) -> str:
        source = row.to_dict()
        source["content_hash"] = content_hash
        payload = {
            "sources": [source],
            "working_sources": [],
            "skipped_by_maturity": [],
            "skipped_expired_working": [],
            "legacy_unknown": [],
            "skipped_cancelled_count": 0,
            "source_partition_counts": source_set.source_partition_counts,
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    assert _compose_source_set_hash_v1(source_set) == independently_compose("hash_v1")
    assert _compose_source_set_hash_v2(source_set) == independently_compose("hash_v2")


def test_revalidation_rejects_unknown_schema_even_when_v3_hash_matches(
    tmp_path,
):
    store = KGRebuildSourceManifest(base_dir=tmp_path)
    source_set = RebuildSourceEnumerator(
        source_store=lambda _b: [_spec_source("hash_v3", "hash_v2", "hash_v1")]
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
        store.revalidate(
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

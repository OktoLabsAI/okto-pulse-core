"""The durable ledger owns a cognitive node's birth stamp, not the graph.

Story e80edf05. The graph is a projection and can fall behind the ledger — a
restore from an older copy, a targeted removal, a rebuild, a DLQ replay. When
it does, consolidation legitimately believes the node is new and stamps a fresh
``created_at``. That re-mints an identity field, so the append re-presents an
immutable revision with divergent content and fails closed
(``cognitive_source_replay_conflict``), dead-lettering the consolidation.

Observed live on ``decision_059d58288dd42978369ab91a``: one node out of 71
carried two different ``created_at`` values across its revisions, with
``attestation_count`` reset from 2 back to 1 and session/recompute provenance
nulled — the exact fingerprint of a fresh-CREATE payload written over a node
the ledger already knew.
"""

from __future__ import annotations

import logging

import pytest

from okto_pulse.core.ports.kg_cognitive_source import (
    COGNITIVE_SOURCE_SEALED_BIRTH_FIELDS,
    CognitiveSourceConflict,
    CognitiveSourceRecord,
    cognitive_source_semantic_key,
    restore_sealed_birth_fields,
)

BOARD = "board-birth-stamp"
SEALED_BIRTH = "2026-08-01T16:27:29.303151"
REDERIVED_BIRTH = "2026-08-02T04:31:04.185244"
CONTENT_HASH = "c" * 64


def _draft(
    *,
    created_at: str,
    content: str = "the assertion body",
    node_id: str = "decision_059d58288dd42978369ab91a",
    generation: int = 0,
    attestation_count: int = 1,
) -> dict:
    return {
        "node_id": node_id,
        "board_id": BOARD,
        "node_type": "Decision",
        "generation": generation,
        "source_revision": max(attestation_count - 1, 0),
        "payload": {
            "title": "Consolidation re-derives node candidates",
            "content": content,
            "created_at": created_at,
            "attestation_count": attestation_count,
            "source_content_hash": CONTENT_HASH,
            "human_curated": False,
        },
        "evidence_refs": ("spec:7bba551d:tr:tr_efb59a38",),
        "source_session_id": "kgses_birth",
        "committed_at": "2026-08-02T04:31:04.185244",
    }


def test_semantic_key_is_type_id_generation():
    assert cognitive_source_semantic_key(_draft(created_at=SEALED_BIRTH)) == (
        "Decision",
        "decision_059d58288dd42978369ab91a",
        0,
    )


def test_created_at_is_the_only_sealed_birth_field():
    """Widening this set changes which re-derived values the ledger overrides.

    It is a deliberate, reviewable decision — never a side effect of a fix.
    """

    assert COGNITIVE_SOURCE_SEALED_BIRTH_FIELDS == frozenset({"created_at"})


def test_sealed_birth_wins_over_the_rederived_stamp():
    draft = _draft(created_at=REDERIVED_BIRTH)
    sealed = {
        ("Decision", "decision_059d58288dd42978369ab91a", 0): {
            "created_at": SEALED_BIRTH
        }
    }

    reconciled, restorations = restore_sealed_birth_fields((draft,), sealed)

    assert reconciled[0]["payload"]["created_at"] == SEALED_BIRTH
    assert len(restorations) == 1
    assert restorations[0].field == "created_at"
    assert restorations[0].rederived == REDERIVED_BIRTH
    assert restorations[0].sealed == SEALED_BIRTH
    # The caller's dict is never mutated in place.
    assert draft["payload"]["created_at"] == REDERIVED_BIRTH


def test_restoration_makes_the_fingerprint_identical_to_the_sealed_record():
    """This is WHY the fix works: the append becomes a byte-identical retry."""

    sealed_record = CognitiveSourceRecord(**_draft(created_at=SEALED_BIRTH))
    rederived = _draft(created_at=REDERIVED_BIRTH)
    assert (
        CognitiveSourceRecord(**rederived).record_fingerprint
        != sealed_record.record_fingerprint
    )

    reconciled, _ = restore_sealed_birth_fields(
        (rederived,),
        {("Decision", "decision_059d58288dd42978369ab91a", 0): sealed_record.payload},
    )
    assert (
        CognitiveSourceRecord(**reconciled[0]).record_fingerprint
        == sealed_record.record_fingerprint
    )


def test_a_changed_assertion_still_diverges_after_restoration():
    """The guard is narrowed, not weakened: new content stays a new record."""

    sealed_record = CognitiveSourceRecord(**_draft(created_at=SEALED_BIRTH))
    rederived = _draft(created_at=REDERIVED_BIRTH, content="a different claim")

    reconciled, restorations = restore_sealed_birth_fields(
        (rederived,),
        {("Decision", "decision_059d58288dd42978369ab91a", 0): sealed_record.payload},
    )
    assert len(restorations) == 1
    assert (
        CognitiveSourceRecord(**reconciled[0]).record_fingerprint
        != sealed_record.record_fingerprint
    )


def test_a_genuinely_new_node_passes_through_untouched():
    draft = _draft(created_at=REDERIVED_BIRTH, node_id="decision_brand_new")
    reconciled, restorations = restore_sealed_birth_fields((draft,), {})
    assert restorations == ()
    assert reconciled[0]["payload"]["created_at"] == REDERIVED_BIRTH


def test_a_different_generation_is_a_different_birth():
    """A supersede successor is a new assertion and keeps its own stamp."""

    draft = _draft(created_at=REDERIVED_BIRTH, generation=1)
    sealed = {
        ("Decision", "decision_059d58288dd42978369ab91a", 0): {
            "created_at": SEALED_BIRTH
        }
    }
    reconciled, restorations = restore_sealed_birth_fields((draft,), sealed)
    assert restorations == ()
    assert reconciled[0]["payload"]["created_at"] == REDERIVED_BIRTH


async def test_append_path_restores_and_reports_the_drift(caplog):
    """End to end over the durable boundary, on the in-memory store double."""

    from kg_registry_testing import _InMemoryCognitiveSourceStore

    from okto_pulse.core.kg.primitives import _append_cognitive_source_records

    store = _InMemoryCognitiveSourceStore()
    context = object()

    await _append_cognitive_source_records(
        BOARD,
        "kgses_first",
        [_draft(created_at=SEALED_BIRTH)],
        context=context,
        store=store,
    )
    assert len(store.records) == 1

    with caplog.at_level(logging.WARNING, logger="okto_pulse.kg.primitives"):
        await _append_cognitive_source_records(
            BOARD,
            "kgses_rematerialize",
            [_draft(created_at=REDERIVED_BIRTH)],
            context=context,
            store=store,
        )

    # Idempotent: the re-materialization resolved to the sealed record.
    assert len(store.records) == 1
    assert store.records[0].payload["created_at"] == SEALED_BIRTH
    # Counted, not silent — the graph lost a node the ledger still holds.
    assert any(
        record.__dict__.get("event") == "kg.cognitive_source.birth_stamp_restored"
        for record in caplog.records
    )


async def test_append_path_without_the_fix_would_poison_the_queue():
    """Pins the defect the restoration prevents.

    Bypassing the reconciliation reproduces the live failure exactly: the
    ledger rejects the re-derived birth as a divergent replay and the whole
    consolidation dead-letters.
    """

    from kg_registry_testing import _InMemoryCognitiveSourceStore

    store = _InMemoryCognitiveSourceStore()
    await store.append_many((CognitiveSourceRecord(**_draft(created_at=SEALED_BIRTH)),))

    with pytest.raises(CognitiveSourceConflict) as excinfo:
        await store.append_many(
            (CognitiveSourceRecord(**_draft(created_at=REDERIVED_BIRTH)),)
        )
    assert excinfo.value.failure_reason == "cognitive_source_replay_conflict"


async def test_a_store_that_cannot_answer_fails_closed():
    """Degrading to the re-derived stamp would reintroduce the corruption."""

    from okto_pulse.core.kg.primitives import (
        KGPrimitiveError,
        _append_cognitive_source_records,
    )

    class _StoreWithoutBirthLookup:
        async def append_many(self, records):
            return tuple(record.node_id for record in records)

        async def append_many_in_context(self, context, records):
            del context
            return await self.append_many(records)

        async def enumerate(self, board_id):
            del board_id
            return ()

    with pytest.raises(KGPrimitiveError) as excinfo:
        await _append_cognitive_source_records(
            BOARD,
            "kgses_legacy",
            [_draft(created_at=REDERIVED_BIRTH)],
            context=object(),
            store=_StoreWithoutBirthLookup(),
        )
    assert excinfo.value.code == "kg_cognitive_source_unavailable"
    assert (
        excinfo.value.details["failure_reason"]
        == "cognitive_source_birth_lookup_unsupported"
    )

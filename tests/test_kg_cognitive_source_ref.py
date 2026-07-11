"""Spec eca49df9 (Cognitive Dedup Granularity, SUPERSEDE Wiring & Counted
Merge) — IMPL-1 tests: per-concept source_artifact_ref + per-candidate probe.

Covers the resolver (contract api_1d7b8fa5 ``cognitive_source_ref``) and the
per-concept granularity / per-candidate idempotency of the live extractors
and the handler.

Current ACs (spec v40 — the ts_* scenario texts are registered as obsolete
in the spec context; the AC text below is the source of truth):
- AC1: N distinct Alternatives -> N distinct refs and N nodes (not 1).
- AC2: N distinct Assumptions -> N distinct refs (Learning out-of-scope).
- AC3: a 2nd distinct concept of the same type is NOT skipped by the probe.
- AC4: content_hash8 is deterministic and stable to reorder/insertion;
  semantically-equivalent content -> same ref.
"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from okto_pulse.core.events.handlers.cognitive_extraction import (
    CognitiveExtractionHandler,
)
from okto_pulse.core.events.types import CardMoved
from okto_pulse.core.kg.agent.extractors import (
    extract_alternatives,
    extract_assumptions,
)
from okto_pulse.core.kg.agent.extractors.source_ref import (
    CognitiveSourceRefError,
    cognitive_source_ref,
    content_hash8,
    per_concept_source_ref,
)
from okto_pulse.core.kg.interfaces import get_kg_registry
from sqlalchemy_test_models import CardType


# --------------------------------------------------------------------------
# resolver: format + validation (contract api_1d7b8fa5)
# --------------------------------------------------------------------------


def test_cognitive_source_ref_format():
    ref = cognitive_source_ref("Alternative", "spec", "abc123", "Use Redis for cache")
    parts = ref.split(":")
    assert parts[0] == "spec"
    assert parts[1] == "abc123"
    assert parts[2] == "alternative"  # type_token lowercased
    assert len(parts[3]) == 8  # content_hash8


def test_cognitive_source_ref_invalid_type_rejects_learning():
    # Learning-from-spec is out of scope — the resolver refuses to mint it.
    with pytest.raises(CognitiveSourceRefError) as ei:
        cognitive_source_ref("Learning", "spec", "abc", "some learning")
    assert ei.value.code == "invalid_cognitive_type"


def test_cognitive_source_ref_missing_content():
    with pytest.raises(CognitiveSourceRefError) as ei:
        cognitive_source_ref("Alternative", "spec", "abc", "")
    assert ei.value.code == "missing_source_content"


def test_cognitive_source_ref_unstable_key_whitespace_content():
    with pytest.raises(CognitiveSourceRefError) as ei:
        cognitive_source_ref("Alternative", "spec", "abc", "   \n\t ")
    assert ei.value.code == "unstable_or_empty_source_key"


def test_cognitive_source_ref_unstable_key_empty_entity():
    with pytest.raises(CognitiveSourceRefError) as ei:
        cognitive_source_ref("Assumption", "spec", "", "real content")
    assert ei.value.code == "unstable_or_empty_source_key"


# --------------------------------------------------------------------------
# content_hash8 / per_concept_source_ref: determinism + stability (AC4)
# --------------------------------------------------------------------------


def test_content_hash8_normalizes_and_is_deterministic():
    # Equivalent content (case + collapsed whitespace) -> same hash.
    assert content_hash8("Use Redis") == content_hash8("  use   redis  ")
    # Distinct content -> distinct hash.
    assert content_hash8("Use Redis") != content_hash8("Use Postgres")


def test_per_concept_ref_independent_of_position():
    base = "spec:s1"
    a = per_concept_source_ref(base, "alternative", "concept A")
    b = per_concept_source_ref(base, "alternative", "concept B")
    # Recomputing in any order yields identical refs (reorder/insertion-stable).
    assert per_concept_source_ref(base, "alternative", "concept B") == b
    assert per_concept_source_ref(base, "alternative", "concept A") == a
    assert a != b


# --------------------------------------------------------------------------
# extractor granularity: AC1 / AC2
# --------------------------------------------------------------------------

_ALT_CONTEXT = (
    "## Analysis\n\n"
    "Considerei a alternativa Redis para o cache.\n"
    "Avaliei a alternativa Postgres para o storage.\n"
    "Estudei a alternativa Kafka para a fila.\n"
)
_ASSUM_CONTEXT = (
    "## Analysis\n\n"
    "Assumindo que o cache hit rate fica acima de 95 porcento.\n"
    "Presume-se que o disco tem espaco suficiente.\n"
)
_ALT_REDIS = "Considerei a alternativa Redis para o cache."


def test_n_distinct_alternatives_yield_n_distinct_refs():
    results = extract_alternatives(spec_context=_ALT_CONTEXT, source_ref="spec:s1")
    assert len(results) == 3
    refs = [c.source_ref for c in results]
    assert len(set(refs)) == 3  # AC1: N distinct refs, not 1-per-type
    for r in refs:
        assert r.startswith("spec:s1:alternative:")


def test_n_distinct_assumptions_yield_n_distinct_refs():
    results = extract_assumptions(spec_context=_ASSUM_CONTEXT, source_ref="spec:s1")
    assert len(results) == 2
    refs = [c.source_ref for c in results]
    assert len(set(refs)) == 2  # AC2
    for r in refs:
        assert r.startswith("spec:s1:assumption:")


def test_reorder_and_insertion_do_not_change_other_refs():
    # AC4: reordering + inserting a concept does not change the others' refs.
    ctx_a = (
        "## Analysis\n\n"
        "Considerei a alternativa Redis para o cache.\n"
        "Avaliei a alternativa Postgres para o storage.\n"
    )
    ctx_b = (
        "## Analysis\n\n"
        "Avaliei a alternativa Postgres para o storage.\n"
        "Estudei a alternativa Kafka para a fila.\n"
        "Considerei a alternativa Redis para o cache.\n"
    )
    refs_a = {c.source_ref for c in extract_alternatives(spec_context=ctx_a, source_ref="spec:s1")}
    refs_b = {c.source_ref for c in extract_alternatives(spec_context=ctx_b, source_ref="spec:s1")}
    # Redis + Postgres refs are identical across both despite reorder + insert.
    assert refs_a.issubset(refs_b)
    assert len(refs_a) == 2 and len(refs_b) == 3


# --------------------------------------------------------------------------
# handler per-candidate idempotency probe: AC3
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_per_candidate_probe_skips_only_existing_concept(caplog, monkeypatch):
    """AC3: when ONE concept already exists in the KG, the other distinct
    concepts of the same type are still emitted — the probe is per-candidate,
    not the old any-exists probe that suppressed the whole type."""
    handler = CognitiveExtractionHandler()
    spec = SimpleNamespace(id="spec-1", context=_ALT_CONTEXT)

    sess = AsyncMock()

    async def fake_get(model, oid):
        name = model.__name__
        if name == "Card":
            return SimpleNamespace(
                id="card-1", card_type=CardType.NORMAL, spec_id="spec-1",
                action_plan=None,
            )
        if name == "Board":
            return SimpleNamespace(settings={})
        if name == "Spec":
            return spec
        return None

    sess.get = AsyncMock(side_effect=fake_get)

    # The Redis concept already exists; the probe must return True ONLY for it.
    existing_ref = per_concept_source_ref("spec:spec-1", "alternative", _ALT_REDIS)

    class _CypherExecutor:
        def execute_read_only(
            self,
            board_id: str,  # noqa: ARG002
            cypher: str,  # noqa: ARG002
            params: dict | None = None,
            *,
            max_rows: int = 1000,  # noqa: ARG002
        ) -> dict:
            ref = (params or {}).get("ref")
            return {
                "rows": [[1 if ref == existing_ref else 0]],
                "row_count": 1,
                "truncated": False,
            }

    event = CardMoved(
        board_id="board-1", card_id="card-1",
        from_status="validation", to_status="done",
    )
    monkeypatch.setattr(get_kg_registry(), "cypher_executor", _CypherExecutor())
    with caplog.at_level(
        logging.DEBUG, logger="okto_pulse.core.events.cognitive_extraction"
    ):
        await handler.handle(event, sess)

    cand_refs = [
        getattr(r, "source_ref", None)
        for r in caplog.records
        if "alternative.candidate" in r.message
    ]
    skip_refs = [
        getattr(r, "source_ref", None)
        for r in caplog.records
        if "alternative.skipped" in r.message
    ]
    # The existing concept is skipped; the other two are still emitted.
    assert existing_ref in skip_refs
    assert existing_ref not in cand_refs
    assert len(cand_refs) == 2


# --------------------------------------------------------------------------
# IMPL-4 guard (FR3/FR4): no cognitive path emits the bare global spec:{id}
# --------------------------------------------------------------------------


def test_no_cognitive_path_emits_global_spec_ref():
    """IMPL-4 guard: neither spec-driven extractor ever stamps the bare
    global spec:{id} ref — every cognitive ref carries the per-concept
    suffix spec:{id}:{type_token}:{content_hash8}."""
    base = "spec:guard1"
    produced = [
        *extract_alternatives(spec_context=_ALT_CONTEXT, source_ref=base),
        *extract_assumptions(spec_context=_ASSUM_CONTEXT, source_ref=base),
    ]
    assert produced  # the contexts do yield candidates
    for cand in produced:
        assert cand.source_ref != base  # never the bare global ref
        parts = cand.source_ref.split(":")
        assert parts[:2] == ["spec", "guard1"]
        assert parts[2] in {"alternative", "assumption"}
        assert len(parts[3]) == 8

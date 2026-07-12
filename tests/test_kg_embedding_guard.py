"""MKG-D C1 — pure verdict matrix of the embedding guard (contract api_5f426482)."""

from __future__ import annotations

import pytest

from okto_pulse.core.kg.embedding_guard import (
    VERDICT_INDETERMINATE,
    VERDICT_MISMATCH,
    VERDICT_OK,
    VERDICT_STAMP,
    EmbeddingIncompatibleError,
    compare_embedding_compat,
)


@pytest.mark.parametrize(
    "persisted, effective, is_stub, expected",
    [
        # effective untrustworthy => indeterminate wins over everything (D2)
        ((None, None), ("m", 384), True, VERDICT_INDETERMINATE),
        (("a", 384), ("m", 384), True, VERDICT_INDETERMINATE),
        (("a", 384), (None, 0), False, VERDICT_INDETERMINATE),
        (("a", 384), ("m", 0), False, VERDICT_INDETERMINATE),
        (("a", 384), (None, 384), False, VERDICT_INDETERMINATE),
        # nothing valid persisted => stamp
        ((None, None), ("m", 384), False, VERDICT_STAMP),
        (("", 0), ("m", 384), False, VERDICT_STAMP),
        (("a", None), ("m", 384), False, VERDICT_STAMP),  # partial persist
        # both valid and equal => ok
        (("m", 384), ("m", 384), False, VERDICT_OK),
        # both valid, different => mismatch (model OR dimension)
        (("a", 384), ("b", 384), False, VERDICT_MISMATCH),
        (("m", 384), ("m", 1536), False, VERDICT_MISMATCH),
        (("a", 384), ("b", 1536), False, VERDICT_MISMATCH),
    ],
)
def test_verdict_matrix(persisted, effective, is_stub, expected):
    verdict = compare_embedding_compat(
        persisted[0],
        persisted[1],
        effective[0],
        effective[1],
        effective_is_stub=is_stub,
    )
    assert verdict == expected


def test_error_carries_all_four_values_and_remediation():
    err = EmbeddingIncompatibleError(
        board_id="b1",
        persisted_model="all-MiniLM-L6-v2",
        persisted_dimension=384,
        effective_model="tenant-model",
        effective_dimension=1536,
    )
    assert err.code == "kg_embedding_incompatible"
    assert err.persisted_model == "all-MiniLM-L6-v2"
    assert err.persisted_dimension == 384
    assert err.effective_model == "tenant-model"
    assert err.effective_dimension == 1536
    assert "rebuild" in err.remediation
    assert "kg_embedding_incompatible" in str(err)


def test_garbage_dimension_is_indeterminate_not_crash():
    assert (
        compare_embedding_compat("a", 384, "b", "not-a-number")  # type: ignore[arg-type]
        == VERDICT_INDETERMINATE
    )

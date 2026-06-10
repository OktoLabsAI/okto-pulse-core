"""Tests for KG-02.4 generation repository + promotion guard.

Covers:
* TR3 — kg_generation_id UUID v4 format invariants.
* IR ir_6d092147 — promote_current rejects without durable report_ref.
* BR br_5c7c5dfa — successful promotion creates history + advances pointer.
* BR br_82deef11 — promotion conflict / invalid status / structural mismatch
  are surfaced through PromotionOutcome + PromotionEvaluation.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path
from uuid import UUID

import pytest

from okto_pulse.core.kg.rebuild_generation import (
    KGGenerationPromotionGuard,
    KGGenerationRepository,
    PROMOTABLE_TERMINAL_STATUSES,
    PromotionOutcome,
    generate_kg_generation_id,
    get_promotion_count,
    get_promotion_counter_labels,
    get_promotion_samples,
    is_valid_kg_generation_id,
    reset_promotion_counter,
)


BOARD = "board-001"
ACTOR = "actor-001"
RUN = "run_test"
REPORT_REF = "/abs/path/to/report_abc.json"
STRUCT_HASH = "a" * 64
SOURCE_HASH = "b" * 64


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    """Isolated base_dir per test — KGGenerationRepository persists state
    on disk; sharing would let one test leak its current pointer into
    the next."""

    # Defensive cleanup in case a prior test crashed leaving state.
    target = tmp_path / "kg-02-4-gen"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counter() -> None:
    reset_promotion_counter()
    yield
    reset_promotion_counter()


# -------- TR3 — UUID v4 ------------------------------------------------------


def test_generate_kg_generation_id_returns_uuid_v4_string() -> None:
    value = generate_kg_generation_id()
    parsed = UUID(value)
    assert parsed.version == 4
    assert is_valid_kg_generation_id(value)


def test_uuid_v4_validator_rejects_v1_and_non_uuid_strings() -> None:
    assert not is_valid_kg_generation_id("not-a-uuid")
    assert not is_valid_kg_generation_id("")
    assert not is_valid_kg_generation_id(
        "12345678-1234-1234-1234-1234567890ab"
    )  # v1-ish, not v4
    assert is_valid_kg_generation_id("11111111-1111-4111-8111-111111111111")


# -------- Repository persistence -------------------------------------------


def test_get_current_returns_none_when_no_pointer(base_dir: Path) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    assert repo.get_current(BOARD) is None


def test_promote_current_persists_pointer_and_history(base_dir: Path) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    gen_id = generate_kg_generation_id()
    result = repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_id,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
        run_id=RUN,
    )
    assert result.outcome == PromotionOutcome.PROMOTED.value
    assert result.current_kg_generation_id == gen_id
    assert result.previous_kg_generation_id is None
    assert result.history_ref is not None
    assert Path(result.history_ref).exists()
    assert repo.get_current(BOARD) == gen_id

    history = repo.load_history(BOARD, gen_id)
    assert history is not None
    assert history["status"] == "completed"
    assert history["structural_hash"] == STRUCT_HASH
    assert history["source_hash"] == SOURCE_HASH
    assert history["run_id"] == RUN


# -------- IR ir_6d092147 — report_ref required -----------------------------


@pytest.mark.parametrize(
    "bad_ref",
    [None, "", "   ", "\t"],
    ids=["none", "empty", "whitespace", "tab"],
)
def test_promote_current_rejects_missing_report_ref(
    base_dir: Path, bad_ref: object
) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    gen_id = generate_kg_generation_id()
    result = repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_id,
        report_ref=bad_ref,  # type: ignore[arg-type]
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    assert result.outcome == PromotionOutcome.REPORT_REF_REQUIRED.value
    assert result.current_kg_generation_id is None
    assert repo.get_current(BOARD) is None
    assert (
        get_promotion_count(
            BOARD,
            outcome=PromotionOutcome.REPORT_REF_REQUIRED.value,
        )
        == 1
    )


def test_promote_current_rejects_invalid_status(base_dir: Path) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    gen_id = generate_kg_generation_id()
    result = repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_id,
        report_ref=REPORT_REF,
        status="rebuild_failed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    assert result.outcome == PromotionOutcome.INVALID_STATUS.value
    assert repo.get_current(BOARD) is None


def test_promote_current_rejects_invalid_generation_id(base_dir: Path) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    result = repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id="not-a-uuid",
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    assert result.outcome == PromotionOutcome.INVALID_KG_GENERATION_ID.value
    assert repo.get_current(BOARD) is None


def test_promote_current_detects_conflict_with_stored_previous(
    base_dir: Path,
) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    gen_a = generate_kg_generation_id()
    repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_a,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    gen_b = generate_kg_generation_id()
    # promote_current is asked to advance from a stale previous pointer.
    wrong_previous = generate_kg_generation_id()
    result = repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=wrong_previous,
        kg_generation_id=gen_b,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    assert result.outcome == PromotionOutcome.GENERATION_CONFLICT.value
    # Pointer stays at gen_a — previous safe generation preserved.
    assert repo.get_current(BOARD) == gen_a


def test_promote_current_after_successful_chain(base_dir: Path) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    gen_a = generate_kg_generation_id()
    repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_a,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    gen_b = generate_kg_generation_id()
    result = repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=gen_a,
        kg_generation_id=gen_b,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    assert result.outcome == PromotionOutcome.PROMOTED.value
    assert repo.get_current(BOARD) == gen_b
    assert repo.get_history_ref(BOARD, gen_a) is not None
    assert repo.get_history_ref(BOARD, gen_b) is not None


# -------- Promotion guard (api_bacb7555) -----------------------------------


def test_guard_eligible_when_all_fields_valid() -> None:
    gen_id = generate_kg_generation_id()
    evaluation = KGGenerationPromotionGuard.evaluate(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_id,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
    )
    assert evaluation.eligible is True
    assert evaluation.reasons == ()
    assert evaluation.required_operator_action is None


def test_guard_requires_report_ref() -> None:
    evaluation = KGGenerationPromotionGuard.evaluate(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=generate_kg_generation_id(),
        report_ref="",
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
    )
    assert evaluation.eligible is False
    assert "report_ref_required" in evaluation.reasons
    assert evaluation.required_operator_action == "persist_rebuild_report"


def test_guard_flags_structural_hash_mismatch_without_override() -> None:
    gen_id = generate_kg_generation_id()
    evaluation = KGGenerationPromotionGuard.evaluate(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_id,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash="a" * 64,
        source_hash=SOURCE_HASH,
        expected_structural_hash="z" * 64,
    )
    assert evaluation.eligible is False
    assert "structural_hash_mismatch" in evaluation.reasons


def test_guard_accepts_structural_mismatch_with_operator_override() -> None:
    gen_id = generate_kg_generation_id()
    evaluation = KGGenerationPromotionGuard.evaluate(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=gen_id,
        report_ref=REPORT_REF,
        status="completed",
        structural_hash="a" * 64,
        source_hash=SOURCE_HASH,
        expected_structural_hash="z" * 64,
        operator_override_ref="override-ticket-123",
    )
    assert evaluation.eligible is True
    assert "structural_hash_mismatch" not in evaluation.reasons


def test_guard_flags_generation_conflict_when_observed_previous_differs() -> None:
    prev = generate_kg_generation_id()
    observed = generate_kg_generation_id()
    evaluation = KGGenerationPromotionGuard.evaluate(
        board_id=BOARD,
        previous_kg_generation_id=prev,
        kg_generation_id=generate_kg_generation_id(),
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        observed_previous_kg_generation_id=observed,
    )
    assert evaluation.eligible is False
    assert "generation_conflict" in evaluation.reasons


def test_guard_rejects_unknown_status() -> None:
    evaluation = KGGenerationPromotionGuard.evaluate(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=generate_kg_generation_id(),
        report_ref=REPORT_REF,
        status="bogus_status",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
    )
    assert evaluation.eligible is False
    assert "invalid_status" in evaluation.reasons


def test_promotable_terminal_statuses_freeze() -> None:
    assert PROMOTABLE_TERMINAL_STATUSES == frozenset(
        {"completed", "partially_rebuilt", "rolled_back"}
    )


def test_promotion_counter_labels_bounded() -> None:
    labels = get_promotion_counter_labels()
    assert labels == ("board_id", "status", "outcome")


def test_promotion_samples_contain_canonical_outcomes(base_dir: Path) -> None:
    repo = KGGenerationRepository(base_dir=base_dir)
    repo.promote_current(
        board_id=BOARD,
        previous_kg_generation_id=None,
        kg_generation_id=generate_kg_generation_id(),
        report_ref=REPORT_REF,
        status="completed",
        structural_hash=STRUCT_HASH,
        source_hash=SOURCE_HASH,
        promoted_by=ACTOR,
    )
    samples = get_promotion_samples()
    assert any(
        s["outcome"] == PromotionOutcome.PROMOTED.value
        and s["status"] == "completed"
        for s in samples
    )

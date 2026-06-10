"""Tests for KG-02.4 rebuild report store + terminal state guard.

Covers:
* TR7 — RebuildReportStore.persist rejects payloads that look like they
  contain sensitive material; canonical hashes + refs pass through.
* TR16 + br_82deef11 — TerminalGuard surfaces report_persist_failed and
  blocks promotion when the report didn't persist.
* OR or_56ec0300 / or_9b4a7726 / or_f933b2ba — counters bump for every
  bounded outcome.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okto_pulse.core.kg.rebuild_report import (
    RebuildReportPayload,
    RebuildReportStore,
    RebuildReportSummary,
    RebuildReportTerminalStateGuard,
    ReportEvent,
    ReportPersistOutcome,
    ReportPersistResult,
    TERMINAL_STATUSES_REQUIRING_REPORT,
    get_persist_count,
    get_persist_counter_labels,
    get_report_count,
    get_report_counter_labels,
    get_terminal_count,
    get_terminal_counter_labels,
    list_sensitive_key_pattern_strings,
    reset_persist_counter,
    reset_report_counter,
    reset_terminal_counter,
)


BOARD = "board-001"
RUN = "run_a"


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg-02-4-report"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_report_counter()
    reset_persist_counter()
    reset_terminal_counter()
    yield
    reset_report_counter()
    reset_persist_counter()
    reset_terminal_counter()


def _summary(status: str = "completed") -> RebuildReportSummary:
    return RebuildReportSummary(
        board_id=BOARD,
        run_id=RUN,
        status=status,
        started_at="2026-05-25T00:00:00+00:00",
        finished_at="2026-05-25T00:00:05+00:00",
        counts={"nodes": 10, "edges": 5},
        triggered_by="actor-1",
        previous_kg_generation_id=None,
        kg_generation_id="11111111-1111-4111-8111-111111111111",
    )


def _clean_payload(status: str = "completed") -> RebuildReportPayload:
    return RebuildReportPayload(
        summary=_summary(status),
        hashes={
            "structural_hash": "a" * 64,
            "source_hash": "b" * 64,
        },
        source_refs=("rebuild_manifest_abc",),
        reconciliation_decisions=(
            {"node": "Spec:1", "decision": "kept", "score": 0.99},
        ),
        drilldown={
            "specs": [{"id": "spec-1", "status": "ok"}],
            "errors": [],
        },
        operator_notes="planned recovery",
    )


# -------- Successful persistence -------------------------------------------


def test_persist_stores_clean_payload_and_returns_report_ref(
    base_dir: Path,
) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    result = store.persist(payload=_clean_payload())
    assert result.outcome == ReportPersistOutcome.STORED.value
    assert result.report_ref is not None
    assert Path(result.report_ref).exists()
    assert result.report_id is not None
    assert result.report_id.startswith("report_")
    # OR or_56ec0300 + or_9b4a7726
    assert (
        get_report_count(BOARD, event=ReportEvent.CREATED.value) == 1
    )
    assert (
        get_persist_count(
            BOARD, outcome=ReportPersistOutcome.STORED.value
        )
        == 1
    )


def test_persist_loaded_payload_round_trips(base_dir: Path) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    result = store.persist(payload=_clean_payload())
    loaded = store.load(result.report_ref)
    assert loaded is not None
    assert loaded["summary"]["board_id"] == BOARD
    assert loaded["hashes"]["structural_hash"] == "a" * 64
    # opened counter bumps
    assert get_report_count(BOARD, event=ReportEvent.OPENED.value) == 1


# -------- TR7 — sensitive payload rejection --------------------------------


@pytest.mark.parametrize(
    "drilldown",
    [
        {"specs": [{"password": "hunter2"}]},
        {"context": {"api_key": "AKIA0000ABCD"}},
        {"meta": {"bearer_token": "abc"}},
        {"creds": {"credential": "leaked"}},
        {"lock": {"owner_token": "X" * 60}},
    ],
    ids=["password", "api_key", "bearer_token", "credential", "owner_token"],
)
def test_persist_rejects_sensitive_keys(
    base_dir: Path, drilldown: dict
) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    payload = RebuildReportPayload(
        summary=_summary(),
        hashes={"structural_hash": "a" * 64},
        source_refs=("manifest_xyz",),
        drilldown=drilldown,
    )
    result = store.persist(payload=payload)
    assert (
        result.outcome
        == ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value
    )
    assert result.report_ref is None
    assert (
        get_persist_count(
            BOARD,
            outcome=ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value,
        )
        == 1
    )
    assert (
        get_report_count(BOARD, event=ReportEvent.FAILED.value) == 1
    )


# val_da282108 regression: owner_token MUST never make it into a report,
# regardless of where it appears in the nested payload. Previously the
# allowlist had ``owner_token`` and these payloads were silently stored.
@pytest.mark.parametrize(
    "payload_factory",
    [
        lambda: RebuildReportPayload(
            summary=_summary(),
            hashes={"structural_hash": "f" * 64},
            source_refs=("manifest_xyz",),
            drilldown={"lock": {"owner_token": "X" * 60}},
        ),
        lambda: RebuildReportPayload(
            summary=_summary(),
            hashes={"structural_hash": "f" * 64},
            source_refs=("manifest_xyz",),
            drilldown={
                "events": [
                    {"step": "acquire", "owner_token": "OPAQUE-LOCK-MATERIAL"}
                ]
            },
        ),
        lambda: RebuildReportPayload(
            summary=_summary(),
            hashes={"structural_hash": "f" * 64},
            source_refs=("manifest_xyz",),
            reconciliation_decisions=(
                {"node": "Spec:1", "owner_token": "kg-01-lock-handle"},
            ),
        ),
        lambda: RebuildReportPayload(
            summary=_summary(),
            hashes={"owner_token": "f" * 64},
            source_refs=("manifest_xyz",),
        ),
    ],
    ids=["nested_dict", "list_of_dicts", "reconciliation_decision", "hashes_key"],
)
def test_persist_rejects_owner_token_anywhere_in_payload(
    base_dir: Path, payload_factory
) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    result = store.persist(payload=payload_factory())
    assert (
        result.outcome
        == ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value
    ), "owner_token must trip TR7 sensitive payload check"
    assert result.report_ref is None
    assert result.report_id is None
    assert (
        get_persist_count(
            BOARD,
            outcome=ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value,
        )
        >= 1
    )


def test_persist_accepts_canonical_refs_that_contain_token_word(
    base_dir: Path,
) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    # report_ref / manifest_ref / confirmation_id are allowlisted even
    # though the values can look opaque; canonical hashes also pass.
    payload = RebuildReportPayload(
        summary=_summary(),
        hashes={"structural_hash": "f" * 64},
        source_refs=("manifest_xyz",),
        drilldown={
            "summary": {
                "manifest_ref": "rebuild_manifest_abcdef0123456789",
                "confirmation_id": "conf_token_ok",
                "audit_ref": "/abs/audit.json",
            }
        },
    )
    result = store.persist(payload=payload)
    assert result.outcome == ReportPersistOutcome.STORED.value


def test_persist_rejects_suspect_long_base64ish_value(base_dir: Path) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    payload = RebuildReportPayload(
        summary=_summary(),
        hashes={"structural_hash": "f" * 64},
        source_refs=("manifest_xyz",),
        drilldown={
            "weird": {
                "leaked": (
                    "AKIAIOSFODNN7EXAMPLE0123456789abcdefABCD+/+/="
                )
            }
        },
    )
    result = store.persist(payload=payload)
    assert (
        result.outcome
        == ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value
    )


def test_persist_persistence_failure_bumps_store_failed(
    base_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = RebuildReportStore(base_dir=base_dir)
    # Force the file write to raise — verifies that the IO error path
    # surfaces store_failed and bumps both counters.
    import json

    original = json.dump

    def boom(*_args, **_kwargs) -> None:
        raise OSError("simulated disk full")

    monkeypatch.setattr(json, "dump", boom)
    result = store.persist(payload=_clean_payload())
    # restore so other tests aren't polluted
    monkeypatch.setattr(json, "dump", original)
    assert result.outcome == ReportPersistOutcome.STORE_FAILED.value
    assert result.report_ref is None
    assert (
        get_persist_count(
            BOARD, outcome=ReportPersistOutcome.STORE_FAILED.value
        )
        == 1
    )


# -------- Terminal state guard (api_1b43931d) ------------------------------


def test_guard_stored_yields_candidate_status() -> None:
    persist = ReportPersistResult(
        outcome=ReportPersistOutcome.STORED.value,
        report_ref="/path/to/report.json",
        report_id="report_abc",
        board_id=BOARD,
        run_id=RUN,
        persisted_at="2026-05-25T00:00:05+00:00",
    )
    decision = RebuildReportTerminalStateGuard.require_report_ref(
        board_id=BOARD,
        run_id=RUN,
        candidate_terminal_status="completed",
        report_persist_result=persist,
        previous_kg_generation_id=None,
        kg_generation_id="11111111-1111-4111-8111-111111111111",
    )
    assert decision.publishable_status == "completed"
    assert decision.report_ref_required is True
    assert decision.promotion_allowed is True
    assert decision.operator_action is None
    assert (
        get_terminal_count(
            BOARD,
            candidate_terminal_status="completed",
            publishable_status="completed",
            with_report_ref=True,
        )
        == 1
    )


def test_guard_store_failed_blocks_promotion() -> None:
    persist = ReportPersistResult(
        outcome=ReportPersistOutcome.STORE_FAILED.value,
        report_ref=None,
        report_id=None,
        board_id=BOARD,
        run_id=RUN,
        persisted_at=None,
        detail="disk full",
    )
    decision = RebuildReportTerminalStateGuard.require_report_ref(
        board_id=BOARD,
        run_id=RUN,
        candidate_terminal_status="completed",
        report_persist_result=persist,
        previous_kg_generation_id=None,
        kg_generation_id=None,
    )
    assert decision.publishable_status == "report_persist_failed"
    assert decision.promotion_allowed is False
    assert decision.operator_action == "retry_report_persist"
    # Counter splits stored vs. with_report_ref so the dashboard shows
    # missing receipts.
    assert (
        get_terminal_count(
            BOARD,
            candidate_terminal_status="completed",
            publishable_status="report_persist_failed",
            with_report_ref=False,
        )
        == 1
    )


def test_guard_sensitive_payload_redact_action() -> None:
    persist = ReportPersistResult(
        outcome=ReportPersistOutcome.SENSITIVE_PAYLOAD_REJECTED.value,
        report_ref=None,
        report_id=None,
        board_id=BOARD,
        run_id=RUN,
        persisted_at=None,
        detail="sensitive",
    )
    decision = RebuildReportTerminalStateGuard.require_report_ref(
        board_id=BOARD,
        run_id=RUN,
        candidate_terminal_status="completed",
        report_persist_result=persist,
        previous_kg_generation_id=None,
        kg_generation_id=None,
    )
    assert decision.publishable_status == "report_persist_failed"
    assert decision.promotion_allowed is False
    assert decision.operator_action == "redact_payload_and_retry"


@pytest.mark.parametrize(
    "status",
    sorted(TERMINAL_STATUSES_REQUIRING_REPORT),
)
def test_guard_accepts_all_canonical_terminal_statuses(status: str) -> None:
    persist = ReportPersistResult(
        outcome=ReportPersistOutcome.STORED.value,
        report_ref="/x/y.json",
        report_id="report_x",
        board_id=BOARD,
        run_id=RUN,
        persisted_at="2026-05-25T00:00:05+00:00",
    )
    decision = RebuildReportTerminalStateGuard.require_report_ref(
        board_id=BOARD,
        run_id=RUN,
        candidate_terminal_status=status,
        report_persist_result=persist,
        previous_kg_generation_id=None,
        kg_generation_id=None,
    )
    assert decision.publishable_status == status
    # rebuild_failed / recovery_failed should NOT promote.
    if status in {"completed", "partially_rebuilt", "rolled_back"}:
        assert decision.promotion_allowed is True
    else:
        assert decision.promotion_allowed is False


def test_guard_rejects_unknown_terminal_status() -> None:
    persist = ReportPersistResult(
        outcome=ReportPersistOutcome.STORED.value,
        report_ref="/x/y.json",
        report_id="report_x",
        board_id=BOARD,
        run_id=RUN,
        persisted_at="2026-05-25T00:00:05+00:00",
    )
    with pytest.raises(ValueError, match="invalid_terminal_status"):
        RebuildReportTerminalStateGuard.require_report_ref(
            board_id=BOARD,
            run_id=RUN,
            candidate_terminal_status="not_a_status",
            report_persist_result=persist,
            previous_kg_generation_id=None,
            kg_generation_id=None,
        )


# -------- Counter labels are bounded (no raw ids) --------------------------


def test_report_counter_labels_are_bounded() -> None:
    assert get_report_counter_labels() == ("board_id", "status", "event")


def test_persist_counter_labels_are_bounded() -> None:
    assert get_persist_counter_labels() == ("board_id", "status", "outcome")


def test_terminal_counter_labels_are_bounded() -> None:
    assert get_terminal_counter_labels() == (
        "board_id",
        "candidate_terminal_status",
        "publishable_status",
        "with_report_ref",
    )


def test_sensitive_pattern_list_covers_expected_families() -> None:
    patterns = list_sensitive_key_pattern_strings()
    joined = " ".join(patterns)
    for needle in ("password", "secret", "token", "api", "credential"):
        assert needle in joined

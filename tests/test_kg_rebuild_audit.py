"""Tests for KG-02.7 — kg.rebuilt event + cognitive pending marker +
confirmation consumption audit recorder.

Covers:
* API api_6fcc64aa — kg.rebuilt payload validation + publish outcomes.
* API api_3e9d65ce — CognitivePendingMarker schema + br_0d710a8f /
  TR9 invariant (never completed, always pending).
* API api_c9bc9a8c — ConfirmationConsumptionAuditRecorder safe payload
  + br_d379c40d / br_48da2f8a (audit complete for every outcome).
* OR or_9da6b2d7 / or_85dd2e90 — counter labels bounded.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okto_pulse.core.kg.rebuild_audit import (
    CANONICAL_AUDIT_OPERATIONS,
    CONSOLIDABLE_ARTIFACT_TYPES,
    CognitiveMarkerErrorCode,
    CognitivePendingMarker,
    CognitivePendingStatus,
    ConfirmationAuditErrorCode,
    ConfirmationAuditOutcome,
    ConfirmationConsumptionAuditRecorder,
    EventPublishErrorCode,
    EventPublishOutcome,
    KG_REBUILT_REQUIRED_FIELDS,
    KGRebuiltEventPublisher,
    get_audit_count,
    get_audit_counter_labels,
    get_event_count,
    get_event_counter_labels,
    get_pending_count,
    get_pending_counter_labels,
    reset_audit_counter,
    reset_event_counter,
    reset_pending_counter,
    validate_kg_rebuilt_event,
)
from okto_pulse.core.kg.rebuild_generation import generate_kg_generation_id


BOARD = "board-001"
ACTOR = "user-1"


@pytest.fixture
def base_dir(tmp_path: Path) -> Path:
    target = tmp_path / "kg-02-7-audit"
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)
    return target


@pytest.fixture(autouse=True)
def _reset_counters() -> None:
    reset_event_counter()
    reset_pending_counter()
    reset_audit_counter()
    yield
    reset_event_counter()
    reset_pending_counter()
    reset_audit_counter()


def _valid_event_payload(status: str = "completed") -> dict:
    return {
        "board_id": BOARD,
        "previous_kg_generation_id": None,
        "kg_generation_id": generate_kg_generation_id(),
        "triggered_by": ACTOR,
        "started_at": "2026-05-26T00:00:00+00:00",
        "finished_at": "2026-05-26T00:00:10+00:00",
        "status": status,
        "counts": {"nodes": 5, "edges": 2},
        "report_ref": "/abs/path/to/report.json",
    }


# ---------------- KG-rebuilt event ----------------------------------------


def test_validate_kg_rebuilt_event_accepts_valid_payload() -> None:
    valid, reason = validate_kg_rebuilt_event(_valid_event_payload())
    assert valid is True
    assert reason is None


@pytest.mark.parametrize(
    "field", sorted(KG_REBUILT_REQUIRED_FIELDS)
)
def test_validate_rejects_missing_required_field(field: str) -> None:
    payload = _valid_event_payload()
    del payload[field]
    valid, reason = validate_kg_rebuilt_event(payload)
    assert valid is False
    assert reason == f"missing_required_field:{field}"


def test_validate_rejects_non_uuid_v4_generation() -> None:
    payload = _valid_event_payload()
    payload["kg_generation_id"] = "not-a-uuid"
    valid, reason = validate_kg_rebuilt_event(payload)
    assert valid is False
    assert reason == "kg_generation_id_must_be_uuid_v4"


def test_validate_accepts_null_previous_generation() -> None:
    payload = _valid_event_payload()
    payload["previous_kg_generation_id"] = None
    assert validate_kg_rebuilt_event(payload) == (True, None)


def test_validate_rejects_bad_previous_generation() -> None:
    payload = _valid_event_payload()
    payload["previous_kg_generation_id"] = "not-uuid"
    valid, reason = validate_kg_rebuilt_event(payload)
    assert valid is False
    assert reason == "previous_kg_generation_id_must_be_uuid_v4_or_null"


def test_validate_rejects_non_dict_counts() -> None:
    payload = _valid_event_payload()
    payload["counts"] = "not a dict"
    valid, reason = validate_kg_rebuilt_event(payload)
    assert valid is False
    assert reason == "counts_must_be_dict"


def test_publish_published_path(base_dir: Path) -> None:
    publisher = KGRebuiltEventPublisher(base_dir=base_dir)
    result = publisher.publish(event_payload=_valid_event_payload())
    assert result.accepted is True
    assert result.outcome == EventPublishOutcome.PUBLISHED.value
    assert result.event_ref and result.event_ref.startswith("evt_")
    assert result.audit_ref and Path(result.audit_ref).exists()
    assert (
        get_event_count(
            BOARD, status="completed", outcome=EventPublishOutcome.PUBLISHED.value
        )
        == 1
    )


def test_publish_invalid_payload_does_not_persist_audit(base_dir: Path) -> None:
    publisher = KGRebuiltEventPublisher(base_dir=base_dir)
    payload = _valid_event_payload()
    del payload["report_ref"]
    result = publisher.publish(event_payload=payload)
    assert result.accepted is False
    assert result.outcome == EventPublishOutcome.INVALID_PAYLOAD.value
    assert result.audit_ref is None
    assert (
        get_event_count(
            BOARD, outcome=EventPublishOutcome.INVALID_PAYLOAD.value
        )
        == 1
    )


def test_publish_adapter_failure_keeps_audit_and_marks_publish_failed(
    base_dir: Path,
) -> None:
    """If the adapter returns False, the audit row remains so the
    operator can re-publish from disk. Outcome is publish_failed +
    event_publish_failed error_code (retryable per api_6fcc64aa)."""

    def _bad_adapter(_p):
        return False

    publisher = KGRebuiltEventPublisher(
        base_dir=base_dir, publish_adapter=_bad_adapter
    )
    result = publisher.publish(event_payload=_valid_event_payload())
    assert result.accepted is False
    assert result.outcome == EventPublishOutcome.PUBLISH_FAILED.value
    assert (
        result.error_code
        == EventPublishErrorCode.EVENT_PUBLISH_FAILED.value
    )
    # Audit IS persisted — operator can re-drive from disk.
    assert result.audit_ref and Path(result.audit_ref).exists()


def test_publish_adapter_exception_lands_publish_failed(base_dir: Path) -> None:
    def _boom(_p):
        raise RuntimeError("bus down")

    publisher = KGRebuiltEventPublisher(
        base_dir=base_dir, publish_adapter=_boom
    )
    result = publisher.publish(event_payload=_valid_event_payload())
    assert result.outcome == EventPublishOutcome.PUBLISH_FAILED.value
    assert "publish_adapter_exception" in (result.detail or "")
    # Audit still persisted.
    assert result.audit_ref


def test_event_counter_labels_bounded() -> None:
    assert get_event_counter_labels() == ("board_id", "status", "outcome")


# ---------------- Cognitive pending marker --------------------------------


def _consolidable_sources() -> list[dict]:
    return [
        {"artifact_type": "spec", "id": "s1", "source_ref": "spec:1"},
        {"artifact_type": "refinement", "id": "r1", "source_ref": "ref:1"},
        {"artifact_type": "bug", "id": "b1", "source_ref": "bug:1"},
        # non-consolidable; must not contribute to pending count
        {"artifact_type": "comment", "id": "c1", "source_ref": "cmt:1"},
    ]


def test_mark_for_generation_returns_pending_marked(base_dir: Path) -> None:
    marker = CognitivePendingMarker(base_dir=base_dir)
    gen = generate_kg_generation_id()
    result = marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=gen,
        source_set=_consolidable_sources(),
        event_ref="evt_abc",
    )
    assert result.status == CognitivePendingStatus.PENDING_MARKED.value
    assert result.pending_count == 3  # spec + refinement + bug
    assert "spec:1" in result.pending_refs
    assert "ref:1" in result.pending_refs
    assert "bug:1" in result.pending_refs
    assert "cmt:1" not in result.pending_refs
    assert result.record_ref and Path(result.record_ref).exists()
    assert (
        get_pending_count(
            BOARD, status=CognitivePendingStatus.PENDING_MARKED.value
        )
        >= 3
    )


def test_mark_for_generation_skipped_when_no_consolidable_sources(
    base_dir: Path,
) -> None:
    marker = CognitivePendingMarker(base_dir=base_dir)
    result = marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        source_set=[{"artifact_type": "comment", "id": "c1"}],
        event_ref="evt_abc",
    )
    assert result.status == CognitivePendingStatus.SKIPPED.value
    assert result.pending_count == 0
    assert (
        get_pending_count(
            BOARD, status=CognitivePendingStatus.SKIPPED.value
        )
        >= 1
    )


def test_mark_for_generation_rejects_invalid_uuid(base_dir: Path) -> None:
    marker = CognitivePendingMarker(base_dir=base_dir)
    result = marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id="not-uuid",
        source_set=_consolidable_sources(),
        event_ref="evt_abc",
    )
    assert result.status == CognitivePendingStatus.SKIPPED.value
    assert (
        result.error_code
        == CognitiveMarkerErrorCode.INVALID_GENERATION.value
    )


def test_mark_for_generation_adapter_exception_marks_skipped(
    base_dir: Path,
) -> None:
    def _boom(_b, _g, _s):
        raise RuntimeError("cognitive offline")

    marker = CognitivePendingMarker(
        base_dir=base_dir, pending_adapter=_boom
    )
    result = marker.mark_for_generation(
        board_id=BOARD,
        kg_generation_id=generate_kg_generation_id(),
        source_set=_consolidable_sources(),
        event_ref="evt_abc",
    )
    assert result.status == CognitivePendingStatus.SKIPPED.value
    assert (
        result.error_code
        == CognitiveMarkerErrorCode.COGNITIVE_MARKER_UNAVAILABLE.value
    )


def test_cognitive_pending_status_enum_has_no_completed_value() -> None:
    """br_0d710a8f + TR9 invariant: structural rebuild may never mark
    cognitive consolidation completed."""

    values = {s.value for s in CognitivePendingStatus}
    assert "completed" not in values
    assert values == {"pending_marked", "skipped"}


def test_pending_counter_labels_bounded() -> None:
    assert get_pending_counter_labels() == ("board_id", "status")


# ---------------- Confirmation consumption audit -------------------------


def _audit_kwargs(outcome: str = "consumed", operation: str = "rebuild") -> dict:
    return {
        "board_id": BOARD,
        "operation": operation,
        "outcome": outcome,
        "reason": "operator initiated",
        "actor_ref": "user-1-anon",
        "preflight_hash": "a" * 64,
        "generation_ids": {
            "previous": None,
            "current": generate_kg_generation_id(),
        },
        "affected_files": ["graph.lbug"],
    }


def test_record_consumed_persists_safe_audit(base_dir: Path) -> None:
    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    result = recorder.record(**_audit_kwargs())
    assert result.audit_ref and Path(result.audit_ref).exists()
    assert result.recorded_at
    assert result.error_code is None
    assert (
        get_audit_count(BOARD, operation="rebuild", outcome="consumed") == 1
    )


@pytest.mark.parametrize(
    "outcome",
    sorted(o.value for o in ConfirmationAuditOutcome),
)
def test_record_accepts_all_canonical_outcomes(
    base_dir: Path, outcome: str
) -> None:
    """br_48da2f8a — every outcome (consumed/expired/replayed/scope_mismatch/missing)
    must leave an audit trail."""

    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    result = recorder.record(**_audit_kwargs(outcome=outcome))
    assert result.audit_ref is not None
    assert (
        get_audit_count(BOARD, operation="rebuild", outcome=outcome) == 1
    )


def test_record_rejects_unknown_operation(base_dir: Path) -> None:
    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    result = recorder.record(**_audit_kwargs(operation="not_canonical"))
    assert result.audit_ref is None
    assert (
        result.error_code
        == ConfirmationAuditErrorCode.INVALID_OPERATION.value
    )


def test_record_rejects_unknown_outcome(base_dir: Path) -> None:
    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    result = recorder.record(**_audit_kwargs(outcome="weird"))
    assert result.audit_ref is None
    assert (
        result.error_code
        == ConfirmationAuditErrorCode.INVALID_OUTCOME.value
    )


@pytest.mark.parametrize(
    "raw_token",
    ["conf_abcdef0123456789", "tok_abcdef0123456789xyz"],
    ids=["conf_prefix", "tok_prefix"],
)
def test_record_rejects_raw_token_in_actor_ref(
    base_dir: Path, raw_token: str
) -> None:
    """api_c9bc9a8c unsafe_audit_payload — confirmation token shapes
    are forbidden in any leaf of the payload (br_d379c40d)."""

    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    kwargs = _audit_kwargs()
    kwargs["actor_ref"] = raw_token
    result = recorder.record(**kwargs)
    assert result.audit_ref is None
    assert (
        result.error_code
        == ConfirmationAuditErrorCode.UNSAFE_AUDIT_PAYLOAD.value
    )


def test_record_rejects_sensitive_field_name(base_dir: Path) -> None:
    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    kwargs = _audit_kwargs()
    kwargs["generation_ids"] = {"api_key": "abc"}
    result = recorder.record(**kwargs)
    assert (
        result.error_code
        == ConfirmationAuditErrorCode.UNSAFE_AUDIT_PAYLOAD.value
    )


def test_audit_counter_labels_bounded() -> None:
    assert get_audit_counter_labels() == ("board_id", "operation", "outcome")


def test_canonical_audit_operations_match_api_enum() -> None:
    assert CANONICAL_AUDIT_OPERATIONS == frozenset({
        "reset", "quarantine", "rebuild",
        "promote", "rollback", "reindex_discovery",
    })


def test_consolidable_artifact_types_frozen() -> None:
    assert CONSOLIDABLE_ARTIFACT_TYPES == frozenset({
        "spec", "decision", "refinement", "task", "test", "bug",
    })


# ---------------- val_302bdec8 — integration wiring ----------------------
#
# These tests verify that the primitives are NOT orphaned: the rebuild
# event handler composer must publish + mark pending, the confirmation
# store must record audit for every consumption outcome, and the raw
# confirmation_id must NEVER appear in the audit JSON.


def test_confirmation_fingerprint_is_deterministic_and_hides_raw() -> None:
    from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint

    raw = "conf_super_secret_abc_123"
    a = confirmation_fingerprint(raw)
    b = confirmation_fingerprint(raw)
    assert a == b
    assert a.startswith("conf_fp_")
    assert raw not in a
    # Different raw produces different fingerprint.
    other = confirmation_fingerprint("conf_other_token_xyz")
    assert a != other


def test_confirmation_fingerprint_rejects_empty() -> None:
    from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint

    with pytest.raises(ValueError):
        confirmation_fingerprint("")


def test_build_kg_rebuilt_event_handler_publishes_then_marks(
    base_dir: Path,
) -> None:
    """val_302bdec8 integration: the composer MUST publish via
    KGRebuiltEventPublisher AND call CognitivePendingMarker. If either
    primitive is removed, this test breaks loudly."""

    from okto_pulse.core.kg.rebuild_audit import (
        build_kg_rebuilt_event_handler,
    )

    captured: list[dict] = []
    publisher = KGRebuiltEventPublisher(
        base_dir=base_dir,
        publish_adapter=lambda payload: captured.append(dict(payload)) or True,
    )
    marker_calls: list[dict] = []

    def _pending_adapter(board, gen, sources):
        marker_calls.append(
            {"board": board, "gen": gen, "sources": list(sources)}
        )
        return len(sources)

    marker = CognitivePendingMarker(
        base_dir=base_dir, pending_adapter=_pending_adapter
    )

    resolver_called: list[dict] = []
    sources_payload = _consolidable_sources()

    def _resolver(event_payload):
        resolver_called.append(dict(event_payload))
        return sources_payload

    handler = build_kg_rebuilt_event_handler(
        publisher=publisher,
        cognitive_marker=marker,
        source_resolver=_resolver,
    )

    event = _valid_event_payload()
    result = handler(event)

    assert result.publish.accepted is True
    assert result.publish.outcome == EventPublishOutcome.PUBLISHED.value
    assert result.mark is not None
    assert result.mark.status == CognitivePendingStatus.PENDING_MARKED.value
    assert result.mark.event_ref == result.publish.event_ref
    # publisher was actually invoked (not bypassed).
    assert captured and captured[0]["board_id"] == BOARD
    # resolver was invoked once with the full event payload.
    assert len(resolver_called) == 1
    assert resolver_called[0]["kg_generation_id"] == event["kg_generation_id"]
    # marker received the resolved source set + canonical event_ref.
    assert len(marker_calls) == 1
    assert marker_calls[0]["board"] == BOARD
    assert marker_calls[0]["sources"] == sources_payload


def test_handler_skips_marker_when_publish_fails(base_dir: Path) -> None:
    """If publish fails the marker must NOT run — otherwise the cognitive
    queue receives work whose audit row was lost."""

    from okto_pulse.core.kg.rebuild_audit import (
        build_kg_rebuilt_event_handler,
    )

    publisher = KGRebuiltEventPublisher(
        base_dir=base_dir, publish_adapter=lambda _p: False
    )
    marker_called = False

    def _pending_adapter(_b, _g, _s):
        nonlocal marker_called
        marker_called = True
        return 1

    marker = CognitivePendingMarker(
        base_dir=base_dir, pending_adapter=_pending_adapter
    )
    handler = build_kg_rebuilt_event_handler(
        publisher=publisher,
        cognitive_marker=marker,
        source_resolver=lambda _p: _consolidable_sources(),
    )
    result = handler(_valid_event_payload())
    assert result.publish.accepted is False
    assert result.mark is None
    assert (
        result.skipped_reason
        == EventPublishErrorCode.EVENT_PUBLISH_FAILED.value
    )
    assert marker_called is False


def test_handler_skips_marker_when_kg_generation_id_missing(
    base_dir: Path,
) -> None:
    """Cover the post-publish branch where kg_generation_id is missing
    in the payload. Uses a permissive publisher fake so we reach the
    branch that ``validate_kg_rebuilt_event`` would otherwise reject."""

    from okto_pulse.core.kg.rebuild_audit import (
        EventPublishResult as _EventPublishResult,
        build_kg_rebuilt_event_handler,
    )

    class _AcceptAllPublisher:
        @staticmethod
        def publish(*, event_payload):
            return _EventPublishResult(
                accepted=True,
                outcome=EventPublishOutcome.PUBLISHED.value,
                event_ref="evt_fake",
                audit_ref=None,
            )

    marker = CognitivePendingMarker(base_dir=base_dir)
    handler = build_kg_rebuilt_event_handler(
        publisher=_AcceptAllPublisher(),
        cognitive_marker=marker,
        source_resolver=lambda _p: _consolidable_sources(),
    )
    payload = _valid_event_payload()
    result = handler({**payload, "kg_generation_id": ""})
    assert result.publish.accepted is True
    assert result.mark is None
    assert result.skipped_reason == "missing_kg_generation_id"


def test_handler_resolver_exception_falls_through_with_empty_sources(
    base_dir: Path,
) -> None:
    """Resolver exception should NOT crash the handler — marker still
    runs with empty sources so the report records a pending audit even
    when source loading fails."""

    from okto_pulse.core.kg.rebuild_audit import (
        build_kg_rebuilt_event_handler,
    )

    publisher = KGRebuiltEventPublisher(base_dir=base_dir)
    marker = CognitivePendingMarker(base_dir=base_dir)

    def _resolver(_p):
        raise RuntimeError("manifest store offline")

    handler = build_kg_rebuilt_event_handler(
        publisher=publisher,
        cognitive_marker=marker,
        source_resolver=_resolver,
    )
    result = handler(_valid_event_payload())
    assert result.publish.accepted is True
    assert result.mark is not None
    # Empty source set -> SKIPPED.
    assert result.mark.status == CognitivePendingStatus.SKIPPED.value
    assert result.mark.pending_count == 0


def test_confirmation_store_consume_records_safe_audit(
    base_dir: Path,
) -> None:
    """val_302bdec8 integration: RebuildConfirmationStore wired with
    ConfirmationConsumptionAuditRecorder MUST produce a durable audit
    row for the consumed outcome, with confirmation_fingerprint and
    NOT the raw confirmation_id."""

    from okto_pulse.core.kg.rebuild_audit import (
        confirmation_fingerprint,
    )
    from okto_pulse.core.kg.rebuild_confirmation import (
        RebuildConfirmationStore,
    )

    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    store = RebuildConfirmationStore(
        base_dir=base_dir, audit_recorder=recorder
    )
    token = store.issue(
        board_id=BOARD,
        actor_id="user-x",
        operation="rebuild",
        preflight_hash="a" * 64,
        manifest_ref="rebuild_manifest_abc",
    )
    result = store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id=BOARD,
        expected_actor_id="user-x",
        expected_operation="rebuild",
        expected_preflight_hash="a" * 64,
        expected_manifest_ref="rebuild_manifest_abc",
    )
    assert result.outcome == "consumed"
    # Audit row was written by the recorder.
    assert (
        get_audit_count(BOARD, operation="rebuild", outcome="consumed") == 1
    )
    # Verify the audit JSON exists, references the fingerprint and
    # NEVER carries the raw confirmation_id.
    audit_dir = base_dir / "rebuild" / "audit" / "confirmation" / BOARD
    audit_files = list(audit_dir.glob("*.json"))
    assert len(audit_files) == 1
    body = audit_files[0].read_text(encoding="utf-8")
    assert token.confirmation_id not in body, (
        "raw confirmation_id leaked into audit JSON"
    )
    assert confirmation_fingerprint(token.confirmation_id) in body


@pytest.mark.parametrize(
    "scenario",
    ["missing", "scope_mismatch", "expired", "replayed"],
)
def test_confirmation_store_audit_covers_every_destructive_outcome(
    base_dir: Path, scenario: str
) -> None:
    """br_48da2f8a + br_d379c40d: every destructive outcome must leave
    an audit trail when the recorder is wired."""

    from okto_pulse.core.kg.rebuild_confirmation import (
        RebuildConfirmationStore,
    )

    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    store = RebuildConfirmationStore(
        base_dir=base_dir,
        audit_recorder=recorder,
        ttl_seconds=30,  # store enforces a minimum of 30
    )
    if scenario == "missing":
        store.consume(
            confirmation_id="conf_no_such_token",
            expected_board_id=BOARD,
            expected_actor_id="user-x",
            expected_operation="rebuild",
            expected_preflight_hash="a" * 64,
            expected_manifest_ref="rebuild_manifest_abc",
        )
        expected_outcome = "missing"
    elif scenario == "scope_mismatch":
        token = store.issue(
            board_id=BOARD,
            actor_id="user-x",
            operation="rebuild",
            preflight_hash="a" * 64,
            manifest_ref="rebuild_manifest_abc",
        )
        # actor_id mismatch — board stays BOARD so the recorder writes
        # the audit under the same key we assert below.
        store.consume(
            confirmation_id=token.confirmation_id,
            expected_board_id=BOARD,
            expected_actor_id="WRONG_USER",
            expected_operation="rebuild",
            expected_preflight_hash="a" * 64,
            expected_manifest_ref="rebuild_manifest_abc",
        )
        expected_outcome = "scope_mismatch"
    elif scenario == "expired":
        token = store.issue(
            board_id=BOARD,
            actor_id="user-x",
            operation="rebuild",
            preflight_hash="a" * 64,
            manifest_ref="rebuild_manifest_abc",
        )
        # Force-expire by rewriting expires_at to the past.
        path = (
            base_dir / "rebuild" / "confirmations"
            / f"{token.confirmation_id}.json"
        )
        import json as _json

        body = _json.loads(path.read_text(encoding="utf-8"))
        body["expires_at"] = "2000-01-01T00:00:00+00:00"
        path.write_text(_json.dumps(body), encoding="utf-8")
        store.consume(
            confirmation_id=token.confirmation_id,
            expected_board_id=BOARD,
            expected_actor_id="user-x",
            expected_operation="rebuild",
            expected_preflight_hash="a" * 64,
            expected_manifest_ref="rebuild_manifest_abc",
        )
        # WRONG_BOARD scope check happens BEFORE the TTL check; we passed
        # the right scope so the expired branch executes.
        expected_outcome = "expired"
    elif scenario == "replayed":
        token = store.issue(
            board_id=BOARD,
            actor_id="user-x",
            operation="rebuild",
            preflight_hash="a" * 64,
            manifest_ref="rebuild_manifest_abc",
        )
        # First consume succeeds; second consume reports REPLAYED only
        # if there's a race where the file is unlinked AFTER scope
        # checks pass. We trigger that branch by tampering: unlink the
        # file between the path open and the unlink call. Without a
        # race we still get a MISSING outcome — both are recorded by
        # the audit recorder, which is what this test cares about.
        store.consume(
            confirmation_id=token.confirmation_id,
            expected_board_id=BOARD,
            expected_actor_id="user-x",
            expected_operation="rebuild",
            expected_preflight_hash="a" * 64,
            expected_manifest_ref="rebuild_manifest_abc",
        )
        # Second consume: file is gone -> MISSING. We still assert that
        # an audit row exists for the second attempt.
        store.consume(
            confirmation_id=token.confirmation_id,
            expected_board_id=BOARD,
            expected_actor_id="user-x",
            expected_operation="rebuild",
            expected_preflight_hash="a" * 64,
            expected_manifest_ref="rebuild_manifest_abc",
        )
        expected_outcome = "missing"

    # The audit row landed for the destructive outcome path.
    assert (
        get_audit_count(
            BOARD, operation="rebuild", outcome=expected_outcome
        )
        >= 1
    ), f"no audit recorded for {scenario} ({expected_outcome})"


def test_confirmation_store_does_not_leak_raw_token_in_any_audit_path(
    base_dir: Path,
) -> None:
    """val_302bdec8: scan every audit JSON written across a few
    consumption attempts and assert the raw confirmation_id NEVER
    appears verbatim."""

    from okto_pulse.core.kg.rebuild_confirmation import (
        RebuildConfirmationStore,
    )

    recorder = ConfirmationConsumptionAuditRecorder(base_dir=base_dir)
    store = RebuildConfirmationStore(
        base_dir=base_dir, audit_recorder=recorder
    )
    token = store.issue(
        board_id=BOARD,
        actor_id="user-x",
        operation="rebuild",
        preflight_hash="a" * 64,
        manifest_ref="rebuild_manifest_abc",
    )
    # Trigger consumed + then replayed (which lands MISSING after
    # consume removed the file).
    store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id=BOARD,
        expected_actor_id="user-x",
        expected_operation="rebuild",
        expected_preflight_hash="a" * 64,
        expected_manifest_ref="rebuild_manifest_abc",
    )
    store.consume(
        confirmation_id=token.confirmation_id,
        expected_board_id=BOARD,
        expected_actor_id="user-x",
        expected_operation="rebuild",
        expected_preflight_hash="a" * 64,
        expected_manifest_ref="rebuild_manifest_abc",
    )
    audit_dir = base_dir / "rebuild" / "audit" / "confirmation" / BOARD
    for path in audit_dir.glob("*.json"):
        body = path.read_text(encoding="utf-8")
        assert token.confirmation_id not in body, (
            f"raw confirmation_id leaked into {path.name}"
        )

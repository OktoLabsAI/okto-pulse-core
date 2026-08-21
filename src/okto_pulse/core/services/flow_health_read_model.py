"""Build governed Flow Health facts from append-only lifecycle events."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusion,
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
    AnalyticsSourceAuthority,
)
from okto_pulse.core.models.schemas import BoardSettings, FlowHealthSettings
from okto_pulse.core.ports.coverage_traceability import CoverageTraceabilityProjection
from okto_pulse.core.ports.flow_health import (
    FlowAuthorityState,
    FlowBlockerCode,
    FlowBlockerFact,
    FlowHealthPolicy,
    FlowHealthProjection,
    FlowLifecycleEvent,
    FlowLifecycleState,
    FlowPolicyOverride,
    FlowSubjectRef,
    FlowSubjectType,
)
from okto_pulse.core.services.flow_health import FlowHealthService, FlowSubjectFacts


_CARD_STATES = {
    "not_started": FlowLifecycleState.PENDING,
    "started": FlowLifecycleState.IN_PROGRESS,
    "in_progress": FlowLifecycleState.IN_PROGRESS,
    "validation": FlowLifecycleState.IN_PROGRESS,
    "on_hold": FlowLifecycleState.IN_PROGRESS,
    "rejected": FlowLifecycleState.REJECTED,
    "done": FlowLifecycleState.DONE,
    "cancelled": FlowLifecycleState.CANCELLED,
}
_SPEC_STATES = {
    "draft": FlowLifecycleState.BACKLOG,
    "review": FlowLifecycleState.PENDING,
    "approved": FlowLifecycleState.PENDING,
    "validated": FlowLifecycleState.PENDING,
    "in_progress": FlowLifecycleState.IN_PROGRESS,
    "done": FlowLifecycleState.DONE,
    "cancelled": FlowLifecycleState.CANCELLED,
}


def _value(value: object) -> str:
    return str(getattr(value, "value", value) or "").strip().lower()


def _utc(value: object) -> datetime | None:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _payload(row: object) -> Mapping[str, Any]:
    value = getattr(row, "payload_json", None)
    return value if isinstance(value, Mapping) else {}


def resolve_flow_health_policy(board: object) -> FlowHealthPolicy:
    """Resolve the closed BoardSettings policy, retaining legacy read support."""

    settings = getattr(board, "settings", None)
    legacy = False
    if isinstance(settings, BoardSettings):
        policy = settings.analytics.flow_health
    else:
        root = settings if isinstance(settings, Mapping) else {}
        analytics = root.get("analytics")
        nested = (
            analytics.get("flow_health") if isinstance(analytics, Mapping) else None
        )
        raw = nested if nested is not None else root.get("flow_health")
        if raw is None:
            policy = BoardSettings.model_validate(root).analytics.flow_health
        else:
            legacy = nested is None
            policy = FlowHealthSettings.model_validate(raw)
    overrides = tuple(
        sorted(
            (
                FlowPolicyOverride(FlowLifecycleState(str(state)), hours)
                for state, hours in policy.overrides.items()
            ),
            key=lambda item: item.state.value,
        )
    )
    return FlowHealthPolicy(
        version=policy.version,
        authority_ref=(
            f"board:{board.id}:flow-health:legacy-v{policy.version}"
            if legacy
            else f"board:{board.id}:settings:analytics:flow-health:v{policy.version}"
        ),
        general_stale_hours=policy.general_stale_hours,
        rejected_stale_hours=policy.rejected_stale_hours,
        overrides=overrides,
    )


def _coverage_blockers(
    coverage: CoverageTraceabilityProjection | None,
) -> dict[str, FlowBlockerFact]:
    if coverage is None:
        return {}
    by_spec: dict[str, list[object]] = {}
    for group in coverage.coverage:
        if group.obligation_type.value not in {"ac", "test_scenario"}:
            continue
        for row in group.rows:
            if row.applicable and row.covered is False:
                by_spec.setdefault(row.identity.spec_id, []).append(row)
    result: dict[str, FlowBlockerFact] = {}
    for spec_id, rows in by_spec.items():
        effective_skip = all(row.skip.effective for row in rows)
        result[spec_id] = FlowBlockerFact(
            FlowBlockerCode.UNCOVERED_TEST,
            FlowAuthorityState.CURRENT,
            f"coverage:{coverage.query_fingerprint}:spec:{spec_id}",
            effective_skip=effective_skip,
        )
    return result


def _validation_blocker(spec: object) -> FlowBlockerFact | None:
    if _value(getattr(spec, "status", None)) != "approved":
        return None
    validations = getattr(spec, "validations", None)
    rows = validations if isinstance(validations, list) else []
    approved = any(
        isinstance(item, Mapping)
        and _value(item.get("recommendation")) == "approve"
        and _value(item.get("outcome")) in {"success", "approved"}
        for item in rows
    )
    if approved:
        return None
    return FlowBlockerFact(
        FlowBlockerCode.SPEC_PENDING_VALIDATION,
        FlowAuthorityState.CURRENT,
        f"spec:{spec.id}:validation:current",
    )


def _subject_events(
    *,
    subject_type: FlowSubjectType,
    subject_id: str,
    rows: tuple[object, ...],
) -> tuple[FlowLifecycleEvent, ...] | None:
    subject = FlowSubjectRef(subject_type, subject_id)
    prefix = subject_type.value
    state_map = _CARD_STATES if subject_type is FlowSubjectType.CARD else _SPEC_STATES
    lifecycle: list[tuple[object, FlowLifecycleState | None, FlowLifecycleState]] = []
    rejection_details: list[object] = []
    for row in rows:
        event_type = str(getattr(row, "event_type", ""))
        payload = _payload(row)
        if (
            event_type == f"{prefix}.completion_rejected"
            and payload.get(f"{prefix}_id") == subject_id
        ):
            rejection_details.append(row)
            continue
        if payload.get(f"{prefix}_id") != subject_id:
            continue
        if event_type == f"{prefix}.created":
            initial = (
                FlowLifecycleState.PENDING
                if subject_type is FlowSubjectType.CARD
                else FlowLifecycleState.BACKLOG
            )
            lifecycle.append((row, None, initial))
        elif event_type == f"{prefix}.moved":
            before = state_map.get(_value(payload.get("from_status")))
            after = state_map.get(_value(payload.get("to_status")))
            if before is None or after is None:
                return None
            lifecycle.append((row, before, after))
    lifecycle.sort(
        key=lambda item: (
            _utc(getattr(item[0], "occurred_at", None))
            or datetime.min.replace(tzinfo=UTC),
            str(getattr(item[0], "id", "")),
        )
    )
    rejection_details.sort(
        key=lambda row: (
            _utc(getattr(row, "occurred_at", None)) or datetime.min.replace(tzinfo=UTC),
            str(getattr(row, "id", "")),
        )
    )
    if not lifecycle or lifecycle[0][1] is not None:
        return None
    events: list[FlowLifecycleEvent] = []
    detail_index = 0
    for sequence, (row, before, after) in enumerate(lifecycle, start=1):
        occurred_at = _utc(getattr(row, "occurred_at", None))
        if occurred_at is None:
            return None
        detail: dict[str, object] = {}
        if after is FlowLifecycleState.REJECTED:
            while (
                detail_index < len(rejection_details)
                and (
                    _utc(getattr(rejection_details[detail_index], "occurred_at", None))
                    or datetime.min.replace(tzinfo=UTC)
                )
                < occurred_at
            ):
                detail_index += 1
            if detail_index >= len(rejection_details):
                return None
            companion = rejection_details[detail_index]
            companion_payload = _payload(companion)
            detail_index += 1
            detail = {
                "rejection_kind": companion_payload.get("cause_kind"),
                "rejection_code": companion_payload.get("cause_code"),
                "rejection_summary": companion_payload.get("cause_summary"),
            }
            if any(
                not isinstance(value, str) or not value for value in detail.values()
            ):
                return None
        events.append(
            FlowLifecycleEvent(
                event_id=str(getattr(row, "id", "")),
                subject=subject,
                sequence=sequence,
                from_state=before,
                to_state=after,
                occurred_at=occurred_at,
                authority_ref=f"domain-event:{getattr(row, 'id', '')}",
                **detail,
            )
        )
    for index, event in enumerate(events):
        if index and event.from_state is not events[index - 1].to_state:
            return None
    return tuple(events)


def build_flow_health_projection(
    *,
    query: AnalyticsFoundationQuery,
    as_of: datetime,
    board: object,
    specs: Iterable[object],
    cards: Iterable[object],
    domain_events: Iterable[object],
    coverage: CoverageTraceabilityProjection | None = None,
) -> FlowHealthProjection:
    """Project current card/spec flow without using mutable ``updated_at``."""
    spec_rows = tuple(specs)
    card_rows = tuple(cards)
    event_rows = tuple(domain_events)
    coverage_by_spec = _coverage_blockers(coverage)
    subjects: list[FlowSubjectFacts] = []
    inactive = 0

    for subject_type, rows, states in (
        (FlowSubjectType.SPEC, spec_rows, _SPEC_STATES),
        (FlowSubjectType.CARD, card_rows, _CARD_STATES),
    ):
        for row in rows:
            subject = FlowSubjectRef(subject_type, str(row.id))
            source = AnalyticsSourceAuthority(
                "domain_events",
                f"domain-events:{query.board_id}:{subject_type.value}:{row.id}",
                "occurred_at",
            )
            events = _subject_events(
                subject_type=subject_type,
                subject_id=str(row.id),
                rows=event_rows,
            )
            expected = states.get(_value(getattr(row, "status", None)))
            archived = getattr(row, "archived", False) is True
            if archived or expected is FlowLifecycleState.CANCELLED:
                inactive += 1
                continue
            if events is None:
                subjects.append(
                    FlowSubjectFacts(subject, FlowAuthorityState.MISSING, source)
                )
                continue
            if expected is None or events[-1].to_state is not expected:
                subjects.append(
                    FlowSubjectFacts(subject, FlowAuthorityState.INCONSISTENT, source)
                )
                continue
            blockers: list[FlowBlockerFact] = []
            if subject_type is FlowSubjectType.SPEC:
                if blocker := _validation_blocker(row):
                    blockers.append(blocker)
                if blocker := coverage_by_spec.get(str(row.id)):
                    blockers.append(blocker)
            subjects.append(
                FlowSubjectFacts(
                    subject,
                    FlowAuthorityState.CURRENT,
                    source,
                    events,
                    tuple(sorted(blockers, key=lambda item: item.code.value)),
                )
            )

    accessible = len(subjects)
    exclusions = AnalyticsExclusionSummary(
        excluded_count=inactive,
        reasons=(AnalyticsExclusion("inactive_work", inactive),) if inactive else (),
    )
    return FlowHealthService.projection(
        query=query,
        as_of=as_of,
        policy=resolve_flow_health_policy(board),
        population_scope=AnalyticsPopulationScope(
            query.actor_scope_ref,
            accessible,
            inactive,
        ),
        exclusions=exclusions,
        subjects=tuple(subjects),
    )


__all__ = ["build_flow_health_projection", "resolve_flow_health_policy"]

"""Build Spec readiness from append-only validation records."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

from okto_pulse.core.ports.analytics_foundation import (
    AnalyticsExclusionSummary,
    AnalyticsFoundationQuery,
    AnalyticsPopulationScope,
)
from okto_pulse.core.ports.spec_readiness import (
    SpecReadinessEvidenceState,
    SpecReadinessRow,
    SpecValidationAttemptFacts,
    SpecValidationMeasures,
    SpecValidationReadiness,
)
from okto_pulse.core.services.spec_readiness import (
    GovernedValidationEpisode,
    SpecReadinessService,
)


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


def _integer(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _validation_records(spec: object) -> tuple[Mapping[str, object], ...]:
    raw = getattr(spec, "validations", None)
    if not isinstance(raw, list):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping))


def _attempts(
    records: tuple[Mapping[str, object], ...], *, spec_id: str, edition: int
) -> SpecValidationAttemptFacts:
    episodes: list[GovernedValidationEpisode] = []
    for item in records:
        item_edition = _integer(item.get("edition") or item.get("validation_edition"))
        occurred_at = _utc(item.get("created_at"))
        identity = item.get("id") or item.get("validation_id")
        if (
            item_edition != edition
            or occurred_at is None
            or not isinstance(identity, str)
            or not identity
        ):
            continue
        episodes.append(
            GovernedValidationEpisode(
                identity,
                spec_id,
                edition,
                f"spec:{spec_id}:validation:{identity}",
                _value(item.get("outcome")) == "success",
                occurred_at,
            )
        )
    episodes.sort(key=lambda item: (item.occurred_at, item.episode_id))
    return SpecReadinessService.attempt_facts(
        tuple(episodes), spec_id=spec_id, edition=edition
    )


def _validation(spec: object) -> SpecValidationReadiness:
    spec_id = str(spec.id)
    edition = int(getattr(spec, "edition", 1))
    records = _validation_records(spec)
    attempts = _attempts(records, spec_id=spec_id, edition=edition)
    current_id = getattr(spec, "current_validation_id", None)
    candidates = tuple(
        item
        for item in records
        if (
            isinstance(current_id, str)
            and (
                item.get("id") == current_id or item.get("validation_id") == current_id
            )
        )
        or (current_id in (None, "") and item.get("is_current") is True)
    )
    if len(candidates) != 1:
        return SpecValidationReadiness(
            state=SpecReadinessEvidenceState.MISSING,
            measures=SpecValidationMeasures(),
            attempts=SpecValidationAttemptFacts(0, None, 0),
        )
    item = candidates[0]
    identity = item.get("id") or item.get("validation_id")
    evidence_edition = _integer(item.get("edition") or item.get("validation_edition"))
    if not isinstance(identity, str) or not identity or evidence_edition is None:
        return SpecValidationReadiness(
            state=SpecReadinessEvidenceState.MISSING,
            measures=SpecValidationMeasures(),
            attempts=SpecValidationAttemptFacts(0, None, 0),
        )
    measures = SpecValidationMeasures(
        confidence=_integer(item.get("confidence")),
        clarity=_integer(item.get("clarity")),
        assertiveness=_integer(item.get("assertiveness")),
        decidability=_integer(item.get("decidability")),
        ambiguity=_integer(item.get("ambiguity")),
        legacy_completeness=_integer(item.get("completeness")),
    )
    state = (
        SpecReadinessEvidenceState.CURRENT
        if evidence_edition == edition
        else SpecReadinessEvidenceState.PREVIOUS
    )
    lifecycle_ready = None
    if state is SpecReadinessEvidenceState.CURRENT:
        lifecycle_ready = bool(
            measures.canonical_complete
            and _value(item.get("outcome")) == "success"
            and _value(getattr(spec, "status", None))
            in {"validated", "in_progress", "done"}
        )
    return SpecValidationReadiness(
        state=state,
        validation_id=identity,
        authority_ref=f"spec:{spec_id}:validation:{identity}",
        evidence_edition=evidence_edition,
        measures=measures,
        attempts=attempts if state is SpecReadinessEvidenceState.CURRENT else attempts,
        lifecycle_ready=lifecycle_ready,
    )


def build_spec_readiness_projection(
    *,
    query: AnalyticsFoundationQuery,
    as_of: datetime,
    specs: Iterable[object],
):
    rows = tuple(
        SpecReadinessRow(
            spec_id=str(spec.id),
            edition=int(getattr(spec, "edition", 1)),
            validation=(validation := _validation(spec)),
            checklist=(),
            requirement_lint=(),
            spec_pending_validation=(
                not validation.lifecycle_ready
                if validation.state is SpecReadinessEvidenceState.CURRENT
                else True
            ),
        )
        for spec in sorted(
            specs, key=lambda item: (str(item.id), int(getattr(item, "edition", 1)))
        )
    )
    return SpecReadinessService.projection(
        query=query,
        as_of=as_of,
        population_scope=AnalyticsPopulationScope(query.actor_scope_ref, len(rows)),
        exclusions=AnalyticsExclusionSummary(),
        specs=rows,
    )


__all__ = ["build_spec_readiness_projection"]

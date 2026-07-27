"""GoldenInboundReplay (spec #09, tr_815bf4c7).

A reusable harness that captures the observable outcome of a flow (result
payload, error, and a test-supplied side-effect snapshot covering audit log /
commits / DB state) and compares a BEFORE run (the legacy direct-service path)
against an AFTER run (the migrated use case / thin-adapter path), reporting any
non-accepted delta. Equivalence of payload + error + side effects is the
behavior-preservation contract for the first-cut REST↔MCP migration.

This is pure comparison logic (no transport import); tests provide the runners
and the side-effect snapshots from Community fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable


@dataclass
class FlowOutcome:
    """The observable result of running a flow once."""

    ok: bool
    payload: Any = None
    error_type: str | None = None
    error_message: str | None = None
    # Side-effect snapshot supplied by the caller (e.g. audit rows, commit count,
    # persisted entity state) — compared key-by-key.
    side_effects: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayDelta:
    field: str
    before: Any
    after: Any


@dataclass
class ReplayReport:
    flow: str
    surface: str
    equivalent: bool
    deltas: list[ReplayDelta] = field(default_factory=list)
    accepted_deltas: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "flow": self.flow,
            "surface": self.surface,
            "equivalent": self.equivalent,
            "deltas": [
                {"field": d.field, "before": d.before, "after": d.after}
                for d in self.deltas
            ],
            "accepted_deltas": self.accepted_deltas,
        }


async def capture_outcome(
    runner: Callable[[], Awaitable[Any]],
    *,
    normalize: Callable[[Any], Any] | None = None,
    side_effects: dict[str, Any] | None = None,
) -> FlowOutcome:
    """Run ``runner`` once and capture its outcome (success payload or error)."""
    try:
        result = await runner()
    except Exception as exc:  # noqa: BLE001 - capturing the observable error is the point
        return FlowOutcome(
            ok=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
            side_effects=dict(side_effects or {}),
        )
    payload = normalize(result) if normalize is not None else result
    return FlowOutcome(ok=True, payload=payload, side_effects=dict(side_effects or {}))


class GoldenInboundReplay:
    """Compare a before/after pair of :class:`FlowOutcome` for one flow+surface."""

    _SCALAR_FIELDS = ("ok", "payload", "error_type", "error_message")

    def __init__(
        self, flow: str, surface: str, *, accepted_deltas: tuple[str, ...] = ()
    ) -> None:
        self.flow = flow
        self.surface = surface
        self.accepted = set(accepted_deltas)

    def compare(self, before: FlowOutcome, after: FlowOutcome) -> ReplayReport:
        deltas: list[ReplayDelta] = []
        for fld in self._SCALAR_FIELDS:
            if fld in self.accepted:
                continue
            b = getattr(before, fld)
            a = getattr(after, fld)
            if b != a:
                deltas.append(ReplayDelta(fld, b, a))
        for key in sorted(set(before.side_effects) | set(after.side_effects)):
            tag = f"side_effects.{key}"
            if tag in self.accepted or key in self.accepted:
                continue
            b = before.side_effects.get(key)
            a = after.side_effects.get(key)
            if b != a:
                deltas.append(ReplayDelta(tag, b, a))
        return ReplayReport(
            flow=self.flow,
            surface=self.surface,
            equivalent=not deltas,
            deltas=deltas,
            accepted_deltas=sorted(self.accepted),
        )

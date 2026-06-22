"""Local watermark/cursor schema for the steady-state delta publish (spec R3A).

R3A-A owns the *schema* of the per-install watermark, its (de)serialisation,
the conservative migration of a legacy ``state.json`` and the stable-ordering
primitive that makes the cursor anchor on a stable event identifier rather than
on a wall-clock timestamp alone (FR ``fr_55e194c2``; decision ``dec_68f16c0e``
"Watermark por evento estavel e delta trusted pos-fix").

The cursor is a *keyset*: ``watermark`` carries the ``occurred_at`` of the last
backend-confirmed event (the ordering component, also reused as a pruning bound)
and ``watermark_event_id`` carries that event's stable ``event_id`` (the anchor).
Pairing the two is what satisfies "sem depender exclusivamente de timestamp": two
events sharing an ``occurred_at`` are disambiguated by their stable ``event_id``,
and the cursor position is reproducible across reloads. ``event_id`` is the
primary anchor (:func:`compare_to_cursor` matches it exactly); ``occurred_at`` is
the secondary ordering used when the anchor event is no longer in the local
stream (e.g. pruned).

Storage contract (IR ``ir_a5df43cf``): the six fields live as **flat top-level
keys** in ``state.json`` — ``watermark``, ``watermark_event_id``,
``watermark_updated_at``, ``pending_event_count``, ``next_batch_seq`` and
``retention_days``. They sit alongside the existing flat keys (``last_send_at``,
``circuit_open_until`` …); in particular ``next_batch_seq`` is the SAME key the
R1 sender already reads/writes, so there is a single source of truth and no
divergent writer (TR ``tr_f5b5d90a``: do not break the existing ``state.json``).

Scope boundary — this card delivers the schema + pure primitives only. The cards
that *consume* them:

* R3A-B (``fr_cfa32c6b``/``fr_169be135``): assemble the steady-state delta batch
  from the still-unconfirmed events and stamp the post-fix/era semantics marker.
  The skew-robust *selection policy* (scenario ``ts_07d9a8b2``) lives there: a
  high-watermark by itself cannot, in general, re-include a brand-new event that
  carries a clock-skewed *old* ``occurred_at`` — B must therefore decide
  inclusion by event_id confirmation, building on :func:`compare_to_cursor`.
* R3A-B/C (rest of ``fr_2dc7b6da``): wire :func:`advance` into ``send_once`` so
  the watermark moves ONLY on a 2xx accept, and preserve/reconcile the cursor
  idempotently for 401/409/5xx/transport.
* R3A-D (``fr_f3425329``): run ``prune_old`` while preserving pending events.
* R3A-E (``or_8f51cac2``): surface the (secret-free) watermark for local audit —
  :func:`public_watermark_projection` is the allowlisted primitive it uses.

Security invariant (mirrors ``failure_state``): every public/diagnostic
projection is built from an allowlist of the schema fields, so a token or secret
accidentally written next to the watermark in ``state.json`` can never leak
through the audit surface (scenario ``ts_0d21a342``: "sem expor token/segredo no
estado diagnostico").
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

# --- Storage contract (IR ir_a5df43cf) -------------------------------------
# The watermark is persisted as these flat top-level keys in state.json. Listed
# explicitly so the public projection is allowlist-based (never *asdict*), the
# same defence-in-depth failure_state uses.
WATERMARK_FIELDS: tuple[str, ...] = (
    "watermark",
    "watermark_event_id",
    "watermark_updated_at",
    "pending_event_count",
    "next_batch_seq",
    "retention_days",
)

# ``next_batch_seq`` is the SAME flat key the R1 sender already manages — kept as
# a named constant so both readers agree on the single source of truth.
NEXT_BATCH_SEQ_KEY = "next_batch_seq"

DEFAULT_RETENTION_DAYS = 30  # IR ir_a5df43cf: retention_days_default
DEFAULT_NEXT_BATCH_SEQ = 1  # send-time sequence starts at 1 (matches sender.py)

# A cursor that sorts before every real event (empty/legacy watermark).
_MIN_DT = datetime(1, 1, 1, tzinfo=timezone.utc)


def _parse_dt(value: Any) -> datetime | None:
    """Parse an ISO-8601 (``...Z`` or offset) string into an aware datetime."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    # Normalise to UTC-aware so naive and ``Z`` timestamps compare consistently.
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_opt_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _coerce_int(value: Any, *, default: int) -> int:
    # bool is an int subclass — reject it so ``True`` never becomes ``1``.
    if isinstance(value, bool):
        return default
    try:
        coerced = int(value)
    except (TypeError, ValueError):
        return default
    return coerced if coerced >= 0 else default


@dataclass(frozen=True)
class Watermark:
    """Per-install steady-state publish cursor.

    All fields are non-secret by construction. Frozen so callers evolve it
    through :func:`advance` / :func:`dataclasses.replace`, keeping a single
    validated mutation path.
    """

    watermark: str | None = None  # occurred_at of the last confirmed event
    watermark_event_id: str | None = None  # stable event_id anchor of the cursor
    watermark_updated_at: str | None = None  # when the cursor last advanced
    pending_event_count: int = 0  # events not yet confirmed (observability)
    next_batch_seq: int = DEFAULT_NEXT_BATCH_SEQ  # shared with the R1 sender
    retention_days: int = DEFAULT_RETENTION_DAYS

    @property
    def is_empty(self) -> bool:
        """``True`` when no event has been confirmed yet (fresh or legacy state).

        An empty cursor means *nothing* is confirmed — the conservative default
        after a legacy upgrade, so no pending event is ever silently treated as
        already sent (scenario ``ts_0d21a342``).
        """
        return self.watermark_event_id is None

    def cursor_tuple(self) -> tuple[datetime, str] | None:
        """Comparable ordering key of the cursor, or ``None`` when empty.

        ``(occurred_at, event_id)`` — the same shape as
        :func:`event_cursor_tuple` so the two are directly comparable.
        """
        if self.is_empty:
            return None
        return (_parse_dt(self.watermark) or _MIN_DT, str(self.watermark_event_id))

    def to_state_fields(self) -> dict[str, Any]:
        """The six flat keys exactly as they are persisted in ``state.json``."""
        return {name: getattr(self, name) for name in WATERMARK_FIELDS}

    def public_dict(self) -> dict[str, Any]:
        """Allowlisted, secret-free view (every field is non-secret by design)."""
        return self.to_state_fields()


def event_cursor_tuple(event: dict[str, Any]) -> tuple[datetime, str]:
    """Stable, orderable key of a *local* event: ``(occurred_at, event_id)``.

    This is the ordering contract of ``fr_55e194c2``. ``occurred_at`` orders
    chronologically; ``event_id`` (a stable UUID assigned at capture) breaks ties
    deterministically and anchors the cursor so ordering never depends on the
    timestamp alone. Events missing ``occurred_at`` sort at the epoch floor; a
    missing ``event_id`` degrades to the empty string (still totally ordered).
    """
    occurred = _parse_dt(event.get("occurred_at")) or _MIN_DT
    return (occurred, str(event.get("event_id") or ""))


def compare_to_cursor(watermark: Watermark, event: dict[str, Any]) -> int:
    """Order a local event against the cursor: ``-1`` before / ``0`` at / ``1`` after.

    The stable ``event_id`` is the PRIMARY anchor: an event whose ``event_id``
    equals ``watermark_event_id`` is the cursor itself (``0``) regardless of its
    timestamp. Otherwise the ``(occurred_at, event_id)`` keyset decides. An empty
    cursor sorts before everything, so every event is "after" (``1``) — i.e.
    nothing is confirmed yet.

    This is the ordering primitive only. The *selection policy* (how a delta
    batch treats skewed-old or duplicate events) is R3A-B's call and builds on
    this comparator.
    """
    if not watermark.is_empty and str(event.get("event_id") or "") == watermark.watermark_event_id:
        return 0
    cursor = watermark.cursor_tuple()
    if cursor is None:
        return 1
    event_key = event_cursor_tuple(event)
    if event_key > cursor:
        return 1
    if event_key < cursor:
        return -1
    return 0


def advance(
    watermark: Watermark,
    *,
    event_id: str,
    occurred_at: str,
    updated_at: str,
    pending_event_count: int | None = None,
    next_batch_seq: int | None = None,
) -> Watermark:
    """Move the cursor to a backend-confirmed event — monotonically forward only.

    Returns a new :class:`Watermark` anchored at ``(occurred_at, event_id)``.
    Idempotency (FR ``fr_2dc7b6da``): if the target is the current cursor or
    sorts *before* it (a replay / duplicate / out-of-order ack), the cursor is
    NOT moved backward — only the bookkeeping counters (``pending_event_count``,
    ``next_batch_seq``) are updated when supplied. This keeps re-processing the
    same confirmed window from rewinding the watermark.

    Note: this is the pure schema primitive. Deciding *when* to call it (only on
    a 2xx accept) and what to do for 401/409/5xx/transport is R3A-B/C.
    """
    candidate = Watermark(
        watermark=occurred_at,
        watermark_event_id=event_id,
        watermark_updated_at=updated_at,
    )
    changes: dict[str, Any] = {}
    if pending_event_count is not None:
        changes["pending_event_count"] = max(0, int(pending_event_count))
    if next_batch_seq is not None:
        changes["next_batch_seq"] = max(DEFAULT_NEXT_BATCH_SEQ, int(next_batch_seq))

    current = watermark.cursor_tuple()
    target = candidate.cursor_tuple()
    # Move the cursor only when it is genuinely advancing. ``target`` is never
    # None here (event_id is provided); ``current`` is None for an empty cursor.
    if current is None or (target is not None and target > current):
        changes["watermark"] = candidate.watermark
        changes["watermark_event_id"] = candidate.watermark_event_id
        changes["watermark_updated_at"] = candidate.watermark_updated_at
    return replace(watermark, **changes)


def set_counters(
    watermark: Watermark,
    *,
    pending_event_count: int | None = None,
    next_batch_seq: int | None = None,
    retention_days: int | None = None,
) -> Watermark:
    """Return a copy with the bookkeeping counters updated (cursor untouched).

    The cursor (``watermark``/``watermark_event_id``) only moves through
    :func:`advance`; this is for the observability counters (pending count,
    send sequence, retention window) that R3A-B/D refresh after a publish.
    """
    changes: dict[str, Any] = {}
    if pending_event_count is not None:
        changes["pending_event_count"] = max(0, int(pending_event_count))
    if next_batch_seq is not None:
        changes["next_batch_seq"] = max(DEFAULT_NEXT_BATCH_SEQ, int(next_batch_seq))
    if retention_days is not None:
        changes["retention_days"] = max(0, int(retention_days))
    return replace(watermark, **changes)


def read_watermark(state: dict[str, Any]) -> Watermark:
    """Read the watermark from a telemetry ``state`` dict, migrating legacy state.

    A legacy ``state.json`` (one that predates this card) has ``next_batch_seq``
    and local events but NO ``watermark``/``watermark_event_id`` keys. Migration
    is deliberately conservative (FR ``fr_55e194c2`` / scenario ``ts_0d21a342``):

    * the cursor is left EMPTY (``watermark``/``watermark_event_id`` = ``None``)
      so every existing local event stays *pending*. We never seed the cursor
      from ``last_send_at`` or "now", which would mark genuinely-unsent events as
      confirmed and silently drop them. Re-sending a confirmed window is harmless
      (the backend dedupes by nonce/batch_seq); dropping unsent data is not.
    * ``next_batch_seq`` and ``retention_days`` are carried over from their
      existing flat keys (so the R1 send-time sequence is preserved), defaulting
      to 1 and 30 when absent.

    Unknown/extra keys are ignored, and nothing here reads a secret, so the
    reader cannot break on future extensions or leak a credential.
    """
    return Watermark(
        watermark=_coerce_opt_str(state.get("watermark")),
        watermark_event_id=_coerce_opt_str(state.get("watermark_event_id")),
        watermark_updated_at=_coerce_opt_str(state.get("watermark_updated_at")),
        pending_event_count=_coerce_int(state.get("pending_event_count"), default=0),
        next_batch_seq=_coerce_int(state.get(NEXT_BATCH_SEQ_KEY), default=DEFAULT_NEXT_BATCH_SEQ),
        retention_days=_coerce_int(state.get("retention_days"), default=DEFAULT_RETENTION_DAYS),
    )


def write_watermark(state: dict[str, Any], watermark: Watermark) -> dict[str, Any]:
    """Return a new state dict with the six watermark keys (re)written.

    Pure (no disk I/O). Every other key in ``state`` — ``mode``, ``failure_state``,
    ``install_token``, history … — is preserved untouched, so the existing
    ``state.json`` is never broken (TR ``tr_f5b5d90a``). ``next_batch_seq`` is
    written to the one shared flat key, keeping a single source of truth with the
    sender.
    """
    new_state = dict(state)
    new_state.update(watermark.to_state_fields())
    return new_state


def public_watermark_projection(state: dict[str, Any]) -> dict[str, Any]:
    """Allowlisted, secret-free view of the watermark from a full state dict.

    Structurally cannot leak a secret: only the six schema fields are emitted,
    never the token sitting next to them. This is the audit/diagnostic surface
    R3A-E builds on (scenario ``ts_0d21a342``).
    """
    return read_watermark(state).public_dict()


def load_watermark(metrics_dir: Any) -> Watermark:
    """Load and migrate the watermark from ``state.json`` under ``metrics_dir``."""
    from okto_pulse.core.telemetry.settings import load_state

    return read_watermark(load_state(metrics_dir))


def persist_watermark(metrics_dir: Any, watermark: Watermark) -> Watermark:
    """Persist ``watermark`` into ``state.json`` without disturbing other keys."""
    from okto_pulse.core.telemetry.settings import load_state, save_state

    state = load_state(metrics_dir)
    save_state(metrics_dir, write_watermark(state, watermark))
    return watermark

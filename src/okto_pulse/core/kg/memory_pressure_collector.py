"""In-process ring-buffer collector for memory-pressure samples and WAL/commit
failure events (Spec R2c, FR1/FR4, TR1).

The ``MemoryPressureCorrelator`` (memory_pressure.py) is intentionally pure —
it operates on pre-collected iterables. This module is its stateful companion:
it keeps per-board ``deque`` buffers (thread-safe, module-level singletons) so
that ``kg_health_service.get_kg_health`` can feed *real* observations to the
correlator instead of the empty-list stubs that always returned ``unconfirmed``.

Design constraints (from the spec):
* Buffers are **module-level** singletons (dict keyed by board_id). Process
  restart clears them — the ring-buffer is an in-process observability aid, not
  durable storage.
* maxlen=200 for ``HighWaterMarkSample`` (AC14: push 201 → len==200).
* maxlen=50 for ``FailureEvent`` (one per WAL/commit failure, rate is low).
* All mutations are guarded by a single ``threading.Lock`` so background
  telemetry threads can call ``record_*`` safely without holding the asyncio
  event loop.
* Reads (``get_samples`` / ``get_failures``) return a **snapshot list** (not the
  deque itself) so the correlator's for-loop sees a stable sequence even if
  another thread appends during iteration.
"""

from __future__ import annotations

from collections import deque
from typing import TYPE_CHECKING

from okto_pulse.core.runtime_context import runtime_lock, runtime_state

if TYPE_CHECKING:
    from okto_pulse.core.kg.memory_pressure import FailureEvent, HighWaterMarkSample

# ---------------------------------------------------------------------------
# Module-level state (singletons)
# ---------------------------------------------------------------------------

_lock = runtime_lock("kg.memory_pressure_collector")

# board_id -> deque[HighWaterMarkSample] (maxlen 200)
_samples = runtime_state("kg.memory_pressure_collector.samples", dict)

# board_id -> deque[FailureEvent] (maxlen 50)
_failures = runtime_state("kg.memory_pressure_collector.failures", dict)

_SAMPLES_MAXLEN = 200
_FAILURES_MAXLEN = 50


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def record_sample(board_id: str, sample: "HighWaterMarkSample") -> None:
    """Append a ``HighWaterMarkSample`` to the per-board ring buffer.

    Thread-safe. When the buffer is full the oldest sample is silently
    dropped (deque maxlen semantics) so the newest observations are always
    accessible to the correlator.
    """
    with _lock:
        if board_id not in _samples:
            _samples[board_id] = deque(maxlen=_SAMPLES_MAXLEN)
        _samples[board_id].append(sample)


def record_failure(board_id: str, event: "FailureEvent") -> None:
    """Append a ``FailureEvent`` to the per-board ring buffer.

    Thread-safe.  maxlen=50 because WAL/commit failures are rare; keeping
    the 50 most recent is more than sufficient for the 10-minute correlation
    window.
    """
    with _lock:
        if board_id not in _failures:
            _failures[board_id] = deque(maxlen=_FAILURES_MAXLEN)
        _failures[board_id].append(event)


def get_samples(board_id: str) -> list["HighWaterMarkSample"]:
    """Return a snapshot list of all buffered samples for ``board_id``.

    Returns an empty list (not None) when no samples have been recorded yet.
    The list is a copy — callers may iterate it freely without holding the
    module lock.
    """
    with _lock:
        buf = _samples.get(board_id)
        return list(buf) if buf is not None else []


def get_failures(board_id: str) -> list["FailureEvent"]:
    """Return a snapshot list of all buffered failure events for ``board_id``.

    Returns an empty list when no failures have been recorded.
    """
    with _lock:
        buf = _failures.get(board_id)
        return list(buf) if buf is not None else []


# Event kinds que indicam falha do WRITE-PATH (WAL/commit). Um commit bem-
# sucedido posterior prova que o write-path está saudável — manter essas
# falhas no buffer (que não tem TTL) realimentava o estado recovery_needed
# para sempre dentro do mesmo processo (feedback loop do gate de degraded).
WRITE_FAILURE_EVENT_KINDS: frozenset[str] = frozenset(
    {"kg.commit.failed", "kg.wal.flush.failed"}
)


def record_write_success(board_id: str) -> None:
    """Drop buffered WRITE-path failures after a successful graph commit.

    Self-heal (catch-22 fix 2026-06-10): o ring buffer não tem TTL; sem esta
    limpeza, falhas antigas de commit mantinham ``wal_or_commit_errors`` no
    health até o restart do processo, mesmo com o write-path comprovadamente
    saudável de novo. Eventos de outras naturezas (memory pressure samples,
    falhas não-write) são preservados.
    """
    with _lock:
        buf = _failures.get(board_id)
        if not buf:
            return
        survivors = [
            e for e in buf
            if getattr(e, "event_kind", None) not in WRITE_FAILURE_EVENT_KINDS
        ]
        if len(survivors) == len(buf):
            return
        buf.clear()
        buf.extend(survivors)


def clear_board(board_id: str) -> None:
    """Remove all buffered data for ``board_id``.

    Intended for tests that need per-test isolation.  Production code
    should not call this; the ring buffers are meant to accumulate until
    process restart.
    """
    with _lock:
        _samples.pop(board_id, None)
        _failures.pop(board_id, None)


def clear_all() -> None:
    """Clear all board buffers.

    Test helper — resets the entire module-level state so tests that share a
    process don't bleed samples into each other.
    """
    with _lock:
        _samples.clear()
        _failures.clear()


__all__ = [
    "WRITE_FAILURE_EVENT_KINDS",
    "clear_all",
    "clear_board",
    "get_failures",
    "get_samples",
    "record_failure",
    "record_sample",
    "record_write_success",
]

"""Bounded in-process metric sample retention.

These buffers are intentionally small and dependency-free: production counters
must remain monotonic while retained test/debug samples stay capped.
"""

from __future__ import annotations

from collections import Counter, deque
from typing import Any, Iterable


METRIC_SAMPLE_RETENTION_LIMIT = 1024


class BoundedSampleBuffer:
    """A capped FIFO buffer for diagnostic metric samples."""

    def __init__(self, *, maxlen: int = METRIC_SAMPLE_RETENTION_LIMIT) -> None:
        self._samples: deque[Any] = deque(maxlen=maxlen)

    def append(self, sample: Any) -> None:
        self._samples.append(sample)

    def snapshot(self) -> list[Any]:
        return list(self._samples)

    def clear(self) -> None:
        self._samples.clear()


class BoundedCounterSampleBuffer:
    """Bounded sample retention with independent monotonic label counters."""

    def __init__(
        self,
        counter_fields: Iterable[str],
        *,
        sum_fields: Iterable[str] = (),
        maxlen: int = METRIC_SAMPLE_RETENTION_LIMIT,
    ) -> None:
        self._samples: deque[dict[str, Any]] = deque(maxlen=maxlen)
        self._counter_fields = tuple(counter_fields)
        self._sum_fields = tuple(sum_fields)
        self._counts: Counter[tuple[tuple[str, str], ...]] = Counter()
        self._sums: dict[str, Counter[tuple[tuple[str, str], ...]]] = {
            field: Counter() for field in self._sum_fields
        }

    def append(self, sample: dict[str, Any]) -> None:
        item = dict(sample)
        self._samples.append(item)
        key = self._key(item)
        self._counts[key] += 1
        for field in self._sum_fields:
            self._sums[field][key] += int(item.get(field) or 0)

    def count(self, **filters: Any) -> int:
        normalized = {
            key: str(value)
            for key, value in filters.items()
            if value is not None
        }
        total = 0
        for key, value in self._counts.items():
            labels = dict(key)
            if all(
                labels.get(filter_key) == filter_value
                for filter_key, filter_value in normalized.items()
            ):
                total += value
        return total

    def sum(self, field: str, **filters: Any) -> int:
        normalized = {
            key: str(value)
            for key, value in filters.items()
            if value is not None
        }
        total = 0
        for key, value in self._sums.get(field, {}).items():
            labels = dict(key)
            if all(
                labels.get(filter_key) == filter_value
                for filter_key, filter_value in normalized.items()
            ):
                total += value
        return total

    def snapshot(self) -> list[dict[str, Any]]:
        return [dict(sample) for sample in self._samples]

    def clear(self) -> None:
        self._samples.clear()
        self._counts.clear()
        for counter in self._sums.values():
            counter.clear()

    def _key(self, sample: dict[str, Any]) -> tuple[tuple[str, str], ...]:
        return tuple((field, str(sample.get(field))) for field in self._counter_fields)

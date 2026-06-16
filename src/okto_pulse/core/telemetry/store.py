"""Append-only local JSONL telemetry store."""

from __future__ import annotations

import json
import shutil
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from okto_pulse.core.telemetry.schema import GUIDED_HELP_ALLOWED_VALUES, canonical_json


def parse_iso(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def ensure_inside(base: Path, candidate: Path) -> Path:
    base_resolved = base.resolve()
    candidate_resolved = candidate.resolve()
    if candidate_resolved != base_resolved and base_resolved not in candidate_resolved.parents:
        raise ValueError("PATH_OUTSIDE_METRICS_DIR")
    return candidate_resolved


def add_guided_help_counts(counts: Counter[str], payload: dict[str, Any]) -> None:
    """Aggregate only closed-schema guided help categories."""
    for field, allowed_values in GUIDED_HELP_ALLOWED_VALUES.items():
        value = payload.get(field)
        if isinstance(value, str) and value in allowed_values:
            counts[f"{field}.{value}"] += 1


class LocalTelemetryStore:
    """Small filesystem store under the user-owned metrics directory."""

    def __init__(self, metrics_dir: Path, retention_days: int = 30):
        self.metrics_dir = metrics_dir.resolve()
        self.retention_days = retention_days

    @property
    def events_dir(self) -> Path:
        return self.metrics_dir / "events"

    @property
    def sent_dir(self) -> Path:
        return self.metrics_dir / "sent"

    @property
    def failures_dir(self) -> Path:
        return self.metrics_dir / "failures"

    @property
    def exports_dir(self) -> Path:
        return self.metrics_dir / "exports"

    @property
    def snapshots_dir(self) -> Path:
        return self.metrics_dir / "snapshots"

    def ensure_dirs(self) -> None:
        for path in (
            self.metrics_dir,
            self.events_dir,
            self.sent_dir,
            self.failures_dir,
            self.exports_dir,
            self.snapshots_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def append_snapshot(self, record: dict[str, Any]) -> Path:
        """Persist a product-telemetry SNAPSHOT locally, append-only (R3A-F).

        Product metrics are cumulative/snapshot and MUST NOT ride inside a
        ``semantics=delta`` batch. There is no safe snapshot ingest endpoint yet
        (the backend ``validate_usage_batch`` rejects unknown fields), so the
        snapshot is recorded here — auditable and never silently dropped — until a
        snapshot ingestion contract exists. Filed by ``snapshot_at`` date.
        """
        self.ensure_dirs()
        dt = str(record.get("snapshot_at", ""))[:10] or datetime.now(timezone.utc).date().isoformat()
        path = self.snapshots_dir / f"snapshot-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(canonical_json(record))
            f.write("\n")
        return path

    def append_event(self, event: dict[str, Any]) -> Path:
        self.ensure_dirs()
        dt = str(event.get("occurred_at", ""))[:10] or datetime.now(timezone.utc).date().isoformat()
        path = self.events_dir / f"events-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(canonical_json(event))
            f.write("\n")
        return path

    def append_sent(self, record: dict[str, Any], *, failed: bool = False) -> Path:
        self.ensure_dirs()
        root = self.failures_dir if failed else self.sent_dir
        dt = str(record.get("sent_at") or record.get("failed_at") or "")[:10]
        if not dt:
            dt = datetime.now(timezone.utc).date().isoformat()
        path = root / f"{'failures' if failed else 'sent'}-{dt}.jsonl"
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(canonical_json(record))
            f.write("\n")
        return path

    def confirmed_event_ids(self) -> set[str]:
        """Set of local ``event_id``s the backend has confirmed (R3A-B/C).

        The durable confirmation ledger is the append-only ``sent/`` store: each
        accepted batch is recorded with a ``confirmed_event_ids`` list, so the
        confirmed set is rebuilt here from disk and SURVIVES a restart — a
        confirmed event never re-enters a delta after reload (``fr_fe9b844d``).
        Confirmation is tracked per stable ``event_id``, not by timestamp, so an
        event that lands with a clock-skewed old ``occurred_at`` is still only
        excluded once it is genuinely confirmed (``br_5b182761`` / ``ts_07d9a8b2``).

        Bounded footprint: the ``sent/`` records are pruned with the events they
        confirm by :meth:`prune_old`, so the set never grows past the retention
        window.
        """
        confirmed: set[str] = set()
        if not self.sent_dir.exists():
            return confirmed
        for path in sorted(self.sent_dir.glob("sent-*.jsonl")):
            ensure_inside(self.metrics_dir, path)
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(record, dict):
                    continue
                for event_id in record.get("confirmed_event_ids") or []:
                    if isinstance(event_id, str) and event_id:
                        confirmed.add(event_id)
        return confirmed

    def iter_events(self, *, since: datetime | None = None) -> Iterable[dict[str, Any]]:
        if not self.events_dir.exists():
            return
        for path in sorted(self.events_dir.glob("events-*.jsonl")):
            ensure_inside(self.metrics_dir, path)
            try:
                lines = path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line in lines:
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                occurred = parse_iso(str(event.get("occurred_at", "")))
                if since and occurred and occurred < since:
                    continue
                if isinstance(event, dict):
                    yield event

    def summarize(self, *, window_days: int = 30) -> dict[str, Any]:
        since = datetime.now(timezone.utc) - timedelta(days=window_days)
        by_type: Counter[str] = Counter()
        by_day: Counter[str] = Counter()
        guided_help_counts: Counter[str] = Counter()
        files = 0
        for path in self.events_dir.glob("events-*.jsonl") if self.events_dir.exists() else []:
            ensure_inside(self.metrics_dir, path)
            files += 1
        for event in self.iter_events(since=since):
            event_type = str(event.get("event_type", "unknown"))
            by_type[event_type] += 1
            day = str(event.get("occurred_at", ""))[:10]
            if day:
                by_day[day] += 1
            payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
            if event_type == "guided_help":
                add_guided_help_counts(guided_help_counts, payload)
        return {
            "event_count": sum(by_type.values()),
            "by_event_type": dict(sorted(by_type.items())),
            "by_day": dict(sorted(by_day.items())),
            "guided_help_counts": dict(sorted(guided_help_counts.items())),
            "files_count": files,
        }

    @staticmethod
    def _file_date(path: Path):
        try:
            return datetime.strptime("-".join(path.stem.split("-")[-3:]), "%Y-%m-%d").date()
        except ValueError:
            return None

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            return []
        records: list[dict[str, Any]] = []
        for line in lines:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                records.append(record)
        return records

    def _atomic_write_jsonl(self, path: Path, records: list[dict[str, Any]]) -> None:
        tmp = path.with_suffix(".tmp")
        with tmp.open("w", encoding="utf-8", newline="\n") as out:
            for record in records:
                out.write(canonical_json(record))
                out.write("\n")
        tmp.replace(path)

    def prune_old(self, *, now: datetime | None = None) -> dict[str, int]:
        """Retention sweep that NEVER deletes an unconfirmed (pending) event.

        Past the retention window, CONFIRMED events are removed but PENDING ones
        are preserved (``br_0cac38aa`` / ``fr_f3425329``): an old events file is
        atomically rewritten keeping only its pending events (deleted only if none
        remain), so a pending event outside the window is never silently lost. The
        ``sent/`` confirmation ledger is then pruned in lockstep — ``confirmed_
        event_ids`` that no longer back a stored event are dropped (and emptied
        ledger records removed), keeping the confirmed set bounded by retention.
        ``failures/`` are pruned by file date (diagnostic logs only). ``now`` is
        injectable so the publish flow can drive the sweep with a testable clock.
        """
        reference = (now or datetime.now(timezone.utc)).date()
        cutoff = reference - timedelta(days=self.retention_days)
        confirmed = self.confirmed_event_ids()
        removed_confirmed = 0
        preserved_pending = 0

        # 1) Events past the cutoff: keep pending, drop confirmed. Recent files
        #    (within retention) are left entirely untouched.
        if self.events_dir.exists():
            for path in sorted(self.events_dir.glob("events-*.jsonl")):
                ensure_inside(self.metrics_dir, path)
                file_date = self._file_date(path)
                if file_date is None or file_date >= cutoff:
                    continue
                events = self._read_jsonl(path)
                pending = [e for e in events if str(e.get("event_id") or "") not in confirmed]
                removed_confirmed += len(events) - len(pending)
                preserved_pending += len(pending)
                if pending:
                    self._atomic_write_jsonl(path, pending)
                else:
                    path.unlink(missing_ok=True)

        # 2) Confirmation ledger pruned in lockstep with the retention window:
        #    a sent/ file past the cutoff only ever confirmed events whose
        #    occurred_at <= sent_at < cutoff (you cannot send an event before it
        #    occurs), so those events are already pruned in (1) — the file is
        #    deleted to bound footprint. Files within retention are orphan-cleaned:
        #    confirmed ids whose event was pruned elsewhere are dropped, never
        #    un-confirming a surviving event.
        surviving = {str(e.get("event_id") or "") for e in self.iter_events()}
        pruned_ledger_ids = 0
        removed_sent_files = 0
        if self.sent_dir.exists():
            for path in sorted(self.sent_dir.glob("sent-*.jsonl")):
                ensure_inside(self.metrics_dir, path)
                file_date = self._file_date(path)
                records = self._read_jsonl(path)
                kept: list[dict[str, Any]] = []
                changed = False
                confirms_survivor = False
                for record in records:
                    ids = record.get("confirmed_event_ids")
                    if isinstance(ids, list):
                        filtered = [i for i in ids if i in surviving]
                        if filtered:
                            confirms_survivor = True
                        if len(filtered) != len(ids):
                            pruned_ledger_ids += len(ids) - len(filtered)
                            record = {**record, "confirmed_event_ids": filtered}
                            changed = True
                    kept.append(record)
                # R3A-H (fr_303c29b9 / br_e316c9bc): an old sent file is deleted ONLY
                # when it no longer confirms ANY surviving event — NEVER just because
                # it fell outside the retention window. A forward clock-skewed event
                # (future occurred_at) survives the events prune while its sole
                # confirmation may sit in an out-of-window sent file; deleting that
                # file by date alone would un-confirm a live event and make
                # _build_delta_batch re-send it as a new delta (fr_9e225ef2).
                if file_date is not None and file_date < cutoff and not confirms_survivor:
                    path.unlink(missing_ok=True)
                    removed_sent_files += 1
                elif changed:
                    self._atomic_write_jsonl(path, kept)

        # 3) failures/: diagnostic logs only, pruned by file date.
        removed_failure_files = 0
        if self.failures_dir.exists():
            for path in self.failures_dir.glob("*.jsonl"):
                ensure_inside(self.metrics_dir, path)
                file_date = self._file_date(path)
                if file_date is not None and file_date < cutoff:
                    path.unlink(missing_ok=True)
                    removed_failure_files += 1

        return {
            "removed_confirmed_events": removed_confirmed,
            "preserved_pending_events": preserved_pending,
            "pruned_ledger_ids": pruned_ledger_ids,
            "removed_sent_files": removed_sent_files,
            "removed_failure_files": removed_failure_files,
        }

    def export_local(self, output_path: Path | None = None) -> Path:
        self.ensure_dirs()
        if output_path is None:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            output_path = self.exports_dir / f"metrics-export-{stamp}.jsonl"
        output_path = ensure_inside(self.metrics_dir, output_path)
        with output_path.open("w", encoding="utf-8", newline="\n") as out:
            for event in self.iter_events():
                out.write(canonical_json(event))
                out.write("\n")
        return output_path

    def purge_local(self) -> dict[str, int]:
        self.ensure_dirs()
        removed_files = 0
        for root in (self.events_dir, self.sent_dir, self.failures_dir, self.exports_dir):
            ensure_inside(self.metrics_dir, root)
            if root.exists():
                for path in root.glob("*"):
                    ensure_inside(self.metrics_dir, path)
                    if path.is_file():
                        path.unlink()
                        removed_files += 1
                    elif path.is_dir():
                        shutil.rmtree(path)
                        removed_files += 1
        return {"purged_files": removed_files}

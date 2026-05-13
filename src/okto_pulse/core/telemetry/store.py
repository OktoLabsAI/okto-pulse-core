"""Append-only local JSONL telemetry store."""

from __future__ import annotations

import json
import shutil
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from okto_pulse.core.telemetry.schema import canonical_json


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

    def ensure_dirs(self) -> None:
        for path in (self.metrics_dir, self.events_dir, self.sent_dir, self.failures_dir, self.exports_dir):
            path.mkdir(parents=True, exist_ok=True)

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
        return {
            "event_count": sum(by_type.values()),
            "by_event_type": dict(sorted(by_type.items())),
            "by_day": dict(sorted(by_day.items())),
            "files_count": files,
        }

    def prune_old(self) -> int:
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=self.retention_days)
        removed = 0
        for root in (self.events_dir, self.sent_dir, self.failures_dir):
            if not root.exists():
                continue
            for path in root.glob("*.jsonl"):
                ensure_inside(self.metrics_dir, path)
                date_part = path.stem.split("-")[-3:]
                try:
                    file_date = datetime.strptime("-".join(date_part), "%Y-%m-%d").date()
                except ValueError:
                    continue
                if file_date < cutoff:
                    path.unlink(missing_ok=True)
                    removed += 1
        return removed

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

"""Source-owned metadata carried internally, never accepted from a KG client."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping


def _timestamp(value: object) -> str | None:
    if value is None:
        return None
    stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    # Pulse relational timestamps use UTC, including historical naive values.
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return stamp.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class SourceProjectionMetadata:
    source_created_at: str | None
    source_updated_at: str | None
    source_status: str | None
    severity: str | None

    @classmethod
    def from_source(cls, source: object, *, is_bug: bool) -> "SourceProjectionMetadata":
        def read(name: str) -> object:
            return source.get(name) if isinstance(source, Mapping) else getattr(source, name, None)

        def raw(name: str) -> str | None:
            value = read(name)
            return str(getattr(value, "value", value)) if value is not None else None

        return cls(
            source_created_at=_timestamp(read("created_at")),
            source_updated_at=_timestamp(read("updated_at")),
            source_status=raw("status"),
            severity=raw("severity") if is_bug else None,
        )

    def graph_attributes(self) -> dict[str, str | None]:
        return {
            "source_created_at": self.source_created_at,
            "source_updated_at": self.source_updated_at,
            "source_status": self.source_status,
            "severity": self.severity,
        }

"""QuarantineRestore port (spec KGD-01, FR4/BR4/TR1, card d9414790).

The KG quarantine (``<kg_base>/quarantine/<quarantine_id>/`` — files +
manifest) was write-only: snapshots were preserved but no restore path
existed, pushing operators toward the destructive rebuild. This port makes
the quarantine restorable behind a pure interface:

* ``plan(quarantine_id)`` — DRY-RUN: an auditable :class:`RestorePlan`
  (files, destinations, conflicts, sizes, board_id) with ZERO mutation.
* ``apply(quarantine_id)`` — backup-swap: the board's live files are moved
  into a NEW quarantine (with manifest) before the snapshot files are copied
  back; the board open is validated and structured audit events are emitted
  (``kg.quarantine.restore_dry_run`` / ``kg.quarantine.restored`` — TR4).

DTOs are pure (no engine, no concrete filesystem types beyond ``str``
paths); the concrete adapter lives in the Community edition
(``okto_pulse.community.adapters.quarantine_restore``), per the board's
hexagonal guideline (TR1) and the ``graph_lifecycle`` port pattern.

Error contract (interface-restore-invoke):

* ``quarantine_not_found`` — quarantine_id does not exist / is unreadable.
* ``board_locked`` — a live server/process holds the board; the apply is
  refused and the operator is pointed at a maintenance window.
* ``partial_restore`` — a mid-flight failure (e.g. NTFS PermissionError);
  the operation manifest records the exact moved/copied state and a
  rollback instruction — never a silent half-restored board (BR4/TR6).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol, runtime_checkable


class QuarantineRestoreErrorCode(str, Enum):
    """Typed error codes per the interface-restore-invoke error contract."""

    QUARANTINE_NOT_FOUND = "quarantine_not_found"
    BOARD_LOCKED = "board_locked"
    PARTIAL_RESTORE = "partial_restore"


class QuarantineRestoreError(Exception):
    """Structured restore failure carrying the contract error ``code``.

    ``details`` carries machine-readable state (e.g. the operation-manifest
    path and the exact moved/copied file lists for ``partial_restore``) so
    no failure mode is ever silent.
    """

    def __init__(
        self,
        code: QuarantineRestoreErrorCode,
        *,
        reason: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.reason = reason
        self.details: dict[str, Any] = dict(details or {})

    def to_payload(self) -> dict[str, Any]:
        """Contract-shaped error payload for MCP/CLI surfaces."""
        return {
            "error": self.code.value,
            "detail": self.reason,
            "details": dict(self.details),
        }


@dataclass(frozen=True)
class RestoreFileEntry:
    """One planned file restore (quarantine snapshot -> board destination)."""

    name: str
    source_path: str
    destination_path: str
    size_bytes: int
    conflict: bool
    live_size_bytes: int | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_path": self.source_path,
            "destination_path": self.destination_path,
            "size_bytes": self.size_bytes,
            "conflict": self.conflict,
            "live_size_bytes": self.live_size_bytes,
        }


@dataclass(frozen=True)
class RestorePlan:
    """Auditable dry-run plan — produced WITHOUT any mutation."""

    quarantine_id: str
    board_id: str
    board_dir: str
    manifest_format: str  # "manifest.json" | "manifest.txt" | "inventory"
    files: tuple[RestoreFileEntry, ...] = ()
    conflicts: tuple[str, ...] = ()
    total_bytes: int = 0

    def to_payload(self) -> list[dict[str, Any]]:
        """The MCP contract ``plan`` array."""
        return [entry.to_payload() for entry in self.files]


@dataclass(frozen=True)
class RestoreReport:
    """Structured outcome of an apply (backup-swap + restore + open probe)."""

    quarantine_id: str
    board_id: str
    applied: bool
    backup_quarantine_id: str | None
    restored_files: tuple[str, ...] = ()
    open_validated: bool = False
    errors: tuple[str, ...] = ()


@runtime_checkable
class QuarantineRestore(Protocol):
    def plan(self, quarantine_id: str) -> RestorePlan:
        """Build the dry-run restore plan for a quarantine snapshot.

        MUST NOT mutate any state. Emits ``kg.quarantine.restore_dry_run``.
        Raises :class:`QuarantineRestoreError` (``quarantine_not_found``).
        """
        ...

    def apply(self, quarantine_id: str) -> RestoreReport:
        """Apply the restore with backup-swap of the board's live files.

        Refuses with ``board_locked`` while the board is held live; a
        mid-flight failure raises ``partial_restore`` with the exact state
        recorded in the operation manifest. Emits ``kg.quarantine.restored``.
        """
        ...


__all__ = [
    "QuarantineRestore",
    "QuarantineRestoreError",
    "QuarantineRestoreErrorCode",
    "RestoreFileEntry",
    "RestorePlan",
    "RestoreReport",
]

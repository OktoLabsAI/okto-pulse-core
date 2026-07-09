"""KG storage backend contingency (KG-01 FR9, contract api_8721ddb7).

Plan B: when corruption persists AFTER the hardening primitives are
proven (KG-01.1–KG-01.6), this service packages the auditable evidence
needed to:

* file an upstream LadybugDB issue with a reproducible timeline;
* feed an eventual backend hot-swap evaluation through the KG storage
  port — WITHOUT importing or depending on any alternate backend in
  this spec (TR16). The decision input is what we produce; the
  decision itself lives in a future refinement.

Inputs:

* ``corruption_timeline_ref`` — pointer to the persisted observability
  timeline (Prometheus snapshot, log bundle, …). Required.
* ``quarantine_ids`` — IDs returned by ``KGQuarantineService.create``
  whose manifests we will reference. At least one is required —
  ``missing_quarantine_evidence`` is the typed error otherwise.
* ``software_version`` — the okto-pulse-core version under which the
  corruption was observed. Required.

Outputs:

* ``contingency_ref`` — path to the persisted contingency manifest.
* ``ready_for_upstream_issue`` — True iff every quarantine_id resolves
  to a readable manifest AND the timeline_ref is present.
* ``ready_for_hot_swap_decision`` — same readiness checks plus the
  software_version matches the boot version (avoiding stale evidence).
  Hot-swap is gated by a separate refinement; this flag only signals
  that the evidence is complete enough for the eventual decision.

Counter OR ``or_ee5a98e4``: ``kg_backend_contingency_prepared_total``
labels = (board_id, outcome, ready_for_upstream_issue,
ready_for_hot_swap_decision). All bounded.
"""

from __future__ import annotations

import json
import logging
import secrets
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    resolve_rebuild_audit_artifact_store,
)

logger = logging.getLogger("okto_pulse.kg.contingency")


CONTINGENCY_DIRNAME = "contingency"
CONTINGENCY_MANIFEST_FILENAME = "contingency.json"


class ContingencyErrorCode(str, Enum):
    """Typed errors per contract api_8721ddb7 response_errors."""

    MISSING_QUARANTINE_EVIDENCE = "missing_quarantine_evidence"
    TIMELINE_UNAVAILABLE = "timeline_unavailable"


class ContingencyError(Exception):
    def __init__(
        self,
        code: ContingencyErrorCode,
        *,
        retryable: bool,
        reason: str,
    ) -> None:
        super().__init__(f"{code.value}: {reason}")
        self.code = code
        self.retryable = retryable
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ContingencyResponse:
    """Frozen contract-shaped response per api_8721ddb7 success body."""

    contingency_ref: str
    ready_for_upstream_issue: bool
    ready_for_hot_swap_decision: bool


# --- Counter (OR or_ee5a98e4) ------------------------------------------------

_CONTINGENCY_LABELS = (
    "board_id",
    "outcome",
    "ready_for_upstream_issue",
    "ready_for_hot_swap_decision",
)

_contingency_counter: dict[tuple[str, str, str, str], int] = {}
_contingency_counter_lock = threading.Lock()


def _bump_contingency(
    *,
    board_id: str,
    outcome: str,
    ready_for_upstream_issue: bool,
    ready_for_hot_swap_decision: bool,
) -> None:
    key = (
        board_id,
        outcome,
        "true" if ready_for_upstream_issue else "false",
        "true" if ready_for_hot_swap_decision else "false",
    )
    with _contingency_counter_lock:
        _contingency_counter[key] = _contingency_counter.get(key, 0) + 1


def get_contingency_count(
    board_id: str,
    outcome: str,
    *,
    ready_for_upstream_issue: bool | None = None,
    ready_for_hot_swap_decision: bool | None = None,
) -> int:
    upstream = (
        None
        if ready_for_upstream_issue is None
        else ("true" if ready_for_upstream_issue else "false")
    )
    swap = (
        None
        if ready_for_hot_swap_decision is None
        else ("true" if ready_for_hot_swap_decision else "false")
    )
    with _contingency_counter_lock:
        total = 0
        for (b, out, up, sw), value in _contingency_counter.items():
            if b != board_id or out != outcome:
                continue
            if upstream is not None and up != upstream:
                continue
            if swap is not None and sw != swap:
                continue
            total += value
        return total


def get_contingency_samples() -> list[dict[str, Any]]:
    with _contingency_counter_lock:
        return [
            {
                "board_id": b,
                "outcome": out,
                "ready_for_upstream_issue": up,
                "ready_for_hot_swap_decision": sw,
                "count": value,
            }
            for (b, out, up, sw), value in _contingency_counter.items()
        ]


def get_contingency_counter_labels() -> tuple[str, ...]:
    return _CONTINGENCY_LABELS


def reset_contingency_counter() -> None:
    with _contingency_counter_lock:
        _contingency_counter.clear()


# --- The service -------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class KGStorageBackendContingency:
    """Packages evidence for upstream issue / hot-swap decision input.

    The service does NOT and MUST NOT instantiate any alternate KG
    backend — that is explicitly TR16 territory. It only persists a
    manifest and returns a contingency_ref + readiness flags.

    ``quarantine_resolver`` is a callable that maps a ``quarantine_id``
    to a manifest dict (or None when missing). Production MUST wire
    this to ``KGQuarantineService.inspect`` so contingency manifests
    never reference non-existent quarantines (val_209622e9 hardening).

    For tests that intentionally want to bypass resolution (covering
    the orchestration logic without a quarantine fixture) set
    ``allow_unverified_quarantine_ids=True`` explicitly. Construction
    raises ``ValueError`` when both knobs are absent — the safe
    default is no longer "trust every id".
    """

    boot_software_version: str
    base_dir: Path | None = None
    quarantine_resolver: Any = None
    allow_unverified_quarantine_ids: bool = False
    artifact_store: RebuildAuditArtifactStore | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "artifact_store",
            resolve_rebuild_audit_artifact_store(
                base_dir=self.base_dir,
                artifact_store=self.artifact_store,
            ),
        )
        if self.quarantine_resolver is None and not self.allow_unverified_quarantine_ids:
            raise ValueError(
                "KGStorageBackendContingency requires either quarantine_resolver "
                "(production) or allow_unverified_quarantine_ids=True (tests only)"
            )

    def prepare(
        self,
        *,
        board_id: str,
        corruption_timeline_ref: str,
        quarantine_ids: list[str],
        software_version: str,
    ) -> ContingencyResponse:
        """Persist the contingency manifest and compute readiness flags."""
        if not corruption_timeline_ref:
            _bump_contingency(
                board_id=board_id,
                outcome="timeline_unavailable",
                ready_for_upstream_issue=False,
                ready_for_hot_swap_decision=False,
            )
            raise ContingencyError(
                ContingencyErrorCode.TIMELINE_UNAVAILABLE,
                retryable=True,
                reason="corruption_timeline_ref is empty",
            )
        if not quarantine_ids:
            _bump_contingency(
                board_id=board_id,
                outcome="missing_quarantine",
                ready_for_upstream_issue=False,
                ready_for_hot_swap_decision=False,
            )
            raise ContingencyError(
                ContingencyErrorCode.MISSING_QUARANTINE_EVIDENCE,
                retryable=False,
                reason="quarantine_ids must include at least one quarantine",
            )

        resolved: list[str] = []
        missing: list[str] = []
        if self.quarantine_resolver is not None:
            for qid in quarantine_ids:
                manifest = self.quarantine_resolver(qid)
                if manifest is None:
                    missing.append(qid)
                else:
                    resolved.append(qid)
            if missing:
                _bump_contingency(
                    board_id=board_id,
                    outcome="missing_quarantine",
                    ready_for_upstream_issue=False,
                    ready_for_hot_swap_decision=False,
                )
                raise ContingencyError(
                    ContingencyErrorCode.MISSING_QUARANTINE_EVIDENCE,
                    retryable=False,
                    reason=(
                        f"quarantine_ids missing manifests: {sorted(missing)}"
                    ),
                )
        else:
            resolved = list(quarantine_ids)

        contingency_id = f"contingency_{secrets.token_urlsafe(12)}"
        ready_for_upstream_issue = bool(corruption_timeline_ref and resolved)
        ready_for_hot_swap_decision = (
            ready_for_upstream_issue
            and software_version == self.boot_software_version
        )

        payload = {
            "contingency_id": contingency_id,
            "board_id": board_id,
            "corruption_timeline_ref": corruption_timeline_ref,
            "quarantine_ids": resolved,
            "software_version": software_version,
            "boot_software_version": self.boot_software_version,
            "ready_for_upstream_issue": ready_for_upstream_issue,
            "ready_for_hot_swap_decision": ready_for_hot_swap_decision,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

        artifact_store = self.artifact_store
        if artifact_store is not None:
            key = RebuildAuditKey(
                namespace="contingency",
                board_id=board_id,
                artifact_id=contingency_id,
            )
            try:
                artifact_store.write_json_atomic(key, payload)
            except Exception as exc:
                raise ContingencyError(
                    ContingencyErrorCode.TIMELINE_UNAVAILABLE,
                    retryable=True,
                    reason=f"contingency manifest write failed: {exc}",
                ) from exc
            contingency_ref = key.to_ref()
        else:
            if self.base_dir is None:
                raise ContingencyError(
                    ContingencyErrorCode.TIMELINE_UNAVAILABLE,
                    retryable=True,
                    reason="base_dir is required when artifact_store is not supplied",
                )
            contingency_dir = self.base_dir / CONTINGENCY_DIRNAME / contingency_id
            try:
                contingency_dir.mkdir(parents=True, exist_ok=False)
            except OSError as exc:
                raise ContingencyError(
                    ContingencyErrorCode.TIMELINE_UNAVAILABLE,
                    retryable=True,
                    reason=f"contingency mkdir failed: {exc}",
                ) from exc

            manifest_path = contingency_dir / CONTINGENCY_MANIFEST_FILENAME
            try:
                with manifest_path.open("w", encoding="utf-8") as fh:
                    json.dump(payload, fh, indent=2)
            except Exception as exc:
                raise ContingencyError(
                    ContingencyErrorCode.TIMELINE_UNAVAILABLE,
                    retryable=True,
                    reason=f"contingency manifest write failed: {exc}",
                ) from exc
            contingency_ref = str(manifest_path)

        _bump_contingency(
            board_id=board_id,
            outcome="prepared",
            ready_for_upstream_issue=ready_for_upstream_issue,
            ready_for_hot_swap_decision=ready_for_hot_swap_decision,
        )
        logger.warning(
            "kg.contingency.prepared board=%s contingency_id=%s "
            "upstream=%s hot_swap=%s",
            board_id, contingency_id,
            ready_for_upstream_issue, ready_for_hot_swap_decision,
            extra={
                "event": "kg.contingency.prepared",
                "board_id": board_id,
                "contingency_id": contingency_id,
                "ready_for_upstream_issue": ready_for_upstream_issue,
                "ready_for_hot_swap_decision": ready_for_hot_swap_decision,
            },
        )
        return ContingencyResponse(
            contingency_ref=contingency_ref,
            ready_for_upstream_issue=ready_for_upstream_issue,
            ready_for_hot_swap_decision=ready_for_hot_swap_decision,
        )


__all__ = [
    "CONTINGENCY_DIRNAME",
    "CONTINGENCY_MANIFEST_FILENAME",
    "ContingencyError",
    "ContingencyErrorCode",
    "ContingencyResponse",
    "KGStorageBackendContingency",
    "get_contingency_count",
    "get_contingency_counter_labels",
    "get_contingency_samples",
    "reset_contingency_counter",
]

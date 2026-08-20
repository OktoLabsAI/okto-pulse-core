"""Relational persistence boundary for the consolidation processor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import hashlib
import json
import re
from typing import Any, Callable, Protocol, Sequence

from okto_pulse.core.runtime_context import (
    register_runtime_value,
    require_runtime_value,
    reset_runtime_values,
)


_EXACT_REBUILD_SOURCE_ARTIFACT_TYPES = frozenset(
    {
        "story",
        "ideation",
        "refinement",
        "spec",
        "sprint",
        "task",
        "test",
        "bug",
        "card",
        "amendment_hotfix_revision",
        "code_investigation_receipt",
        "code_evidence",
        "implementation_target",
    }
)


@dataclass(slots=True)
class ConsolidationQueueRecord:
    id: str
    board_id: str
    artifact_type: str
    artifact_id: str
    status: str
    attempts: int
    last_error: str | None
    next_retry_at: datetime | None
    claimed_at: datetime | None
    claim_timeout_at: datetime | None
    worker_id: str | None
    claimed_by_session_id: str | None
    triggered_at: datetime | None
    priority: str
    work_kind: str = "consolidate"
    generation: int = 0
    payload: dict[str, Any] | None = None
    delete_event_id: str | None = None
    claim_token: str | None = None
    triggered_by_event: str | None = None
    # Added after the original positional surface.  Keep this at the tail so
    # integrations that still construct queue records positionally do not
    # silently shift ``work_kind``/generation/payload into the wrong fields.
    source: str = "state_transition"


@dataclass(frozen=True, slots=True)
class ConsolidationClaimScope:
    """Exact queue membership admitted by an offline recovery processor.

    This scope is intentionally narrower than the ordinary runner contract:
    one board, one rebuild source fence and only consolidation work.  It is
    consumed directly by recovery tooling; it does not authorize global
    stale-claim recovery or DLQ auto-drain.
    """

    board_id: str
    source: str
    work_kind: str = "consolidate"
    # Stable across a crash/reacquired reservation for this exact governed
    # rebuild run.  The current ephemeral reservation token/epoch is proven
    # separately by ``reservation_authority_probe`` at every mutation fence.
    reservation_lineage_id: str | None = None

    def __post_init__(self) -> None:
        if not self.board_id.strip():
            raise ValueError("consolidation_claim_scope_board_id_required")
        if self.work_kind != "consolidate":
            raise ValueError("consolidation_claim_scope_work_kind_invalid")
        if (
            not self.source.startswith("rebuild:")
            or not self.source.removeprefix("rebuild:").strip()
        ):
            raise ValueError("consolidation_claim_scope_source_invalid")
        if self.reservation_lineage_id is not None and (
            type(self.reservation_lineage_id) is not str
            or len(self.reservation_lineage_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.reservation_lineage_id
            )
        ):
            raise ValueError("consolidation_claim_scope_reservation_lineage_invalid")

    def admits(self, entry: ConsolidationQueueRecord) -> bool:
        return (
            entry.board_id == self.board_id
            and entry.source == self.source
            and entry.work_kind == self.work_kind
        )


class ExactConsolidationDisposition(str, Enum):
    ACKED = "acked"
    RETRY_SCHEDULED = "retry_scheduled"
    TERMINAL_FAILURE = "terminal_failure"
    NEUTRAL_FENCE_LOSS = "neutral_fence_loss"


class ExactConsolidationResultOrigin(str, Enum):
    NEW = "new"
    REPLAYED = "replayed"


class ExactConsolidationMutationState(str, Enum):
    UNCHANGED = "unchanged"
    COMMITTED = "committed"
    COMPENSATED = "compensated"
    AMBIGUOUS = "ambiguous"


def _canonical_sha256(payload: dict[str, Any]) -> str:
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


@dataclass(frozen=True, slots=True)
class ExactConsolidationAckReceipt:
    """Durable relational effects owned by one exact rebuild queue ACK.

    The receipt is staged in the same relational transaction as the audit,
    node references, integration events and queue deletion.  It lets governed
    rebuild compensation reverse only the relational commits whose candidate
    graph was later discarded, including after a process crash erased the
    in-memory batch result.
    """

    queue_id: str
    board_id: str
    source: str
    reservation_lineage_id: str
    work_kind: str
    artifact_type: str
    artifact_id: str
    generation: int
    membership_source_ref: str
    membership_source_version: str
    membership_content_hash: str
    audit_content_hash: str
    consolidation_session_id: str
    outbox_event_id: str
    generation_event_id: str
    previous_materialization_generation: str
    materialization_generation: str
    node_ref_count: int
    node_refs_sha256: str
    receipt_sha256: str

    @classmethod
    def create(
        cls,
        *,
        queue_id: str,
        board_id: str,
        source: str,
        reservation_lineage_id: str,
        work_kind: str,
        artifact_type: str,
        artifact_id: str,
        generation: int,
        membership_source_ref: str,
        membership_source_version: str,
        membership_content_hash: str,
        audit_content_hash: str,
        consolidation_session_id: str,
        outbox_event_id: str,
        generation_event_id: str,
        previous_materialization_generation: str,
        materialization_generation: str,
        node_ref_count: int,
        node_refs_sha256: str,
    ) -> "ExactConsolidationAckReceipt":
        values: dict[str, Any] = {
            "artifact_id": artifact_id,
            "artifact_type": artifact_type,
            "audit_content_hash": audit_content_hash,
            "board_id": board_id,
            "consolidation_session_id": consolidation_session_id,
            "generation": generation,
            "generation_event_id": generation_event_id,
            "materialization_generation": materialization_generation,
            "membership_content_hash": membership_content_hash,
            "membership_source_ref": membership_source_ref,
            "membership_source_version": membership_source_version,
            "node_ref_count": node_ref_count,
            "node_refs_sha256": node_refs_sha256,
            "outbox_event_id": outbox_event_id,
            "previous_materialization_generation": (
                previous_materialization_generation
            ),
            "queue_id": queue_id,
            "reservation_lineage_id": reservation_lineage_id,
            "schema": "exact_consolidation_ack_receipt.v2",
            "source": source,
            "work_kind": work_kind,
        }
        return cls(
            queue_id=queue_id,
            board_id=board_id,
            source=source,
            reservation_lineage_id=reservation_lineage_id,
            work_kind=work_kind,
            artifact_type=artifact_type,
            artifact_id=artifact_id,
            generation=generation,
            membership_source_ref=membership_source_ref,
            membership_source_version=membership_source_version,
            membership_content_hash=membership_content_hash,
            audit_content_hash=audit_content_hash,
            consolidation_session_id=consolidation_session_id,
            outbox_event_id=outbox_event_id,
            generation_event_id=generation_event_id,
            previous_materialization_generation=(previous_materialization_generation),
            materialization_generation=materialization_generation,
            node_ref_count=node_ref_count,
            node_refs_sha256=node_refs_sha256,
            receipt_sha256=_canonical_sha256(values),
        )

    def __post_init__(self) -> None:
        strings = (
            self.queue_id,
            self.board_id,
            self.source,
            self.reservation_lineage_id,
            self.work_kind,
            self.artifact_type,
            self.artifact_id,
            self.membership_source_ref,
            self.membership_source_version,
            self.membership_content_hash,
            self.audit_content_hash,
            self.consolidation_session_id,
            self.outbox_event_id,
            self.generation_event_id,
            self.previous_materialization_generation,
            self.materialization_generation,
            self.node_refs_sha256,
            self.receipt_sha256,
        )
        source_type, separator, source_id = self.membership_source_ref.partition(":")
        queue_type = (
            "card" if source_type in {"task", "test", "bug", "card"} else source_type
        )
        if (
            any(type(value) is not str or not value for value in strings)
            or type(self.generation) is not int
            or self.generation < 0
            or type(self.node_ref_count) is not int
            or self.node_ref_count < 0
            or not self.source.startswith("rebuild:")
            or self.work_kind != "consolidate"
            or len(self.reservation_lineage_id) != 64
            or any(
                value not in "0123456789abcdef" for value in self.reservation_lineage_id
            )
            or separator != ":"
            or ":" in source_id
            or source_id != self.artifact_id
            or source_type not in _EXACT_REBUILD_SOURCE_ARTIFACT_TYPES
            or queue_type != self.artifact_type
            or len(self.membership_content_hash) != 64
            or any(
                value not in "0123456789abcdef"
                for value in self.membership_content_hash
            )
            or len(self.audit_content_hash) != 64
            or any(value not in "0123456789abcdef" for value in self.audit_content_hash)
            or len(self.node_refs_sha256) != 64
            or any(value not in "0123456789abcdef" for value in self.node_refs_sha256)
            or len(self.receipt_sha256) != 64
            or any(value not in "0123456789abcdef" for value in self.receipt_sha256)
            or self.previous_materialization_generation
            == self.materialization_generation
        ):
            raise ValueError("exact_consolidation_ack_receipt_invalid")
        expected = _canonical_sha256(
            {
                "artifact_id": self.artifact_id,
                "artifact_type": self.artifact_type,
                "audit_content_hash": self.audit_content_hash,
                "board_id": self.board_id,
                "consolidation_session_id": self.consolidation_session_id,
                "generation": self.generation,
                "generation_event_id": self.generation_event_id,
                "materialization_generation": self.materialization_generation,
                "membership_content_hash": self.membership_content_hash,
                "membership_source_ref": self.membership_source_ref,
                "membership_source_version": self.membership_source_version,
                "node_ref_count": self.node_ref_count,
                "node_refs_sha256": self.node_refs_sha256,
                "outbox_event_id": self.outbox_event_id,
                "previous_materialization_generation": (
                    self.previous_materialization_generation
                ),
                "queue_id": self.queue_id,
                "reservation_lineage_id": self.reservation_lineage_id,
                "schema": "exact_consolidation_ack_receipt.v2",
                "source": self.source,
                "work_kind": self.work_kind,
            }
        )
        if self.receipt_sha256 != expected:
            raise ValueError("exact_consolidation_ack_receipt_digest_mismatch")

    def to_payload(self) -> dict[str, object]:
        return {
            "schema": "exact_consolidation_ack_receipt.v2",
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        }

    @classmethod
    def from_payload(cls, value: object) -> "ExactConsolidationAckReceipt":
        field_names = frozenset(cls.__dataclass_fields__)
        if (
            type(value) is not dict
            or set(value) != {*field_names, "schema"}
            or value.get("schema") != "exact_consolidation_ack_receipt.v2"
        ):
            raise ValueError("exact_consolidation_ack_receipt_payload_invalid")
        try:
            return cls(**{name: value[name] for name in field_names})
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("exact_consolidation_ack_receipt_payload_invalid") from exc


@dataclass(frozen=True, slots=True)
class ExactConsolidationCompensationReceipt:
    """Terminal, replayable proof that one exact ACK journal was reversed."""

    board_id: str
    source: str
    reservation_lineage_id: str
    baseline_materialization_generation: str
    terminal_materialization_generation: str
    ack_count: int
    node_ref_count: int
    ack_receipts_sha256: str
    audit_session_ids: tuple[str, ...]
    outbox_event_ids: tuple[str, ...]
    generation_event_ids: tuple[str, ...]
    compensation_id: str
    compensated_at: datetime
    receipt_sha256: str

    @classmethod
    def create(
        cls,
        *,
        board_id: str,
        source: str,
        reservation_lineage_id: str,
        baseline_materialization_generation: str,
        terminal_materialization_generation: str,
        ack_count: int,
        node_ref_count: int,
        ack_receipts_sha256: str,
        audit_session_ids: tuple[str, ...],
        outbox_event_ids: tuple[str, ...],
        generation_event_ids: tuple[str, ...],
        compensation_id: str,
        compensated_at: datetime,
    ) -> "ExactConsolidationCompensationReceipt":
        values: dict[str, Any] = {
            "ack_count": ack_count,
            "ack_receipts_sha256": ack_receipts_sha256,
            "audit_session_ids": list(audit_session_ids),
            "baseline_materialization_generation": (
                baseline_materialization_generation
            ),
            "board_id": board_id,
            "compensated_at": compensated_at.isoformat(),
            "compensation_id": compensation_id,
            "generation_event_ids": list(generation_event_ids),
            "node_ref_count": node_ref_count,
            "outbox_event_ids": list(outbox_event_ids),
            "reservation_lineage_id": reservation_lineage_id,
            "schema": "exact_consolidation_compensation_receipt.v1",
            "source": source,
            "terminal_materialization_generation": (
                terminal_materialization_generation
            ),
        }
        return cls(
            board_id=board_id,
            source=source,
            reservation_lineage_id=reservation_lineage_id,
            baseline_materialization_generation=(baseline_materialization_generation),
            terminal_materialization_generation=(terminal_materialization_generation),
            ack_count=ack_count,
            node_ref_count=node_ref_count,
            ack_receipts_sha256=ack_receipts_sha256,
            audit_session_ids=audit_session_ids,
            outbox_event_ids=outbox_event_ids,
            generation_event_ids=generation_event_ids,
            compensation_id=compensation_id,
            compensated_at=compensated_at,
            receipt_sha256=_canonical_sha256(values),
        )

    def __post_init__(self) -> None:
        if (
            any(
                type(value) is not str or not value
                for value in (
                    self.board_id,
                    self.source,
                    self.reservation_lineage_id,
                    self.baseline_materialization_generation,
                    self.terminal_materialization_generation,
                    self.ack_receipts_sha256,
                    self.compensation_id,
                    self.receipt_sha256,
                )
            )
            or not self.source.startswith("rebuild:")
            or len(self.reservation_lineage_id) != 64
            or any(
                value not in "0123456789abcdef" for value in self.reservation_lineage_id
            )
            or type(self.ack_count) is not int
            or self.ack_count < 1
            or type(self.node_ref_count) is not int
            or self.node_ref_count < 0
            or type(self.audit_session_ids) is not tuple
            or type(self.outbox_event_ids) is not tuple
            or type(self.generation_event_ids) is not tuple
            or any(
                type(value) is not str or not value
                for values in (
                    self.audit_session_ids,
                    self.outbox_event_ids,
                    self.generation_event_ids,
                )
                for value in values
            )
            or len(self.audit_session_ids) != self.ack_count
            or len(self.outbox_event_ids) != self.ack_count
            or len(self.generation_event_ids) != self.ack_count
            or len(set(self.audit_session_ids)) != self.ack_count
            or len(set(self.outbox_event_ids)) != self.ack_count
            or len(set(self.generation_event_ids)) != self.ack_count
            or len(self.ack_receipts_sha256) != 64
            or any(
                value not in "0123456789abcdef" for value in self.ack_receipts_sha256
            )
            or len(self.receipt_sha256) != 64
            or any(value not in "0123456789abcdef" for value in self.receipt_sha256)
            or type(self.compensated_at) is not datetime
            or self.compensated_at.tzinfo is None
            or self.compensated_at.utcoffset() is None
            or self.baseline_materialization_generation
            == self.terminal_materialization_generation
        ):
            raise ValueError("exact_consolidation_compensation_receipt_invalid")
        expected = _canonical_sha256(
            {
                "ack_count": self.ack_count,
                "ack_receipts_sha256": self.ack_receipts_sha256,
                "audit_session_ids": list(self.audit_session_ids),
                "baseline_materialization_generation": (
                    self.baseline_materialization_generation
                ),
                "board_id": self.board_id,
                "compensated_at": self.compensated_at.isoformat(),
                "compensation_id": self.compensation_id,
                "generation_event_ids": list(self.generation_event_ids),
                "node_ref_count": self.node_ref_count,
                "outbox_event_ids": list(self.outbox_event_ids),
                "reservation_lineage_id": self.reservation_lineage_id,
                "schema": "exact_consolidation_compensation_receipt.v1",
                "source": self.source,
                "terminal_materialization_generation": (
                    self.terminal_materialization_generation
                ),
            }
        )
        if self.receipt_sha256 != expected:
            raise ValueError("exact_consolidation_compensation_receipt_digest_mismatch")

    def to_payload(self) -> dict[str, object]:
        return {
            "ack_count": self.ack_count,
            "ack_receipts_sha256": self.ack_receipts_sha256,
            "audit_session_ids": list(self.audit_session_ids),
            "baseline_materialization_generation": (
                self.baseline_materialization_generation
            ),
            "board_id": self.board_id,
            "compensated_at": self.compensated_at.isoformat(),
            "compensation_id": self.compensation_id,
            "generation_event_ids": list(self.generation_event_ids),
            "node_ref_count": self.node_ref_count,
            "outbox_event_ids": list(self.outbox_event_ids),
            "receipt_sha256": self.receipt_sha256,
            "reservation_lineage_id": self.reservation_lineage_id,
            "schema": "exact_consolidation_compensation_receipt.v1",
            "source": self.source,
            "terminal_materialization_generation": (
                self.terminal_materialization_generation
            ),
        }

    @classmethod
    def from_payload(
        cls,
        value: object,
    ) -> "ExactConsolidationCompensationReceipt":
        if (
            type(value) is not dict
            or set(value)
            != {
                "ack_count",
                "ack_receipts_sha256",
                "audit_session_ids",
                "baseline_materialization_generation",
                "board_id",
                "compensated_at",
                "compensation_id",
                "generation_event_ids",
                "node_ref_count",
                "outbox_event_ids",
                "receipt_sha256",
                "reservation_lineage_id",
                "schema",
                "source",
                "terminal_materialization_generation",
            }
            or value.get("schema") != "exact_consolidation_compensation_receipt.v1"
        ):
            raise ValueError("exact_consolidation_compensation_receipt_payload_invalid")
        try:
            compensated_at = value["compensated_at"]
            if type(compensated_at) is not str:
                raise TypeError
            return cls(
                board_id=value["board_id"],
                source=value["source"],
                reservation_lineage_id=value["reservation_lineage_id"],
                baseline_materialization_generation=(
                    value["baseline_materialization_generation"]
                ),
                terminal_materialization_generation=(
                    value["terminal_materialization_generation"]
                ),
                ack_count=value["ack_count"],
                node_ref_count=value["node_ref_count"],
                ack_receipts_sha256=value["ack_receipts_sha256"],
                audit_session_ids=tuple(value["audit_session_ids"]),
                outbox_event_ids=tuple(value["outbox_event_ids"]),
                generation_event_ids=tuple(value["generation_event_ids"]),
                compensation_id=value["compensation_id"],
                compensated_at=datetime.fromisoformat(compensated_at),
                receipt_sha256=value["receipt_sha256"],
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                "exact_consolidation_compensation_receipt_payload_invalid"
            ) from exc


@dataclass(frozen=True, slots=True)
class ExactConsolidationCompensationResult:
    receipt: ExactConsolidationCompensationReceipt
    replayed: bool

    def __post_init__(self) -> None:
        if type(self.receipt) is not ExactConsolidationCompensationReceipt:
            raise TypeError("exact_consolidation_compensation_result_receipt_invalid")
        if type(self.replayed) is not bool:
            raise TypeError("exact_consolidation_compensation_result_origin_invalid")


class ExactConsolidationCompensationError(RuntimeError):
    """Typed failure at the exact relational compensation boundary."""

    __slots__ = ("_code", "_committed_result")

    def __init__(
        self,
        code: str,
        *,
        committed_result: ExactConsolidationCompensationResult | None = None,
    ) -> None:
        if type(code) is not str or not code:
            raise TypeError("exact_consolidation_compensation_error_code_invalid")
        if committed_result is not None and (
            type(committed_result) is not ExactConsolidationCompensationResult
        ):
            raise TypeError("exact_consolidation_compensation_error_result_invalid")
        self._code = code
        self._committed_result = committed_result
        super().__init__(code)

    @property
    def code(self) -> str:
        return self._code

    @property
    def committed_result(self) -> ExactConsolidationCompensationResult | None:
        return self._committed_result


_EXACT_CONSOLIDATION_ACK_INTEGRITY_CODES = frozenset(
    {
        "exact_consolidation_ack_after_compensation",
        "exact_consolidation_ack_audit_invalid",
        "exact_consolidation_ack_generation_event_invalid",
        "exact_consolidation_ack_generation_event_published",
        "exact_consolidation_ack_generation_head_invalid",
        "exact_consolidation_ack_node_ref_counts_invalid",
        "exact_consolidation_ack_node_refs_invalid",
        "exact_consolidation_ack_outbox_invalid",
        "exact_consolidation_ack_queue_reused",
    }
)


class ExactConsolidationAckIntegrityError(RuntimeError):
    """Typed, pre-commit failure of one exact ACK relational proof.

    These codes mean the claimed queue row may still be owned, but its staged
    relational audit/effects do not satisfy the exact rebuild contract. They
    are deterministic blockers, never evidence of a neutral claim/fence loss.
    """

    def __init__(self, code: str) -> None:
        if (
            type(code) is not str
            or code not in _EXACT_CONSOLIDATION_ACK_INTEGRITY_CODES
        ):
            raise ValueError("exact_consolidation_ack_integrity_code_invalid")
        self._code = code
        super().__init__(code)

    @property
    def code(self) -> str:
        return self._code


def exact_consolidation_ack_receipts_sha256(
    receipts: tuple[ExactConsolidationAckReceipt, ...],
) -> str:
    """Canonical digest for one complete, ordered exact ACK journal."""

    if (
        type(receipts) is not tuple
        or not receipts
        or any(
            type(receipt) is not ExactConsolidationAckReceipt for receipt in receipts
        )
    ):
        raise TypeError("exact_consolidation_ack_receipts_invalid")
    return _canonical_sha256(
        {
            "receipt_sha256": [receipt.receipt_sha256 for receipt in receipts],
            "schema": "exact_consolidation_ack_receipt_chain.v2",
        }
    )


def build_exact_consolidation_compensation_binding(
    *,
    board_id: str,
    source: str,
    reservation_lineage_id: str,
    result: ExactConsolidationCompensationResult | None,
) -> dict[str, object]:
    """JSON-safe binding embedded in the enclosing F06 compensation receipt."""

    if result is not None and type(result) is not ExactConsolidationCompensationResult:
        raise TypeError("exact_consolidation_compensation_binding_result_invalid")
    receipt = result.receipt if result is not None else None
    if receipt is not None and (
        receipt.board_id != board_id
        or receipt.source != source
        or receipt.reservation_lineage_id != reservation_lineage_id
    ):
        raise ValueError("exact_consolidation_compensation_binding_mismatch")
    return {
        "ack_count": receipt.ack_count if receipt is not None else 0,
        "board_id": board_id,
        "receipt": receipt.to_payload() if receipt is not None else None,
        "reservation_lineage_id": reservation_lineage_id,
        "schema": "exact_consolidation_compensation_binding.v1",
        "source": source,
        "status": (
            "replayed"
            if result is not None and result.replayed
            else "compensated"
            if result is not None
            else "not_required"
        ),
    }


def validate_exact_consolidation_compensation_binding(
    value: object,
    *,
    board_id: str,
    source: str,
    reservation_lineage_id: str,
) -> ExactConsolidationCompensationReceipt | None:
    """Validate the exact relational proof carried by an F06 receipt."""

    if type(value) is not dict or set(value) != {
        "ack_count",
        "board_id",
        "receipt",
        "reservation_lineage_id",
        "schema",
        "source",
        "status",
    }:
        raise ValueError("exact_consolidation_compensation_binding_invalid")
    status = value.get("status")
    ack_count = value.get("ack_count")
    if (
        value.get("schema") != "exact_consolidation_compensation_binding.v1"
        or value.get("board_id") != board_id
        or value.get("source") != source
        or value.get("reservation_lineage_id") != reservation_lineage_id
        or status not in {"not_required", "compensated", "replayed"}
        or type(ack_count) is not int
        or ack_count < 0
    ):
        raise ValueError("exact_consolidation_compensation_binding_invalid")
    payload = value.get("receipt")
    if status == "not_required":
        if ack_count != 0 or payload is not None:
            raise ValueError("exact_consolidation_compensation_binding_invalid")
        return None
    if type(payload) is not dict or set(payload) != {
        "ack_count",
        "ack_receipts_sha256",
        "audit_session_ids",
        "baseline_materialization_generation",
        "board_id",
        "compensated_at",
        "compensation_id",
        "generation_event_ids",
        "node_ref_count",
        "outbox_event_ids",
        "receipt_sha256",
        "reservation_lineage_id",
        "schema",
        "source",
        "terminal_materialization_generation",
    }:
        raise ValueError("exact_consolidation_compensation_binding_invalid")
    try:
        receipt = ExactConsolidationCompensationReceipt.from_payload(payload)
    except (TypeError, ValueError) as exc:
        raise ValueError("exact_consolidation_compensation_binding_invalid") from exc
    if (
        receipt.board_id != board_id
        or receipt.source != source
        or receipt.reservation_lineage_id != reservation_lineage_id
        or receipt.ack_count != ack_count
    ):
        raise ValueError("exact_consolidation_compensation_binding_mismatch")
    return receipt


@dataclass(frozen=True, slots=True)
class ExactConsolidationRowResult:
    queue_id: str
    board_id: str
    source: str
    reservation_lineage_id: str
    work_kind: str
    artifact_type: str
    artifact_id: str
    generation: int
    membership_source_ref: str
    membership_source_version: str
    membership_content_hash: str
    attempt_ordinal: int
    disposition: ExactConsolidationDisposition
    origin: ExactConsolidationResultOrigin
    mutation_state: ExactConsolidationMutationState
    error_code: str | None = None
    error_message: str | None = None
    next_retry_at: datetime | None = None
    diagnostic_json: str | None = None
    ack_receipt: ExactConsolidationAckReceipt | None = None

    def __post_init__(self) -> None:
        string_fields = (
            self.queue_id,
            self.board_id,
            self.source,
            self.reservation_lineage_id,
            self.work_kind,
            self.artifact_type,
            self.artifact_id,
            self.membership_source_ref,
            self.membership_source_version,
            self.membership_content_hash,
        )
        if any(type(value) is not str or not value for value in string_fields):
            raise TypeError("exact_consolidation_row_identity_invalid")
        membership_artifact_type, separator, membership_artifact_id = (
            self.membership_source_ref.partition(":")
        )
        membership_queue_type = (
            "card"
            if membership_artifact_type in {"task", "test", "bug", "card"}
            else membership_artifact_type
        )
        if type(self.generation) is not int or self.generation < 0:
            raise TypeError("exact_consolidation_row_generation_invalid")
        if type(self.attempt_ordinal) is not int or self.attempt_ordinal < 1:
            raise TypeError("exact_consolidation_row_attempt_invalid")
        if type(self.disposition) is not ExactConsolidationDisposition:
            raise TypeError("exact_consolidation_row_disposition_invalid")
        if type(self.origin) is not ExactConsolidationResultOrigin:
            raise TypeError("exact_consolidation_row_origin_invalid")
        if type(self.mutation_state) is not ExactConsolidationMutationState:
            raise TypeError("exact_consolidation_row_mutation_state_invalid")
        if (self.error_code is None) is not (self.error_message is None):
            raise ValueError("exact_consolidation_row_error_invalid")
        if self.error_code is not None and (
            type(self.error_code) is not str
            or not self.error_code
            or len(self.error_code) > 128
            or re.fullmatch(r"[A-Za-z][A-Za-z0-9_.:-]{0,127}", self.error_code) is None
            or type(self.error_message) is not str
            or not self.error_message
            or len(self.error_message) > 480
        ):
            raise ValueError("exact_consolidation_row_error_invalid")
        if self.diagnostic_json is not None:
            if (
                type(self.diagnostic_json) is not str
                or not self.diagnostic_json
                or len(self.diagnostic_json) > 16384
            ):
                raise ValueError("exact_consolidation_row_diagnostic_invalid")
            try:
                diagnostic = json.loads(self.diagnostic_json)
            except (TypeError, ValueError) as exc:
                raise ValueError("exact_consolidation_row_diagnostic_invalid") from exc
            if (
                type(diagnostic) is not dict
                or json.dumps(
                    diagnostic,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                != self.diagnostic_json
            ):
                raise ValueError("exact_consolidation_row_diagnostic_invalid")
        if self.next_retry_at is not None and (
            type(self.next_retry_at) is not datetime
            or self.next_retry_at.tzinfo is None
            or self.next_retry_at.utcoffset() is None
        ):
            raise TypeError("exact_consolidation_row_retry_at_invalid")
        if (
            not self.source.startswith("rebuild:")
            or not self.source.removeprefix("rebuild:").strip()
            or self.work_kind != "consolidate"
            or len(self.reservation_lineage_id) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.reservation_lineage_id
            )
            or separator != ":"
            or ":" in membership_artifact_id
            or membership_artifact_id != self.artifact_id
            or membership_artifact_type not in _EXACT_REBUILD_SOURCE_ARTIFACT_TYPES
            or membership_queue_type != self.artifact_type
            or not self.membership_source_version.strip()
            or len(self.membership_content_hash) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.membership_content_hash
            )
        ):
            raise ValueError("exact_consolidation_row_binding_invalid")
        has_error = self.error_code is not None
        if self.disposition is ExactConsolidationDisposition.ACKED:
            valid_semantics = (
                self.origin is ExactConsolidationResultOrigin.NEW
                and self.mutation_state is ExactConsolidationMutationState.COMMITTED
                and not has_error
                and self.next_retry_at is None
                and self.diagnostic_json is None
                and type(self.ack_receipt) is ExactConsolidationAckReceipt
                and self.ack_receipt.queue_id == self.queue_id
                and self.ack_receipt.board_id == self.board_id
                and self.ack_receipt.source == self.source
                and self.ack_receipt.reservation_lineage_id
                == self.reservation_lineage_id
                and self.ack_receipt.work_kind == self.work_kind
                and self.ack_receipt.artifact_type == self.artifact_type
                and self.ack_receipt.artifact_id == self.artifact_id
                and self.ack_receipt.generation == self.generation
                and self.ack_receipt.membership_source_ref == self.membership_source_ref
                and self.ack_receipt.membership_source_version
                == self.membership_source_version
                and self.ack_receipt.membership_content_hash
                == self.membership_content_hash
            )
        elif self.disposition is ExactConsolidationDisposition.RETRY_SCHEDULED:
            valid_semantics = (
                self.origin
                in {
                    ExactConsolidationResultOrigin.NEW,
                    ExactConsolidationResultOrigin.REPLAYED,
                }
                and self.mutation_state is ExactConsolidationMutationState.UNCHANGED
                and has_error
                and self.next_retry_at is not None
                and self.ack_receipt is None
            )
        elif self.disposition is ExactConsolidationDisposition.TERMINAL_FAILURE:
            valid_semantics = (
                self.origin
                in {
                    ExactConsolidationResultOrigin.NEW,
                    ExactConsolidationResultOrigin.REPLAYED,
                }
                and self.mutation_state
                in {
                    ExactConsolidationMutationState.UNCHANGED,
                    ExactConsolidationMutationState.COMPENSATED,
                    ExactConsolidationMutationState.AMBIGUOUS,
                }
                and has_error
                and self.next_retry_at is None
                and self.ack_receipt is None
            )
        else:
            valid_semantics = (
                self.origin is ExactConsolidationResultOrigin.NEW
                and self.mutation_state
                in {
                    ExactConsolidationMutationState.UNCHANGED,
                    ExactConsolidationMutationState.COMPENSATED,
                    ExactConsolidationMutationState.AMBIGUOUS,
                }
                and has_error
                and self.next_retry_at is None
                and self.ack_receipt is None
            )
        if not valid_semantics:
            raise ValueError("exact_consolidation_row_semantics_invalid")


@dataclass(frozen=True, slots=True)
class ExactConsolidationBatchResult:
    claim_scope: ConsolidationClaimScope
    rows: tuple[ExactConsolidationRowResult, ...]

    def __post_init__(self) -> None:
        if type(self.claim_scope) is not ConsolidationClaimScope:
            raise TypeError("exact_consolidation_batch_scope_invalid")
        if type(self.rows) is not tuple or any(
            type(row) is not ExactConsolidationRowResult for row in self.rows
        ):
            raise TypeError("exact_consolidation_batch_rows_invalid")
        if any(
            row.board_id != self.claim_scope.board_id
            or row.source != self.claim_scope.source
            or row.work_kind != self.claim_scope.work_kind
            or row.reservation_lineage_id != self.claim_scope.reservation_lineage_id
            for row in self.rows
        ):
            raise ValueError("exact_consolidation_batch_scope_mismatch")
        if self.claim_scope.reservation_lineage_id is None:
            raise ValueError("exact_consolidation_batch_reservation_lineage_required")
        if len({row.queue_id for row in self.rows}) != len(self.rows):
            raise ValueError("exact_consolidation_batch_duplicate_queue_id")

    @property
    def acked_count(self) -> int:
        return sum(
            row.disposition is ExactConsolidationDisposition.ACKED for row in self.rows
        )

    @property
    def new_attempt_count(self) -> int:
        return sum(
            row.origin is ExactConsolidationResultOrigin.NEW for row in self.rows
        )

    @property
    def replayed_count(self) -> int:
        return sum(
            row.origin is ExactConsolidationResultOrigin.REPLAYED for row in self.rows
        )

    @property
    def terminal_failures(self) -> tuple[ExactConsolidationRowResult, ...]:
        return tuple(
            row
            for row in self.rows
            if row.disposition is ExactConsolidationDisposition.TERMINAL_FAILURE
        )

    @property
    def retry_scheduled(self) -> tuple[ExactConsolidationRowResult, ...]:
        return tuple(
            row
            for row in self.rows
            if row.disposition is ExactConsolidationDisposition.RETRY_SCHEDULED
        )

    @property
    def earliest_retry_at(self) -> datetime | None:
        values = [
            row.next_retry_at
            for row in self.retry_scheduled
            if row.next_retry_at is not None
        ]
        return min(values) if values else None


class ExactConsolidationPostCommitError(RuntimeError):
    """A durable exact row outcome whose graph cleanup could not complete.

    ``batch_result`` is the authoritative partial result through the durable
    transaction, including the affected row. Recovery orchestration must use
    it for totality/gating and then compensate the enclosing F06 operation;
    it must never infer committed identities from queue-depth deltas.
    """

    __slots__ = ("_batch_result", "_error_code", "_failed_queue_id")

    def __init__(
        self,
        *,
        batch_result: ExactConsolidationBatchResult,
        failed_queue_id: str,
        error_code: str,
    ) -> None:
        if type(batch_result) is not ExactConsolidationBatchResult:
            raise TypeError("exact_post_commit_batch_result_invalid")
        if type(failed_queue_id) is not str or not failed_queue_id:
            raise TypeError("exact_post_commit_queue_id_invalid")
        if type(error_code) is not str or not error_code:
            raise TypeError("exact_post_commit_error_code_invalid")
        matching = tuple(
            row for row in batch_result.rows if row.queue_id == failed_queue_id
        )
        if (
            len(matching) != 1
            or matching[0].origin is not ExactConsolidationResultOrigin.NEW
        ):
            raise ValueError("exact_post_commit_failed_row_invalid")
        self._batch_result = batch_result
        self._failed_queue_id = failed_queue_id
        self._error_code = error_code
        super().__init__(error_code)

    @property
    def batch_result(self) -> ExactConsolidationBatchResult:
        return self._batch_result

    @property
    def failed_queue_id(self) -> str:
        return self._failed_queue_id

    @property
    def error_code(self) -> str:
        return self._error_code


@dataclass(frozen=True, slots=True)
class ConsolidationPoisonRow:
    id: str
    attempts: int


@dataclass(frozen=True, slots=True)
class CurrentQualityAssessmentSummary:
    """Current quality head joined to its immutable receipt."""

    board_id: str
    subject_type: str
    subject_id: str
    subject_version: int
    subject_edition: int | None
    assessment_kind: str
    receipt_id: str
    head_revision: int
    outcome: str
    score: float
    justification: str
    scale_kind: str
    scale_minimum: float
    scale_maximum: float
    scale_direction: str
    content_digest: str
    clarification_digest: str
    ruleset_digest: str
    taxonomy_digest: str
    policy_digest: str
    input_digest: str
    canonicalization_version: str
    ruleset_version: str
    taxonomy_version: str
    analyzer_version: str
    policy_version: str
    created_at: datetime
    updated_at: datetime
    projection_fingerprint: str

    def to_worker_dict(self) -> dict[str, Any]:
        """Return the bounded root-summary shape consumed by the pure worker."""

        return {
            "board_id": self.board_id,
            "subject_type": self.subject_type,
            "subject_id": self.subject_id,
            "subject_version": self.subject_version,
            "subject_edition": self.subject_edition,
            "assessment_kind": self.assessment_kind,
            "receipt_id": self.receipt_id,
            "head_revision": self.head_revision,
            "outcome": self.outcome,
            "score": self.score,
            "justification": self.justification,
            "scale_kind": self.scale_kind,
            "scale_minimum": self.scale_minimum,
            "scale_maximum": self.scale_maximum,
            "scale_direction": self.scale_direction,
            "input_digest": self.input_digest,
            "ruleset_version": self.ruleset_version,
            "taxonomy_version": self.taxonomy_version,
            "analyzer_version": self.analyzer_version,
            "policy_version": self.policy_version,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "projection_fingerprint": self.projection_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class CurrentResearchDecisionSummary:
    """Current RDL head joined to its immutable entry."""

    board_id: str
    refinement_id: str
    refinement_version: int
    ledger_id: str
    entry_id: str
    head_revision: int
    predecessor_entry_id: str | None
    unknown: str
    status: str
    anchor_type: str
    anchor_ref: str
    evidence_refs: tuple[str, ...]
    alternatives: tuple[str, ...]
    decision: str | None
    rationale: str | None
    confidence: float | None
    evidence_absence_justification: str | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    projection_fingerprint: str

    def to_worker_dict(self) -> dict[str, Any]:
        """Return the current-head shape consumed by the pure worker."""

        return {
            "board_id": self.board_id,
            "refinement_id": self.refinement_id,
            "refinement_version": self.refinement_version,
            "ledger_id": self.ledger_id,
            "entry_id": self.entry_id,
            "head_revision": self.head_revision,
            "predecessor_entry_id": self.predecessor_entry_id,
            "unknown": self.unknown,
            "status": self.status,
            "anchor_type": self.anchor_type,
            "anchor_ref": self.anchor_ref,
            "evidence_refs": list(self.evidence_refs),
            "alternatives": list(self.alternatives),
            "decision": self.decision,
            "rationale": self.rationale,
            "confidence": self.confidence,
            "evidence_absence_justification": (self.evidence_absence_justification),
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "projection_fingerprint": self.projection_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class CurrentSpecDependencyProjection:
    """One authoritative active Spec dependency plus endpoint projection."""

    dependency_id: str
    board_id: str
    dependent_spec_id: str
    prerequisite_spec_id: str
    prerequisite_title: str
    prerequisite_status: str
    prerequisite_version: int

    def to_worker_dict(self) -> dict[str, Any]:
        return {
            "dependency_id": self.dependency_id,
            "board_id": self.board_id,
            "dependent_spec_id": self.dependent_spec_id,
            "prerequisite_spec_id": self.prerequisite_spec_id,
            "prerequisite_title": self.prerequisite_title,
            "prerequisite_status": self.prerequisite_status,
            "prerequisite_version": self.prerequisite_version,
        }


@dataclass(frozen=True, slots=True)
class ConsolidationProjectionInputs:
    """Bounded relational projections loaded alongside one source artifact."""

    quality_assessments: tuple[CurrentQualityAssessmentSummary, ...] = ()
    research_decisions: tuple[CurrentResearchDecisionSummary, ...] = ()
    spec_dependencies: tuple[CurrentSpecDependencyProjection, ...] = ()


class ConsolidationPersistencePort(Protocol):
    async def load_artifact(
        self,
        context: Any,
        *,
        artifact_type: str,
        artifact_id: str,
    ) -> Any | None: ...

    async def load_projection_inputs(
        self,
        context: Any,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        artifact: Any,
    ) -> ConsolidationProjectionInputs:
        """Load current relational heads for an already-loaded artifact."""

        ...

    async def list_artifacts(
        self,
        context: Any,
        *,
        artifact_type: str,
        artifact_ids: Sequence[str],
        board_id: str | None = None,
    ) -> tuple[Any, ...]: ...

    async def list_stale_claims(
        self,
        context: Any,
        *,
        now: datetime,
        legacy_cutoff: datetime,
    ) -> tuple[ConsolidationQueueRecord, ...]: ...

    async def count_pending(self, context: Any) -> int: ...

    async def list_claimed_board_ids(self, context: Any) -> frozenset[str]: ...

    async def list_ready_pending(
        self, context: Any, *, now: datetime
    ) -> tuple[ConsolidationQueueRecord, ...]: ...

    async def list_ready_pending_exact(
        self,
        context: Any,
        *,
        now: datetime,
        board_id: str,
        source: str,
        work_kind: str,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        """List only exact recovery membership before any row is claimed."""

        ...

    async def list_pending_exact(
        self,
        context: Any,
        *,
        board_id: str,
        source: str,
        work_kind: str,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        """List all pending exact members, including delayed dispositions."""

        ...

    async def list_claimed_exact(
        self,
        context: Any,
        *,
        board_id: str,
        source: str,
        work_kind: str,
    ) -> tuple[ConsolidationQueueRecord, ...]:
        """List only claimed members of one offline recovery fence."""

        ...

    async def claim_ready_pending_exact(
        self,
        context: Any,
        *,
        entry_id: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        now: datetime,
        claim_timeout_at: datetime,
        worker_id: str,
        claim_token: str,
    ) -> ConsolidationQueueRecord | None:
        """CAS one exact ready row into claimed state."""

        ...

    async def board_administrative_rebuild_source(
        self,
        context: Any,
        *,
        board_id: str,
    ) -> str | None: ...

    async def get_queue_entry(
        self, context: Any, *, entry_id: str
    ) -> ConsolidationQueueRecord | None: ...

    async def queue_claim_is_current_and_unfenced(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        work_kind: str,
        source: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Validate claim ownership and the governed-deletion generation fence.

        ``consolidate`` is valid only while no tombstone exists for the
        artifact. ``stale_reconcile`` is valid only while the tombstone has
        the exact generation and delete-event identity carried by the work
        row. Implementations must evaluate the claim and tombstone predicates
        in one storage statement/snapshot.
        """
        ...

    async def ack_claimed_queue_entry(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Delete one claimed row by compare-and-swap identity.

        Returns ``True`` only when exactly one row matched the id, claimed
        status, claim token, generation and null-safe delete-event identity.
        """
        ...

    async def ack_exact_rebuild_commit(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
        reservation_lineage_id: str,
        membership_source_ref: str,
        membership_source_version: str,
        membership_content_hash: str,
        consolidation_session_id: str,
        expected_attempts: int,
        expected_last_error: str | None,
        expected_next_retry_at: datetime | None,
        expected_payload: dict[str, Any],
        reservation_authority_probe: Callable[[], bool],
    ) -> ExactConsolidationAckReceipt | None:
        """Atomically journal relational effects and ACK one exact row.

        The implementation must validate the transaction-staged audit, node
        references, outbox event, materialization-generation event and current
        generation head. The returned v2 receipt must derive and bind the audit
        row's own ``audit_content_hash`` separately from the supplied manifest
        ``membership_content_hash``; the two hash domains must never be equated.
        It then persists the immutable ACK receipt and deletes the claimed queue
        row by the complete supplied identity in that same transaction. ``None``
        is a neutral claim or authority loss. Deterministic staged-proof failures
        raise :class:`ExactConsolidationAckIntegrityError` before commit.
        """

        ...

    async def list_exact_rebuild_ack_receipts(
        self,
        context: Any,
        *,
        board_id: str,
        source: str,
        reservation_lineage_id: str,
    ) -> tuple[ExactConsolidationAckReceipt, ...]:
        """Load the complete durable ACK journal in generation-chain order."""

        ...

    async def compensate_exact_rebuild_commits(
        self,
        context: Any,
        *,
        board_id: str,
        source: str,
        reservation_lineage_id: str,
        expected_receipts: tuple[ExactConsolidationAckReceipt, ...],
        reservation_authority_probe: Callable[[], bool],
    ) -> ExactConsolidationCompensationResult | None:
        """Atomically reverse the complete exact ACK journal.

        Implementations must revalidate the exact receipt chain, current
        materialization head, active audits and node-reference digests, plus
        prove every bound outbox/domain event is still unpublished and has no
        handler execution. Only then may they mark audits undone, neutralize the
        pending integration facts, restore the baseline materialization head and
        persist a replayable compensation receipt. ``None`` is an authority or
        CAS loss; partial mutation is forbidden.
        """

        ...

    async def repend_claimed_queue_entry(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
    ) -> bool:
        """Neutrally release an exact claim after an administrative fence.

        Implementations must compare every supplied identity in one atomic
        update, preserve source/payload/attempts and clear claim ownership and
        timing fields while returning the row to ``pending``.
        """

        ...

    async def save_exact_rebuild_disposition(
        self,
        context: Any,
        *,
        entry_id: str,
        claim_token: str,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
        source: str,
        work_kind: str,
        generation: int,
        delete_event_id: str | None,
        expected_attempts: int,
        expected_last_error: str | None,
        expected_next_retry_at: datetime | None,
        expected_payload: dict[str, Any],
        reservation_authority_probe: Callable[[], bool],
        payload: dict[str, Any],
        attempts: int,
        last_error: str,
        next_retry_at: datetime | None,
    ) -> ConsolidationQueueRecord | None:
        """CAS a claimed exact row to pending with its durable disposition.

        Implementations must compare the complete claim identity and exact
        prior payload, then invoke the token/epoch-bound reservation probe
        immediately before mutation in the same transaction. The durable
        marker binds the stable run lineage instead of that ephemeral token,
        so a governed successor lease for the same run can replay it. They
        replace payload/retry fields, clear claim ownership, and return the
        stored row. ``None`` is a neutral ownership/fence loss. Core re-proves
        the current ephemeral authority immediately before and after commit.
        """

        ...

    async def save_queue_entries(
        self, context: Any, entries: Sequence[ConsolidationQueueRecord]
    ) -> None: ...

    async def delete_queue_entry(self, context: Any, *, entry_id: str) -> None: ...

    async def discard_artifact_work(
        self,
        context: Any,
        *,
        board_id: str,
        artifact_type: str,
        artifact_id: str,
    ) -> None:
        """Discard legacy KG work without committing the caller's unit of work."""
        ...

    async def board_exists(self, context: Any, *, board_id: str) -> bool: ...

    async def list_dlq_auto_drain_board_ids(self, context: Any) -> tuple[str, ...]: ...

    async def count_dead_letters(self, context: Any, *, board_id: str) -> int: ...

    async def delete_poison_dead_letters(
        self, context: Any, *, board_id: str, max_attempts: int
    ) -> tuple[ConsolidationPoisonRow, ...]: ...

    async def commit(self, context: Any) -> None: ...

    async def rollback(self, context: Any) -> None: ...


_RUNTIME_KEY = "ports.consolidation.port"


def register_consolidation_persistence_port(
    port: ConsolidationPersistencePort,
) -> None:
    register_runtime_value(_RUNTIME_KEY, port)


def get_consolidation_persistence_port() -> ConsolidationPersistencePort:
    return require_runtime_value(
        _RUNTIME_KEY, "consolidation_persistence_port_not_configured"
    )


def reset_consolidation_persistence_port_for_tests() -> None:
    reset_runtime_values(_RUNTIME_KEY)


__all__ = [
    "ConsolidationClaimScope",
    "ConsolidationPersistencePort",
    "ConsolidationPoisonRow",
    "ConsolidationProjectionInputs",
    "ConsolidationQueueRecord",
    "CurrentQualityAssessmentSummary",
    "CurrentResearchDecisionSummary",
    "CurrentSpecDependencyProjection",
    "ExactConsolidationBatchResult",
    "ExactConsolidationAckReceipt",
    "ExactConsolidationAckIntegrityError",
    "ExactConsolidationCompensationReceipt",
    "ExactConsolidationCompensationResult",
    "ExactConsolidationCompensationError",
    "ExactConsolidationDisposition",
    "ExactConsolidationMutationState",
    "ExactConsolidationPostCommitError",
    "ExactConsolidationResultOrigin",
    "ExactConsolidationRowResult",
    "exact_consolidation_ack_receipts_sha256",
    "build_exact_consolidation_compensation_binding",
    "validate_exact_consolidation_compensation_binding",
    "get_consolidation_persistence_port",
    "register_consolidation_persistence_port",
    "reset_consolidation_persistence_port_for_tests",
]

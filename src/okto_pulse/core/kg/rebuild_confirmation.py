"""Rebuild confirmation store (KG-02.2 / IR ir_b4b43304 / ir_cf1be9e8).

The confirmation token is the protocol boundary between UI approval
and any destructive KG mutation. The store guarantees:

* ``confirmation_id`` is opaque, single-use, and tied to a precise
  ``(actor_id, board_id, operation, preflight_hash, manifest_ref)``
  scope at issue time (IR ``ir_cf1be9e8`` data contract).
* TTL-based expiration via configurable ``ttl_seconds`` (TR14).
* ``consume`` is atomic: the second consume of the same id returns
  ``replayed``; an expired id returns ``expired``; a scope mismatch
  returns ``scope_mismatch``; missing returns ``missing``.
* Outcomes feed the counter ``kg_rebuild_confirmation_total`` (OR
  ``or_f9b83da6`` / ``or_0752f230``) with bounded labels only —
  raw confirmation_id and actor_id NEVER appear in counter labels.

The store is file-system backed so confirmation survives the process
that issued it (the UI may show the prompt for minutes before the
operator clicks confirm and a worker calls run). Layout:

    <base>/rebuild/confirmations/<confirmation_id>.json
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from okto_pulse.core.runtime_context import runtime_lock, runtime_state

from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    REBUILD_AUDIT_GLOBAL_BOARD_ID,
    RebuildAuditArtifactStore,
    RebuildAuditKey,
)
from okto_pulse.core.kg.rebuild_audit import (
    resolve_rebuild_audit_artifact_store,
)

logger = logging.getLogger("okto_pulse.kg.rebuild_confirmation")


REBUILD_DIRNAME = "rebuild"
CONFIRMATION_DIRNAME = "confirmations"
DEFAULT_CONFIRMATION_TTL_SECONDS = 300  # 5 minutes — fits UI confirm-dialog UX.
MAX_CONFIRMATION_TTL_SECONDS = 3600  # 1 hour — defence against runaway prompts.


class ConfirmationOutcome(str, Enum):
    """Bounded outcomes for OR or_f9b83da6 / or_0752f230 counter."""

    ISSUED = "issued"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    REPLAYED = "replayed"
    SCOPE_MISMATCH = "scope_mismatch"
    MISSING = "missing"


# Canonical operations a confirmation can bind to. Adding a new one is
# explicit (KG-02.4 will add reset/promote/rollback/reindex_discovery).
CANONICAL_OPERATIONS: frozenset[str] = frozenset(
    {
        "rebuild",
        "reset",
        "quarantine",
        "promote",
        "rollback",
        "reindex_discovery",
    }
)


@dataclass(frozen=True, slots=True)
class ConfirmationToken:
    """Issued token. The UI presents ``confirmation_id`` to the operator
    and the worker passes the same id back to ``consume``."""

    confirmation_id: str
    board_id: str
    actor_id: str
    operation: str
    preflight_hash: str
    manifest_ref: str
    issued_at: str
    expires_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "confirmation_id": self.confirmation_id,
            "board_id": self.board_id,
            "actor_id": self.actor_id,
            "operation": self.operation,
            "preflight_hash": self.preflight_hash,
            "manifest_ref": self.manifest_ref,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
        }


@dataclass(frozen=True, slots=True)
class ConfirmationResult:
    """Result of ``consume``. ``token`` is None when the outcome is
    anything other than ``consumed`` — callers MUST refuse to mutate
    when ``token is None``."""

    outcome: str  # ConfirmationOutcome value
    reason: str | None
    token: ConfirmationToken | None


# --- Counter (OR or_f9b83da6 / or_0752f230) -----------------------------------
#
# Labels are bounded: (board_id, operation, outcome, reason, actor_type).
# actor_type is "agent" | "human" | "unknown" — derived from the
# caller; the raw actor_id is NEVER in labels.

_CONF_LABELS = ("board_id", "operation", "outcome", "reason", "actor_type")
_conf_counter = runtime_state("kg.rebuild_confirmation.counter", dict)
_conf_counter_lock = runtime_lock("kg.rebuild_confirmation.counter")


def _bump_conf(
    *,
    board_id: str,
    operation: str,
    outcome: str,
    reason: str | None,
    actor_type: str,
) -> None:
    key = (board_id, operation, outcome, reason or "n/a", actor_type)
    with _conf_counter_lock:
        _conf_counter[key] = _conf_counter.get(key, 0) + 1


def get_confirmation_count(
    board_id: str,
    outcome: str,
    *,
    operation: str | None = None,
    reason: str | None = None,
    actor_type: str | None = None,
) -> int:
    with _conf_counter_lock:
        total = 0
        for (b, op, out, rsn, at), value in _conf_counter.items():
            if b != board_id or out != outcome:
                continue
            if operation is not None and op != operation:
                continue
            if reason is not None and rsn != reason:
                continue
            if actor_type is not None and at != actor_type:
                continue
            total += value
        return total


def get_confirmation_samples() -> list[dict[str, Any]]:
    with _conf_counter_lock:
        return [
            {
                "board_id": b,
                "operation": op,
                "outcome": out,
                "reason": rsn,
                "actor_type": at,
                "count": value,
            }
            for (b, op, out, rsn, at), value in _conf_counter.items()
        ]


def get_confirmation_counter_labels() -> tuple[str, ...]:
    return _CONF_LABELS


def reset_confirmation_counter() -> None:
    with _conf_counter_lock:
        _conf_counter.clear()


# --- Store -------------------------------------------------------------------


def _classify_actor(actor_id: str) -> str:
    """Map an actor_id to the bounded ``actor_type`` label vocabulary.

    Heuristic: agent ids tend to start with ``agent-`` or contain
    the literal ``agent``; everything else is treated as ``human`` if
    non-empty, ``unknown`` otherwise. Production may swap this for a
    proper identity service lookup; for now the bucketing is safe.
    """
    if not actor_id:
        return "unknown"
    lowered = actor_id.lower()
    if lowered.startswith("agent-") or "agent" in lowered.split("-"):
        return "agent"
    return "human"


@dataclass(frozen=True, slots=True)
class RebuildConfirmationStore:
    """Issue + consume tokens with single-use, TTL and scope binding.

    val_302bdec8 wiring: when ``audit_recorder`` is supplied, every
    ``consume(...)`` exit path emits a durable safe audit row via the
    KG-02.7 ``ConfirmationConsumptionAuditRecorder`` (api_c9bc9a8c).
    The raw ``confirmation_id`` is NEVER persisted in the audit — only
    its fingerprint (sha256 hex) is, so an operator can join audit
    rows by token without exposing the secret.
    """

    base_dir: object | None = None
    ttl_seconds: int = DEFAULT_CONFIRMATION_TTL_SECONDS
    # Optional KG-02.7 ``ConfirmationConsumptionAuditRecorder``. Kept
    # optional so existing callers (KG-02.2 endpoint, KG-02.3 service)
    # keep working without wiring; production sets it.
    audit_recorder: Any | None = None
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
        if self.ttl_seconds < 30:
            raise ValueError(f"ttl_seconds={self.ttl_seconds} below safety floor of 30")
        if self.ttl_seconds > MAX_CONFIRMATION_TTL_SECONDS:
            raise ValueError(
                f"ttl_seconds={self.ttl_seconds} exceeds max {MAX_CONFIRMATION_TTL_SECONDS}"
            )

    # --- public API --------------------------------------------------------

    def issue(
        self,
        *,
        board_id: str,
        actor_id: str,
        operation: str,
        preflight_hash: str,
        manifest_ref: str,
    ) -> ConfirmationToken:
        """Mint a fresh single-use token bound to the supplied scope."""
        if operation not in CANONICAL_OPERATIONS:
            raise ValueError(f"unsupported operation: {operation}")
        if not preflight_hash:
            raise ValueError("preflight_hash is required")
        if not manifest_ref:
            raise ValueError("manifest_ref is required")
        issued_at = datetime.now(timezone.utc)
        expires_at = issued_at + timedelta(seconds=self.ttl_seconds)
        for _attempt in range(5):
            token = ConfirmationToken(
                confirmation_id=f"conf_{secrets.token_urlsafe(24)}",
                board_id=board_id,
                actor_id=actor_id,
                operation=operation,
                preflight_hash=preflight_hash,
                manifest_ref=manifest_ref,
                issued_at=issued_at.isoformat(),
                expires_at=expires_at.isoformat(),
            )
            try:
                self._write(token)
                break
            except FileExistsError:
                continue
        else:
            raise RuntimeError("confirmation_id_collision_budget_exhausted")
        _bump_conf(
            board_id=board_id,
            operation=operation,
            outcome=ConfirmationOutcome.ISSUED.value,
            reason=None,
            actor_type=_classify_actor(actor_id),
        )
        logger.info(
            "kg.rebuild_confirmation.issued board=%s actor=%s operation=%s "
            "expires_at=%s",
            board_id,
            actor_id,
            operation,
            token.expires_at,
        )
        return token

    def revoke_unconsumed(
        self,
        *,
        confirmation_id: str,
        expected_board_id: str,
        expected_actor_id: str,
        expected_operation: str,
        expected_preflight_hash: str,
        expected_manifest_ref: str,
    ) -> bool:
        """Atomically revoke one exact unconsumed recovery confirmation.

        The offline executor calls this in failure cleanup between ``issue``
        and ``run``.  Revocation is scope-bound and leaves only a deterministic
        non-secret receipt, so a process crash cannot strand the raw token and
        a retry can prove that cleanup already completed.
        """

        from okto_pulse.core.kg.rebuild_audit import confirmation_fingerprint

        confirmation_ref = confirmation_fingerprint(confirmation_id)
        revocation_key = RebuildAuditKey(
            namespace="confirmation_audit",
            board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
            artifact_id=f"revoked_{confirmation_ref.removeprefix('conf_fp_')}",
        )
        expected_binding = {
            "schema_version": "kg_rebuild_confirmation_revocation.v1",
            "state": "revoked_unconsumed",
            "confirmation_ref": confirmation_ref,
            "board_id": expected_board_id,
            "actor_id": expected_actor_id,
            "operation": expected_operation,
            "preflight_hash": expected_preflight_hash,
            "manifest_ref": expected_manifest_ref,
        }
        existing_revocation = self.artifact_store.read_json(revocation_key)
        if existing_revocation is not None:
            if existing_revocation != expected_binding:
                raise RuntimeError("rebuild_confirmation_revocation_binding_conflict")
            # Complete the receipt-before-unlink crash cut if the source still
            # exists; the atomic adapter recognizes the exact receipt.

        data = self._read(confirmation_id)
        if data is None:
            return existing_revocation == expected_binding
        scope_fields = (
            ("confirmation_id", data.get("confirmation_id"), confirmation_id),
            ("board_id", data.get("board_id"), expected_board_id),
            ("actor_id", data.get("actor_id"), expected_actor_id),
            ("operation", data.get("operation"), expected_operation),
            ("preflight_hash", data.get("preflight_hash"), expected_preflight_hash),
            ("manifest_ref", data.get("manifest_ref"), expected_manifest_ref),
        )
        mismatch = next(
            (name for name, actual, expected in scope_fields if actual != expected),
            None,
        )
        if mismatch is not None:
            raise RuntimeError(
                f"rebuild_confirmation_revocation_scope_mismatch:{mismatch}"
            )
        outcome = self.artifact_store.consume_json_with_receipt(
            source_key=self._key(confirmation_id),
            expected_source=data,
            receipt_key=revocation_key,
            receipt_payload=expected_binding,
        )
        if outcome in {"consumed", "receipt_exists"}:
            return True
        raise RuntimeError(f"rebuild_confirmation_revocation_failed:{outcome}")

    def _record_consumption_audit(
        self,
        *,
        confirmation_id: str,
        expected_board_id: str,
        expected_actor_id: str,
        expected_operation: str,
        outcome: str,
        reason: str | None,
        token: ConfirmationToken | None = None,
    ) -> None:
        """val_302bdec8 — emit a safe audit row for every consume()
        exit path when an ``audit_recorder`` is wired. Uses the SHA256
        fingerprint of the confirmation_id so the raw token never lands
        in the audit JSON. Best-effort: a recorder failure is logged but
        never propagates because the lifecycle outcome itself is what
        the caller depends on."""

        if self.audit_recorder is None:
            return
        try:
            from okto_pulse.core.kg.rebuild_audit import (
                confirmation_fingerprint,
            )

            confirmation_ref = confirmation_fingerprint(confirmation_id)
            actor_type = _classify_actor(expected_actor_id)
            actor_ref = f"actor:{actor_type}"
            generation_ids: dict[str, str | None] = {}
            if token is not None:
                generation_ids = {
                    "manifest_ref": token.manifest_ref,
                }
            self.audit_recorder.record(
                board_id=expected_board_id,
                operation=expected_operation,
                outcome=outcome,
                reason=reason or "n/a",
                actor_ref=actor_ref,
                preflight_hash=token.preflight_hash if token else None,
                generation_ids=generation_ids,
                confirmation_ref=confirmation_ref,
            )
        except Exception as exc:
            logger.error(
                "kg.rebuild_confirmation.audit_recorder_failed "
                "board=%s operation=%s outcome=%s err=%s",
                expected_board_id,
                expected_operation,
                outcome,
                exc,
            )

    def consume(
        self,
        *,
        confirmation_id: str,
        expected_board_id: str,
        expected_actor_id: str,
        expected_operation: str,
        expected_preflight_hash: str,
        expected_manifest_ref: str,
        consumption_receipt_key: RebuildAuditKey | None = None,
        consumption_receipt_payload: Mapping[str, Any] | None = None,
        expected_terminal_receipt: Mapping[str, Any] | None = None,
    ) -> ConfirmationResult:
        """Atomically consume a token if it matches every expected
        scope field. Returns ``ConfirmationResult`` — caller MUST
        refuse to mutate unless ``outcome == 'consumed'``."""
        if (consumption_receipt_key is None) != (consumption_receipt_payload is None):
            raise ValueError(
                "consumption receipt key and payload must be supplied together"
            )
        if expected_terminal_receipt is not None and consumption_receipt_key is None:
            raise ValueError("terminal receipt replacement requires receipt key")
        actor_type = _classify_actor(expected_actor_id)
        # Read first to evaluate scope. Then attempt atomic unlink.
        try:
            data = self._read(confirmation_id)
            if data is None:
                raise FileNotFoundError
        except FileNotFoundError:
            _bump_conf(
                board_id=expected_board_id,
                operation=expected_operation,
                outcome=ConfirmationOutcome.MISSING.value,
                reason="not_found",
                actor_type=actor_type,
            )
            self._record_consumption_audit(
                confirmation_id=confirmation_id,
                expected_board_id=expected_board_id,
                expected_actor_id=expected_actor_id,
                expected_operation=expected_operation,
                outcome=ConfirmationOutcome.MISSING.value,
                reason="not_found",
            )
            return ConfirmationResult(
                outcome=ConfirmationOutcome.MISSING.value,
                reason="not_found",
                token=None,
            )
        except (OSError, ValueError) as exc:
            _bump_conf(
                board_id=expected_board_id,
                operation=expected_operation,
                outcome=ConfirmationOutcome.MISSING.value,
                reason="corrupt",
                actor_type=actor_type,
            )
            logger.warning(
                "kg.rebuild_confirmation.read_failed id_prefix=%s err=%s",
                confirmation_id[:12],
                exc,
            )
            self._record_consumption_audit(
                confirmation_id=confirmation_id,
                expected_board_id=expected_board_id,
                expected_actor_id=expected_actor_id,
                expected_operation=expected_operation,
                outcome=ConfirmationOutcome.MISSING.value,
                reason="corrupt",
            )
            return ConfirmationResult(
                outcome=ConfirmationOutcome.MISSING.value,
                reason="corrupt",
                token=None,
            )

        token = ConfirmationToken(
            confirmation_id=str(data["confirmation_id"]),
            board_id=str(data["board_id"]),
            actor_id=str(data["actor_id"]),
            operation=str(data["operation"]),
            preflight_hash=str(data["preflight_hash"]),
            manifest_ref=str(data["manifest_ref"]),
            issued_at=str(data["issued_at"]),
            expires_at=str(data["expires_at"]),
        )

        # Scope mismatch — record + refuse + leave the token in place
        # so its true owner can still consume it within the TTL.
        scope_fields = (
            ("board_id", token.board_id, expected_board_id),
            ("actor_id", token.actor_id, expected_actor_id),
            ("operation", token.operation, expected_operation),
            ("preflight_hash", token.preflight_hash, expected_preflight_hash),
            ("manifest_ref", token.manifest_ref, expected_manifest_ref),
        )
        for name, actual, expected in scope_fields:
            if actual != expected:
                _bump_conf(
                    board_id=expected_board_id,
                    operation=expected_operation,
                    outcome=ConfirmationOutcome.SCOPE_MISMATCH.value,
                    reason=name,
                    actor_type=actor_type,
                )
                logger.warning(
                    "kg.rebuild_confirmation.scope_mismatch board=%s "
                    "operation=%s field=%s",
                    expected_board_id,
                    expected_operation,
                    name,
                )
                self._record_consumption_audit(
                    confirmation_id=confirmation_id,
                    expected_board_id=expected_board_id,
                    expected_actor_id=expected_actor_id,
                    expected_operation=expected_operation,
                    outcome=ConfirmationOutcome.SCOPE_MISMATCH.value,
                    reason=name,
                    token=token,
                )
                return ConfirmationResult(
                    outcome=ConfirmationOutcome.SCOPE_MISMATCH.value,
                    reason=name,
                    token=None,
                )

        # TTL — expired tokens are unlinked so future attempts return
        # missing rather than expired indefinitely.
        try:
            expires = datetime.fromisoformat(token.expires_at)
        except ValueError:
            expires = datetime.now(timezone.utc) - timedelta(seconds=1)
        if expires <= datetime.now(timezone.utc):
            self._delete(confirmation_id)
            _bump_conf(
                board_id=expected_board_id,
                operation=expected_operation,
                outcome=ConfirmationOutcome.EXPIRED.value,
                reason="ttl_exceeded",
                actor_type=actor_type,
            )
            self._record_consumption_audit(
                confirmation_id=confirmation_id,
                expected_board_id=expected_board_id,
                expected_actor_id=expected_actor_id,
                expected_operation=expected_operation,
                outcome=ConfirmationOutcome.EXPIRED.value,
                reason="ttl_exceeded",
                token=token,
            )
            return ConfirmationResult(
                outcome=ConfirmationOutcome.EXPIRED.value,
                reason="ttl_exceeded",
                token=None,
            )

        # The recovery path creates a durable authorization receipt before
        # deleting the token.  Generic rebuild callers retain the historical
        # unlink-only behavior.  Both operations are edition-serialized.
        atomic_outcome = "consumed"
        if consumption_receipt_key is not None:
            if expected_terminal_receipt is not None:
                atomic_outcome = (
                    self.artifact_store.consume_json_replacing_terminal_receipt(
                        source_key=self._key(confirmation_id),
                        expected_source=data,
                        receipt_key=consumption_receipt_key,
                        expected_terminal_receipt=expected_terminal_receipt,
                        receipt_payload=consumption_receipt_payload or {},
                    )
                )
            else:
                atomic_outcome = self.artifact_store.consume_json_with_receipt(
                    source_key=self._key(confirmation_id),
                    expected_source=data,
                    receipt_key=consumption_receipt_key,
                    receipt_payload=consumption_receipt_payload or {},
                )
        elif not self._delete(confirmation_id):
            atomic_outcome = "source_missing"
        if atomic_outcome != "consumed":
            replay_reason = {
                "receipt_exists": "authorization_already_recorded",
                "source_missing": "already_consumed",
                "source_mismatch": "token_changed_during_consume",
                "receipt_conflict": "authorization_receipt_conflict",
            }.get(atomic_outcome, "already_consumed")
            _bump_conf(
                board_id=expected_board_id,
                operation=expected_operation,
                outcome=ConfirmationOutcome.REPLAYED.value,
                reason=replay_reason,
                actor_type=actor_type,
            )
            self._record_consumption_audit(
                confirmation_id=confirmation_id,
                expected_board_id=expected_board_id,
                expected_actor_id=expected_actor_id,
                expected_operation=expected_operation,
                outcome=ConfirmationOutcome.REPLAYED.value,
                reason=replay_reason,
                token=token,
            )
            return ConfirmationResult(
                outcome=ConfirmationOutcome.REPLAYED.value,
                reason=replay_reason,
                token=None,
            )

        _bump_conf(
            board_id=expected_board_id,
            operation=expected_operation,
            outcome=ConfirmationOutcome.CONSUMED.value,
            reason=None,
            actor_type=actor_type,
        )
        logger.info(
            "kg.rebuild_confirmation.consumed board=%s actor=%s operation=%s",
            expected_board_id,
            expected_actor_id,
            expected_operation,
        )
        self._record_consumption_audit(
            confirmation_id=confirmation_id,
            expected_board_id=expected_board_id,
            expected_actor_id=expected_actor_id,
            expected_operation=expected_operation,
            outcome=ConfirmationOutcome.CONSUMED.value,
            reason=None,
            token=token,
        )
        return ConfirmationResult(
            outcome=ConfirmationOutcome.CONSUMED.value,
            reason=None,
            token=token,
        )

    # --- internals ---------------------------------------------------------

    @staticmethod
    def _key(confirmation_id: str) -> RebuildAuditKey:
        return RebuildAuditKey(
            namespace="confirmation_token",
            board_id=REBUILD_AUDIT_GLOBAL_BOARD_ID,
            artifact_id=confirmation_id,
        )

    def _read(self, confirmation_id: str) -> dict[str, Any] | None:
        return self.artifact_store.read_json(self._key(confirmation_id))

    def _delete(self, confirmation_id: str) -> bool:
        return self.artifact_store.delete_json(self._key(confirmation_id))

    def _write(self, token: ConfirmationToken) -> None:
        collided = False

        def create_only(current: dict[str, Any] | None) -> dict[str, Any]:
            nonlocal collided
            if current is not None:
                collided = True
                return dict(current)
            return token.to_dict()

        self.artifact_store.replace_json(self._key(token.confirmation_id), create_only)
        if collided:
            raise FileExistsError(token.confirmation_id)


__all__ = [
    "CANONICAL_OPERATIONS",
    "CONFIRMATION_DIRNAME",
    "ConfirmationOutcome",
    "ConfirmationResult",
    "ConfirmationToken",
    "DEFAULT_CONFIRMATION_TTL_SECONDS",
    "MAX_CONFIRMATION_TTL_SECONDS",
    "RebuildConfirmationStore",
    "get_confirmation_count",
    "get_confirmation_counter_labels",
    "get_confirmation_samples",
    "reset_confirmation_counter",
]

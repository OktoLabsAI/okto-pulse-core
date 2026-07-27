"""Public cross-edition contracts for durable Global Discovery recovery.

The policy implementation remains Core-owned. Editions import these names
through the public ``core.ports`` surface instead of reaching into Core KG
implementation modules.
"""

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime

from okto_pulse.core.kg.global_discovery_recovery import (
    GlobalDiscoveryBoardInventory,
    GlobalDiscoveryRecoveryError,
    GlobalDiscoveryRecoveryManifestStore as _GlobalDiscoveryRecoveryManifestStore,
    GlobalDiscoveryRecoveryPreparedInputStore,
    GlobalDiscoveryRecoveryPreparedInputs,
    GlobalDiscoveryRecoveryPreparedRevocation,
    GlobalDiscoveryRecoveryService as _GlobalDiscoveryRecoveryService,
    GlobalDiscoveryRecoveryWorkerInputStore,
    GlobalDiscoveryRecoveryWorkerInputs,
    empty_global_discovery_recovery_board_seed,
    global_discovery_recovery_snapshot_fingerprint,
    inventory_from_source_rows,
)
from okto_pulse.core.kg.interfaces.global_discovery_recovery import (
    GlobalDiscoveryBoardSeed,
    GlobalDiscoveryRecovery,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import (
    RebuildAuditArtifactStore,
)
from okto_pulse.core.kg.rebuild_audit import (
    CognitivePendingOverlaySnapshot,
    CognitivePendingOverlaySnapshotError,
    CognitivePendingOverlaySnapshotService,
)
from okto_pulse.core.kg.global_discovery_writer import (
    GlobalDiscoveryWriterContention,
    GlobalDiscoveryWriterFenceLost,
    GlobalDiscoveryWriterLease,
    assert_global_discovery_writer_fence,
)

from okto_pulse.core.kg.global_discovery_recovery_control import (
    DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS,
    GLOBAL_RECOVERY_SLOT_ID,
    GLOBAL_RECOVERY_STATUS_TOOL,
    MAX_RECOVERY_CUMULATIVE_BUDGET_MS,
    RECOVERY_HEARTBEAT_INTERVAL_MS,
    RECOVERY_PREPARED_TTL_SECONDS,
    RECOVERY_WORKER_LEASE_MS,
    RecoveryAttempt,
    RecoveryAuditReasonInvalid,
    RecoveryBindingConflict,
    RecoveryCheckpoint,
    RecoveryCheckpointConflict,
    RecoveryControlPlane,
    RecoveryControlPlaneUnavailable,
    RecoveryConfirmationState,
    RecoveryDispatchClaimConflict,
    RecoveryDispatchKind,
    RecoveryExplicitResumeStore,
    RecoveryInProgress,
    RecoveryLeaseTakeoverDecision,
    RecoveryLeaseTakeoverPolicy,
    RecoveryProgressCounts,
    RecoveryProgressInvariantViolation,
    RecoveryPhaseIncoherent,
    RecoveryPhysicalTruth,
    RecoveryPreparationCommand,
    RecoveryPreparationStore,
    RecoveryPreparedResult,
    RecoveryResumeDecision,
    RecoveryResumePolicy,
    RecoveryResumeRejected,
    RecoveryRunBinding,
    RecoveryRunNotFound,
    RecoveryRunState,
    RecoveryRunPhase,
    RecoveryRunStatus,
    RecoveryRunStore,
    RecoveryStartCommand,
    RecoverySlotOwnership,
    RecoveryTerminalAttempt,
    RecoveryTerminalOutcome,
    RecoveryWorkerDispatcher,
    RecoveryWorkerResult,
    RecoveryWorkerRunStore,
    RecoveryTransitionEvent,
    RecoveryTransitionObserver,
    normalize_recovery_audit_reason,
    recovery_attempt_id,
    register_recovery_control_plane,
    reset_recovery_control_plane,
    resolve_recovery_control_plane,
)
from okto_pulse.core.kg.rebuild_sources import cognitive_durable_digest_from_rows


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryRecoveryBoardSeedInput:
    """Immutable handoff from the relational capture to graph materialization."""

    board_id: str
    board_name: str
    board_summary: str
    overlay_exclusions: tuple[tuple[str, str], ...]

    def __post_init__(self) -> None:
        board_id = str(self.board_id).strip()
        if not board_id:
            raise ValueError("board_id is required")
        exclusions = tuple(
            (str(artifact_id), str(reason))
            for artifact_id, reason in self.overlay_exclusions
        )
        if any(not artifact_id or not reason for artifact_id, reason in exclusions):
            raise ValueError("overlay exclusions must be non-empty")
        if len({artifact_id for artifact_id, _reason in exclusions}) != len(
            exclusions
        ):
            raise ValueError("overlay exclusions must be unique by artifact")
        object.__setattr__(self, "board_id", board_id)
        object.__setattr__(self, "board_name", str(self.board_name))
        object.__setattr__(self, "board_summary", str(self.board_summary))
        object.__setattr__(self, "overlay_exclusions", tuple(sorted(exclusions)))


class GlobalDiscoveryRecoveryBoardSeedInputService:
    """UoW-bound capture of the relational half of one recovery seed."""

    async def capture_board_seed_input(
        self,
        db: object,
        *,
        board_id: str,
        board_name: str,
        board_summary: str,
        captured_cognitive_pending_exclusions: Mapping[str, str],
    ) -> GlobalDiscoveryRecoveryBoardSeedInput:
        """Capture every relational overlay required by the graph-only phase."""

        from okto_pulse.core.kg.canonical_partition_integrity import (
            canonical_debt_exclusions,
        )
        from okto_pulse.core.kg.connectivity_guard import (
            CANONICAL_LEARNING_WORKING_ONLY_REASON,
        )

        overlay = {
            str(artifact_id): str(reason)
            for artifact_id, reason in captured_cognitive_pending_exclusions.items()
        }
        if any(
            not artifact_id
            or reason != CANONICAL_LEARNING_WORKING_ONLY_REASON
            for artifact_id, reason in overlay.items()
        ):
            raise ValueError("captured cognitive pending exclusions are invalid")

        # Debt outranks a captured cognitive hold for the same artifact.  This
        # is the final relational read; no session/context escapes in the DTO.
        overlay.update(await canonical_debt_exclusions(db, board_id=board_id))
        return GlobalDiscoveryRecoveryBoardSeedInput(
            board_id=str(board_id),
            board_name=str(board_name),
            board_summary=str(board_summary),
            overlay_exclusions=tuple(sorted(overlay.items())),
        )


class GlobalDiscoveryRecoveryBoardSeedService:
    """Graph-only recovery projection facade with no relational capability."""

    def __init__(self, *, blocking_execution: object | None = None) -> None:
        from okto_pulse.core.application.processors.global_outbox import (
            GlobalOutboxProcessor,
        )

        self.__processor = GlobalOutboxProcessor(
            blocking_execution=blocking_execution,
        )

    async def build_board_seed(
        self,
        seed_input: GlobalDiscoveryRecoveryBoardSeedInput,
    ) -> GlobalDiscoveryBoardSeed:
        """Materialize one seed without retaining a relational capability."""

        if not isinstance(seed_input, GlobalDiscoveryRecoveryBoardSeedInput):
            raise TypeError(
                "seed_input must be GlobalDiscoveryRecoveryBoardSeedInput"
            )
        return await self.__processor.build_recovery_board_seed(
            board_id=seed_input.board_id,
            board_name=seed_input.board_name,
            board_summary=seed_input.board_summary,
            captured_overlay_exclusions=dict(seed_input.overlay_exclusions),
        )


class GlobalDiscoveryRecoveryPreparationService:
    """Narrow edition facade for fenced PREPARATION artifact publication.

    The legacy Core service also owns direct preflight/run compatibility
    methods.  Editions must not receive those methods because physical work is
    admitted only through the durable Community slot and dispatch lanes.
    """

    def __init__(
        self,
        *,
        recovery: GlobalDiscoveryRecovery,
        artifact_store: RebuildAuditArtifactStore,
    ) -> None:
        self.__service = _GlobalDiscoveryRecoveryService(
            recovery=recovery,
            artifact_store=artifact_store,
        )

    def stage_prepared_inputs(
        self,
        *,
        run_id: str,
        epoch: int,
        actor_id: str,
        boards: Iterable[GlobalDiscoveryBoardInventory],
        health_evidence: Iterable[dict[str, object]],
        candidate_boards: Iterable[GlobalDiscoveryBoardSeed],
        expected_snapshot_fingerprint: str,
        fence_check: Callable[[], None],
        prepared_at: datetime | None = None,
        terminal_counts: RecoveryProgressCounts | None = None,
    ) -> RecoveryPreparedResult:
        return self.__service.stage_prepared_inputs(
            run_id=run_id,
            epoch=epoch,
            actor_id=actor_id,
            boards=boards,
            health_evidence=health_evidence,
            candidate_boards=candidate_boards,
            expected_snapshot_fingerprint=expected_snapshot_fingerprint,
            fence_check=fence_check,
            prepared_at=prepared_at,
            terminal_counts=terminal_counts,
        )


class GlobalDiscoveryPreparedRevocationService:
    """Narrow create-only revocation boundary for edition coordination."""

    def __init__(self, *, artifact_store: RebuildAuditArtifactStore) -> None:
        self.__manifests = _GlobalDiscoveryRecoveryManifestStore(artifact_store)

    def revoke_prepared(
        self,
        *,
        run_id: str,
        epoch: int,
        manifest_ref: str,
        revoked_at: datetime,
        requested_by_actor_id: str,
        reason: str | None,
    ) -> GlobalDiscoveryRecoveryPreparedRevocation:
        return self.__manifests.revoke_prepared(
            run_id=run_id,
            epoch=epoch,
            manifest_ref=manifest_ref,
            revoked_at=revoked_at,
            requested_by_actor_id=requested_by_actor_id,
            reason=reason,
        )

    def resolve_attempt_manifest_ref(
        self,
        *,
        run_id: str,
        epoch: int,
    ) -> str | None:
        """Resolve a create-only manifest from its durable attempt binding."""

        manifest = self.__manifests.load_attempt_manifest(
            str(run_id),
            epoch=int(epoch),
        )
        return None if manifest is None else manifest.manifest_ref

    def is_prepared_revoked(
        self,
        *,
        run_id: str,
        epoch: int,
        manifest_ref: str,
    ) -> bool:
        evidence = self.__manifests.read_prepared_revocation(
            run_id=run_id,
            epoch=epoch,
        )
        if evidence is None:
            return False
        if (
            evidence.run_id != str(run_id)
            or evidence.epoch != int(epoch)
            or evidence.manifest_ref != str(manifest_ref)
        ):
            raise GlobalDiscoveryRecoveryError(
                "prepared_revocation_invalid",
                "prepared revocation evidence does not match its attempt binding",
            )
        return True


def resolve_global_discovery_recovery_runtime_dependencies() -> tuple[
    GlobalDiscoveryRecovery,
    RebuildAuditArtifactStore,
]:
    """Resolve only the two edition providers needed by recovery composition."""

    from okto_pulse.core.kg.interfaces import get_kg_registry

    registry = get_kg_registry()
    return (
        registry.require_global_discovery_recovery(),
        registry.require_rebuild_audit_artifact_store(),
    )


__all__ = [
    "DEFAULT_RECOVERY_ATTEMPT_BUDGET_MS",
    "GLOBAL_RECOVERY_SLOT_ID",
    "GLOBAL_RECOVERY_STATUS_TOOL",
    "MAX_RECOVERY_CUMULATIVE_BUDGET_MS",
    "RECOVERY_HEARTBEAT_INTERVAL_MS",
    "RECOVERY_PREPARED_TTL_SECONDS",
    "RECOVERY_WORKER_LEASE_MS",
    "GlobalDiscoveryBoardInventory",
    "GlobalDiscoveryRecoveryBoardSeedInput",
    "GlobalDiscoveryRecoveryBoardSeedInputService",
    "GlobalDiscoveryRecoveryBoardSeedService",
    "GlobalDiscoveryRecoveryWorkerInputStore",
    "GlobalDiscoveryRecoveryWorkerInputs",
    "GlobalDiscoveryWriterLease",
    "GlobalDiscoveryWriterContention",
    "GlobalDiscoveryWriterFenceLost",
    "assert_global_discovery_writer_fence",
    "CognitivePendingOverlaySnapshot",
    "CognitivePendingOverlaySnapshotError",
    "CognitivePendingOverlaySnapshotService",
    "GlobalDiscoveryPreparedRevocationService",
    "GlobalDiscoveryRecoveryPreparedInputStore",
    "GlobalDiscoveryRecoveryPreparedInputs",
    "GlobalDiscoveryRecoveryPreparedRevocation",
    "GlobalDiscoveryRecoveryPreparationService",
    "GlobalDiscoveryRecoveryError",
    "RecoveryAttempt",
    "RecoveryAuditReasonInvalid",
    "RecoveryBindingConflict",
    "RecoveryCheckpoint",
    "RecoveryCheckpointConflict",
    "RecoveryControlPlane",
    "RecoveryControlPlaneUnavailable",
    "RecoveryConfirmationState",
    "RecoveryDispatchClaimConflict",
    "RecoveryDispatchKind",
    "RecoveryExplicitResumeStore",
    "RecoveryInProgress",
    "RecoveryLeaseTakeoverDecision",
    "RecoveryLeaseTakeoverPolicy",
    "RecoveryProgressCounts",
    "RecoveryProgressInvariantViolation",
    "RecoveryPhaseIncoherent",
    "RecoveryPhysicalTruth",
    "RecoveryPreparationCommand",
    "RecoveryPreparationStore",
    "RecoveryPreparedResult",
    "RecoveryResumeDecision",
    "RecoveryResumePolicy",
    "RecoveryResumeRejected",
    "RecoveryRunBinding",
    "RecoveryRunNotFound",
    "RecoveryRunState",
    "RecoveryRunPhase",
    "RecoveryRunStatus",
    "RecoveryRunStore",
    "RecoveryStartCommand",
    "RecoverySlotOwnership",
    "RecoveryTerminalAttempt",
    "RecoveryTerminalOutcome",
    "RecoveryWorkerDispatcher",
    "RecoveryWorkerResult",
    "RecoveryWorkerRunStore",
    "RecoveryTransitionEvent",
    "RecoveryTransitionObserver",
    "normalize_recovery_audit_reason",
    "cognitive_durable_digest_from_rows",
    "empty_global_discovery_recovery_board_seed",
    "global_discovery_recovery_snapshot_fingerprint",
    "inventory_from_source_rows",
    "recovery_attempt_id",
    "register_recovery_control_plane",
    "resolve_global_discovery_recovery_runtime_dependencies",
    "reset_recovery_control_plane",
    "resolve_recovery_control_plane",
]

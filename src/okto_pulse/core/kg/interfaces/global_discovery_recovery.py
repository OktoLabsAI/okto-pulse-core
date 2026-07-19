"""Edition boundary for replacing an unreadable Global Discovery store.

The Core owns approval, manifests and recovery policy.  An edition owns the
physical artifact and is the only layer allowed to inspect, quarantine or
atomically replace it.  Implementations must fingerprint the live artifact
without opening it: this port is specifically used when opening that artifact
can fail with corruption or ``MemoryError``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryArtifactSnapshot:
    exists: bool
    artifact_count: int
    total_bytes: int
    sha256: str

    def to_dict(self) -> dict[str, object]:
        return {
            "exists": self.exists,
            "artifact_count": self.artifact_count,
            "total_bytes": self.total_bytes,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryDigestSeed:
    """Semantic input for one rebuilt digest.

    ``source_artifact_ref`` participates in layer policy and the candidate
    fingerprint.  The current DecisionDigest projection intentionally has no
    provenance column, so editions must not silently invent one.
    """

    original_node_id: str
    title: str
    summary: str
    node_type: str
    graph_layer: str
    source_artifact_ref: str
    embedding: tuple[float, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "original_node_id": self.original_node_id,
            "title": self.title,
            "summary": self.summary,
            "node_type": self.node_type,
            "graph_layer": self.graph_layer,
            "source_artifact_ref": self.source_artifact_ref,
            "embedding": list(self.embedding),
        }


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryBoardSeed:
    """Complete authoritative projection used to materialize one board."""

    board_id: str
    board_name: str
    summary: str
    summary_embedding: tuple[float, ...]
    digests: tuple[GlobalDiscoveryDigestSeed, ...]
    source_inventory_hash: str

    def to_dict(self) -> dict[str, object]:
        return {
            "board_id": self.board_id,
            "board_name": self.board_name,
            "summary": self.summary,
            "summary_embedding": list(self.summary_embedding),
            "digests": [row.to_dict() for row in self.digests],
            "source_inventory_hash": self.source_inventory_hash,
        }


@dataclass(frozen=True, slots=True)
class GlobalDiscoveryCutoverResult:
    outcome: str
    candidate_sha256: str
    quarantine_ref: str | None
    schema_object_count: int
    rollback_performed: bool = False
    failure_code: str | None = None
    directory_fsync_supported: bool = False
    cutover_atomicity: str = "unknown"
    recovery_journal_ref: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "candidate_sha256": self.candidate_sha256,
            "quarantine_ref": self.quarantine_ref,
            "schema_object_count": self.schema_object_count,
            "rollback_performed": self.rollback_performed,
            "failure_code": self.failure_code,
            "directory_fsync_supported": self.directory_fsync_supported,
            "cutover_atomicity": self.cutover_atomicity,
            "recovery_journal_ref": self.recovery_journal_ref,
        }


@runtime_checkable
class GlobalDiscoveryRecovery(Protocol):
    """Physical, edition-owned half of the governed recovery operation."""

    def inspect_live_artifact(self) -> GlobalDiscoveryArtifactSnapshot:
        """Fingerprint live + sidecars without opening the graph."""
        ...

    def rebuild_candidate_and_cutover(
        self,
        *,
        run_id: str,
        epoch: int,
        attempt_id: str,
        expected_live_sha256: str,
        boards: tuple[GlobalDiscoveryBoardSeed, ...],
        fence_check: Callable[[], None],
    ) -> GlobalDiscoveryCutoverResult:
        """Materialize/validate a new store, switch atomically and read back."""
        ...

    def recover_and_cutover(
        self,
        *,
        run_id: str,
        epoch: int,
        attempt_id: str,
        expected_live_sha256: str,
        boards: tuple[GlobalDiscoveryBoardSeed, ...],
        fence_check: Callable[[], None],
    ) -> GlobalDiscoveryCutoverResult:
        """Unified recovery entry: adopt a validated complete live primary
        (preserving its exact content) or fall back to authoritative-seed
        rebuild.  Same run/epoch/attempt/expected-SHA/boards/fence contract as
        ``rebuild_candidate_and_cutover``; this is the method the production
        worker invokes."""
        ...

    def current_snapshot_fingerprint(self) -> str:
        """Read the required authoritative source fence without artifact fallback."""
        ...


__all__ = [
    "GlobalDiscoveryArtifactSnapshot",
    "GlobalDiscoveryBoardSeed",
    "GlobalDiscoveryCutoverResult",
    "GlobalDiscoveryDigestSeed",
    "GlobalDiscoveryRecovery",
]

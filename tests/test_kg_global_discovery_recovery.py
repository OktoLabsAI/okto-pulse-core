from __future__ import annotations

import asyncio
import hashlib
import threading
from datetime import datetime, timedelta, timezone

import pytest

from memory_rebuild_audit_storage import InMemoryRebuildAuditArtifactStore
from okto_pulse.core.kg.blocking_io import run_blocking_graph_io
from okto_pulse.core.kg.global_discovery_recovery import (
    GLOBAL_RECOVERY_SCOPE,
    GlobalDiscoveryBoardInventory,
    GlobalDiscoveryRecoveryError,
    GlobalDiscoveryRecoveryManifestStore,
    GlobalDiscoveryRecoveryPreparedInputStore,
    GlobalDiscoveryRecoveryPreparedInputs,
    GlobalDiscoveryRecoveryService,
    GlobalDiscoveryRecoveryWorkerInputStore,
    GlobalDiscoveryRecoveryWorkerInputs,
    empty_global_discovery_recovery_board_seed,
    inventory_from_source_rows,
)
from okto_pulse.core.kg.global_discovery_recovery_control import (
    RECOVERY_PREPARED_TTL_SECONDS,
    RecoveryProgressCounts,
    RecoveryRunBinding,
    RecoveryStartCommand,
    recovery_attempt_id,
)
from okto_pulse.core.kg.interfaces.global_discovery_recovery import (
    GlobalDiscoveryArtifactSnapshot,
    GlobalDiscoveryBoardSeed,
    GlobalDiscoveryCutoverResult,
    GlobalDiscoveryDigestSeed,
    GlobalDiscoveryRecovery,
)
from okto_pulse.core.kg.interfaces.rebuild_audit_storage import RebuildAuditKey
from okto_pulse.core.kg.rebuild_sources import cognitive_durable_digest_from_rows
from okto_pulse.core.ports.global_discovery_recovery_control import (
    GlobalDiscoveryRecoveryBoardSeedService,
    GlobalDiscoveryRecoveryPreparationService,
)
from okto_pulse.core.kg.single_writer_lock import LockAcquisition
from okto_pulse.core.kg.write_barrier import has_active_global_guard


class _Lock:
    def __init__(self) -> None:
        self.released = False
        self._mutex = threading.Lock()

    def acquire(self, **_kwargs) -> LockAcquisition:
        if not self._mutex.acquire(blocking=False):
            return LockAcquisition(False, None, None, "current-owner", True)
        self.released = False
        return LockAcquisition(True, "owner-token", None, None, True)

    def release(self, **_kwargs) -> bool:
        if not self._mutex.locked():
            return False
        self.released = True
        self._mutex.release()
        return True

    def is_owner(self, _board_id: str, owner_token: str) -> bool:
        return (
            self._mutex.locked() and not self.released and owner_token == "owner-token"
        )


class _Recovery:
    def __init__(
        self,
        *,
        entered: threading.Event | None = None,
        outcome: str = "completed",
    ) -> None:
        self.inspect_calls = 0
        self.run_calls = 0
        self.entered = entered
        self.outcome = outcome
        self.release = threading.Event()
        if entered is None:
            self.release.set()

    def inspect_live_artifact(self) -> GlobalDiscoveryArtifactSnapshot:
        self.inspect_calls += 1
        return GlobalDiscoveryArtifactSnapshot(True, 2, 42, "live-sha")

    def current_snapshot_fingerprint(self) -> str:
        return "authoritative-source-fingerprint"

    def rebuild_candidate_and_cutover(
        self,
        *,
        run_id: str,
        epoch: int,
        attempt_id: str,
        expected_live_sha256: str,
        boards,
        fence_check,
    ) -> GlobalDiscoveryCutoverResult:
        self.run_calls += 1
        assert attempt_id == recovery_attempt_id(run_id, epoch)
        assert expected_live_sha256 == "live-sha"
        assert [row.board_id for row in boards] == ["a", "b"]
        assert has_active_global_guard()
        fence_check()
        if self.entered is not None:
            self.entered.set()
        self.release.wait(timeout=5)
        return GlobalDiscoveryCutoverResult(
            self.outcome,
            "candidate-sha",
            "quarantine:1",
            3,
            rollback_performed=self.outcome == "rolled_back",
            failure_code=(
                "global_discovery_post_cutover_readback_failed"
                if self.outcome == "rolled_back"
                else None
            ),
        )

    def recover_and_cutover(
        self,
        *,
        run_id: str,
        epoch: int,
        attempt_id: str,
        expected_live_sha256: str,
        boards,
        fence_check,
    ) -> GlobalDiscoveryCutoverResult:
        # Deliberate Protocol-parity migration: the unified recovery entry.  This
        # Core fake models a provider whose live primary is never adoptable, so
        # it delegates to the authoritative seed-rebuild path under the exact
        # same run_id/epoch/attempt/fence/authority contract.  Subclasses that
        # override ``rebuild_candidate_and_cutover`` (fail-once / always-fail)
        # are honoured unchanged because the delegation dispatches on ``self``.
        return self.rebuild_candidate_and_cutover(
            run_id=run_id,
            epoch=epoch,
            attempt_id=attempt_id,
            expected_live_sha256=expected_live_sha256,
            boards=boards,
            fence_check=fence_check,
        )


class _RecoveryWithoutAuthoritativeFingerprint(_Recovery):
    current_snapshot_fingerprint = None


def test_core_recovery_fake_satisfies_unified_protocol_and_delegates():
    """R1: the migrated Core recovery fake satisfies the runtime_checkable
    ``GlobalDiscoveryRecovery`` Protocol INCLUDING ``recover_and_cutover``, and
    the unified entry dispatches to ``rebuild_candidate_and_cutover`` on
    ``self`` so subclassed rebuild behaviour is honoured.  Non-tautological: a
    subclass override of rebuild is observed to receive the exact forwarded
    kwargs, and an old-only provider is still rejected."""

    assert isinstance(_Recovery(), GlobalDiscoveryRecovery)
    assert "recover_and_cutover" in GlobalDiscoveryRecovery.__protocol_attrs__
    assert (
        "rebuild_candidate_and_cutover" in GlobalDiscoveryRecovery.__protocol_attrs__
    )

    seen: dict = {}

    class _DelegationProbe(_Recovery):
        def rebuild_candidate_and_cutover(self, **kwargs):
            seen.update(kwargs)
            return GlobalDiscoveryCutoverResult(
                "completed",
                "delegated-sha",
                "quarantine:delegated",
                5,
                rollback_performed=False,
                failure_code=None,
            )

    probe = _DelegationProbe()
    assert isinstance(probe, GlobalDiscoveryRecovery)
    boards = _boards()
    fence = lambda: None  # noqa: E731
    result = probe.recover_and_cutover(
        run_id="gdr_core_delegation",
        epoch=4,
        attempt_id=recovery_attempt_id("gdr_core_delegation", 4),
        expected_live_sha256="live-sha",
        boards=boards,
        fence_check=fence,
    )
    # The unified entry forwarded EXACTLY to rebuild on ``self``.
    assert result.outcome == "completed"
    assert result.candidate_sha256 == "delegated-sha"
    assert result.quarantine_ref == "quarantine:delegated"
    assert seen["run_id"] == "gdr_core_delegation"
    assert seen["epoch"] == 4
    assert seen["attempt_id"] == recovery_attempt_id("gdr_core_delegation", 4)
    assert seen["expected_live_sha256"] == "live-sha"
    assert seen["boards"] is boards
    assert seen["fence_check"] is fence

    # The explicit OLD-ONLY negative provider (lacking recover_and_cutover) must
    # still be REJECTED by the runtime Protocol — parity migration did not erase
    # the negative contract that the Community runtime Protocol regression relies
    # on.
    class _OldOnlyProvider:
        def inspect_live_artifact(self):  # pragma: no cover - structural only
            raise NotImplementedError

        def current_snapshot_fingerprint(self):  # pragma: no cover
            raise NotImplementedError

        def rebuild_candidate_and_cutover(self, **_kwargs):  # pragma: no cover
            raise NotImplementedError

    assert not isinstance(_OldOnlyProvider(), GlobalDiscoveryRecovery)


class _MutableFingerprintRecovery(_Recovery):
    def __init__(self, fingerprints: list[str]) -> None:
        super().__init__()
        self._fingerprints = list(fingerprints)

    def current_snapshot_fingerprint(self) -> str:
        if len(self._fingerprints) > 1:
            return self._fingerprints.pop(0)
        return self._fingerprints[0]


def _boards(*, drift: bool = False):
    return (
        GlobalDiscoveryBoardInventory("b", "Beta", 2, "b-hash"),
        GlobalDiscoveryBoardInventory(
            "a", "Alpha", 1, "changed" if drift else "a-hash"
        ),
    )


def _health(*, discovery_state: str = "recovery_needed"):
    return tuple(
        {
            "board_id": board_id,
            "graph_state": "healthy",
            "discovery_state": discovery_state,
            "discovery_recovery_required": discovery_state == "recovery_needed",
            "primary_health_cause": (
                "discovery_recovery_required"
                if discovery_state == "recovery_needed"
                else "healthy"
            ),
        }
        for board_id in ("a", "b")
    )


def _seeds():
    digest = GlobalDiscoveryDigestSeed(
        original_node_id="node-1",
        title="Decision",
        summary="Decision",
        node_type="Decision",
        graph_layer="canonical",
        source_artifact_ref="artifact-1",
        embedding=(0.1, 0.2),
    )
    return (
        GlobalDiscoveryBoardSeed(
            "b", "Beta", "Beta summary", (0.3, 0.4), (digest,), "seed-b"
        ),
        GlobalDiscoveryBoardSeed(
            "a", "Alpha", "Alpha summary", (0.3, 0.4), (digest,), "seed-a"
        ),
    )


def test_current_snapshot_fingerprint_fails_closed_without_authoritative_reader() -> (
    None
):
    recovery = _RecoveryWithoutAuthoritativeFingerprint()
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=InMemoryRebuildAuditArtifactStore(),
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        service.current_snapshot_fingerprint()

    assert exc_info.value.code == "snapshot_fingerprint_unavailable"
    assert recovery.inspect_calls == 0


def test_public_preparation_facade_cannot_reach_legacy_preflight_or_run() -> None:
    facade = GlobalDiscoveryRecoveryPreparationService(
        recovery=_Recovery(),
        artifact_store=InMemoryRebuildAuditArtifactStore(),
    )

    assert not hasattr(facade, "preflight")
    assert not hasattr(facade, "confirm")
    assert not hasattr(facade, "run")

    seed_facade = GlobalDiscoveryRecoveryBoardSeedService()
    assert hasattr(seed_facade, "build_board_seed")
    assert not hasattr(seed_facade, "process_once")


def test_empty_recovery_seed_uses_canonical_zero_source_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _EmbeddingProvider:
        def encode(self, text: str) -> list[float]:
            assert text == "Board Empty Board"
            return [0.25, 0.75]

    monkeypatch.setattr(
        "okto_pulse.core.kg.embedding.get_embedding_provider",
        lambda: _EmbeddingProvider(),
    )

    seed = empty_global_discovery_recovery_board_seed(
        board_id="board-empty",
        board_name="Empty Board",
        board_summary="No materialized sources yet",
    )

    assert seed.board_id == "board-empty"
    assert seed.summary_embedding == (0.25, 0.75)
    assert seed.digests == ()
    assert (
        seed.source_inventory_hash
        == hashlib.sha256(b'{"digests":[],"source_types":[]}').hexdigest()
    )


@pytest.mark.asyncio
async def test_recovery_seed_uses_captured_overlay_without_live_ledger_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.processors.global_outbox import (
        GlobalOutboxProcessor,
    )
    from okto_pulse.core.kg import canonical_partition_integrity as partition
    from okto_pulse.core.kg.connectivity_guard import (
        CANONICAL_LEARNING_WORKING_ONLY_REASON,
    )

    class _Processor(GlobalOutboxProcessor):
        @staticmethod
        def _read_board_digestable_node_types(_board_id):
            return {"learning-1": "Learning"}

        @staticmethod
        def _read_board_nodes_for_refs(_board_id, _refs):
            return [
                {
                    "id": "learning-1",
                    "title": "Captured learning",
                    "embedding": [0.1, 0.2],
                }
            ]

        @staticmethod
        def _read_board_layer_meta(_board_id, _source_types):
            return {
                "learning-1": {
                    "node_type": "Learning",
                    "graph_layer": "canonical",
                    "source_artifact_ref": "bug:ticket-1",
                    "canonical_bug_count": 1,
                    "relates_to_endpoints": (),
                }
            }

    async def no_debt(_db, *, board_id):
        assert board_id == "board-1"
        return {}

    async def forbidden_live_overlay(*_args, **_kwargs):
        raise AssertionError("recovery must not read the live cognitive ledger")

    class _EmbeddingProvider:
        def encode(self, text: str) -> list[float]:
            assert text == "Board Board One"
            return [0.3, 0.4]

    monkeypatch.setattr(partition, "canonical_debt_exclusions", no_debt)
    monkeypatch.setattr(
        partition,
        "pending_or_debt_exclusions",
        forbidden_live_overlay,
    )
    monkeypatch.setattr(
        "okto_pulse.core.kg.embedding.get_embedding_provider",
        lambda: _EmbeddingProvider(),
    )

    seed = await _Processor().build_recovery_board_seed(
        object(),
        board_id="board-1",
        board_name="Board One",
        board_summary="Summary",
        captured_cognitive_pending_exclusions={
            "bug:ticket-1": CANONICAL_LEARNING_WORKING_ONLY_REASON
        },
    )

    assert len(seed.digests) == 1
    assert seed.digests[0].graph_layer == "working"


def test_stage_rejects_candidate_board_name_drift() -> None:
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(),
        artifact_store=InMemoryRebuildAuditArtifactStore(),
        single_writer_lock=_Lock(),
    )
    seeds = list(_seeds())
    seeds[0] = GlobalDiscoveryBoardSeed(
        board_id=seeds[0].board_id,
        board_name="Renamed after census",
        summary=seeds[0].summary,
        summary_embedding=seeds[0].summary_embedding,
        digests=seeds[0].digests,
        source_inventory_hash=seeds[0].source_inventory_hash,
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        service.stage_prepared_inputs(
            run_id="gdr-board-name-drift",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=seeds,
            expected_snapshot_fingerprint="authoritative-source-fingerprint",
            fence_check=lambda: None,
        )

    assert exc_info.value.code == "candidate_inventory_mismatch"


def test_preloaded_source_rows_use_public_payload_free_inventory_projection() -> None:
    inventory = inventory_from_source_rows(
        board_id="board-1",
        board_name="Board One",
        source_rows=(
            {
                "artifact_type": "task",
                "source_ref": "card-1",
                "source_version": "1",
                "content_hash": "a" * 64,
                "created_at": "2026-07-17T00:00:00+00:00",
                "id": "card-1",
                "status": "done",
            },
        ),
        cognitive_durable_digest={},
    )

    assert inventory.board_id == "board-1"
    assert inventory.source_count == 1
    assert len(inventory.source_set_hash) == 64


def test_preloaded_cognitive_digest_is_order_and_json_representation_stable() -> None:
    first = {
        "node_id": "node-b",
        "node_type": "Learning",
        "generation": 2,
        "payload": '{"title":"B","score":2}',
        "committed_at": "2026-07-17T00:00:02+00:00",
    }
    second = {
        "node_id": "node-a",
        "node_type": "Decision",
        "generation": 1,
        "payload": {"score": 1, "title": "A"},
        "committed_at": "2026-07-17T00:00:01+00:00",
    }

    forward = cognitive_durable_digest_from_rows((first, second))
    reverse = cognitive_durable_digest_from_rows(
        (
            {**second, "payload": '{"title":"A","score":1}'},
            {**first, "payload": {"score": 2, "title": "B"}},
        )
    )

    assert forward == reverse
    assert forward["count"] == 2
    assert len(str(forward["digest"])) == 64


def test_stage_rejects_census_revision_that_changed_before_core_staging() -> None:
    service = GlobalDiscoveryRecoveryService(
        recovery=_MutableFingerprintRecovery(["revision-2"]),
        artifact_store=InMemoryRebuildAuditArtifactStore(),
        single_writer_lock=_Lock(),
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        service.stage_prepared_inputs(
            run_id="gdr-stale-census",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            expected_snapshot_fingerprint="revision-1",
            fence_check=lambda: None,
        )

    assert exc_info.value.code == "snapshot_drift"


def test_stage_detects_revision_change_during_final_prepared_input_write() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    recovery = _MutableFingerprintRecovery(
        ["revision-1", "revision-1", "revision-1", "revision-1", "revision-2"]
    )
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=store,
        single_writer_lock=_Lock(),
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        service.stage_prepared_inputs(
            run_id="gdr-final-race",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            expected_snapshot_fingerprint="revision-1",
            fence_check=lambda: None,
        )

    assert exc_info.value.code == "snapshot_drift"
    prepared_inputs = GlobalDiscoveryRecoveryPreparedInputStore(store).load(
        "gdr-final-race", epoch=1
    )
    assert prepared_inputs is not None
    assert (
        service._manifests.read_prepared_revocation(  # noqa: SLF001
            run_id="gdr-final-race", epoch=1
        )
        is not None
    )
    with pytest.raises(GlobalDiscoveryRecoveryError) as replay_error:
        service.stage_prepared_inputs(
            run_id="gdr-final-race",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            expected_snapshot_fingerprint="revision-1",
            fence_check=lambda: None,
        )
    assert replay_error.value.code == "preparation_not_prepared"


def test_stage_revokes_manifest_when_cancel_fence_trips_after_publication() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(),
        artifact_store=store,
        single_writer_lock=_Lock(),
    )
    fence_calls = 0

    def cancellation_fence() -> None:
        nonlocal fence_calls
        fence_calls += 1
        if fence_calls == 5:
            raise RuntimeError("preparation_cancelled")

    with pytest.raises(RuntimeError, match="preparation_cancelled"):
        service.stage_prepared_inputs(
            run_id="gdr-cancel-publication-race",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            expected_snapshot_fingerprint="authoritative-source-fingerprint",
            fence_check=cancellation_fence,
        )

    rows = store.list_json(
        RebuildAuditKey(
            namespace="global_discovery_recovery",
            board_id=GLOBAL_RECOVERY_SCOPE,
        )
    )
    manifest_rows = [row for row in rows if row.get("manifest_version") == 2]
    assert len(manifest_rows) == 1
    assert (
        service._manifests.read_prepared_revocation(  # noqa: SLF001
            run_id="gdr-cancel-publication-race", epoch=1
        )
        is not None
    )
    assert (
        service._prepared_inputs.load(  # noqa: SLF001
            "gdr-cancel-publication-race", epoch=1
        )
        is None
    )


def test_stage_base_exception_after_manifest_is_revoked_and_cannot_duplicate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SyntheticProcessStop(BaseException):
        pass

    store = InMemoryRebuildAuditArtifactStore()
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(),
        artifact_store=store,
        single_writer_lock=_Lock(),
    )

    def stop_before_inputs(_inputs) -> None:
        raise SyntheticProcessStop()

    monkeypatch.setattr(service._prepared_inputs, "put", stop_before_inputs)  # noqa: SLF001
    with pytest.raises(SyntheticProcessStop):
        service.stage_prepared_inputs(
            run_id="gdr-process-stop-publication-race",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            expected_snapshot_fingerprint="authoritative-source-fingerprint",
            fence_check=lambda: None,
        )

    rows = store.list_json(
        RebuildAuditKey(
            namespace="global_discovery_recovery",
            board_id=GLOBAL_RECOVERY_SCOPE,
        )
    )
    manifests = [row for row in rows if row.get("manifest_version") == 2]
    assert len(manifests) == 1
    assert (
        service._manifests.read_prepared_revocation(  # noqa: SLF001
            run_id="gdr-process-stop-publication-race",
            epoch=1,
        )
        is not None
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as replay_error:
        service.stage_prepared_inputs(
            run_id="gdr-process-stop-publication-race",
            epoch=1,
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            expected_snapshot_fingerprint="authoritative-source-fingerprint",
            fence_check=lambda: None,
        )
    assert replay_error.value.code == "preparation_not_prepared"
    assert (
        len(
            [
                row
                for row in store.list_json(
                    RebuildAuditKey(
                        namespace="global_discovery_recovery",
                        board_id=GLOBAL_RECOVERY_SCOPE,
                    )
                )
                if row.get("manifest_version") == 2
            ]
        )
        == 1
    )


def test_stage_replay_revokes_existing_inputs_when_cancel_fence_trips() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(),
        artifact_store=store,
        single_writer_lock=_Lock(),
    )
    prepared = service.stage_prepared_inputs(
        run_id="gdr-cancel-replay-race",
        epoch=1,
        actor_id="agent-1",
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        expected_snapshot_fingerprint="authoritative-source-fingerprint",
        fence_check=lambda: None,
    )

    def cancelled() -> None:
        raise RuntimeError("preparation_cancelled")

    with pytest.raises(RuntimeError, match="preparation_cancelled"):
        service.stage_prepared_inputs(
            run_id="gdr-cancel-replay-race",
            epoch=1,
            actor_id="agent-1",
            boards=(),
            health_evidence=(),
            candidate_boards=(),
            expected_snapshot_fingerprint="authoritative-source-fingerprint",
            fence_check=cancelled,
        )

    assert (
        service._manifests.read_prepared_revocation(  # noqa: SLF001
            run_id="gdr-cancel-replay-race", epoch=1
        ).manifest_ref
        == prepared.manifest_ref
    )


@pytest.mark.parametrize("field", ["board_id", "kg_generation_id", "artifact_id"])
def test_rebuild_audit_key_rejects_path_components(field: str) -> None:
    values = {
        "namespace": "global_discovery_recovery",
        "board_id": "_global",
        "kg_generation_id": None,
        "artifact_id": "safe",
    }
    values[field] = "x/../../escaped"
    with pytest.raises(ValueError, match="safe logical identifier"):
        RebuildAuditKey(**values)


def test_recovery_attempt_artifact_keys_hash_untrusted_run_id() -> None:
    run_id = "x/../../../../escaped"
    keys = (
        GlobalDiscoveryRecoveryWorkerInputStore._key(run_id, 1),  # noqa: SLF001
        GlobalDiscoveryRecoveryPreparedInputStore._key(run_id, 1),  # noqa: SLF001
        GlobalDiscoveryRecoveryManifestStore.prepared_revocation_key(
            run_id=run_id, epoch=1
        ),
    )

    assert all(key.artifact_id is not None for key in keys)
    assert all(run_id not in str(key.artifact_id) for key in keys)
    assert all("/" not in str(key.artifact_id) for key in keys)


def test_manifest_v2_is_content_addressed_run_bound_and_exactly_five_minutes() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    prepared_at = datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc)

    first = manifests.build(
        run_id="gdr_preparation_one",
        epoch=3,
        actor_id="preparing-agent",
        boards=_boards(),
        live_artifact=GlobalDiscoveryArtifactSnapshot(True, 2, 42, "live-sha"),
        health_evidence=_health(),
        snapshot_fingerprint="authoritative-source-fingerprint",
        prepared_at=prepared_at,
    )
    replay = manifests.build(
        run_id="gdr_preparation_one",
        epoch=3,
        actor_id="preparing-agent",
        boards=_boards(),
        live_artifact=GlobalDiscoveryArtifactSnapshot(True, 2, 42, "live-sha"),
        health_evidence=_health(),
        snapshot_fingerprint="authoritative-source-fingerprint",
        prepared_at=prepared_at,
    )

    assert replay == first
    assert first.run_id == "gdr_preparation_one"
    assert first.epoch == 3
    assert first.attempt_id == recovery_attempt_id(first.run_id, first.epoch)
    assert first.manifest_ref.startswith("global_discovery_manifest_")
    assert first.prepared_at == prepared_at
    assert first.expires_at - first.prepared_at == timedelta(
        seconds=RECOVERY_PREPARED_TTL_SECONDS
    )
    assert first.snapshot_fingerprint == "authoritative-source-fingerprint"
    assert manifests.load(first.manifest_ref) == first


def test_manifest_attempt_binding_reuses_first_publication_time_after_restart() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    first_prepared_at = datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc)

    first = manifests.build(
        run_id="gdr_restart_bound_manifest",
        epoch=2,
        actor_id="preparing-agent",
        boards=_boards(),
        live_artifact=GlobalDiscoveryArtifactSnapshot(True, 2, 42, "live-sha"),
        health_evidence=_health(),
        snapshot_fingerprint="authoritative-source-fingerprint",
        prepared_at=first_prepared_at,
    )
    replay = GlobalDiscoveryRecoveryManifestStore(store).build(
        run_id="gdr_restart_bound_manifest",
        epoch=2,
        actor_id="preparing-agent",
        boards=_boards(),
        live_artifact=GlobalDiscoveryArtifactSnapshot(True, 2, 42, "live-sha"),
        health_evidence=_health(),
        snapshot_fingerprint="authoritative-source-fingerprint",
        prepared_at=first_prepared_at + timedelta(seconds=1),
    )

    assert replay == first
    assert replay.prepared_at == first_prepared_at
    assert (
        manifests.load_attempt_manifest(
            "gdr_restart_bound_manifest",
            epoch=2,
        )
        == first
    )


def test_prepared_inputs_exist_before_confirmation_and_are_attempt_bound() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    prepared_store = GlobalDiscoveryRecoveryPreparedInputStore(store)
    prepared = GlobalDiscoveryRecoveryPreparedInputs(
        run_id="gdr_preparation_one",
        epoch=3,
        attempt_id="gdr_preparation_one/attempt-3",
        manifest_ref="global_discovery_manifest_content",
        preflight_hash="preflight-hash",
        snapshot_fingerprint="authoritative-source-fingerprint",
        expected_live_sha256="live-sha",
        boards=_seeds(),
        terminal_counts=RecoveryProgressCounts(
            boards_total=2,
            boards_scanned=2,
            sources_total=3,
            sources_processed=3,
        ),
    )

    assert prepared_store.put(prepared) == prepared
    assert prepared_store.load(prepared.run_id, epoch=prepared.epoch) == prepared
    next_attempt = prepared_store.put(
        GlobalDiscoveryRecoveryPreparedInputs(
            run_id=prepared.run_id,
            epoch=4,
            attempt_id="gdr_preparation_one/attempt-4",
            manifest_ref=prepared.manifest_ref,
            preflight_hash=prepared.preflight_hash,
            snapshot_fingerprint=prepared.snapshot_fingerprint,
            expected_live_sha256=prepared.expected_live_sha256,
            boards=prepared.boards,
            terminal_counts=prepared.terminal_counts,
        )
    )
    assert prepared_store.load(prepared.run_id, epoch=3) == prepared
    assert prepared_store.load(prepared.run_id, epoch=4) == next_attempt
    assert next_attempt.attempt_id != prepared.attempt_id


def test_confirm_and_start_use_staged_inputs_without_rescanning_request_inputs() -> (
    None
):
    store = InMemoryRebuildAuditArtifactStore()
    recovery = _Recovery()
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=store,
        single_writer_lock=_Lock(),
    )
    now = datetime.now(timezone.utc)
    preparation = service.new_preparation_command(
        actor_id="preparing-agent",
        admitted_at=now,
        run_id="gdr_staged_start",
    )
    durable_high_water = RecoveryProgressCounts(
        boards_total=2,
        boards_scanned=2,
        sources_total=3,
        sources_processed=3,
        nodes_written=4,
        edges_written=2,
        errors=1,
    )
    prepared = service.stage_prepared_inputs(
        run_id=preparation.binding.run_id,
        epoch=1,
        actor_id=preparation.binding.actor_id,
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        expected_snapshot_fingerprint="authoritative-source-fingerprint",
        fence_check=lambda: None,
        prepared_at=now,
        terminal_counts=durable_high_water,
    )
    inspect_calls_after_preparation = recovery.inspect_calls
    confirmed = service.confirm(
        actor_id="different-global-admin",
        run_id=preparation.binding.run_id,
        manifest_ref=prepared.manifest_ref,
        preflight_hash=prepared.preflight_hash,
        current_snapshot_fingerprint="authoritative-source-fingerprint",
        now=now + timedelta(seconds=1),
    )

    command = service.prepare_durable_start(
        actor_id="different-global-admin",
        confirmation_id=str(confirmed["confirmation_id"]),
        manifest_ref=prepared.manifest_ref,
        preflight_hash=prepared.preflight_hash,
        reason="approved staged recovery",
        current_snapshot_fingerprint="authoritative-source-fingerprint",
        started_at=now + timedelta(seconds=2),
    )

    assert command.binding.run_id == preparation.binding.run_id
    assert command.binding.actor_id == "preparing-agent"
    assert command.confirmed_by_actor_id == "different-global-admin"
    assert command.expected_epoch == 1
    assert prepared.counts == durable_high_water
    assert command.counts == durable_high_water
    assert recovery.inspect_calls == inspect_calls_after_preparation
    staged = GlobalDiscoveryRecoveryPreparedInputStore(store).load(
        preparation.binding.run_id, epoch=1
    )
    assert staged is not None
    assert staged.terminal_counts == durable_high_water
    worker_inputs = GlobalDiscoveryRecoveryWorkerInputStore(store).load(
        preparation.binding.run_id, epoch=1
    )
    assert worker_inputs is not None
    assert worker_inputs.terminal_counts == durable_high_water


def test_confirm_rejects_expired_or_drifted_manifest_as_manifest_stale() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(), artifact_store=store, single_writer_lock=_Lock()
    )
    now = datetime.now(timezone.utc)
    preparation = service.new_preparation_command(
        actor_id="preparing-agent", admitted_at=now, run_id="gdr_stale"
    )
    prepared = service.stage_prepared_inputs(
        run_id=preparation.binding.run_id,
        epoch=1,
        actor_id=preparation.binding.actor_id,
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        expected_snapshot_fingerprint="authoritative-source-fingerprint",
        fence_check=lambda: None,
        prepared_at=now,
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        service.confirm(
            actor_id="admin",
            run_id=preparation.binding.run_id,
            manifest_ref=prepared.manifest_ref,
            preflight_hash=prepared.preflight_hash,
            current_snapshot_fingerprint="authoritative-source-fingerprint",
            now=now + timedelta(seconds=RECOVERY_PREPARED_TTL_SECONDS),
        )
    assert exc_info.value.code == "manifest_stale"


def test_worker_input_roundtrip_preserves_epoch_two_and_confirmer_audit() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    now = datetime.now(timezone.utc)
    command = RecoveryStartCommand(
        binding=RecoveryRunBinding(
            run_id="gdr_epoch_two",
            actor_id="preparing-agent",
            confirmation_fingerprint="confirmation-fingerprint",
            manifest_ref="global_discovery_manifest_epoch_two",
            preflight_hash="preflight-hash",
            reason="resume approved",
        ),
        started_at=now,
        counts=RecoveryProgressCounts(boards_total=2, boards_scanned=2),
        expected_epoch=2,
        confirmed_by_actor_id="confirming-admin",
        confirmation_consumed_at=now - timedelta(seconds=1),
    )
    inputs = GlobalDiscoveryRecoveryWorkerInputs(
        command=command,
        expected_live_sha256="live-sha",
        boards=_seeds(),
        terminal_counts=RecoveryProgressCounts(
            boards_total=2,
            boards_scanned=2,
            sources_total=3,
            sources_processed=3,
        ),
    )

    stored = GlobalDiscoveryRecoveryWorkerInputStore(store).put(inputs)
    loaded = GlobalDiscoveryRecoveryWorkerInputStore(store).load(
        "gdr_epoch_two", epoch=2
    )
    assert stored.command.expected_epoch == 2
    assert loaded == stored
    assert loaded is not None
    assert loaded.command.confirmed_by_actor_id == "confirming-admin"
    assert loaded.command.confirmation_consumed_at == now - timedelta(seconds=1)


def test_exact_durable_start_replay_returns_receipted_command_after_expiry() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(), artifact_store=store, single_writer_lock=_Lock()
    )
    now = datetime.now(timezone.utc)
    preparation = service.new_preparation_command(
        actor_id="preparing-agent", admitted_at=now, run_id="gdr_expired_replay"
    )
    prepared = service.stage_prepared_inputs(
        run_id=preparation.binding.run_id,
        epoch=1,
        actor_id=preparation.binding.actor_id,
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        expected_snapshot_fingerprint="authoritative-source-fingerprint",
        fence_check=lambda: None,
        prepared_at=now,
    )
    confirmed = service.confirm(
        actor_id="confirming-admin",
        run_id=preparation.binding.run_id,
        manifest_ref=prepared.manifest_ref,
        preflight_hash=prepared.preflight_hash,
        current_snapshot_fingerprint="authoritative-source-fingerprint",
        now=now + timedelta(seconds=1),
    )
    first = service.prepare_durable_start(
        actor_id="confirming-admin",
        confirmation_id=str(confirmed["confirmation_id"]),
        manifest_ref=prepared.manifest_ref,
        preflight_hash=prepared.preflight_hash,
        reason="approved exact replay",
        current_snapshot_fingerprint="authoritative-source-fingerprint",
        started_at=now + timedelta(seconds=2),
    )

    replay = service.prepare_durable_start(
        actor_id="confirming-admin",
        confirmation_id=str(confirmed["confirmation_id"]),
        manifest_ref=prepared.manifest_ref,
        preflight_hash=prepared.preflight_hash,
        reason="approved exact replay",
        current_snapshot_fingerprint="drifted-after-accepted-start",
        started_at=now + timedelta(seconds=RECOVERY_PREPARED_TTL_SECONDS + 1),
    )
    assert replay == first

    service.revoke_prepared_run(
        run_id=preparation.binding.run_id,
        epoch=1,
        manifest_ref=prepared.manifest_ref,
        requested_by_actor_id="cancelling-admin",
        reason="revoked after persisted input race",
        revoked_at=now + timedelta(seconds=3),
    )
    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        service.prepare_durable_start(
            actor_id="confirming-admin",
            confirmation_id=str(confirmed["confirmation_id"]),
            manifest_ref=prepared.manifest_ref,
            preflight_hash=prepared.preflight_hash,
            reason="approved exact replay",
            current_snapshot_fingerprint="authoritative-source-fingerprint",
            started_at=now + timedelta(seconds=4),
        )
    assert exc_info.value.code == "preparation_not_prepared"


def test_prepared_revocation_preserves_create_only_manifest_and_inputs() -> None:
    store = InMemoryRebuildAuditArtifactStore()
    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(), artifact_store=store, single_writer_lock=_Lock()
    )
    now = datetime.now(timezone.utc)
    preparation = service.new_preparation_command(
        actor_id="preparing-agent", admitted_at=now, run_id="gdr_revoked"
    )
    prepared = service.stage_prepared_inputs(
        run_id=preparation.binding.run_id,
        epoch=1,
        actor_id=preparation.binding.actor_id,
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        expected_snapshot_fingerprint="authoritative-source-fingerprint",
        fence_check=lambda: None,
        prepared_at=now,
    )

    evidence = service.revoke_prepared_run(
        run_id=preparation.binding.run_id,
        epoch=1,
        manifest_ref=prepared.manifest_ref,
        requested_by_actor_id="cancelling-admin",
        reason="operator cancelled before confirmation",
        revoked_at=now + timedelta(seconds=1),
    )
    timestamp_replay = service.revoke_prepared_run(
        run_id=preparation.binding.run_id,
        epoch=1,
        manifest_ref=prepared.manifest_ref,
        requested_by_actor_id="cancelling-admin",
        reason="operator cancelled before confirmation",
        revoked_at=now + timedelta(seconds=2),
    )

    assert evidence.run_id == preparation.binding.run_id
    assert evidence.attempt_id == "gdr_revoked/attempt-1"
    assert evidence.requested_by_actor_id == "cancelling-admin"
    assert timestamp_replay == evidence
    assert service._manifests.load(prepared.manifest_ref) is not None  # noqa: SLF001
    assert (
        service._prepared_inputs.load(  # noqa: SLF001
            preparation.binding.run_id, epoch=1
        )
        is not None
    )
    assert (
        service._manifests.read_prepared_revocation(  # noqa: SLF001
            run_id=preparation.binding.run_id,
            epoch=1,
        )
        == evidence
    )


def _approved_service(recovery: _Recovery | None = None):
    store = InMemoryRebuildAuditArtifactStore()
    recovery = recovery or _Recovery()
    lock = _Lock()
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=store,
        single_writer_lock=lock,
    )
    preparation = service.new_preparation_command(actor_id="agent-1")
    prepared = service.stage_prepared_inputs(
        run_id=preparation.binding.run_id,
        epoch=1,
        actor_id="agent-1",
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        expected_snapshot_fingerprint="authoritative-source-fingerprint",
        fence_check=lambda: None,
    )
    preflight = {
        "outcome": "confirmation_required",
        "action_required": "call_okto_pulse_kg_global_discovery_recovery_confirm",
        "scope": GLOBAL_RECOVERY_SCOPE,
        "run_id": preparation.binding.run_id,
        "manifest_ref": prepared.manifest_ref,
        "preflight_hash": prepared.preflight_hash,
        "board_count": 2,
        "source_count": 3,
        "board_inventory_hash": service._manifests.load(  # noqa: SLF001
            prepared.manifest_ref
        ).board_inventory_hash,
        "live_artifact": GlobalDiscoveryArtifactSnapshot(
            True, 2, 42, "live-sha"
        ).to_dict(),
    }
    confirmed = service.confirm(
        actor_id="agent-1",
        run_id=preparation.binding.run_id,
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
        current_snapshot_fingerprint="authoritative-source-fingerprint",
    )
    return service, store, recovery, lock, preflight, confirmed


def _run_args(preflight, confirmed):
    return {
        "actor_id": "agent-1",
        "confirmation_id": str(confirmed["confirmation_id"]),
        "manifest_ref": str(preflight["manifest_ref"]),
        "preflight_hash": str(preflight["preflight_hash"]),
        "boards": _boards(),
        "health_evidence": _health(),
        "candidate_boards": _seeds(),
        "reason": "repair corrupt global discovery WAL",
    }


def _start_args(preflight, confirmed):
    args = _run_args(preflight, confirmed)
    for key in ("boards", "health_evidence", "candidate_boards"):
        args.pop(key)
    args["current_snapshot_fingerprint"] = "authoritative-source-fingerprint"
    return args


def test_prepare_durable_start_consumes_confirmation_and_persists_inputs() -> None:
    service, store, recovery, _lock, preflight, confirmed = _approved_service()
    args = _start_args(preflight, confirmed)

    command = service.prepare_durable_start(**args)
    inputs = GlobalDiscoveryRecoveryWorkerInputStore(store).load(command.binding.run_id)

    assert recovery.run_calls == 0
    assert command.binding.actor_id == "agent-1"
    assert (
        command.binding.confirmation_fingerprint
        == hashlib.sha256(str(confirmed["confirmation_id"]).encode("utf-8")).hexdigest()
    )
    assert command.counts.sources_total == 3
    assert command.counts.sources_processed == 3
    assert inputs is not None
    assert inputs.command == command
    assert inputs.expected_live_sha256 == "live-sha"
    assert [row.board_id for row in inputs.boards] == ["a", "b"]
    assert inputs.terminal_counts.sources_processed == 3
    assert service._confirmation._read(str(confirmed["confirmation_id"])) is None
    assert service._manifests.has_confirmation_receipt(  # noqa: SLF001
        run_id=command.binding.run_id,
        expected=service._confirmation_receipt(  # noqa: SLF001
            run_id=command.binding.run_id,
            confirmation_id=str(confirmed["confirmation_id"]),
            actor_id="agent-1",
            manifest=service._manifests.load(str(preflight["manifest_ref"])),  # type: ignore[arg-type]  # noqa: SLF001
        ),
    )


def test_durable_start_replays_a_receipt_after_the_input_write_crash_window() -> None:
    service, store, _recovery, _lock, preflight, confirmed = _approved_service()
    args = _start_args(preflight, confirmed)
    first = service.prepare_durable_start(**args)
    input_store = GlobalDiscoveryRecoveryWorkerInputStore(store)
    assert store.delete_json(input_store._key(first.binding.run_id)) is True

    replay = service.prepare_durable_start(**args)

    assert replay.binding == first.binding
    assert replay.counts == first.counts
    assert replay.started_at >= first.started_at
    assert input_store.load(first.binding.run_id) is not None


def test_durable_worker_inputs_fail_closed_when_payload_is_tampered() -> None:
    service, store, _recovery, _lock, preflight, confirmed = _approved_service()
    command = service.prepare_durable_start(**_start_args(preflight, confirmed))
    input_store = GlobalDiscoveryRecoveryWorkerInputStore(store)
    key = input_store._key(command.binding.run_id)
    raw = store.read_json(key)
    assert raw is not None
    raw["boards"][0]["board_name"] = "tampered"
    store.write_json_atomic(key, raw)

    with pytest.raises(GlobalDiscoveryRecoveryError) as exc_info:
        input_store.load(command.binding.run_id)

    assert exc_info.value.code == "recovery_worker_inputs_invalid"


def test_first_durable_start_rechecks_live_health_before_consuming_token() -> None:
    service, _store, _recovery, _lock, preflight, confirmed = _approved_service()
    args = _start_args(preflight, confirmed)
    args["health_evidence"] = _health(discovery_state="healthy")

    with pytest.raises(TypeError, match="health_evidence"):
        service.prepare_durable_start(**args)

    assert service._confirmation._read(str(confirmed["confirmation_id"])) is not None


def test_preflight_manifest_covers_every_board_and_is_hash_bound():
    service, store, recovery, _lock, preflight, _confirmed = _approved_service()

    assert recovery.inspect_calls == 1
    assert preflight["outcome"] == "confirmation_required"
    assert preflight["board_count"] == 2
    assert preflight["source_count"] == 3
    manifest = GlobalDiscoveryRecoveryManifestStore(store).load(
        str(preflight["manifest_ref"])
    )
    assert manifest is not None
    assert [row.board_id for row in manifest.boards] == ["a", "b"]

    raw = store.read_json(
        GlobalDiscoveryRecoveryManifestStore._key(str(preflight["manifest_ref"]))
    )
    assert raw is not None
    raw["boards"][0]["source_count"] = 999
    store.write_json_atomic(
        GlobalDiscoveryRecoveryManifestStore._key(str(preflight["manifest_ref"])),
        raw,
    )
    assert (
        GlobalDiscoveryRecoveryManifestStore(store).load(str(preflight["manifest_ref"]))
        is None
    )


def test_manifest_refs_are_content_addressed_and_confirmation_ids_are_create_only(
    monkeypatch,
):
    from okto_pulse.core.kg import global_discovery_recovery as recovery_module
    from okto_pulse.core.kg import rebuild_confirmation as confirmation_module

    service = GlobalDiscoveryRecoveryService(
        recovery=_Recovery(),
        artifact_store=InMemoryRebuildAuditArtifactStore(),
        single_writer_lock=_Lock(),
    )
    monkeypatch.setattr(
        recovery_module.secrets,
        "token_urlsafe",
        lambda _size: "fixed_manifest_reference_123",
    )
    preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    second_preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    assert second_preflight["manifest_ref"] != preflight["manifest_ref"]

    monkeypatch.setattr(
        confirmation_module.secrets,
        "token_urlsafe",
        lambda _size: "fixed_confirmation_reference_123",
    )
    first = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
    )
    with pytest.raises(RuntimeError, match="confirmation_id_collision"):
        service.confirm(
            actor_id="agent-1",
            manifest_ref=str(preflight["manifest_ref"]),
            preflight_hash=str(preflight["preflight_hash"]),
        )
    token = service._confirmation._read(str(first["confirmation_id"]))  # noqa: SLF001
    assert token is not None and token["actor_id"] == "agent-1"


def test_active_run_pointer_cannot_be_overwritten_by_another_live_run():
    manifests = GlobalDiscoveryRecoveryManifestStore(
        InMemoryRebuildAuditArtifactStore()
    )
    base = {
        "actor_id": "agent-1",
        "manifest_ref": "global_discovery_manifest_binding",
        "preflight_hash": "preflight",
    }
    assert manifests.write_status("run-a", {**base, "state": "running"}) is True
    assert manifests.write_status("run-b", {**base, "state": "running"}) is False
    active = manifests.read_active_status()
    assert active is not None and active["run_id"] == "run-a"

    assert manifests.write_status("run-a", {**base, "state": "completed"}) is True
    assert manifests.write_status("run-b", {**base, "state": "running"}) is True
    active = manifests.read_active_status()
    assert active is not None and active["run_id"] == "run-b"


@pytest.mark.parametrize(
    ("released_state", "extra"),
    [
        ("completed", {}),
        ("rolled_back", {"rollback_performed": True}),
        ("precondition_failed", {}),
        ("failed", {"physical_cutover_started": False}),
    ],
)
def test_stale_active_pointer_is_cas_repaired_only_by_releasable_owner_truth(
    released_state, extra
):
    store = InMemoryRebuildAuditArtifactStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    base = {
        "actor_id": "agent-1",
        "manifest_ref": "global_discovery_manifest_binding",
        "preflight_hash": "preflight",
    }
    assert manifests.write_status("run-a", {**base, "state": "running"}) is True
    store.write_json_atomic(
        manifests.status_key("run-a"),
        {"run_id": "run-a", **base, "state": released_state, **extra},
    )

    truth = manifests.read_reconciled_active_status()

    assert truth is not None and truth["state"] == released_state
    active = manifests.read_active_status()
    assert active is not None and active["state"] == released_state
    assert manifests.write_status("run-b", {**base, "state": "running"}) is True


@pytest.mark.parametrize(
    "owner_truth",
    [
        {"state": "running", "physical_cutover_started": False},
        {"state": "failed", "physical_cutover_started": True},
        {"state": "failed"},
    ],
)
def test_active_pointer_never_steals_running_or_ambiguous_legacy_truth(owner_truth):
    store = InMemoryRebuildAuditArtifactStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    base = {
        "actor_id": "agent-1",
        "manifest_ref": "global_discovery_manifest_binding",
        "preflight_hash": "preflight",
    }
    assert manifests.write_status("run-a", {**base, "state": "running"}) is True
    store.write_json_atomic(
        manifests.status_key("run-a"),
        {"run_id": "run-a", **base, **owner_truth},
    )

    truth = manifests.read_reconciled_active_status()

    assert truth is not None and truth["run_id"] == "run-a"
    assert manifests.write_status("run-b", {**base, "state": "running"}) is False
    active = manifests.read_active_status()
    assert active is not None and active["run_id"] == "run-a"


@pytest.mark.parametrize("invalid_truth", ["missing", "binding_mismatch"])
def test_active_pointer_fails_closed_without_exact_owner_status(invalid_truth):
    store = InMemoryRebuildAuditArtifactStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    base = {
        "actor_id": "agent-1",
        "manifest_ref": "global_discovery_manifest_binding",
        "preflight_hash": "preflight",
    }
    assert manifests.write_status("run-a", {**base, "state": "running"}) is True
    if invalid_truth == "missing":
        store.delete_json(manifests.status_key("run-a"))
    else:
        store.write_json_atomic(
            manifests.status_key("run-a"),
            {
                "run_id": "run-a",
                **base,
                "actor_id": "agent-other",
                "state": "completed",
            },
        )

    with pytest.raises(GlobalDiscoveryRecoveryError) as invalid:
        manifests.read_reconciled_active_status()
    assert invalid.value.code == "active_run_status_invalid"


def test_active_pointer_repair_cas_preserves_a_concurrent_new_owner():
    from okto_pulse.core.kg.global_discovery_recovery import _canonical_sha256

    class InterleavingStore(InMemoryRebuildAuditArtifactStore):
        replacement = None

        def replace_json(self, key, transform):
            if key.artifact_id == "active_run" and self.replacement is not None:
                replacement = self.replacement
                self.replacement = None
                super().write_json_atomic(key, replacement)
            return super().replace_json(key, transform)

    store = InterleavingStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    base = {
        "actor_id": "agent-1",
        "manifest_ref": "global_discovery_manifest_binding",
        "preflight_hash": "preflight",
    }
    assert manifests.write_status("run-a", {**base, "state": "running"}) is True
    store.write_json_atomic(
        manifests.status_key("run-a"),
        {"run_id": "run-a", **base, "state": "completed"},
    )
    store.write_json_atomic(
        manifests.status_key("run-c"),
        {"run_id": "run-c", **base, "state": "running"},
    )
    concurrent_binding = {"run_id": "run-c", "state": "running", **base}
    store.replacement = {
        **concurrent_binding,
        "active_status_sha256": _canonical_sha256(concurrent_binding),
    }

    result = manifests.read_reconciled_active_status()

    assert result is not None
    assert result["run_id"] == "run-c"
    assert result["state"] == "unknown"
    active = manifests.read_active_status()
    assert active is not None and active["run_id"] == "run-c"


def test_active_pointer_transition_id_prevents_same_run_aba_repair():
    class SameRunInterleavingStore(InMemoryRebuildAuditArtifactStore):
        before_active_replace = None

        def replace_json(self, key, transform):
            if (
                key.artifact_id == "active_run"
                and self.before_active_replace is not None
            ):
                callback = self.before_active_replace
                self.before_active_replace = None
                callback()
            return super().replace_json(key, transform)

    store = SameRunInterleavingStore()
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    base = {
        "actor_id": "agent-1",
        "manifest_ref": "global_discovery_manifest_binding",
        "preflight_hash": "preflight",
    }
    assert manifests.write_status(
        "run-a",
        {**base, "state": "running", "physical_cutover_started": False},
    )
    # Crash-gap shape: owner truth released, pointer still says running.
    store.write_json_atomic(
        manifests.status_key("run-a"),
        {
            "run_id": "run-a",
            **base,
            "state": "failed",
            "physical_cutover_started": False,
        },
    )
    observed_document = store.read_json(manifests._key("active_run"))  # noqa: SLF001
    assert observed_document is not None

    def same_run_resumes() -> None:
        assert manifests.write_status(
            "run-a",
            {**base, "state": "running", "physical_cutover_started": False},
        )

    store.before_active_replace = same_run_resumes
    result = manifests.read_reconciled_active_status()

    assert result is not None and result["state"] == "unknown"
    current_document = store.read_json(manifests._key("active_run"))  # noqa: SLF001
    assert current_document is not None
    assert current_document["run_id"] == "run-a"
    assert current_document["state"] == "running"
    assert (
        current_document["active_transition_id"]
        != observed_document["active_transition_id"]
    )


@pytest.mark.parametrize("state", ["healthy", "quarantined"])
def test_preflight_refuses_without_concrete_compatible_recovery_signal(state):
    recovery = _Recovery()
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=InMemoryRebuildAuditArtifactStore(),
        single_writer_lock=_Lock(),
    )
    with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
        service.preflight(
            actor_id="agent-1",
            boards=_boards(),
            health_evidence=_health(discovery_state=state),
        )
    assert refused.value.code == "discovery_recovery_not_admitted"
    assert recovery.inspect_calls == 1


def test_confirm_allows_any_admin_and_run_fails_closed_on_drift():
    service, _store, recovery, _lock, preflight, confirmed = _approved_service()
    other_admin = service.confirm(
        actor_id="agent-2",
        run_id=str(preflight["run_id"]),
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
        current_snapshot_fingerprint="authoritative-source-fingerprint",
    )
    assert other_admin["run_id"] == preflight["run_id"]

    with pytest.raises(GlobalDiscoveryRecoveryError) as drift_error:
        service.run(
            actor_id="agent-1",
            confirmation_id=str(confirmed["confirmation_id"]),
            manifest_ref=str(preflight["manifest_ref"]),
            preflight_hash=str(preflight["preflight_hash"]),
            boards=_boards(drift=True),
            health_evidence=_health(),
            candidate_boards=_seeds(),
            reason="repair corrupt global discovery WAL",
        )
    assert drift_error.value.code == "board_inventory_drift"
    assert recovery.run_calls == 0


def test_run_revalidates_live_health_before_consuming_confirmation():
    service, _store, recovery, _lock, preflight, confirmed = _approved_service()
    with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
        service.run(
            actor_id="agent-1",
            confirmation_id=str(confirmed["confirmation_id"]),
            manifest_ref=str(preflight["manifest_ref"]),
            preflight_hash=str(preflight["preflight_hash"]),
            boards=_boards(),
            health_evidence=_health(discovery_state="healthy"),
            candidate_boards=_seeds(),
            reason="repair corrupt global discovery WAL",
        )
    assert refused.value.code == "discovery_recovery_not_admitted"
    assert recovery.run_calls == 0


def test_success_is_terminal_audited_and_confirmation_retry_is_idempotent():
    service, store, recovery, lock, preflight, confirmed = _approved_service()
    kwargs = {
        "actor_id": "agent-1",
        "confirmation_id": str(confirmed["confirmation_id"]),
        "manifest_ref": str(preflight["manifest_ref"]),
        "preflight_hash": str(preflight["preflight_hash"]),
        "boards": _boards(),
        "health_evidence": _health(),
        "candidate_boards": _seeds(),
        "reason": "repair corrupt global discovery WAL",
    }
    result = service.run(**kwargs)
    assert result["state"] == "delivery_pending"
    assert result["outcome"] == "completed"
    assert result["quarantine_ref"] == "quarantine:1"
    assert lock.released
    status = GlobalDiscoveryRecoveryManifestStore(store).read_status(
        str(result["run_id"])
    )
    assert status is not None and status["state"] == "delivery_pending"
    resumable = service.read_bound_status(
        actor_id="agent-1",
        confirmation_id=str(confirmed["confirmation_id"]),
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
    )
    assert resumable is not None
    assert resumable["board_ids"] == ["a", "b"]

    replay = service.run(
        **{
            **kwargs,
            "boards": _boards(drift=True),
            "health_evidence": _health(discovery_state="healthy"),
            "candidate_boards": (),
        }
    )
    assert replay["run_id"] == result["run_id"]
    assert replay["state"] == "delivery_pending"
    assert recovery.run_calls == 1

    completed = service.finalize_delivery(
        run_id=str(result["run_id"]),
        actor_id="agent-1",
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
        delivery={"committed": True},
    )
    assert completed["state"] == "completed"
    assert completed["delivery"] == {"committed": True}


@pytest.mark.parametrize(
    "invalid_mode",
    ["missing", "scope_mismatch", "expired", "already_consumed"],
)
def test_confirmation_refusal_never_establishes_canonical_run_status(
    invalid_mode,
):
    service, store, recovery, _lock, preflight, confirmed = _approved_service()
    confirmation_id = str(confirmed["confirmation_id"])
    token_key = service._confirmation._key(confirmation_id)  # noqa: SLF001
    token = store.read_json(token_key)
    assert token is not None
    if invalid_mode == "missing":
        store.delete_json(token_key)
    elif invalid_mode == "scope_mismatch":
        store.write_json_atomic(token_key, {**token, "actor_id": "agent-other"})
    elif invalid_mode == "expired":
        store.write_json_atomic(
            token_key,
            {
                **token,
                "expires_at": (
                    datetime.now(timezone.utc) - timedelta(seconds=1)
                ).isoformat(),
            },
        )
    else:
        consumed = service._confirmation.consume(  # noqa: SLF001
            confirmation_id=confirmation_id,
            expected_board_id=GLOBAL_RECOVERY_SCOPE,
            expected_actor_id="agent-1",
            expected_operation="reindex_discovery",
            expected_preflight_hash=str(preflight["preflight_hash"]),
            expected_manifest_ref=str(preflight["manifest_ref"]),
        )
        assert consumed.token is not None

    kwargs = _run_args(preflight, confirmed)
    for _attempt in range(2):
        with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
            service.run(**kwargs)
        assert refused.value.code == "confirmation_refused"

    run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
    status = GlobalDiscoveryRecoveryManifestStore(store).read_status(run_id)
    assert status is None
    assert recovery.run_calls == 0
    assert (
        store.read_json(
            GlobalDiscoveryRecoveryManifestStore.confirmation_receipt_key(run_id)
        )
        is None
    )


@pytest.mark.parametrize("poisoning", ["actor", "manifest", "scope"])
def test_unauthenticated_poisoning_cannot_block_the_confirmation_owner(poisoning):
    service, store, recovery, _lock, preflight, confirmed = _approved_service()
    confirmation_id = str(confirmed["confirmation_id"])
    token_key = service._confirmation._key(confirmation_id)  # noqa: SLF001
    original_token = store.read_json(token_key)
    assert original_token is not None

    if poisoning == "actor":
        attacker_preflight = service.preflight(
            actor_id="agent-evil", boards=_boards(), health_evidence=_health()
        )
        poisoned_args = {
            **_run_args(attacker_preflight, confirmed),
            "actor_id": "agent-evil",
        }
    elif poisoning == "manifest":
        attacker_preflight = service.preflight(
            actor_id="agent-1", boards=_boards(), health_evidence=_health()
        )
        poisoned_args = _run_args(attacker_preflight, confirmed)
    else:
        store.write_json_atomic(
            token_key,
            {**original_token, "board_id": "another-scope"},
        )
        poisoned_args = _run_args(preflight, confirmed)

    with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
        service.run(**poisoned_args)
    assert refused.value.code == "confirmation_refused"
    run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
    assert GlobalDiscoveryRecoveryManifestStore(store).read_status(run_id) is None
    if poisoning == "scope":
        store.write_json_atomic(token_key, original_token)
    assert store.read_json(token_key) is not None

    owner_result = service.run(**_run_args(preflight, confirmed))

    assert owner_result["state"] == "delivery_pending"
    assert recovery.run_calls == 1


def test_expired_confirmation_cannot_poison_a_fresh_confirmation():
    service, store, recovery, _lock, preflight, confirmed = _approved_service()
    confirmation_id = str(confirmed["confirmation_id"])
    token_key = service._confirmation._key(confirmation_id)  # noqa: SLF001
    token = store.read_json(token_key)
    assert token is not None
    store.write_json_atomic(
        token_key,
        {
            **token,
            "expires_at": (
                datetime.now(timezone.utc) - timedelta(seconds=1)
            ).isoformat(),
        },
    )

    with pytest.raises(GlobalDiscoveryRecoveryError) as expired:
        service.run(**_run_args(preflight, confirmed))
    assert expired.value.code == "confirmation_refused"
    expired_run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
    assert (
        GlobalDiscoveryRecoveryManifestStore(store).read_status(expired_run_id) is None
    )

    fresh = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
    )
    result = service.run(**_run_args(preflight, fresh))

    assert result["state"] == "delivery_pending"
    assert recovery.run_calls == 1


def test_atomic_source_mismatch_leaves_status_empty_and_owner_can_retry():
    class SourceRaceStore(InMemoryRebuildAuditArtifactStore):
        mutate_once = True

        def consume_json_with_receipt(self, **kwargs):
            if self.mutate_once:
                self.mutate_once = False
                source = self.read_json(kwargs["source_key"])
                assert source is not None
                self.write_json_atomic(
                    kwargs["source_key"],
                    {**source, "issued_at": str(source["issued_at"]) + "-changed"},
                )
            return super().consume_json_with_receipt(**kwargs)

    store = SourceRaceStore()
    recovery = _Recovery()
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=store,
        single_writer_lock=_Lock(),
    )
    preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    confirmed = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
    )
    confirmation_id = str(confirmed["confirmation_id"])
    token_key = service._confirmation._key(confirmation_id)  # noqa: SLF001
    original_token = store.read_json(token_key)
    assert original_token is not None

    with pytest.raises(GlobalDiscoveryRecoveryError) as raced:
        service.run(**_run_args(preflight, confirmed))
    assert raced.value.code == "confirmation_refused"
    run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
    assert GlobalDiscoveryRecoveryManifestStore(store).read_status(run_id) is None
    assert (
        store.read_json(
            GlobalDiscoveryRecoveryManifestStore.confirmation_receipt_key(run_id)
        )
        is None
    )

    store.write_json_atomic(token_key, original_token)
    result = service.run(**_run_args(preflight, confirmed))

    assert result["state"] == "delivery_pending"
    assert recovery.run_calls == 1


def test_failed_run_resumes_only_with_exact_durable_confirmation_receipt():
    class FailOnceRecovery(_Recovery):
        def rebuild_candidate_and_cutover(self, **kwargs):
            if self.run_calls == 0:
                self.run_calls += 1
                raise RuntimeError("synthetic_pre_cutover_failure")
            return super().rebuild_candidate_and_cutover(**kwargs)

    service, store, recovery, _lock, preflight, confirmed = _approved_service(
        FailOnceRecovery()
    )
    kwargs = _run_args(preflight, confirmed)
    with pytest.raises(GlobalDiscoveryRecoveryError):
        service.run(**kwargs)
    run_id = (
        f"gdr_{hashlib.sha256(kwargs['confirmation_id'].encode()).hexdigest()[:24]}"
    )
    receipt_key = GlobalDiscoveryRecoveryManifestStore.confirmation_receipt_key(run_id)
    receipt = store.read_json(receipt_key)
    assert receipt is not None
    assert receipt["run_id"] == run_id
    assert receipt["manifest_ref"] == preflight["manifest_ref"]
    assert receipt["preflight_hash"] == preflight["preflight_hash"]
    assert receipt["board_inventory_hash"] == preflight["board_inventory_hash"]

    resumed = service.run(**kwargs)
    assert resumed["state"] == "delivery_pending"
    assert recovery.run_calls == 2


def test_failed_run_without_receipt_preserves_canonical_failure_truth():
    class AlwaysFailRecovery(_Recovery):
        def rebuild_candidate_and_cutover(self, **_kwargs):
            self.run_calls += 1
            raise RuntimeError("synthetic_pre_cutover_failure")

    service, store, recovery, _lock, preflight, confirmed = _approved_service(
        AlwaysFailRecovery()
    )
    kwargs = _run_args(preflight, confirmed)
    with pytest.raises(GlobalDiscoveryRecoveryError):
        service.run(**kwargs)
    run_id = (
        f"gdr_{hashlib.sha256(kwargs['confirmation_id'].encode()).hexdigest()[:24]}"
    )
    receipt_key = GlobalDiscoveryRecoveryManifestStore.confirmation_receipt_key(run_id)
    assert store.delete_json(receipt_key)

    with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
        service.run(**kwargs)
    assert refused.value.code == "confirmation_refused"
    assert recovery.run_calls == 1
    status = GlobalDiscoveryRecoveryManifestStore(store).read_status(run_id)
    assert status is not None and status["state"] == "failed"
    assert status["physical_cutover_started"] is True


def test_proven_prephysical_failure_releases_fence_without_receipt(monkeypatch):
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterLease,
    )

    service, store, recovery, _lock, preflight, confirmed = _approved_service()

    def fail_before_physical_phase(_cls, **_kwargs):
        raise RuntimeError("synthetic_lease_failure")

    with monkeypatch.context() as scoped:
        scoped.setattr(
            GlobalDiscoveryWriterLease,
            "acquire",
            classmethod(fail_before_physical_phase),
        )
        with pytest.raises(GlobalDiscoveryRecoveryError):
            service.run(**_run_args(preflight, confirmed))

    confirmation_id = str(confirmed["confirmation_id"])
    run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    status = manifests.read_status(run_id)
    assert status is not None and status["state"] == "failed"
    assert status["physical_cutover_started"] is False
    assert store.delete_json(manifests.confirmation_receipt_key(run_id))
    preserved = manifests.read_status(run_id)
    with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
        service.run(**_run_args(preflight, confirmed))
    assert refused.value.code == "confirmation_refused"
    assert manifests.read_status(run_id) == preserved

    next_preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    next_confirmation = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(next_preflight["manifest_ref"]),
        preflight_hash=str(next_preflight["preflight_hash"]),
    )
    result = service.run(**_run_args(next_preflight, next_confirmation))

    assert result["state"] == "delivery_pending"
    assert recovery.run_calls == 1


@pytest.mark.parametrize(
    "durable_state", ["delivery_pending", "completed", "rolled_back"]
)
def test_missing_receipt_never_erases_post_cutover_truth(durable_state):
    recovery = _Recovery(
        outcome="rolled_back" if durable_state == "rolled_back" else "completed"
    )
    service, store, recovery, _lock, preflight, confirmed = _approved_service(recovery)
    kwargs = _run_args(preflight, confirmed)
    result = service.run(**kwargs)
    if durable_state == "completed":
        result = service.finalize_delivery(
            run_id=str(result["run_id"]),
            actor_id="agent-1",
            manifest_ref=str(preflight["manifest_ref"]),
            preflight_hash=str(preflight["preflight_hash"]),
            delivery={"committed": True},
        )
    assert result["state"] == durable_state
    run_id = str(result["run_id"])
    manifests = GlobalDiscoveryRecoveryManifestStore(store)
    before = manifests.read_status(run_id)
    assert before is not None
    assert store.delete_json(manifests.confirmation_receipt_key(run_id))

    with pytest.raises(GlobalDiscoveryRecoveryError) as refused:
        service.run(**kwargs)

    assert refused.value.code == "confirmation_refused"
    assert manifests.read_status(run_id) == before
    assert recovery.run_calls == 1


def test_post_cutover_status_write_failure_retains_evidence_and_exact_resume():
    class FailDeliveryStatusOnceStore(InMemoryRebuildAuditArtifactStore):
        failed = False

        def write_json_atomic(self, key, payload):
            if (
                not self.failed
                and key.namespace == "global_discovery_recovery"
                and str(key.artifact_id).startswith("status_")
                and payload.get("state") == "delivery_pending"
            ):
                self.failed = True
                raise OSError("synthetic_delivery_status_write_failure")
            return super().write_json_atomic(key, payload)

    store = FailDeliveryStatusOnceStore()
    recovery = _Recovery()
    service = GlobalDiscoveryRecoveryService(
        recovery=recovery,
        artifact_store=store,
        single_writer_lock=_Lock(),
    )
    preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    confirmed = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
    )
    kwargs = _run_args(preflight, confirmed)

    with pytest.raises(GlobalDiscoveryRecoveryError):
        service.run(**kwargs)
    run_id = (
        f"gdr_{hashlib.sha256(kwargs['confirmation_id'].encode()).hexdigest()[:24]}"
    )
    status = GlobalDiscoveryRecoveryManifestStore(store).read_status(run_id)
    assert status is not None and status["state"] == "failed"
    assert status["physical_cutover_started"] is True
    assert status["physical_cutover_outcome"] == "completed"
    assert status["candidate_sha256"] == "candidate-sha"
    assert status["quarantine_ref"] == "quarantine:1"

    other_preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    other_confirmation = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(other_preflight["manifest_ref"]),
        preflight_hash=str(other_preflight["preflight_hash"]),
    )
    with pytest.raises(GlobalDiscoveryRecoveryError) as fenced:
        service.run(**_run_args(other_preflight, other_confirmation))
    assert fenced.value.code == "recovery_in_progress"

    resumed = service.run(
        **{
            **kwargs,
            "health_evidence": _health(discovery_state="healthy"),
        }
    )

    assert resumed["state"] == "delivery_pending"
    assert recovery.run_calls == 2


def test_concurrent_same_confirmation_runs_cutover_once_and_replay_from_receipt():
    entered = threading.Event()
    recovery = _Recovery(entered=entered)
    service, store, recovery, _lock, preflight, confirmed = _approved_service(recovery)
    kwargs = _run_args(preflight, confirmed)
    outcomes: list[object] = []

    def invoke() -> None:
        try:
            outcomes.append(service.run(**kwargs))
        except Exception as exc:  # noqa: BLE001 - concurrency evidence captures outcome
            outcomes.append(exc)

    first = threading.Thread(target=invoke)
    first.start()
    assert entered.wait(timeout=5)
    second = threading.Thread(target=invoke)
    second.start()
    second.join(timeout=1)
    recovery.release.set()
    first.join(timeout=5)

    assert recovery.run_calls == 1
    assert sum(isinstance(row, dict) for row in outcomes) == 1
    errors = [row for row in outcomes if isinstance(row, Exception)]
    assert len(errors) == 1
    assert getattr(errors[0], "code", None) in {
        "recovery_in_progress",
        "recovery_lock_contention",
    }
    replay = service.run(**kwargs)
    assert replay["state"] == "delivery_pending"
    run_id = str(replay["run_id"])
    assert (
        store.read_json(
            GlobalDiscoveryRecoveryManifestStore.confirmation_receipt_key(run_id)
        )
        is not None
    )


@pytest.mark.parametrize("health_after_owner", ["recovery_needed", "healthy"])
def test_preconsume_contention_leaves_confirmation_and_status_untouched(
    health_after_owner,
):
    service, store, recovery, _lock, preflight, confirmed = _approved_service()
    owner = service.run(**_run_args(preflight, confirmed))
    contender_preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    contender_confirmation = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(contender_preflight["manifest_ref"]),
        preflight_hash=str(contender_preflight["preflight_hash"]),
    )
    contender_id = str(contender_confirmation["confirmation_id"])
    contender_run_id = f"gdr_{hashlib.sha256(contender_id.encode()).hexdigest()[:24]}"
    contender_token_key = service._confirmation._key(contender_id)  # noqa: SLF001
    manifests = GlobalDiscoveryRecoveryManifestStore(store)

    with pytest.raises(GlobalDiscoveryRecoveryError) as contention:
        service.run(**_run_args(contender_preflight, contender_confirmation))

    assert contention.value.code == "recovery_in_progress"
    assert manifests.read_status(contender_run_id) is None
    assert store.read_json(contender_token_key) is not None
    assert store.read_json(manifests.confirmation_receipt_key(contender_run_id)) is None

    service.finalize_delivery(
        run_id=str(owner["run_id"]),
        actor_id="agent-1",
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
        delivery={"committed": True},
    )
    retry_args = {
        **_run_args(contender_preflight, contender_confirmation),
        "health_evidence": _health(discovery_state=health_after_owner),
    }
    if health_after_owner == "recovery_needed":
        assert service.run(**retry_args)["state"] == "delivery_pending"
        assert recovery.run_calls == 2
    else:
        with pytest.raises(GlobalDiscoveryRecoveryError) as not_admitted:
            service.run(**retry_args)
        assert not_admitted.value.code == "discovery_recovery_not_admitted"
        assert manifests.read_status(contender_run_id) is None
        assert store.read_json(contender_token_key) is not None
        assert (
            store.read_json(manifests.confirmation_receipt_key(contender_run_id))
            is None
        )
        assert recovery.run_calls == 1


def test_ambiguous_failed_run_blocks_different_recovery_until_same_run_resumes():
    class FailOnceRecovery(_Recovery):
        def rebuild_candidate_and_cutover(self, **kwargs):
            if self.run_calls == 0:
                self.run_calls += 1
                raise RuntimeError("synthetic_pre_cutover_failure")
            return super().rebuild_candidate_and_cutover(**kwargs)

    recovery = FailOnceRecovery()
    service, _store, _recovery, _lock, preflight, confirmed = _approved_service(
        recovery
    )
    first = {
        "actor_id": "agent-1",
        "confirmation_id": str(confirmed["confirmation_id"]),
        "manifest_ref": str(preflight["manifest_ref"]),
        "preflight_hash": str(preflight["preflight_hash"]),
        "boards": _boards(),
        "health_evidence": _health(),
        "candidate_boards": _seeds(),
        "reason": "repair corrupt global discovery WAL",
    }
    with pytest.raises(GlobalDiscoveryRecoveryError):
        service.run(**first)

    second_preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    second_confirmed = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(second_preflight["manifest_ref"]),
        preflight_hash=str(second_preflight["preflight_hash"]),
    )
    with pytest.raises(GlobalDiscoveryRecoveryError) as ambiguous:
        service.run(
            **{
                **first,
                "confirmation_id": str(second_confirmed["confirmation_id"]),
                "manifest_ref": str(second_preflight["manifest_ref"]),
                "preflight_hash": str(second_preflight["preflight_hash"]),
            }
        )
    assert ambiguous.value.code == "recovery_in_progress"

    resumed = service.run(**first)
    assert resumed["state"] == "delivery_pending"

    third_preflight = service.preflight(
        actor_id="agent-1", boards=_boards(), health_evidence=_health()
    )
    third_confirmed = service.confirm(
        actor_id="agent-1",
        manifest_ref=str(third_preflight["manifest_ref"]),
        preflight_hash=str(third_preflight["preflight_hash"]),
    )
    from okto_pulse.core.kg.global_discovery_recovery import _active_run

    _active_run.clear()  # simulate a fresh process; durable pending still fences
    with pytest.raises(GlobalDiscoveryRecoveryError) as contention:
        service.run(
            **{
                **first,
                "confirmation_id": str(third_confirmed["confirmation_id"]),
                "manifest_ref": str(third_preflight["manifest_ref"]),
                "preflight_hash": str(third_preflight["preflight_hash"]),
            }
        )
    assert contention.value.code == "recovery_in_progress"


def test_successful_rollback_is_a_distinct_bounded_terminal_status():
    service, store, recovery, _lock, preflight, confirmed = _approved_service(
        _Recovery(outcome="rolled_back")
    )
    result = service.run(
        actor_id="agent-1",
        confirmation_id=str(confirmed["confirmation_id"]),
        manifest_ref=str(preflight["manifest_ref"]),
        preflight_hash=str(preflight["preflight_hash"]),
        boards=_boards(),
        health_evidence=_health(),
        candidate_boards=_seeds(),
        reason="repair corrupt global discovery WAL",
    )
    assert result["state"] == "rolled_back"
    assert result["rollback_performed"] is True
    assert result["failure_code"] == ("global_discovery_post_cutover_readback_failed")
    assert "local" not in str(result)
    status = GlobalDiscoveryRecoveryManifestStore(store).read_status(
        str(result["run_id"])
    )
    assert status is not None and status["state"] == "rolled_back"
    assert recovery.run_calls == 1


@pytest.mark.asyncio
async def test_cancellation_waits_for_native_cutover_and_persists_terminal_status():
    entered = threading.Event()
    recovery = _Recovery(entered=entered)
    service, store, _recovery, _lock, preflight, confirmed = _approved_service(recovery)
    confirmation_id = str(confirmed["confirmation_id"])
    task = asyncio.create_task(
        run_blocking_graph_io(
            lambda: service.run(
                actor_id="agent-1",
                confirmation_id=confirmation_id,
                manifest_ref=str(preflight["manifest_ref"]),
                preflight_hash=str(preflight["preflight_hash"]),
                boards=_boards(),
                health_evidence=_health(),
                candidate_boards=_seeds(),
                reason="repair corrupt global discovery WAL",
            ),
            task_name="test.global.discovery.recovery",
        )
    )
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    recovery.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    run_id = f"gdr_{hashlib.sha256(confirmation_id.encode()).hexdigest()[:24]}"
    status = GlobalDiscoveryRecoveryManifestStore(store).read_status(run_id)
    assert status is not None
    assert status["state"] == "delivery_pending"
    assert status["outcome"] == "completed"


def test_scope_is_explicitly_global():
    assert GLOBAL_RECOVERY_SCOPE == "_global"


def test_recovery_registry_slot_fails_closed_when_not_composed():
    from okto_pulse.core.composition import RuntimeProviderMissing
    from okto_pulse.core.kg.interfaces.registry import KGProviderRegistry

    with pytest.raises(RuntimeProviderMissing):
        KGProviderRegistry().require_global_discovery_recovery()


def test_workflow_adds_candidate_before_similarity_and_proposal():
    from pathlib import Path

    workflow = (
        Path(__file__).parents[1] / "src/okto_pulse/core/mcp/resources/workflows/kg.md"
    ).read_text(encoding="utf-8")
    block = workflow.split("**Consolidation workflow:**", 1)[1].split("```", 2)[1]
    add_at = block.index("okto_pulse_kg_add_node_candidate")
    similar_at = block.index("okto_pulse_kg_get_similar_nodes")
    propose_at = block.index("okto_pulse_kg_propose_reconciliation")
    assert add_at < similar_at < propose_at
    assert "candidate_not_found" in workflow


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        ({"state": "delivery_pending"}, True),
        ({"state": "completed"}, False),
        ({"state": "rolled_back"}, False),
        ({"state": "failed"}, False),
        (None, False),
    ],
)
def test_cancel_cleanup_reopens_delivery_only_after_completed_cutover(status, expected):
    from okto_pulse.core.mcp.server import (
        _global_recovery_should_finish_delivery_after_cancel,
    )

    assert _global_recovery_should_finish_delivery_after_cancel(status) is expected


def test_delivery_uses_manifest_board_ids_when_current_inventory_changed():
    from okto_pulse.core.mcp.server import (
        _global_recovery_delivery_board_pairs,
    )

    assert _global_recovery_delivery_board_pairs(
        {"board_ids": ["b", "a", "b"]}, [("current", "Current", "Summary")]
    ) == [("a", "a", ""), ("b", "b", "")]


@pytest.mark.asyncio
async def test_async_delivery_cleanup_drains_repeated_cancellation():
    from okto_pulse.core.mcp.server import _global_recovery_drain_async

    entered = asyncio.Event()
    release = asyncio.Event()
    completed = False

    async def operation():
        nonlocal completed
        entered.set()
        await release.wait()
        completed = True
        return "done"

    task = asyncio.create_task(
        _global_recovery_drain_async(operation, task_name="test.delivery.drain")
    )
    await entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert completed is True


@pytest.mark.asyncio
async def test_outbox_writer_uses_same_durable_global_fence():
    from coordination_fakes import FakeWriteLockPort
    from okto_pulse.core.application.processors.global_outbox import (
        GlobalOutboxProcessor,
    )
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterLease,
    )
    from okto_pulse.core.ports.coordination import (
        get_write_lock_port,
        register_coordination_providers,
    )

    previous_port = get_write_lock_port()
    register_coordination_providers(write_lock_port=FakeWriteLockPort())
    recovery_lease = GlobalDiscoveryWriterLease.acquire(
        operation="global_discovery_recovery",
        owner_id="recovery",
    )
    try:
        processor = GlobalOutboxProcessor()
        assert await processor.process_once() == 0
    finally:
        recovery_lease.release()
        register_coordination_providers(write_lock_port=previous_port)


def test_global_writer_guard_rejects_an_expired_exact_fencing_token():
    from coordination_fakes import FakeWriteLockPort
    from okto_pulse.core.kg.global_discovery_writer import (
        GlobalDiscoveryWriterFenceLost,
        GlobalDiscoveryWriterLease,
    )
    from okto_pulse.core.kg.single_writer_lock import KGSingleWriterLock

    port = FakeWriteLockPort()
    lease = GlobalDiscoveryWriterLease.acquire(
        operation="global_discovery_recovery",
        owner_id="recovery",
        lock=KGSingleWriterLock(write_lock_port=port),
    )
    for manifest in port._single_writer_locks.values():  # noqa: SLF001
        manifest["expires_at_epoch"] = 0.0
    with pytest.raises(GlobalDiscoveryWriterFenceLost):
        with lease.guard():
            pass


@pytest.mark.asyncio
async def test_global_recovery_authorization_fails_closed(monkeypatch):
    from okto_pulse.core.mcp import server

    async def unauthenticated():
        return None

    monkeypatch.setattr(server, "_get_global_agent_ctx", unauthenticated)
    ctx, error = await server._global_recovery_authorize()
    assert ctx is None
    assert "auth" in error.lower() or "unauthorized" in error.lower()

    async def underprivileged():
        return server.AgentContext("agent-1", "Agent", "", [])

    monkeypatch.setattr(server, "_get_global_agent_ctx", underprivileged)
    ctx, error = await server._global_recovery_authorize()
    assert ctx is None
    assert "permission" in error.lower()

    async def admin():
        return server.AgentContext(
            "agent-1",
            "Agent",
            "",
            ["kg.admin.historical_consolidation"],
        )

    monkeypatch.setattr(server, "_get_global_agent_ctx", admin)
    ctx, error = await server._global_recovery_authorize()
    assert ctx is not None and error is None


@pytest.mark.asyncio
async def test_board_rebuild_preflight_redirects_discovery_only_recovery(
    monkeypatch,
):
    import json
    from types import SimpleNamespace

    from okto_pulse.core.application.use_cases import RebuildAdmissionGateUseCase
    from okto_pulse.core.mcp import server

    async def board_ctx(_board_id):
        return server.AgentContext("agent-1", "Agent", "board-1", [])

    async def execute(_self, _command, **_kwargs):
        return SimpleNamespace(
            refusal=None,
            raw_health={
                "graph_state": "healthy",
                "discovery_state": "recovery_needed",
                "discovery_recovery_required": True,
            },
        )

    class Uow:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

    monkeypatch.setattr(server, "_get_agent_ctx", board_ctx)
    monkeypatch.setattr(RebuildAdmissionGateUseCase, "execute", execute)
    monkeypatch.setattr(
        server,
        "get_unit_of_work_factory_for_mcp",
        lambda: lambda **_kwargs: Uow(),
    )
    tool = server.mcp._tool_manager._tools["okto_pulse_kg_rebuild_preflight"].fn
    result = json.loads(await tool(board_id="board-1"))
    assert result["error"] == "board_rebuild_wrong_recovery_scope"
    assert result["outcome"] == "redirected"
    assert result["action_required"] == (
        "call_okto_pulse_kg_global_discovery_recovery_preflight"
    )


class _OutboxStore:
    def __init__(self, rows):
        self.rows = rows
        self.saved = []
        self.commit_calls = 0

    async def list_dead_letters(self, _context, *, after=None, **_kwargs):
        return tuple(self.rows) if after is None else ()

    async def save_events(self, _context, events):
        self.saved.extend(events)

    async def commit(self, _context):
        self.commit_calls += 1


def _outbox_row(event_id: str, error: str):
    from okto_pulse.core.ports.global_outbox import GlobalOutboxEventRecord

    now = datetime.now(timezone.utc)
    return GlobalOutboxEventRecord(
        id=event_id,
        event_id=event_id,
        board_id="board-1",
        session_id=None,
        payload={},
        retry_count=-1,
        last_error=error,
        processed_at=None,
        created_at=now,
    )


@pytest.mark.asyncio
async def test_delivery_requeues_only_global_open_dlq_and_leaves_commit_to_uow(
    monkeypatch,
):
    from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations
    from okto_pulse.core.kg import canonical_demotion_global_sync as sync_module
    from okto_pulse.core.ports import global_outbox as outbox_module

    global_open = _outbox_row("global", "graph_unavailable: bad WAL")
    semantic = _outbox_row("semantic", "candidate_not_found")
    store = _OutboxStore([global_open, semantic])
    enqueued = []

    async def enqueue(_context, *, board_id, reason, idempotency_key):
        enqueued.append((board_id, reason, idempotency_key))
        return {"enqueued": True}

    monkeypatch.setattr(outbox_module, "get_global_outbox_store", lambda: store)
    monkeypatch.setattr(sync_module, "enqueue_digest_layer_reconciliation", enqueue)
    result = await CoreKnowledgeGraphOperations(
        object()
    ).recover_global_discovery_delivery(
        run_id="gdr_test",
        board_ids=["b2", "b1", "b1"],
        dead_letter_limit=100,
    )
    assert result["dead_letters_requeued"] == 1
    assert [row.event_id for row in store.saved] == ["global"]
    assert global_open.retry_count == 0 and global_open.last_error is None
    assert semantic.retry_count == -1 and semantic.last_error == "candidate_not_found"
    assert enqueued == [
        ("b1", "global_discovery_recovery", "gdr_test:b1"),
        ("b2", "global_discovery_recovery", "gdr_test:b2"),
    ]
    assert store.commit_calls == 0


@pytest.mark.asyncio
async def test_uow_seed_builder_requires_exact_captured_overlay_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations

    relational_context = object()
    calls: list[tuple[object, str, dict[str, str]]] = []

    async def build_seed(
        _self,
        db,
        *,
        board_id,
        captured_cognitive_pending_exclusions,
        **_kwargs,
    ):
        calls.append(
            (
                db,
                board_id,
                dict(captured_cognitive_pending_exclusions),
            )
        )
        return board_id

    monkeypatch.setattr(
        GlobalDiscoveryRecoveryBoardSeedService,
        "build_board_seed",
        build_seed,
    )
    operations = CoreKnowledgeGraphOperations(relational_context)

    with pytest.raises(ValueError, match="cover every recovery board"):
        await operations.build_global_discovery_recovery_seeds(
            boards=[("b1", "One", "Summary")],
            captured_cognitive_pending_exclusions={},
        )

    result = await operations.build_global_discovery_recovery_seeds(
        boards=[("b2", "Two", "Summary"), ("b1", "One", "Summary")],
        captured_cognitive_pending_exclusions={
            "b1": {"bug:1": "canonical_learning_working_only"},
            "b2": {},
        },
    )

    assert result == ("b1", "b2")
    assert calls == [
        (relational_context, "b1", {"bug:1": "canonical_learning_working_only"}),
        (relational_context, "b2", {}),
    ]


@pytest.mark.asyncio
async def test_delivery_failure_between_dlq_reset_and_enqueue_never_commits(
    monkeypatch,
):
    from okto_pulse.core.application.kg_operations import CoreKnowledgeGraphOperations
    from okto_pulse.core.kg import canonical_demotion_global_sync as sync_module
    from okto_pulse.core.ports import global_outbox as outbox_module

    row = _outbox_row("global", "graph_corruption: bad WAL")
    store = _OutboxStore([row])

    async def fail_enqueue(*_args, **_kwargs):
        raise RuntimeError("synthetic enqueue failure")

    monkeypatch.setattr(outbox_module, "get_global_outbox_store", lambda: store)
    monkeypatch.setattr(
        sync_module, "enqueue_digest_layer_reconciliation", fail_enqueue
    )
    with pytest.raises(RuntimeError, match="synthetic enqueue failure"):
        await CoreKnowledgeGraphOperations(object()).recover_global_discovery_delivery(
            run_id="gdr_test",
            board_ids=["b1"],
            dead_letter_limit=100,
        )
    # The enclosing UoW owns commit/rollback. This operation never commits a
    # half-step after staging the DLQ reset.
    assert store.commit_calls == 0

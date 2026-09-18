"""KG-01.4 — KGQuarantineService (FR7+FR10, contract api_ee77f56f, TR8, TR9).

Deterministic tests against the quarantine-before-purge boundary. No
real KG storage — the service operates on plain filesystem so we feed
it temp files and assert (a) the contract response shape, (b) manifest
fields per TR8, (c) scope validation per FR10, (d) counter labels per
OR or_05fd5cd3.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okto_pulse.core.kg.quarantine import (
    DEFAULT_RETENTION_DAYS,
    KGQuarantineService,
    MANIFEST_FILENAME,
    QUARANTINE_DIRNAME,
    QuarantineConfig,
    QuarantineError,
    QuarantineErrorCode,
    QuarantineReason,
    QuarantineResponse,
    get_quarantine_counter,
    get_quarantine_counter_labels,
    get_quarantine_counter_samples,
    reset_quarantine_counter,
)
from okto_pulse.core.kg.interfaces.storage_ref import StorageRef


def _storage_ref(path: Path) -> StorageRef:
    return StorageRef(str(path), "testing_local_storage")


@pytest.fixture(autouse=True)
def _reset_counter():
    reset_quarantine_counter()
    yield
    reset_quarantine_counter()


@pytest.fixture
def kg_root(tmp_path: Path) -> Path:
    """KG storage root with a board_graph subdir + a global_discovery dir."""
    root = tmp_path / "kg"
    (root / "boards" / "b1").mkdir(parents=True)
    (root / "global").mkdir(parents=True)
    return root


@pytest.fixture
def quarantine_base(tmp_path: Path) -> Path:
    base = tmp_path / "quarantine-store"
    base.mkdir()
    return base


@pytest.fixture
def service(kg_root: Path, quarantine_base: Path) -> KGQuarantineService:
    return KGQuarantineService(
        base_storage_ref_hint=_storage_ref(quarantine_base),
        scope_storage_refs=[
            _storage_ref(kg_root / "boards"),
            _storage_ref(kg_root / "global"),
        ],
    )


def _make_file(path: Path, content: str = "fake-lbug") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


# --- FR7 / api_ee77f56f: create happy path ------------------------------------


def test_create_moves_files_and_writes_manifest(
    service: KGQuarantineService, kg_root: Path, quarantine_base: Path,
):
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug", "corrupted-bytes")
    sidecar = _make_file(kg_root / "boards" / "b1" / "graph.lbug.wal", "wal")

    response = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src), _storage_ref(sidecar)],
        reason="corruption detected during open",
        correlation_ids=["corr-1", "corr-2"],
        kg_generation_id="gen-42",
    )

    assert isinstance(response, QuarantineResponse)
    assert response.quarantine_id.startswith("q_")
    assert response.files_moved == 2
    assert response.retention_until  # ISO timestamp string

    # Source files were actually moved (not copied).
    assert not src.exists()
    assert not sidecar.exists()

    # Quarantine dir contains the moved files + manifest.
    qroot = quarantine_base / QUARANTINE_DIRNAME / response.quarantine_id
    assert qroot.exists()
    assert (qroot / "graph.lbug").read_text() == "corrupted-bytes"
    assert (qroot / "graph.lbug.wal").read_text() == "wal"
    assert (qroot / MANIFEST_FILENAME).exists()
    assert response.manifest_ref == str(qroot / MANIFEST_FILENAME)


def test_manifest_has_all_TR8_fields(
    service: KGQuarantineService, kg_root: Path, quarantine_base: Path,
):
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    response = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src)],
        reason="orphaned lock cleanup",
        correlation_ids=["corr-x"],
        kg_generation_id="gen-7",
    )

    manifest_path = (
        quarantine_base / QUARANTINE_DIRNAME / response.quarantine_id / MANIFEST_FILENAME
    )
    data = json.loads(manifest_path.read_text(encoding="utf-8"))

    # TR8 mandatory fields:
    for field in (
        "quarantine_id",
        "board_id",
        "graph_type",
        "reason",
        "reason_bucket",
        "correlation_ids",
        "kg_generation_id",
        "software_version",
        "quarantined_at",
        "retention_until",
        "files_moved",
    ):
        assert field in data, f"manifest missing TR8 field {field}"

    assert data["board_id"] == "b1"
    assert data["graph_type"] == "board_graph"
    assert data["kg_generation_id"] == "gen-7"
    assert data["correlation_ids"] == ["corr-x"]
    assert data["files_moved"] == 1
    assert data["reason_bucket"] == QuarantineReason.ORPHANED_LOCK.value


# --- FR10 / scope boundary -----------------------------------------------------


def test_path_outside_scope_is_rejected(
    service: KGQuarantineService, kg_root: Path, tmp_path: Path,
):
    """An app-data path outside the configured scope_roots MUST be rejected
    with affected_path_out_of_scope BEFORE any move happens."""
    outside = _make_file(tmp_path / "app-data" / "sensitive.db", "leak-target")

    with pytest.raises(QuarantineError) as excinfo:
        service.create(
            board_id="b1",
            graph_type="board_graph",
            affected_storage_refs=[_storage_ref(outside)],
            reason="corruption",
            correlation_ids=["corr-1"],
        )
    assert excinfo.value.code is QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE
    assert excinfo.value.retryable is False
    # The app-data file was NOT moved.
    assert outside.exists()
    assert outside.read_text() == "leak-target"
    assert get_quarantine_counter("b1", "out_of_scope") == 1


def test_partial_scope_violation_aborts_whole_request(
    service: KGQuarantineService, kg_root: Path, tmp_path: Path,
):
    """If ANY affected_path is out of scope, the WHOLE request is rejected —
    no partial quarantine. Otherwise app-data could be leaked alongside
    legitimate KG storage."""
    inside = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    outside = _make_file(tmp_path / "elsewhere" / "user.db")

    with pytest.raises(QuarantineError) as excinfo:
        service.create(
            board_id="b1",
            graph_type="board_graph",
            affected_storage_refs=[_storage_ref(inside), _storage_ref(outside)],
            reason="corruption detected",
            correlation_ids=["c1"],
        )
    assert excinfo.value.code is QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE
    # Both files are intact — abort happened before any move.
    assert inside.exists()
    assert outside.exists()


def test_unknown_graph_type_rejected(service: KGQuarantineService, kg_root: Path):
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    with pytest.raises(QuarantineError) as excinfo:
        service.create(
            board_id="b1",
            graph_type="bogus",
            affected_storage_refs=[_storage_ref(src)],
            reason="corruption",
            correlation_ids=["c1"],
        )
    assert excinfo.value.code is QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE
    assert src.exists()


def test_empty_affected_storage_refs_rejected(service: KGQuarantineService):
    with pytest.raises(QuarantineError) as excinfo:
        service.create(
            board_id="b1",
            graph_type="board_graph",
            affected_storage_refs=[],
            reason="corruption",
            correlation_ids=["c1"],
        )
    assert excinfo.value.code is QuarantineErrorCode.STORAGE_REF_OUT_OF_SCOPE


# --- TR9: retention ------------------------------------------------------------


def test_default_retention_is_30_days(
    service: KGQuarantineService, kg_root: Path,
):
    from datetime import datetime, timedelta, timezone

    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    response = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src)],
        reason="corruption",
        correlation_ids=["c1"],
    )
    retention = datetime.fromisoformat(response.retention_until)
    expected = datetime.now(timezone.utc) + timedelta(days=DEFAULT_RETENTION_DAYS)
    # Allow ±5min skew so the test isn't timing-flaky.
    assert abs((retention - expected).total_seconds()) < 300


def test_retention_under_safety_floor_rejected():
    with pytest.raises(ValueError):
        QuarantineConfig(retention_days=3)


def test_configurable_retention_is_honoured(
    kg_root: Path, quarantine_base: Path,
):
    from datetime import datetime, timedelta, timezone

    service = KGQuarantineService(
        base_storage_ref_hint=_storage_ref(quarantine_base),
        scope_storage_refs=[
            _storage_ref(kg_root / "boards"),
            _storage_ref(kg_root / "global"),
        ],
        config=QuarantineConfig(retention_days=14),
    )
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    response = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src)],
        reason="corruption",
        correlation_ids=["c1"],
    )
    retention = datetime.fromisoformat(response.retention_until)
    expected = datetime.now(timezone.utc) + timedelta(days=14)
    assert abs((retention - expected).total_seconds()) < 300


# --- OR or_05fd5cd3: counter shape --------------------------------------------


def test_kg_quarantine_total_carries_required_or_labels(
    service: KGQuarantineService, kg_root: Path,
):
    assert get_quarantine_counter_labels() == (
        "board_id", "graph_type", "outcome", "reason",
    )

    # Created — happy path with reason="corruption..."
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src)],
        reason="corruption detected",
        correlation_ids=["c1"],
    )

    # out_of_scope failure
    with pytest.raises(QuarantineError):
        service.create(
            board_id="b1",
            graph_type="bogus",
            affected_storage_refs=[_storage_ref(src)],
            reason="corruption",
            correlation_ids=["c1"],
        )

    samples = get_quarantine_counter_samples()
    keys = {(s["graph_type"], s["outcome"], s["reason"]) for s in samples}
    assert (
        "board_graph",
        "created",
        QuarantineReason.CORRUPTION_DETECTED.value,
    ) in keys
    assert (
        "bogus",
        "out_of_scope",
        QuarantineReason.UNKNOWN.value,
    ) in keys

    # Every sample carries 4 labels + count.
    for s in samples:
        for label in get_quarantine_counter_labels():
            assert label in s and isinstance(s[label], str) and s[label]
        assert isinstance(s["count"], int) and s["count"] >= 1


def test_reason_buckets_keep_label_cardinality_bounded(
    service: KGQuarantineService, kg_root: Path,
):
    """The counter must NOT carry free-text reason — operator strings
    are bucketed into the QuarantineReason enum."""
    for n, reason_text in enumerate([
        "Corruption detected at byte 42",
        "WAL truncation suspected",
        "Manual operator cleanup",
        "Something else entirely random",
    ]):
        src = _make_file(kg_root / "boards" / "b1" / f"graph.lbug.{n}")
        service.create(
            board_id="b1",
            graph_type="board_graph",
            affected_storage_refs=[_storage_ref(src)],
            reason=reason_text,
            correlation_ids=[f"c{n}"],
        )

    samples = get_quarantine_counter_samples()
    reasons = {s["reason"] for s in samples if s["outcome"] == "created"}
    # All reasons are within the enum vocabulary.
    enum_values = {r.value for r in QuarantineReason}
    assert reasons.issubset(enum_values)


# --- Inspect / list ------------------------------------------------------------


def test_inspect_returns_manifest_for_known_id(
    service: KGQuarantineService, kg_root: Path,
):
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    response = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src)],
        reason="corruption",
        correlation_ids=["c1"],
        kg_generation_id="gen-99",
    )
    manifest = service.inspect(response.quarantine_id)
    assert manifest is not None
    assert manifest.board_id == "b1"
    assert manifest.kg_generation_id == "gen-99"


def test_inspect_returns_none_for_unknown(service: KGQuarantineService):
    assert service.inspect("q_no_such_thing") is None


def test_list_active_returns_unexpired_manifests(
    service: KGQuarantineService, kg_root: Path,
):
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug")
    resp = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src)],
        reason="corruption",
        correlation_ids=["c1"],
    )
    active = service.list_active()
    assert any(m.quarantine_id == resp.quarantine_id for m in active)


# --- Concurrency safety --------------------------------------------------------


def test_two_creates_get_distinct_quarantine_ids(
    service: KGQuarantineService, kg_root: Path,
):
    src_a = _make_file(kg_root / "boards" / "b1" / "a.lbug")
    src_b = _make_file(kg_root / "boards" / "b1" / "b.lbug")
    resp_a = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src_a)],
        reason="corruption",
        correlation_ids=["a"],
    )
    resp_b = service.create(
        board_id="b1",
        graph_type="board_graph",
        affected_storage_refs=[_storage_ref(src_b)],
        reason="corruption",
        correlation_ids=["b"],
    )
    assert resp_a.quarantine_id != resp_b.quarantine_id


# --- Configuration sanity ------------------------------------------------------


def test_empty_scope_storage_refs_rejected(quarantine_base: Path):
    with pytest.raises(ValueError):
        KGQuarantineService(
            base_storage_ref_hint=_storage_ref(quarantine_base),
            scope_storage_refs=[],
        )


# --- val_79e6f555 rework: integration with real purge call sites --------------


def test_manifest_failed_preserves_evidence_as_partial(
    service: KGQuarantineService, kg_root: Path, quarantine_base: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    """val_79e6f555: rollback in manifest_failed must NOT rmtree the
    quarantine dir — preserved as `<id>.partial` so the operator can
    recover evidence."""
    src = _make_file(kg_root / "boards" / "b1" / "graph.lbug", "important-evidence")

    # Force json.dump to raise so the manifest write fails.
    import okto_pulse.core.kg.quarantine as module

    def boom_dump(*args, **kwargs):
        raise OSError("simulated disk full during manifest write")

    monkeypatch.setattr(module.json, "dump", boom_dump)

    with pytest.raises(QuarantineError) as excinfo:
        service.create(
            board_id="b1",
            graph_type="board_graph",
            affected_storage_refs=[_storage_ref(src)],
            reason="corruption",
            correlation_ids=["c1"],
        )
    assert excinfo.value.code is QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE

    # The quarantine dir was NOT rmtreed — it was renamed to .partial
    # so the operator has the evidence on disk.
    qroot = quarantine_base / QUARANTINE_DIRNAME
    partials = list(qroot.glob("*.partial*"))
    assert len(partials) >= 1, f"expected preserved partial dir, got {list(qroot.iterdir())}"
    # The original (corrupted-bytes) file is still inside.
    preserved_file = partials[0] / "graph.lbug"
    assert preserved_file.exists()
    assert preserved_file.read_text() == "important-evidence"




def _initialize_real_global_storage():
    """Publish the routed Global binding and bootstrap real storage.

    The routed Global graph is fail-closed until the composition's explicit
    initialization door runs (inside the durable writer fence), so tests that
    exercise purge/rebuild drive the real route instead of fabricating files.
    """

    from coordination_fakes import FakeWriteLockPort
    from global_graph_testing import (
        bootstrap_global_discovery,
        global_discovery_writer_scope,
    )
    from kg_schema_testing import graph_composition
    from okto_pulse.core.ports.coordination import (
        CoordinationProviderMissing,
        get_write_lock_port,
        register_coordination_providers,
    )

    try:
        get_write_lock_port()
    except CoordinationProviderMissing:
        register_coordination_providers(write_lock_port=FakeWriteLockPort())
    composition = graph_composition()
    with global_discovery_writer_scope(operation="test_global_route_init"):
        composition.initialize_global_route()
    bootstrap_global_discovery()
    return composition


def _global_active_path(composition):
    return composition.resolver.inspect_global_route().active_path


def _quarantine_manifests(kg_root: Path):
    return sorted(
        kg_root.glob(f"{QUARANTINE_DIRNAME}/*/{MANIFEST_FILENAME}")
    )


def test_purge_global_discovery_storage_goes_through_quarantine():
    """val_79e6f555 regression: purge_global_discovery_storage MUST move the
    global discovery storage (primary + sidecars) to quarantine first."""
    import global_graph_testing as gd_schema
    import okto_pulse.core.kg.quarantine as quarantine_module

    composition = _initialize_real_global_storage()
    active = _global_active_path(composition)
    assert active.exists()

    quarantine_module.reset_quarantine_counter()

    kg_root = composition.binding_store.root
    manifests_before = set(_quarantine_manifests(kg_root))

    removed = gd_schema.purge_global_discovery_storage(
        reason="corruption detected on open"
    )
    # The routed runtime reports no concrete paths across the port boundary —
    # the durable evidence is the quarantine manifest, not path strings.
    assert removed == []
    assert not active.exists()

    manifests = set(_quarantine_manifests(kg_root)) - manifests_before
    assert len(manifests) == 1
    manifest_data = json.loads(manifests.pop().read_text())
    assert manifest_data["graph_type"] == "global_discovery"
    assert manifest_data["reason"] == "corruption detected on open"
    assert (
        quarantine_module.get_quarantine_counter("_global", "created")
        == 1
    )


def test_rebuild_from_scratch_quarantines_discovery_before_drop():
    """val_b15ff42f residual regression: clustering.rebuild_from_scratch
    used to call shutil.rmtree + unlink on discovery storage directly. It
    MUST route through the runtime purge so quarantine is created (with a
    manifest) before any drop, and no ad-hoc ``discovery_backup_*`` copy is
    produced. Driven against the real routed Global storage: (a) the
    pre-existing graph storage is quarantined, (b) a manifest documents it,
    (c) NO ``discovery_backup_*`` artifact exists, (d) bootstrap
    re-materializes a fresh graph.
    """
    import okto_pulse.core.kg.global_discovery.clustering as clustering
    import okto_pulse.core.kg.quarantine as quarantine_module

    composition = _initialize_real_global_storage()
    active = _global_active_path(composition)
    assert active.exists()

    quarantine_module.reset_quarantine_counter()
    kg_root = composition.binding_store.root
    manifests_before = set(_quarantine_manifests(kg_root))

    result = clustering.rebuild_from_scratch()

    assert result["status"] == "rebuilt"
    # The old ad-hoc backup field is gone — replaced by quarantined paths.
    assert "backup_path" not in result
    assert result["quarantined_storage_refs"] == ["global-discovery"]

    # Quarantine dir + manifest exist for the dropped generation.
    manifests = set(_quarantine_manifests(kg_root)) - manifests_before
    assert len(manifests) == 1
    manifest_data = json.loads(manifests.pop().read_text())
    assert manifest_data["graph_type"] == "global_discovery"
    assert (
        manifest_data["reason"].startswith("rebuild_from_scratch")
        or manifest_data["reason"] == "rebuild_from_scratch"
    )
    assert (
        quarantine_module.get_quarantine_counter("_global", "created") == 1
    )

    # No discovery_backup_* artifact created by the old code path.
    backup_artifacts = list(
        composition.binding_store.root.glob("discovery_backup_*")
    )
    assert backup_artifacts == [], (
        f"rebuild must not create ad-hoc backup; found {backup_artifacts}"
    )

    # Bootstrap re-materialized a fresh graph at the bound route.
    assert _global_active_path(composition).exists()


def test_purge_global_discovery_aborts_when_quarantine_fails():
    """Quarantine-before-purge is fail-closed: if the quarantine store cannot
    take the artifacts, the routed purge reports failure and the live global
    storage stays intact — it is never dropped un-quarantined."""
    import global_graph_testing as gd_schema
    import okto_pulse.core.kg.quarantine as quarantine_module

    composition = _initialize_real_global_storage()
    active = _global_active_path(composition)
    assert active.exists()

    kg_root = composition.binding_store.root
    manifests_before = set(_quarantine_manifests(kg_root))

    monkey = pytest.MonkeyPatch()

    def boom_create(self, **kwargs):
        raise quarantine_module.QuarantineError(
            quarantine_module.QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
            retryable=True,
            reason="simulated",
        )

    monkey.setattr(
        quarantine_module.KGQuarantineService, "create", boom_create
    )

    try:
        removed = gd_schema.purge_global_discovery_storage(reason="corruption")
        assert removed == []
        # The live global storage survives the failed quarantine and no new
        # quarantine manifest appeared.
        assert active.exists()
        assert set(_quarantine_manifests(kg_root)) == manifests_before
    finally:
        monkey.undo()

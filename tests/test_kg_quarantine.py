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


def test_purge_board_graph_storage_goes_through_quarantine(tmp_path: Path):
    """val_79e6f555 regression: purge_board_graph_storage MUST move
    files to quarantine first, never unlink directly."""
    import kg_schema_testing as schema_module
    import okto_pulse.community.adapters.kg_runtime as kg_runtime

    storage_root = tmp_path / "okto-data" / "boards"
    storage_root.mkdir(parents=True)

    def fake_board_kuzu_path(board_id: str) -> Path:
        d = storage_root / board_id
        d.mkdir(parents=True, exist_ok=True)
        return d / "graph.lbug"

    # Replace the path resolver so the quarantine service sees the
    # right scope_roots (tmp_path/okto-data/boards).
    import okto_pulse.core.kg.quarantine as quarantine_module
    quarantine_module.reset_quarantine_counter()
    monkey = pytest.MonkeyPatch()
    monkey.setattr(kg_runtime, "board_kuzu_path", fake_board_kuzu_path)
    monkey.setattr(kg_runtime, "close_board_db_cache", lambda *_: None)

    try:
        primary = fake_board_kuzu_path("b1")
        primary.write_text("corrupted-graph-bytes")
        sidecar = primary.parent / "graph.lbug.wal"
        sidecar.write_text("wal-bytes")

        removed = schema_module.purge_board_graph_storage(
            "b1", reason="corruption detected"
        )
        # Files are no longer at the original location.
        assert not primary.exists()
        assert not sidecar.exists()
        # purge returned the original paths it cleared.
        assert len(removed) == 2

        # Quarantine dir under storage root contains the files + manifest.
        quarantine_root = (
            storage_root.parent / quarantine_module.QUARANTINE_DIRNAME
        )
        assert quarantine_root.exists()
        qdirs = list(quarantine_root.iterdir())
        assert len(qdirs) == 1
        manifest = qdirs[0] / quarantine_module.MANIFEST_FILENAME
        assert manifest.exists()
        body = json.loads(manifest.read_text())
        assert body["board_id"] == "b1"
        assert body["graph_type"] == "board_graph"
        assert body["files_moved"] == 2
        # Counter emitted for created outcome.
        assert quarantine_module.get_quarantine_counter("b1", "created") == 1
    finally:
        monkey.undo()


def test_purge_board_graph_storage_aborts_when_quarantine_fails(tmp_path: Path):
    """val_79e6f555 regression: if KGQuarantineService.create raises,
    purge MUST be aborted and the original files preserved."""
    import kg_schema_testing as schema_module
    import okto_pulse.community.adapters.kg_runtime as kg_runtime
    import okto_pulse.core.kg.quarantine as quarantine_module

    storage_root = tmp_path / "okto-data" / "boards"
    storage_root.mkdir(parents=True)

    def fake_board_kuzu_path(board_id: str) -> Path:
        d = storage_root / board_id
        d.mkdir(parents=True, exist_ok=True)
        return d / "graph.lbug"

    monkey = pytest.MonkeyPatch()
    monkey.setattr(kg_runtime, "board_kuzu_path", fake_board_kuzu_path)
    monkey.setattr(kg_runtime, "close_board_db_cache", lambda *_: None)
    # Make KGQuarantineService.create always raise.
    original_create = quarantine_module.KGQuarantineService.create

    def boom_create(self, **kwargs):
        raise quarantine_module.QuarantineError(
            quarantine_module.QuarantineErrorCode.QUARANTINE_STORAGE_UNAVAILABLE,
            retryable=True,
            reason="simulated disk full",
        )

    monkey.setattr(
        quarantine_module.KGQuarantineService, "create", boom_create
    )

    try:
        primary = fake_board_kuzu_path("b1")
        primary.write_text("evidence-bytes")
        sidecar = primary.parent / "graph.lbug.wal"
        sidecar.write_text("wal-bytes")

        removed = schema_module.purge_board_graph_storage(
            "b1", reason="corruption"
        )
        # FR7: purge aborted, evidence preserved.
        assert removed == []
        assert primary.exists()
        assert primary.read_text() == "evidence-bytes"
        assert sidecar.exists()
        assert sidecar.read_text() == "wal-bytes"
    finally:
        monkey.undo()
        # Restore the original method explicitly to keep other tests clean.
        quarantine_module.KGQuarantineService.create = original_create


def test_purge_global_discovery_storage_goes_through_quarantine(
    tmp_path: Path,
):
    """val_79e6f555 regression: purge_global_discovery_storage MUST move
    discovery.lbug + sidecars to quarantine first."""
    import global_graph_testing as gd_schema
    import okto_pulse.core.kg.quarantine as quarantine_module
    from okto_pulse.core.kg.interfaces import get_kg_registry

    storage_root = tmp_path / "okto-data" / "global"
    storage_root.mkdir(parents=True)
    primary = storage_root / "discovery.lbug"
    primary.write_text("global-evidence")
    sidecar = storage_root / "discovery.lbug.wal"
    sidecar.write_text("global-wal")

    monkey = pytest.MonkeyPatch()
    runtime = get_kg_registry().global_discovery_runtime
    monkey.setattr(runtime, "_global_graph_path", lambda: primary)
    monkey.setattr(runtime, "close", lambda: None)

    quarantine_module.reset_quarantine_counter()

    try:
        removed = gd_schema.purge_global_discovery_storage(
            reason="corruption detected on open"
        )

        assert not primary.exists()
        assert not sidecar.exists()
        assert len(removed) == 2

        quarantine_root = (
            storage_root.parent / quarantine_module.QUARANTINE_DIRNAME
        )
        assert quarantine_root.exists()
        qdirs = list(quarantine_root.iterdir())
        assert len(qdirs) == 1
        manifest_data = json.loads(
            (qdirs[0] / quarantine_module.MANIFEST_FILENAME).read_text()
        )
        assert manifest_data["graph_type"] == "global_discovery"
        assert manifest_data["files_moved"] == 2
        assert (
            quarantine_module.get_quarantine_counter("_global", "created")
            == 1
        )
    finally:
        monkey.undo()


def test_rebuild_from_scratch_quarantines_discovery_before_drop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """val_b15ff42f residual regression: clustering.rebuild_from_scratch
    used to call shutil.rmtree + unlink on discovery.lbug and sidecars
    directly. After the rework it MUST route through
    purge_global_discovery_storage so quarantine is created before any
    drop. Test forces a scenario with a pre-existing discovery.lbug +
    sidecar and asserts (a) discovery.lbug is gone from the original
    location, (b) a quarantine dir with manifest exists, (c) NO
    ``discovery_backup_*`` artifact is created (manifest replaces the
    ad-hoc backup), (d) bootstrap is called and the new file is created.
    """
    import global_graph_testing as gd_schema
    import okto_pulse.core.kg.global_discovery.clustering as clustering
    import okto_pulse.core.kg.quarantine as quarantine_module
    from okto_pulse.core.kg.interfaces import get_kg_registry

    storage_root = tmp_path / "okto-data" / "global"
    storage_root.mkdir(parents=True)
    primary = storage_root / "discovery.lbug"
    primary.write_text("existing-discovery-bytes")
    sidecar = storage_root / "discovery.lbug.wal"
    sidecar.write_text("existing-wal-bytes")

    bootstrap_called = {"count": 0}

    def fake_bootstrap():
        bootstrap_called["count"] += 1
        # Simulate bootstrap recreating the primary file.
        primary.write_text("freshly-bootstrapped")
        return primary

    runtime = get_kg_registry().global_discovery_runtime
    monkeypatch.setattr(runtime, "_legacy_global_graph_path", lambda: primary)
    monkeypatch.setattr(runtime, "_global_graph_path", lambda: primary)
    monkeypatch.setattr(runtime, "close", lambda: None)
    monkeypatch.setattr(runtime, "bootstrap", fake_bootstrap)
    monkeypatch.setattr(gd_schema, "bootstrap_global_discovery", fake_bootstrap)
    # clustering resolves lifecycle through GlobalDiscoveryRuntime; keep the
    # schema patch only as a compatibility assertion for callers still using
    # the facade directly.

    quarantine_module.reset_quarantine_counter()

    result = clustering.rebuild_from_scratch()

    assert result["status"] == "rebuilt"
    # The old ad-hoc backup field is gone — replaced by quarantined paths.
    assert "backup_path" not in result
    assert result["quarantined_storage_refs"] == ["global-discovery"]

    # bootstrap was actually called.
    assert bootstrap_called["count"] == 1
    # Fresh discovery.lbug exists, with new content from bootstrap.
    assert primary.exists()
    assert primary.read_text() == "freshly-bootstrapped"
    # No discovery_backup_* artifact created by the old code path.
    backup_artifacts = list(storage_root.glob("discovery_backup_*"))
    assert backup_artifacts == [], (
        f"rebuild must not create ad-hoc backup; found {backup_artifacts}"
    )

    # Quarantine dir + manifest exist with the original bytes preserved.
    quarantine_root = (
        storage_root.parent / quarantine_module.QUARANTINE_DIRNAME
    )
    qdirs = list(quarantine_root.iterdir())
    assert len(qdirs) == 1
    manifest = qdirs[0] / quarantine_module.MANIFEST_FILENAME
    assert manifest.exists()
    body = json.loads(manifest.read_text())
    assert body["graph_type"] == "global_discovery"
    assert body["reason"].startswith("rebuild_from_scratch") or body[
        "reason"
    ] == "rebuild_from_scratch"
    assert body["files_moved"] == 2

    # Both originals (with their original bytes) are now under quarantine.
    quarantined_primary = qdirs[0] / "discovery.lbug"
    quarantined_sidecar = qdirs[0] / "discovery.lbug.wal"
    assert quarantined_primary.exists()
    # NOTE: the primary path was reused by bootstrap, but the bytes
    # under quarantine match the ORIGINAL pre-rebuild content.
    assert quarantined_primary.read_text() == "existing-discovery-bytes"
    assert quarantined_sidecar.read_text() == "existing-wal-bytes"


def test_purge_global_discovery_aborts_when_quarantine_fails(tmp_path: Path):
    import global_graph_testing as gd_schema
    import okto_pulse.core.kg.quarantine as quarantine_module
    from okto_pulse.core.kg.interfaces import get_kg_registry

    storage_root = tmp_path / "okto-data" / "global"
    storage_root.mkdir(parents=True)
    primary = storage_root / "discovery.lbug"
    primary.write_text("global-evidence")

    monkey = pytest.MonkeyPatch()
    runtime = get_kg_registry().global_discovery_runtime
    monkey.setattr(runtime, "_global_graph_path", lambda: primary)
    monkey.setattr(runtime, "close", lambda: None)

    original_create = quarantine_module.KGQuarantineService.create

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
        assert primary.exists()
        assert primary.read_text() == "global-evidence"
    finally:
        monkey.undo()
        quarantine_module.KGQuarantineService.create = original_create

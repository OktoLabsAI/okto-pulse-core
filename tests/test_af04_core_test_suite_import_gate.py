from __future__ import annotations

import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.core_test_suite_import_gate import (
    CoreTestCommunityImportClassification,
    DEFAULT_CORE_TEST_COMMUNITY_RUNTIME_DEPENDENCIES,
    community_runtime_dependency_skip_reason,
    find_core_test_community_runtime_dependency,
    run_core_test_community_import_gate,
    scan_core_test_community_imports,
)
from repository_checkout_testing import community_repo_for


CORE_REPO_ROOT = Path(__file__).resolve().parents[1]


def _classification(
    file: str,
    import_kind: str,
    imported: str,
    expected_count: int,
    action: str,
) -> CoreTestCommunityImportClassification:
    return CoreTestCommunityImportClassification(
        file=file,
        import_kind=import_kind,
        imported=imported,
        expected_count=expected_count,
        action=action,  # type: ignore[arg-type]
        owner="test-owner",
        justification="test classification",
    )


def test_af04_inventory_mode_classifies_every_real_community_import_site():
    report = scan_core_test_community_imports(CORE_REPO_ROOT / "tests")

    # The historical migration inventory is intentionally incomplete after the
    # REST and adapter ownership move. The gate must report those integration
    # imports rather than silently accepting new Core-test dependencies.
    assert report.status == "blocking"
    assert report.scanned_files > 500
    assert len(report.sites) > 400
    assert len({site.file for site in report.sites}) > 100
    assert report.blocking
    assert report.count_violations == ()
    assert {
        (
            "tests/test_kg_board_rebuild_adapter.py",
            "from",
            "okto_pulse.community.adapters",
        ),
        (
            "tests/test_kg_relevance_dynamic.py",
            "importorskip",
            "okto_pulse.community.adapters.kuzu_graph_store",
        ),
    }.issubset(
        {(site.file, site.import_kind, site.imported) for site in report.sites}
    )


def test_af04_gate_blocks_unclassified_indented_imports_and_importorskip(tmp_path: Path):
    (tmp_path / "bad.py").write_text(
        'TEXT = "from okto_pulse.community.adapters import kg_runtime"\n'
        "def uses_community():\n"
        "    from okto_pulse.community.adapters import kg_runtime\n"
        "    pytest.importorskip('okto_pulse.community.adapters.kuzu_graph_store')\n",
        encoding="utf-8",
    )

    report = scan_core_test_community_imports(tmp_path, classifications=())

    assert report.status == "blocking"
    assert [(site.line, site.import_kind, site.imported) for site in report.blocking] == [
        (3, "from", "okto_pulse.community.adapters"),
        (4, "importorskip", "okto_pulse.community.adapters.kuzu_graph_store"),
    ]


def test_af04_gate_ignores_textual_community_mentions(tmp_path: Path):
    (tmp_path / "text_only.py").write_text(
        'DOC = "import okto_pulse.community.adapters.kg_runtime"\n'
        'fixture = "pytest.importorskip(\\"okto_pulse.community.adapters.kg_runtime\\")"\n',
        encoding="utf-8",
    )

    report = scan_core_test_community_imports(tmp_path, classifications=())

    assert report.status == "passed"
    assert report.sites == ()


def test_af04_r17_is_the_only_default_allowlisted_conformance_import(tmp_path: Path):
    tests_dir = tmp_path / "tests"
    tests_dir.mkdir()
    (tests_dir / "test_r17_core_settings_defaults_gate.py").write_text(
        "import pytest\n"
        "_community_config = pytest.importorskip('okto_pulse.community.config')\n"
        "CommunitySettings = _community_config.CommunitySettings\n",
        encoding="utf-8",
    )
    (tests_dir / "bad.py").write_text(
        "from okto_pulse.community.config import CommunitySettings\n",
        encoding="utf-8",
    )
    classifications = (
        _classification(
            "tests/test_r17_core_settings_defaults_gate.py",
            "importorskip",
            "okto_pulse.community.config",
            1,
            "allowlisted_conformance",
        ),
    )

    report = scan_core_test_community_imports(tests_dir, classifications=classifications)

    assert report.status == "blocking"
    assert [(site.file, site.action, site.allowed) for site in report.sites] == [
        ("tests/bad.py", None, False),
        ("tests/test_r17_core_settings_defaults_gate.py", "allowlisted_conformance", True),
    ]


def test_af04_strict_mode_blocks_inventory_until_migration_completes():
    gate = run_core_test_community_import_gate(CORE_REPO_ROOT / "tests", mode="strict")

    assert gate.status == "blocking"
    assert gate.observed_value > 400
    assert all(
        site["action"] != "allowlisted_conformance"
        for site in gate.evidence["blocking"]
    )


def test_af04_runtime_dependency_inventory_marks_whole_modules_and_mixed_nodes():
    module = find_core_test_community_runtime_dependency(
        "tests/test_kg_r2_imp1.py::test_reconcile_demotes_stale_deterministic_nodes_and_is_idempotent"
    )
    mixed = find_core_test_community_runtime_dependency(
        "tests/test_r09_global_discovery_runtime.py::test_query_global_uses_global_discovery_runtime_provider"
    )
    community_node = find_core_test_community_runtime_dependency(
        "tests/test_r09_global_discovery_runtime.py::test_community_global_discovery_bootstrap_with_token_runs_schema_ddl"
    )
    rkg04_ts2 = find_core_test_community_runtime_dependency(
        "tests/test_rkg04_connectivity_dlq_reprocess.py::test_ts2_connectivity_dlq_drains_to_graph"
    )
    barrier_bootstrap = find_core_test_community_runtime_dependency(
        "tests/test_kg_write_barrier_wiring.py::test_bootstrap_global_discovery_blocked_in_strict_without_guard"
    )
    barrier_purge = find_core_test_community_runtime_dependency(
        "tests/test_kg_write_barrier_wiring.py::test_purge_global_discovery_storage_blocked_in_strict_without_guard"
    )

    assert len(DEFAULT_CORE_TEST_COMMUNITY_RUNTIME_DEPENDENCIES) >= 50
    assert module is not None
    assert module.scope == "module"
    assert mixed is None
    assert community_node is not None
    assert community_node.scope == "node"
    assert community_node.adapter == "okto_pulse.community.adapters.global_discovery_runtime"
    assert rkg04_ts2 is not None
    assert rkg04_ts2.scope == "node"
    assert rkg04_ts2.adapter == "okto_pulse.community.adapters.kg_runtime"
    assert barrier_bootstrap is not None
    assert barrier_bootstrap.scope == "node"
    assert barrier_bootstrap.adapter == "okto_pulse.community.adapters.global_discovery_runtime"
    assert barrier_purge is not None
    assert barrier_purge.scope == "node"
    assert barrier_purge.adapter == "okto_pulse.community.adapters.global_discovery_runtime"


def test_af04_runtime_dependency_skip_reason_is_core_only_and_explicit():
    nodeid = (
        "tests/test_kg_tier_power.py::TestNLQuery::"
        "test_query_exact_fallback_finds_bug_without_vector_index_hit"
    )

    reason = community_runtime_dependency_skip_reason(
        nodeid,
        community_available=False,
    )

    assert reason is not None
    assert "Community KG integration adapter is unavailable" in reason
    assert "in-memory fakes" in reason
    assert (
        community_runtime_dependency_skip_reason(
            nodeid,
            community_available=True,
        )
        is None
    )
    assert (
        community_runtime_dependency_skip_reason(
            "tests/test_r1_budget_gate.py::test_r1_uses_shared_scanner_tool_descriptions_profile_and_live_passes",
            community_available=False,
        )
        is None
    )


def test_af04_conftest_prefers_checked_out_edition_source_trees():
    source = (CORE_REPO_ROOT / "tests" / "conftest.py").read_text(encoding="utf-8")

    assert "activate_repository_checkout_paths" in source
    assert "anchor_repo=_CORE_SRC.parent" in source
    assert "required=False" in source
    assert "sys.path.insert(0, _core_source_text)" in source


def test_af04_checked_out_core_and_community_modules_win_over_site_packages():
    try:
        community_src = community_repo_for(CORE_REPO_ROOT) / "src"
    except RuntimeError:
        pytest.skip("Community sibling source tree is not checked out")

    core_config = importlib.import_module("okto_pulse.core.infra.config")
    community_config = importlib.import_module("okto_pulse.community.config")
    core_origin = Path(core_config.__file__).resolve()
    community_origin = Path(community_config.__file__).resolve()

    assert core_origin.is_relative_to(CORE_REPO_ROOT / "src"), core_origin
    assert community_origin.is_relative_to(community_src), community_origin
    assert "site-packages" not in str(core_origin).lower()
    assert "site-packages" not in str(community_origin).lower()


def test_af04_boundary_gate_import_smoke_does_not_load_community_package():
    env = dict(os.environ)
    env["PYTHONPATH"] = str(CORE_REPO_ROOT / "src")
    code = (
        "import sys; "
        "from okto_pulse.core.application.boundary.core_test_suite_import_gate "
        "import scan_core_test_community_imports; "
        "scan_core_test_community_imports('missing-tests-root'); "
        "bad=[m for m in sys.modules if m == 'okto_pulse.community' "
        "or m.startswith('okto_pulse.community.')]; "
        "assert bad == [], bad"
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=CORE_REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
    )

    assert result.returncode == 0, result.stderr

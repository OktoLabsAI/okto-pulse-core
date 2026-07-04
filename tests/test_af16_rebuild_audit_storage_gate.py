from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.rebuild_audit_storage_gate import (
    run_rebuild_audit_storage_gate,
)


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def test_af16_rebuild_audit_storage_gate_current_tree_is_clean() -> None:
    assert run_rebuild_audit_storage_gate(CORE_ROOT) == ()


def test_af16_rebuild_audit_storage_gate_blocks_parallel_store(
    tmp_path: Path,
) -> None:
    root = tmp_path / "core"
    bad = root / "kg" / "bad_store.py"
    bad.parent.mkdir(parents=True)
    bad.write_text(
        "from typing import Protocol\n"
        "class DurableArtifactStore(Protocol):\n"
        "    pass\n",
        encoding="utf-8",
    )

    violations = run_rebuild_audit_storage_gate(root)

    assert {violation.rule for violation in violations} == {
        "parallel_durable_artifact_store",
    }
    assert violations[0].path == "kg/bad_store.py"


def test_af16_rebuild_audit_storage_gate_blocks_core_tempdir(
    tmp_path: Path,
) -> None:
    root = tmp_path / "core"
    bad = root / "kg" / "new_generation_store.py"
    bad.parent.mkdir(parents=True)
    bad.write_text(
        "import tempfile\n"
        "from pathlib import Path\n"
        "def build_path(board_id: str):\n"
        "    base = Path(tempfile.gettempdir()) / 'okto_pulse_kg_rebuild'\n"
        "    (base / board_id).mkdir(parents=True, exist_ok=True)\n"
        "    return base / board_id\n",
        encoding="utf-8",
    )

    violations = run_rebuild_audit_storage_gate(root)

    assert {violation.rule for violation in violations} == {
        "durable_tempdir_in_core",
    }
    assert violations[0].path == "kg/new_generation_store.py"


def test_af16_rebuild_audit_storage_gate_blocks_new_base_dir_path_consumer(
    tmp_path: Path,
) -> None:
    root = tmp_path / "core"
    bad = root / "kg" / "new_durable_store.py"
    bad.parent.mkdir(parents=True)
    bad.write_text(
        "from pathlib import Path\n"
        "class NewDurableStore:\n"
        "    def __init__(self, base_dir: Path) -> None:\n"
        "        self.base_dir = base_dir\n",
        encoding="utf-8",
    )

    violations = run_rebuild_audit_storage_gate(root)

    assert {violation.rule for violation in violations} == {
        "base_dir_path_consumer",
    }
    assert violations[0].path == "kg/new_durable_store.py"


def test_af16_rebuild_audit_storage_gate_blocks_retired_legacy_seam(
    tmp_path: Path,
) -> None:
    root = tmp_path / "core"
    seam = root / "kg" / "rebuild_audit.py"
    seam.parent.mkdir(parents=True)
    seam.write_text(
        "import os\n"
        "import tempfile\n"
        "from pathlib import Path\n"
        "def default_rebuild_base_dir() -> Path:\n"
        "    configured = os.getenv('OKTO_PULSE_REBUILD_BASE_DIR')\n"
        "    return Path(configured) if configured else Path(tempfile.gettempdir())\n",
        encoding="utf-8",
    )

    violations = run_rebuild_audit_storage_gate(root)

    assert {violation.rule for violation in violations} == {
        "durable_path_env_in_core",
        "durable_tempdir_in_core",
        "legacy_rebuild_base_dir_helper",
    }
    assert {violation.path for violation in violations} == {"kg/rebuild_audit.py"}

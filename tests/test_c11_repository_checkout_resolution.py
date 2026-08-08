from __future__ import annotations

import multiprocessing
import os
import sys
from pathlib import Path

import pytest

from okto_pulse.core.application.boundary.repository_checkout import (
    RepositoryCheckoutNotFound,
    activate_repository_checkout_paths,
    resolve_repository_checkout,
)


def _checkout(workspace: Path, name: str, edition: str) -> Path:
    repo = workspace / name
    marker = repo / "src" / "okto_pulse" / edition
    marker.mkdir(parents=True)
    (repo / "pyproject.toml").write_text(
        f'[project]\nname = "okto-pulse-{edition}"\nversion = "0"\n',
        encoding="utf-8",
    )
    return repo


def _capture_spawned_import_paths(queue) -> None:  # noqa: ANN001
    queue.put(
        {
            "sys_path": tuple(sys.path),
            "pythonpath": os.environ.get("PYTHONPATH", ""),
        }
    )


def test_c11_explicit_repository_override_wins_and_is_provenanced(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    anchor = _checkout(workspace, "okto-pulse-core", "core")
    _checkout(workspace, "okto-pulse", "community")
    configured = _checkout(tmp_path / "configured", "community-checkout", "community")

    resolved = resolve_repository_checkout(
        "community",
        anchor_repo=anchor,
        environ={"OKTO_PULSE_COMMUNITY_REPO": str(configured)},
    )

    assert resolved is not None
    assert resolved.repo_root == configured.resolve()
    assert resolved.selected_by == "OKTO_PULSE_COMMUNITY_REPO"
    assert resolved.checked == (str(configured.resolve()),)


def test_c11_current_name_wins_globally_before_legacy_name(
    tmp_path: Path,
) -> None:
    outer = tmp_path / "outer"
    nested = outer / "nested"
    anchor = _checkout(nested, "okto-pulse-core", "core")
    legacy = _checkout(nested, "okto_labs_pulse_community", "community")
    current = _checkout(outer, "okto-pulse", "community")

    resolved = resolve_repository_checkout(
        "community",
        anchor_repo=anchor,
        environ={},
    )

    assert resolved is not None
    assert resolved.repo_root == current.resolve()
    assert resolved.repo_root != legacy.resolve()
    assert resolved.selected_by.startswith("inferred-current:")


def test_c11_invalid_explicit_override_fails_closed(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    anchor = _checkout(workspace, "okto-pulse-core", "core")
    _checkout(workspace, "okto-pulse", "community")
    invalid = tmp_path / "not-a-checkout"
    invalid.mkdir()

    with pytest.raises(
        RepositoryCheckoutNotFound,
        match="OKTO_PULSE_COMMUNITY_REPO",
    ):
        resolve_repository_checkout(
            "community",
            anchor_repo=anchor,
            environ={"OKTO_PULSE_COMMUNITY_REPO": str(invalid)},
        )


def test_c11_optional_resolution_returns_none_only_without_override(
    tmp_path: Path,
) -> None:
    anchor = _checkout(tmp_path / "workspace", "okto-pulse-core", "core")

    assert (
        resolve_repository_checkout(
            "community",
            anchor_repo=anchor,
            environ={},
            required=False,
        )
        is None
    )


def test_c11_activation_removes_legacy_roots_from_parent_and_child_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = tmp_path / "workspace"
    current_core = _checkout(workspace, "okto-pulse-core", "core")
    current_community = _checkout(workspace, "okto-pulse", "community")
    legacy_core = _checkout(workspace, "okto_labs_pulse_core", "core")
    legacy_community = _checkout(
        workspace,
        "okto_labs_pulse_community",
        "community",
    )

    monkeypatch.setattr(
        sys,
        "path",
        [
            str(legacy_core / "src"),
            str(legacy_community / "src"),
            *sys.path,
        ],
    )
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join(
            (
                str(legacy_core / "src"),
                str(legacy_community / "src"),
            )
        ),
    )

    activation = activate_repository_checkout_paths(
        anchor_repo=current_core,
    )

    expected = (
        str((current_core / "src").resolve()),
        str((current_community / "src").resolve()),
    )
    assert tuple(sys.path[:2]) == expected
    assert activation.pythonpath[:2] == expected
    assert not any("okto_labs_pulse_" in value for value in sys.path)
    assert "okto_labs_pulse_" not in os.environ["PYTHONPATH"]

    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=_capture_spawned_import_paths, args=(queue,))
    process.start()
    process.join(timeout=20)
    assert process.exitcode == 0
    child = queue.get(timeout=5)
    queue.close()

    assert tuple(child["sys_path"][:2]) == expected
    assert "okto_labs_pulse_" not in os.pathsep.join(child["sys_path"])
    assert "okto_labs_pulse_" not in child["pythonpath"]

from __future__ import annotations

import importlib
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


def test_c11_hyphenated_anchor_family_wins_before_alternate_name(
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


@pytest.mark.parametrize(
    ("anchor_edition", "paired_edition"),
    (("core", "community"), ("community", "core")),
)
def test_c11_okto_labs_anchor_selects_its_paired_family_before_stale_sibling(
    tmp_path: Path,
    anchor_edition: str,
    paired_edition: str,
) -> None:
    workspace = tmp_path / "workspace"
    labs_names = {
        "core": "okto_labs_pulse_core",
        "community": "okto_labs_pulse_community",
    }
    hyphenated_names = {
        "core": "okto-pulse-core",
        "community": "okto-pulse",
    }
    anchor = _checkout(workspace, labs_names[anchor_edition], anchor_edition)
    expected = _checkout(workspace, labs_names[paired_edition], paired_edition)
    stale = _checkout(
        workspace,
        hyphenated_names[paired_edition],
        paired_edition,
    )

    resolved = resolve_repository_checkout(
        paired_edition,
        anchor_repo=anchor,
        environ={},
    )

    assert resolved is not None
    assert resolved.repo_root == expected.resolve()
    assert resolved.repo_root != stale.resolve()
    assert resolved.selected_by.startswith("inferred-legacy:")


def test_c11_anchor_family_falls_back_to_other_supported_layout(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    anchor = _checkout(workspace, "okto_labs_pulse_core", "core")
    compatible = _checkout(workspace, "okto-pulse", "community")

    resolved = resolve_repository_checkout(
        "community",
        anchor_repo=anchor,
        environ={},
    )

    assert resolved is not None
    assert resolved.repo_root == compatible.resolve()
    assert resolved.selected_by.startswith("inferred-current:")


def test_c11_workspace_override_preserves_anchor_family_precedence(
    tmp_path: Path,
) -> None:
    anchor = _checkout(
        tmp_path / "anchor-workspace",
        "okto_labs_pulse_core",
        "core",
    )
    configured_workspace = tmp_path / "configured-workspace"
    expected = _checkout(
        configured_workspace,
        "okto_labs_pulse_community",
        "community",
    )
    _checkout(configured_workspace, "okto-pulse", "community")

    resolved = resolve_repository_checkout(
        "community",
        anchor_repo=anchor,
        environ={"OKTO_PULSE_WORKSPACE_ROOT": str(configured_workspace)},
    )

    assert resolved is not None
    assert resolved.repo_root == expected.resolve()
    assert resolved.selected_by == "OKTO_PULSE_WORKSPACE_ROOT"


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
    # This test exercises inferred current-vs-legacy selection. The suite may
    # pin the real paired checkouts explicitly; those process-level overrides
    # must not replace the synthetic workspace under test.
    monkeypatch.delenv("OKTO_PULSE_CORE_REPO", raising=False)
    monkeypatch.delenv("OKTO_PULSE_COMMUNITY_REPO", raising=False)
    workspace = tmp_path / "workspace"
    current_core = _checkout(workspace, "okto-pulse-core", "core")
    current_community = _checkout(workspace, "okto-pulse", "community")
    probe_module = "c11_spawn_path_probe"
    (current_core / "src" / f"{probe_module}.py").write_text(
        "import os\n"
        "import sys\n\n"
        "def capture(queue):\n"
        "    queue.put({\n"
        "        'sys_path': tuple(sys.path),\n"
        "        'pythonpath': os.environ.get('PYTHONPATH', ''),\n"
        "    })\n",
        encoding="utf-8",
    )
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

    probe = importlib.import_module(probe_module)
    context = multiprocessing.get_context("spawn")
    queue = context.Queue()
    process = context.Process(target=probe.capture, args=(queue,))
    process.start()
    process.join(timeout=20)
    assert process.exitcode == 0
    child = queue.get(timeout=5)
    queue.close()

    assert tuple(child["sys_path"][:2]) == expected
    assert "okto_labs_pulse_" not in os.pathsep.join(child["sys_path"])
    assert "okto_labs_pulse_" not in child["pythonpath"]
    sys.modules.pop(probe_module, None)


def test_c11_labs_activation_removes_stale_hyphenated_roots(
    tmp_path: Path,
) -> None:
    workspace = tmp_path / "workspace"
    labs_core = _checkout(workspace, "okto_labs_pulse_core", "core")
    labs_community = _checkout(
        workspace,
        "okto_labs_pulse_community",
        "community",
    )
    stale_core = _checkout(workspace, "okto-pulse-core", "core")
    stale_community = _checkout(workspace, "okto-pulse", "community")
    search_path = [
        str(stale_core / "src"),
        str(stale_community / "src"),
        str(labs_core / "src"),
        str(labs_community / "src"),
    ]
    environ = {"PYTHONPATH": os.pathsep.join(search_path)}

    activation = activate_repository_checkout_paths(
        anchor_repo=labs_community,
        environ=environ,
        search_path=search_path,
    )

    expected = (
        str((labs_core / "src").resolve()),
        str((labs_community / "src").resolve()),
    )
    stale_sources = {
        str((stale_core / "src").resolve()),
        str((stale_community / "src").resolve()),
    }
    assert tuple(search_path[:2]) == expected
    assert activation.pythonpath[:2] == expected
    assert stale_sources.isdisjoint(search_path)
    assert stale_sources.isdisjoint(environ["PYTHONPATH"].split(os.pathsep))

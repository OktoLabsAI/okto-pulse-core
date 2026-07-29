"""Deterministic paired-repository checkout resolution for release gates.

Release and cross-edition audits must never silently select a stale sibling
checkout merely because it uses an older repository name.  Resolution is:

1. the edition-specific environment override;
2. an optional workspace-root environment override;
3. current repository names in inferred workspace roots;
4. legacy repository names in those same roots.

An explicitly configured but invalid path fails closed.  Callers that can also
operate against an installed distribution may request ``required=False`` and
fall back only when no checkout was configured or discovered.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping, MutableMapping, MutableSequence, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


RepositoryEdition = Literal["core", "community"]

_CURRENT_NAMES: dict[RepositoryEdition, str] = {
    "core": "okto-pulse-core",
    "community": "okto-pulse",
}
_LEGACY_NAMES: dict[RepositoryEdition, str] = {
    "core": "okto_labs_pulse_core",
    "community": "okto_labs_pulse_community",
}
_REPO_ENV: dict[RepositoryEdition, str] = {
    "core": "OKTO_PULSE_CORE_REPO",
    "community": "OKTO_PULSE_COMMUNITY_REPO",
}
_PACKAGE_MARKER: dict[RepositoryEdition, tuple[str, ...]] = {
    "core": ("src", "okto_pulse", "core"),
    "community": ("src", "okto_pulse", "community"),
}
WORKSPACE_ROOT_ENV = "OKTO_PULSE_WORKSPACE_ROOT"


class RepositoryCheckoutNotFound(RuntimeError):
    """A required checkout was absent or an explicit override was invalid."""


@dataclass(frozen=True, slots=True)
class RepositoryCheckout:
    edition: RepositoryEdition
    repo_root: Path
    selected_by: str
    checked: tuple[str, ...]

    @property
    def source_root(self) -> Path:
        return self.repo_root / "src"


@dataclass(frozen=True, slots=True)
class RepositoryImportPathActivation:
    """The checkout sources made authoritative for this process and its children."""

    checkouts: tuple[RepositoryCheckout, ...]
    source_roots: tuple[Path, ...]
    removed_paths: tuple[str, ...]
    pythonpath: tuple[str, ...]


def _is_checkout(path: Path, edition: RepositoryEdition) -> bool:
    return (
        (path / "pyproject.toml").is_file()
        and path.joinpath(*_PACKAGE_MARKER[edition]).is_dir()
    )


def _resolved(path: str | Path) -> Path:
    return Path(path).expanduser().resolve()


def _inferred_workspace_roots(anchor_repo: Path) -> tuple[Path, ...]:
    """Return nearby workspace layouts without assuming one checkout nesting."""

    anchor = anchor_repo.resolve()
    roots = (anchor.parent, anchor.parent.parent)
    return tuple(dict.fromkeys(roots))


def resolve_repository_checkout(
    edition: RepositoryEdition,
    *,
    anchor_repo: str | Path,
    environ: Mapping[str, str] | None = None,
    required: bool = True,
) -> RepositoryCheckout | None:
    """Resolve one checkout with env-first/current-before-legacy precedence."""

    if edition not in _CURRENT_NAMES:
        raise ValueError(f"unsupported repository edition: {edition!r}")

    env = os.environ if environ is None else environ
    checked: list[str] = []
    repo_env = _REPO_ENV[edition]
    configured_repo = str(env.get(repo_env, "")).strip()
    if configured_repo:
        candidate = _resolved(configured_repo)
        checked.append(str(candidate))
        if _is_checkout(candidate, edition):
            return RepositoryCheckout(
                edition=edition,
                repo_root=candidate,
                selected_by=repo_env,
                checked=tuple(checked),
            )
        raise RepositoryCheckoutNotFound(
            f"{repo_env} does not point to a valid {edition} checkout: {candidate}"
        )

    configured_workspace = str(env.get(WORKSPACE_ROOT_ENV, "")).strip()
    if configured_workspace:
        workspace = _resolved(configured_workspace)
        for name in (_CURRENT_NAMES[edition], _LEGACY_NAMES[edition]):
            candidate = workspace / name
            checked.append(str(candidate))
            if _is_checkout(candidate, edition):
                return RepositoryCheckout(
                    edition=edition,
                    repo_root=candidate,
                    selected_by=WORKSPACE_ROOT_ENV,
                    checked=tuple(checked),
                )
        raise RepositoryCheckoutNotFound(
            f"{WORKSPACE_ROOT_ENV} contains no valid {edition} checkout; "
            f"checked: {checked}"
        )

    workspace_roots = _inferred_workspace_roots(_resolved(anchor_repo))
    candidates = (
        (
            root / _CURRENT_NAMES[edition],
            f"inferred-current:{root}",
        )
        for root in workspace_roots
    )
    legacy_candidates = (
        (
            root / _LEGACY_NAMES[edition],
            f"inferred-legacy:{root}",
        )
        for root in workspace_roots
    )
    for candidate, selected_by in (*candidates, *legacy_candidates):
        resolved = candidate.resolve()
        if str(resolved) in checked:
            continue
        checked.append(str(resolved))
        if _is_checkout(resolved, edition):
            return RepositoryCheckout(
                edition=edition,
                repo_root=resolved,
                selected_by=selected_by,
                checked=tuple(checked),
            )

    if not required:
        return None
    raise RepositoryCheckoutNotFound(
        f"Unable to locate the paired {edition} repository. Checked: {checked}. "
        f"Set {_REPO_ENV[edition]} explicitly."
    )


def _path_edition(path: Path, names: Mapping[RepositoryEdition, str]) -> RepositoryEdition | None:
    for part in (path, *path.parents):
        for edition, name in names.items():
            if part.name.casefold() == name.casefold():
                return edition
    return None


def _normalise_import_entry(raw: str) -> Path | None:
    try:
        return _resolved(raw or os.curdir)
    except (OSError, RuntimeError, ValueError):
        return None


def activate_repository_checkout_paths(
    *,
    anchor_repo: str | Path,
    editions: Sequence[RepositoryEdition] = ("core", "community"),
    environ: MutableMapping[str, str] | None = None,
    search_path: MutableSequence[str] | None = None,
    required: bool = True,
) -> RepositoryImportPathActivation:
    """Make selected checkout sources authoritative across process boundaries.

    Merely prepending a current checkout is insufficient for namespace packages:
    a pre-existing ``okto_labs_*`` source can remain in ``sys.path`` and be
    inherited verbatim by ``multiprocessing`` spawn children.  Subprocesses can
    inherit the same stale source through ``PYTHONPATH``.

    This activation resolves every requested edition with the same
    current-before-legacy policy as :func:`resolve_repository_checkout`, removes
    legacy roots for editions backed by a current worktree, prepends the selected
    sources, and mirrors the result into ``PYTHONPATH``.  An explicitly selected
    legacy checkout remains supported when no current worktree is available.
    """

    env = os.environ if environ is None else environ
    paths = sys.path if search_path is None else search_path
    requested = tuple(dict.fromkeys(editions))
    invalid = tuple(edition for edition in requested if edition not in _CURRENT_NAMES)
    if invalid:
        raise ValueError(f"unsupported repository editions: {invalid!r}")

    checkouts: list[RepositoryCheckout] = []
    for edition in requested:
        checkout = resolve_repository_checkout(
            edition,
            anchor_repo=anchor_repo,
            environ=env,
            required=required,
        )
        if checkout is not None:
            checkouts.append(checkout)

    selected_by_edition = {checkout.edition: checkout for checkout in checkouts}
    current_editions = {
        edition
        for edition, checkout in selected_by_edition.items()
        if checkout.repo_root.name.casefold() == _CURRENT_NAMES[edition].casefold()
    }

    removed: list[str] = []

    def _keep(raw: str) -> bool:
        resolved = _normalise_import_entry(raw)
        if resolved is None:
            return True
        legacy_edition = _path_edition(resolved, _LEGACY_NAMES)
        if legacy_edition in current_editions:
            removed.append(raw)
            return False
        return True

    retained_search_path = [raw for raw in paths if _keep(raw)]
    source_roots = tuple(checkout.source_root.resolve() for checkout in checkouts)
    source_texts = tuple(str(source) for source in source_roots)
    selected_keys = {source.casefold() for source in source_texts}
    retained_search_path = [
        raw
        for raw in retained_search_path
        if str(_normalise_import_entry(raw)).casefold() not in selected_keys
    ]
    paths[:] = [*source_texts, *retained_search_path]

    inherited_pythonpath = str(env.get("PYTHONPATH", ""))
    inherited_entries = [
        entry for entry in inherited_pythonpath.split(os.pathsep) if entry
    ]
    retained_pythonpath = [entry for entry in inherited_entries if _keep(entry)]
    retained_pythonpath = [
        entry
        for entry in retained_pythonpath
        if str(_normalise_import_entry(entry)).casefold() not in selected_keys
    ]
    pythonpath = (*source_texts, *retained_pythonpath)
    env["PYTHONPATH"] = os.pathsep.join(pythonpath)

    return RepositoryImportPathActivation(
        checkouts=tuple(checkouts),
        source_roots=source_roots,
        removed_paths=tuple(removed),
        pythonpath=pythonpath,
    )


__all__ = [
    "RepositoryCheckout",
    "RepositoryImportPathActivation",
    "RepositoryCheckoutNotFound",
    "RepositoryEdition",
    "WORKSPACE_ROOT_ENV",
    "activate_repository_checkout_paths",
    "resolve_repository_checkout",
]

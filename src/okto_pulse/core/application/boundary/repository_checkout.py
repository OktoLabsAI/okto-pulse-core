"""Deterministic paired-repository checkout resolution for release gates.

Release and cross-edition audits must never silently select a stale sibling
checkout merely because it uses another supported repository name. Resolution
is:

1. the edition-specific environment override;
2. an optional workspace-root environment override;
3. the repository-name family of the anchor in inferred workspace roots;
4. the other supported repository-name family in those same roots.

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


def _anchor_uses_legacy_names(anchor_repo: Path) -> bool:
    """Return whether ``anchor_repo`` belongs to the ``okto_labs_*`` family.

    Both checkout-name families remain valid.  The anchor is the only safe
    deterministic signal that two co-located repositories belong together;
    global current-before-legacy ordering can otherwise pair an
    ``okto_labs_*`` checkout with a stale hyphenated sibling.
    """

    anchor = anchor_repo.resolve()
    return _path_edition(anchor, _LEGACY_NAMES) is not None


def _name_precedence(
    anchor_repo: Path,
) -> tuple[
    tuple[Mapping[RepositoryEdition, str], str],
    tuple[Mapping[RepositoryEdition, str], str],
]:
    if _anchor_uses_legacy_names(anchor_repo):
        return (
            (_LEGACY_NAMES, "legacy"),
            (_CURRENT_NAMES, "current"),
        )
    return (
        (_CURRENT_NAMES, "current"),
        (_LEGACY_NAMES, "legacy"),
    )


def resolve_repository_checkout(
    edition: RepositoryEdition,
    *,
    anchor_repo: str | Path,
    environ: Mapping[str, str] | None = None,
    required: bool = True,
) -> RepositoryCheckout | None:
    """Resolve one checkout with env-first/anchor-family precedence."""

    if edition not in _CURRENT_NAMES:
        raise ValueError(f"unsupported repository edition: {edition!r}")

    env = os.environ if environ is None else environ
    anchor = _resolved(anchor_repo)
    name_precedence = _name_precedence(anchor)
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
        for names, _family in name_precedence:
            name = names[edition]
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

    workspace_roots = _inferred_workspace_roots(anchor)
    for names, family in name_precedence:
        for root in workspace_roots:
            candidate = (root / names[edition]).resolve()
            if str(candidate) in checked:
                continue
            checked.append(str(candidate))
            if _is_checkout(candidate, edition):
                return RepositoryCheckout(
                    edition=edition,
                    repo_root=candidate,
                    selected_by=f"inferred-{family}:{root}",
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

    This activation resolves every requested edition with the same anchor-family
    policy as :func:`resolve_repository_checkout`, removes non-selected roots for
    those editions, prepends the selected sources, and mirrors the result into
    ``PYTHONPATH``. Both repository-name families remain supported.
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
    removed: list[str] = []

    def _keep(raw: str) -> bool:
        resolved = _normalise_import_entry(raw)
        if resolved is None:
            return True
        path_edition = _path_edition(resolved, _CURRENT_NAMES)
        if path_edition is None:
            path_edition = _path_edition(resolved, _LEGACY_NAMES)
        selected = selected_by_edition.get(path_edition) if path_edition else None
        if selected is not None and not resolved.is_relative_to(selected.repo_root):
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

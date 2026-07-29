"""Paired-checkout helpers for cross-repository Core tests."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.repository_checkout import (
    resolve_repository_checkout,
)


def community_repo_for(core_repo: str | Path) -> Path:
    checkout = resolve_repository_checkout(
        "community",
        anchor_repo=core_repo,
    )
    assert checkout is not None
    return checkout.repo_root


def community_source_for(core_repo: str | Path) -> Path:
    return community_repo_for(core_repo) / "src" / "okto_pulse" / "community"


__all__ = ["community_repo_for", "community_source_for"]

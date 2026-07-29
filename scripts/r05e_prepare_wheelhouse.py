#!/usr/bin/env python3
"""R05-E IMP2 — ``prepare-wheelhouse`` step (reproducible; MAY use network).

Part 1 of the two-part offline distribution proof. Populates a SELF-CONTAINED
wheelhouse with the freshly-built core + Community wheels AND their full
dependency closure, LOCK-PINNED, so the acceptance criterion
(``test_offline_install_from_wheelhouse`` /
``tests/test_r05e_imp2_build_reinstall_smoke.py``) can do

    uv pip install --no-index --find-links <wheelhouse> <core.whl> <community.whl>

with NO network / PyPI. This step is the ONLY one allowed to reach the network
(or the warm cache); the smoke that gates the card does not.

The wheelhouse is NOT committed (large — the ML/torch stack is ~230 MB). It lands
in a temp dir: ``$R05E_IMP2_WHEELHOUSE`` if set, else
``<system-temp>/r05e_imp2_wheelhouse``. The constraints come from a READ-ONLY
``uv export`` of the Community lock — this never writes ``uv.lock``.

Usage:
    uv run --no-project --with pip python scripts/r05e_prepare_wheelhouse.py
    R05E_IMP2_WHEELHOUSE=/some/dir uv run --no-project --with pip python scripts/r05e_prepare_wheelhouse.py

(``--with pip`` only matters if this interpreter lacks pip; the script invokes the
``pip download`` step through ``uv run --no-project --with pip`` regardless.)
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

_CORE_REPO = Path(__file__).resolve().parents[1]
_CORE_SRC = _CORE_REPO / "src"
if str(_CORE_SRC) not in sys.path:
    sys.path.insert(0, str(_CORE_SRC))

from okto_pulse.core.application.boundary.repository_checkout import (  # noqa: E402
    RepositoryCheckoutNotFound,
    resolve_repository_checkout,
)


def wheelhouse_dir() -> Path:
    """The wheelhouse location, shared with the offline-install-smoke test."""
    override = os.environ.get("R05E_IMP2_WHEELHOUSE")
    return Path(override) if override else (Path(tempfile.gettempdir()) / "r05e_imp2_wheelhouse")


def main() -> int:
    uv = shutil.which("uv")
    if uv is None:
        print("uv not on PATH", file=sys.stderr)
        return 2
    try:
        community_checkout = resolve_repository_checkout(
            "community",
            anchor_repo=_CORE_REPO,
        )
    except RepositoryCheckoutNotFound as exc:
        print(str(exc), file=sys.stderr)
        return 2
    assert community_checkout is not None
    community_repo = community_checkout.repo_root

    wh = wheelhouse_dir()
    if wh.exists():
        shutil.rmtree(wh)
    wh.mkdir(parents=True)

    # 1. Build BOTH wheels fresh into the wheelhouse (offline; reflects the real
    #    uncommitted working tree).
    for label, repo in (("core", _CORE_REPO), ("community", community_repo)):
        build = subprocess.run(
            (uv, "build", "--wheel", "--offline", "--out-dir", str(wh), str(repo)),
            capture_output=True, text=True,
        )
        if build.returncode != 0:
            print(f"{label} wheel build failed: {build.stderr.strip()[-400:]}", file=sys.stderr)
            return 1

    # 2. Lock-pinned constraints — READ-ONLY export (never writes uv.lock).
    constraints = wh / "_constraints.txt"
    export = subprocess.run(
        (uv, "export", "--frozen", "--no-hashes", "--no-emit-project",
         "--directory", str(community_repo), "-o", str(constraints)),
        capture_output=True, text=True,
    )
    if export.returncode != 0:
        print(f"lock export failed: {export.stderr.strip()[-400:]}", file=sys.stderr)
        return 1

    core_whl = next(wh.glob("okto_pulse_core-*.whl"))
    comm_whl = next(wh.glob("okto_pulse-0*.whl"))

    # 3. Download the full closure (MAY use the network/cache), constrained to the
    #    Community lock, into the wheelhouse — wheels only, no sdists.
    download = subprocess.run(
        (uv, "run", "--no-project", "--with", "pip", "python", "-m", "pip", "download",
         "--only-binary=:all:", "-c", str(constraints), "-d", str(wh),
         str(core_whl), str(comm_whl)),
        text=True,
    )
    if download.returncode != 0:
        print("pip download of the dependency closure failed", file=sys.stderr)
        return 1

    wheels = sorted(wh.glob("*.whl"))
    total_mb = sum(w.stat().st_size for w in wheels) / 1e6
    print(f"WHEELHOUSE_READY: {wh}")
    print(f"  wheels        : {len(wheels)}  (~{total_mb:.0f} MB)")
    print(f"  core wheel    : {core_whl.name}")
    print(f"  community wheel: {comm_whl.name}")
    print("  offline-install-smoke (no network):")
    print(f"    uv pip install --no-index --find-links {wh} {core_whl.name} {comm_whl.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

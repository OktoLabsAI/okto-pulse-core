"""CLI for the core-pure boundary auditor (spec #12, tr_b5f136b3).

``okto-pulse-core-boundary audit --format json`` orchestrates ALL FOUR gates of
card 19f982ec and always emits a deterministic report for each:

- import_boundary          (always; ``--mode`` bootstrap|blocking)
- package_manifest         (always; ``--wheel-record`` to audit a wheel RECORD)
- dependency_audit         (versioned default policy; ``--dependency-policy`` to override)
- import_side_effect_smoke (default pure-layer probes; ``--side-effect-module`` to add)

Static analysis only (``ast``/``tomllib``/``pathlib``) plus a controlled
subprocess import for the side-effect probe; no runtime is initialised in-process.
Exit code is non-zero when any gate is ``blocking``/``reject``.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .dependency_conformance import audit_dependency_conformance, render_report
from .gates import (
    DEFAULT_CORE_PURE_FORBIDDEN_DEPENDENCIES,
    DependencyAuditGate,
    DependencyAuditGateInput,
    ImportBoundaryGate,
    ImportBoundaryGateInput,
    ImportSideEffectSmokeGate,
    ImportSideEffectSmokeInput,
    PackageManifestGate,
    PackageManifestGateInput,
)
from .report import GateReport

_FAILING_STATUSES = frozenset({"blocking", "reject"})

#: Default pure-layer modules whose import must not start a runtime.
_DEFAULT_SIDE_EFFECT_MODULES: tuple[str, ...] = (
    "okto_pulse.core.application.boundary.report",
    "okto_pulse.core.application.boundary.layer_resolver",
)


def _load_dependency_policy(path: Path | None) -> tuple[list[str], list[str]]:
    """Return (allowed, forbidden) from a JSON policy file, or the default."""
    if path is None:
        return [], list(DEFAULT_CORE_PURE_FORBIDDEN_DEPENDENCIES)
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return list(data.get("allowed") or []), list(data.get("forbidden") or [])


def _run_audit(
    *,
    source_root: Path | None,
    mode: str,
    wheel_record: Path | None,
    dependency_policy: Path | None,
    side_effect_modules: list[str],
) -> list[GateReport]:
    allowed, forbidden = _load_dependency_policy(dependency_policy)
    return [
        ImportBoundaryGate().run(
            ImportBoundaryGateInput(source_root=source_root, mode=mode)
        ),
        PackageManifestGate().run(
            PackageManifestGateInput(source_root=source_root, wheel_record_path=wheel_record)
        ),
        DependencyAuditGate().run(
            DependencyAuditGateInput(
                allowed_dependencies=allowed,
                forbidden_dependencies=forbidden,
                dependency_policy_ref=str(dependency_policy) if dependency_policy else "default-core-pure-policy",
                mode=mode,
            )
        ),
        ImportSideEffectSmokeGate().run(
            ImportSideEffectSmokeInput(modules=side_effect_modules)
        ),
    ]


def _render(reports: list[GateReport], fmt: str) -> str:
    if fmt == "json":
        return json.dumps([r.as_dict() for r in reports], indent=2, sort_keys=True)
    lines = []
    for r in reports:
        lines.append(f"[{r.status.upper():14}] {r.gate_id}: {r.subject}")
        if r.remediation_hint and r.status in _FAILING_STATUSES:
            lines.append(f"    hint: {r.remediation_hint}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="okto-pulse-core-boundary")
    sub = parser.add_subparsers(dest="command", required=True)
    audit = sub.add_parser("audit", help="Run the four core-pure boundary gates.")
    audit.add_argument("--format", choices=("json", "text"), default="json")
    audit.add_argument("--source-root", type=Path, default=None)
    audit.add_argument("--mode", choices=("bootstrap", "blocking"), default="bootstrap")
    audit.add_argument("--wheel-record", type=Path, default=None)
    audit.add_argument("--dependency-policy", type=Path, default=None)
    audit.add_argument(
        "--side-effect-module", action="append", default=None, dest="side_effect_modules"
    )

    # spec R05-E (card 0c066d12): fail-closed dependency conformance auditor over
    # the manifest, uv.lock, source tree and installed wheel.
    dep = sub.add_parser(
        "dependency-conformance",
        aliases=["r05e"],
        help="Run the R05-E fail-closed dependency conformance auditor.",
    )
    dep.add_argument("--format", choices=("json", "text"), default="text")
    dep.add_argument("--repo-root", type=Path, default=None)
    dep.add_argument("--pyproject", type=Path, default=None)
    dep.add_argument("--lock", type=Path, default=None)
    dep.add_argument("--source-root", type=Path, default=None)
    dep.add_argument("--wheel-metadata", type=Path, default=None)
    dep.add_argument("--no-wheel", action="store_true", help="Skip the wheel surface.")

    args = parser.parse_args(argv)

    if args.command in ("dependency-conformance", "r05e"):
        report = audit_dependency_conformance(
            repo_root=args.repo_root,
            pyproject_path=args.pyproject,
            lock_path=args.lock,
            source_root=args.source_root,
            wheel_metadata_path=args.wheel_metadata,
            audit_wheel=not args.no_wheel,
        )
        if args.format == "json":
            sys.stdout.write(json.dumps(report.as_dict(), indent=2, sort_keys=True) + "\n")
        else:
            sys.stdout.write(render_report(report) + "\n")
        return 0 if report.ok else 1

    if args.command == "audit":
        modules = args.side_effect_modules or list(_DEFAULT_SIDE_EFFECT_MODULES)
        reports = _run_audit(
            source_root=args.source_root,
            mode=args.mode,
            wheel_record=args.wheel_record,
            dependency_policy=args.dependency_policy,
            side_effect_modules=modules,
        )
        sys.stdout.write(_render(reports, args.format) + "\n")
        return 1 if any(r.status in _FAILING_STATUSES for r in reports) else 0
    return 2


if __name__ == "__main__":  # pragma: no cover - module entry point
    raise SystemExit(main())

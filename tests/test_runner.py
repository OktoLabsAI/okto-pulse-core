#!/usr/bin/env python3
"""Test runner utility for okto-pulse-core.

Run tests asynchronously with real-time streaming, hang detection,
and structured reporting. Designed to be called from agents or CI.

Usage::

    python -m tests.test_runner                        # run all tests
    python -m tests.test_runner test_kg_foundation.py  # run specific file
    python -m tests.test_runner -k "test_bootstrap"    # run by keyword
    python -m tests.test_runner --timeout 60           # custom timeout

Environment variables:
    TEST_RUNNER_LOG_DIR   Override log directory (default: .test-logs/)
    TEST_RUNNER_TIMEOUT   Default timeout per test (default: 120)
    TEST_RUNNER_MAX_IDLE  Max idle seconds before hang detection (default: 60)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path
from typing import Any

# Ensure the test directory is on the path
sys.path.insert(0, str(Path(__file__).parent))

from test_logging import (  # noqa: E402
    LOG_DIR,
    cleanup_all_logs,
    get_all_log_files,
    run_tests_async,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run okto-pulse-core tests with streaming output and hang detection.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        help="Test file(s) or directory to run (default: all tests in tests/)",
    )
    parser.add_argument(
        "-k", "--keyword",
        default="",
        help="Only run tests matching this keyword expression.",
    )
    parser.add_argument(
        "-m", "--marker",
        default="",
        help="Only run tests with this marker (e.g. 'e2e').",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=float(os.environ.get("TEST_RUNNER_TIMEOUT", "120")),
        help="Per-test timeout in seconds (default: 120).",
    )
    parser.add_argument(
        "--max-idle",
        type=float,
        default=float(os.environ.get("TEST_RUNNER_MAX_IDLE", "60")),
        help="Max idle seconds before hang detection (default: 60).",
    )
    parser.add_argument(
        "--total-timeout",
        type=float,
        default=300.0,
        help="Total wall-clock timeout for the entire pytest run (default: 300).",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress real-time output streaming.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results as JSON at the end.",
    )
    parser.add_argument(
        "--clean-logs",
        action="store_true",
        help="Remove .test-logs/ before running.",
    )
    return parser.parse_args(argv)


def _format_result(result: dict[str, Any]) -> str:
    """Format test results into a human-readable summary."""
    lines: list[str] = []
    lines.append("=" * 60)
    lines.append("TEST RUNNER RESULTS")
    lines.append("=" * 60)

    rc = result.get("returncode", -1)
    lines.append(f"Return code: {rc}")
    lines.append(f"Duration: {result.get('duration_seconds', 0):.2f}s")
    lines.append(f"Hanged: {result.get('hanged', False)}")

    log_files = result.get("log_files", [])
    if log_files:
        lines.append(f"\nLog files ({len(log_files)}):")
        for lf in sorted(log_files):
            lines.append(f"  {lf}")

    # Parse pytest output for summary
    stdout = result.get("stdout", "")
    for line in stdout.splitlines():
        if "passed" in line or "failed" in line or "error" in line:
            lines.append(f"  {line.strip()}")

    if result.get("hanged"):
        lines.append("\n[!] TEST RUNNER DETECTED A HANG")
        lines.append("    Check log files for the last messages before the freeze.")

    lines.append("=" * 60)
    return "\n".join(lines)


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Clean logs if requested
    if args.clean_logs:
        count = cleanup_all_logs()
        print(f"[test_runner] Cleaned {count} old log files")

    # Build test paths
    test_dir = Path(__file__).parent
    paths = args.paths if args.paths else [str(test_dir)]

    # Build pytest args list
    pytest_args: list[str] = []
    if args.keyword:
        pytest_args.extend(["-k", args.keyword])
    if args.marker:
        pytest_args.extend(["-m", args.marker])
    pytest_args.extend(paths)

    # Run tests
    print(f"[test_runner] Running pytest with args: {pytest_args}")
    print(f"[test_runner] Timeout: {args.total_timeout}s, Max idle: {args.max_idle_seconds}s")
    print(f"[test_runner] Log directory: {LOG_DIR}")
    print()

    result = await run_tests_async(
        *pytest_args,
        timeout=args.total_timeout,
        verbose=not args.quiet,
        log_dir=LOG_DIR,
        max_idle_seconds=args.max_idle_seconds,
    )

    # Output results
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print()
        print(_format_result(result))

    return result.get("returncode", 1)


if __name__ == "__main__":
    rc = asyncio.run(main())
    sys.exit(rc or 0)

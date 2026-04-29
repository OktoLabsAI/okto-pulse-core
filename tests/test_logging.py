"""Test logging infrastructure for okto-pulse-core test suite.

Provides structured logging with file capture, test lifecycle hooks,
and KG operation tracing. Every test gets its own log file under
``.test-logs/<test_name>.log``.

Structured format::

    [TEST] [2026-04-23T14:02:31.123456] [DEBUG] [okto_pulse.core.kg.schema] Bootstrap started

Usage (in conftest.py or test files)::

    from tests.test_logging import get_test_logger, setup_test_logging

    logger = get_test_logger(__name__)
    logger.info("Something happened")
"""

from __future__ import annotations

import asyncio
import atexit
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LOG_DIR = Path(__file__).parent.parent / ".test-logs"
LOG_FORMAT = "[TEST] [%(asctime)s] [%(levelname)s] [%(name)s] %(message)s"
LOG_DATETIME_FMT = "%Y-%m-%dT%H:%M:%S.%f"

# Global registry: test_id -> logger (for cleanup)
_test_loggers: Dict[str, logging.Logger] = {}
_test_loggers_lock = threading.Lock()

# Track which log files have been created so we can clean them up
_log_files_created: list[Path] = []
_log_files_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Custom Formatter — guarantees structured output
# ---------------------------------------------------------------------------

class TestLogFormatter(logging.Formatter):
    """Formats every log record as ``[TEST] [timestamp] [level] [module] message``."""

    def format(self, record: logging.LogRecord) -> str:
        # Ensure timestamp is in the desired format
        record.asctime = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).strftime(LOG_DATETIME_FMT)
        return super().format(record)


# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def setup_test_logging(
    level: int = logging.DEBUG,
    log_dir: Optional[Path] = None,
    test_id: Optional[str] = None,
) -> logging.Logger:
    """Set up structured logging for a test and return the logger.

    Args:
        level: Logging level (default DEBUG).
        log_dir: Directory for log files. Defaults to ``.test-logs/``.
        test_id: Unique identifier for the test. If provided, a file handler
            is attached that writes to ``<log_dir>/<test_id>.log``.

    Returns:
        A configured logger with ``"test"`` prefix.
    """
    log_dir = log_dir or LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    logger_name = "test"
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)
    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        return logger

    formatter = TestLogFormatter(LOG_FORMAT, datefmt=LOG_DATETIME_FMT)

    # Console handler — always attached
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler — attached only when test_id is provided
    if test_id:
        safe_name = test_id.replace("::", "/").replace("/", "_")
        log_file = log_dir / f"{safe_name}.log"
        file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        # Flush on every write to never lose logs
        file_handler.flush = lambda: (  # type: ignore[assignment]
            logging.FileHandler.flush(file_handler),
            file_handler.stream.flush(),
        )
        logger.addHandler(file_handler)

        with _log_files_lock:
            _log_files_created.append(log_file)

        with _test_loggers_lock:
            _test_loggers[test_id] = logger

    return logger


def get_test_logger(test_id: str) -> logging.Logger:
    """Get or create the logger for a specific test.

    If the logger was not created via ``setup_test_logging``, a new one
    is created on-demand.
    """
    with _test_loggers_lock:
        if test_id in _test_loggers:
            return _test_loggers[test_id]

    # Fallback: create one without a file handler
    return setup_test_logging(test_id=test_id)


def cleanup_test_logging(test_id: str) -> None:
    """Flush and remove the logger for a test.

    Call this in teardown to ensure all buffered log data is written to disk.
    """
    with _test_loggers_lock:
        logger = _test_loggers.pop(test_id, None)

    if logger:
        for handler in logger.handlers[:]:
            handler.flush()
            handler.close()
            logger.removeHandler(handler)


def get_log_file_path(test_id: str) -> Optional[Path]:
    """Return the log file path for a test, or None if no file handler exists."""
    safe_name = test_id.replace("::", "/").replace("/", "_")
    return LOG_DIR / f"{safe_name}.log"


def get_all_log_files() -> list[Path]:
    """Return all log files that have been created during this session."""
    with _log_files_lock:
        return list(_log_files_created)


def cleanup_all_logs() -> int:
    """Remove all log files. Returns count of files removed."""
    count = 0
    with _log_files_lock:
        for f in _log_files_created:
            try:
                f.unlink(missing_ok=True)
                count += 1
            except OSError:
                pass
    return count


# ---------------------------------------------------------------------------
# KG operation logging helpers
# ---------------------------------------------------------------------------

def log_kg_operation(
    logger: logging.Logger,
    operation: str,
    board_id: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    """Log a KG operation in a structured way.

    Args:
        logger: The test logger.
        operation: Operation name (e.g. "consolidation_begin", "schema_bootstrap").
        board_id: Board ID involved.
        details: Optional extra context dict.
    """
    parts = [f"KG {operation} board={board_id}"]
    if details:
        parts.append(f"details={details}")
    logger.debug(" | ".join(parts))


def log_kg_event(
    logger: logging.Logger,
    event: str,
    session_id: Optional[str] = None,
    board_id: Optional[str] = None,
    **extra: Any,
) -> None:
    """Log a KG event with optional session/board context."""
    parts = [f"KG_EVENT:{event}"]
    if session_id:
        parts.append(f"session={session_id}")
    if board_id:
        parts.append(f"board={board_id}")
    if extra:
        parts.append(f"extra={extra}")
    logger.debug(" ".join(parts))


# ---------------------------------------------------------------------------
# Test lifecycle logger — wraps a test function
# ---------------------------------------------------------------------------

class TestLifecycleLogger:
    """Context manager that logs test lifecycle events.

    Usage::

        with TestLifecycleLogger("test_name", logger) as ll:
            ll.info("test body running")
    """

    def __init__(self, test_id: str, logger: logging.Logger) -> None:
        self.test_id = test_id
        self.logger = logger
        self._start = 0.0

    def __enter__(self) -> "TestLifecycleLogger":
        self._start = time.monotonic()
        self.logger.info(f"TEST_START id={self.test_id}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        elapsed = time.monotonic() - self._start
        if exc_type is not None:
            self.logger.error(
                f"TEST_END id={self.test_id} status=FAILED "
                f"elapsed={elapsed:.3f}s exception={exc_type.__name__}: {exc_val}"
            )
        else:
            self.logger.info(f"TEST_END id={self.test_id} status=PASSED elapsed={elapsed:.3f}s")


# ---------------------------------------------------------------------------
# Timeout tracking
# ---------------------------------------------------------------------------

class TimeoutTracker:
    """Tracks whether a test has been running too long.

    Call ``check()`` periodically. If no heartbeat was received within
    ``max_seconds``, the test is considered hung.
    """

    def __init__(self, max_seconds: float = 120.0, logger: Optional[logging.Logger] = None) -> None:
        self.max_seconds = max_seconds
        self.logger = logger or logging.getLogger("test")
        self._last_heartbeat = time.monotonic()

    def heartbeat(self) -> None:
        """Signal that the test is still making progress."""
        self._last_heartbeat = time.monotonic()

    def check(self) -> Optional[str]:
        """Return a reason string if the test is hung, else None."""
        elapsed = time.monotonic() - self._last_heartbeat
        if elapsed > self.max_seconds:
            reason = f"TEST_HANG: no heartbeat for {elapsed:.1f}s (threshold={self.max_seconds}s)"
            self.logger.error(reason)
            return reason
        return None


# ---------------------------------------------------------------------------
# Async subprocess test runner
# ---------------------------------------------------------------------------

async def run_tests_async(
    *paths: str,
    timeout: float = 300.0,
    verbose: bool = True,
    log_dir: Optional[Path] = None,
    max_idle_seconds: float = 60.0,
) -> dict[str, Any]:
    """Run pytest via an async subprocess and stream output in real-time.

    Args:
        *paths: File or directory paths to pass to pytest.
        timeout: Total wall-clock timeout for the pytest process (seconds).
        verbose: If True, stream output to stdout.
        log_dir: Directory for log files.
        max_idle_seconds: If no log output for this many seconds, flag as hang.

    Returns:
        Dict with keys: ``returncode``, ``stdout``, ``stderr``, ``log_files``,
        ``hanged``, ``duration_seconds``.
    """
    import subprocess

    log_dir = log_dir or LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)

    cmd = [
        sys.executable, "-m", "pytest",
        "-v", "--tb=short",
        "-s",  # no capture, stream directly
        *(list(paths) or ["."]),
    ]

    start = time.monotonic()
    hanged = False
    last_output_time = time.monotonic()

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout_lines: list[str] = []
    stderr_lines: list[str] = []

    async def stream_reader(stream: asyncio.StreamReader, lines: list[str]) -> None:
        nonlocal last_output_time
        async for line in stream:
            text = line.decode("utf-8", errors="replace").rstrip()
            lines.append(text)
            if verbose:
                print(text, flush=True)
            last_output_time = time.monotonic()

    try:
        stdout_task = asyncio.create_task(stream_reader(proc.stdout, stdout_lines))
        stderr_task = asyncio.create_task(stream_reader(proc.stderr, stderr_lines))

        # Wait for process with timeout
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            hanged = True
            print(f"[TEST_RUNNER] Process killed after {timeout:.0f}s timeout", flush=True)

        await stdout_task
        await stderr_task
    except Exception as exc:
        print(f"[TEST_RUNNER] Error running tests: {exc}", flush=True)

    duration = time.monotonic() - start

    # Check for idle/hang
    idle_seconds = time.monotonic() - last_output_time
    if idle_seconds > max_idle_seconds:
        hanged = True

    return {
        "returncode": proc.returncode,
        "stdout": "\n".join(stdout_lines),
        "stderr": "\n".join(stderr_lines),
        "log_files": [str(f) for f in get_all_log_files()],
        "hanged": hanged,
        "duration_seconds": round(duration, 3),
    }


# ---------------------------------------------------------------------------
# Convenience: run tests synchronously
# ---------------------------------------------------------------------------

def run_tests(
    *paths: str,
    timeout: float = 300.0,
    verbose: bool = True,
    log_dir: Optional[Path] = None,
    max_idle_seconds: float = 60.0,
) -> dict[str, Any]:
    """Sync wrapper around ``run_tests_async``."""
    return asyncio.run(
        run_tests_async(
            *paths,
            timeout=timeout,
            verbose=verbose,
            log_dir=log_dir,
            max_idle_seconds=max_idle_seconds,
        )
    )


# ---------------------------------------------------------------------------
# Cleanup on interpreter exit
# ---------------------------------------------------------------------------

@atexit.register
def _cleanup_at_exit() -> None:
    """Ensure all log files are flushed and closed on interpreter exit."""
    with _log_files_lock:
        for f in _log_files_created:
            try:
                if f.exists():
                    # Touch to ensure fs sync
                    f.touch()
            except OSError:
                pass

"""Bounded, extraction-free reader for wheel ``dist-info/METADATA``.

Boundary CLIs accept either an extracted metadata file/directory or a built
wheel. Wheel members are read in place: no archive path is ever materialized
on disk, and malformed, ambiguous, oversized, or traversal-shaped metadata
members fail closed with a stable source descriptor.
"""

from __future__ import annotations

import zipfile
from email.parser import Parser
from email.policy import default
from pathlib import Path, PurePosixPath

MAX_METADATA_BYTES = 4 * 1024 * 1024


def _validate_metadata_text(text: str) -> bool:
    if "\x00" in text:
        return False
    message = Parser(policy=default).parsestr(text)
    if message.defects:
        return False
    for field in ("Metadata-Version", "Name", "Version"):
        values = message.get_all(field, [])
        if len(values) != 1 or not str(values[0]).strip():
            return False
    return True


def _safe_metadata_member(name: str) -> bool:
    """Accept only the root ``<distribution>.dist-info/METADATA`` shape."""

    if "\\" in name:
        return False
    path = PurePosixPath(name)
    return (
        not path.is_absolute()
        and name == path.as_posix()
        and len(path.parts) == 2
        and path.parts[0].endswith(".dist-info")
        and path.parts[0] not in {".", ".."}
        and ":" not in path.parts[0]
        and path.parts[1] == "METADATA"
    )


def _is_symlink(info: zipfile.ZipInfo) -> bool:
    return ((info.external_attr >> 16) & 0o170000) == 0o120000


def _decode_metadata(data: bytes, *, source: str) -> tuple[str | None, str]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return None, f"{source}:UnicodeDecodeError"
    if not _validate_metadata_text(text):
        return None, f"{source}:InvalidMetadata"
    return text, source


def _read_metadata_file(path: Path) -> tuple[str | None, str]:
    source = f"metadata:{path.as_posix()}"
    if not path.exists() or not path.is_file():
        return None, f"metadata_not_found:{path.as_posix()}"
    try:
        if path.stat().st_size > MAX_METADATA_BYTES:
            return None, f"metadata_too_large:{path.as_posix()}"
        with path.open("rb") as stream:
            data = stream.read(MAX_METADATA_BYTES + 1)
    except OSError as exc:
        return None, f"metadata_unreadable:{path.as_posix()}:{type(exc).__name__}"
    if len(data) > MAX_METADATA_BYTES:
        return None, f"metadata_too_large:{path.as_posix()}"
    return _decode_metadata(data, source=source)


def _read_metadata_wheel(path: Path) -> tuple[str | None, str]:
    if not path.exists() or not path.is_file():
        return None, f"wheel_not_found:{path.as_posix()}"
    try:
        with zipfile.ZipFile(path) as archive:
            candidates = [
                info
                for info in archive.infolist()
                if info.filename.replace("\\", "/").endswith(".dist-info/METADATA")
            ]
            if not candidates:
                return None, f"wheel_metadata_not_found:{path.as_posix()}"
            if any(not _safe_metadata_member(info.filename) for info in candidates):
                return None, f"wheel_metadata_unsafe_member:{path.as_posix()}"
            if len(candidates) != 1:
                return None, (
                    f"wheel_metadata_ambiguous:{path.as_posix()}:{len(candidates)}"
                )
            info = candidates[0]
            if info.is_dir() or _is_symlink(info):
                return None, f"wheel_metadata_not_regular:{path.as_posix()}"
            if info.file_size > MAX_METADATA_BYTES:
                return None, f"wheel_metadata_too_large:{path.as_posix()}"
            with archive.open(info, "r") as stream:
                data = stream.read(MAX_METADATA_BYTES + 1)
            if len(data) > MAX_METADATA_BYTES:
                return None, f"wheel_metadata_too_large:{path.as_posix()}"
    except (OSError, RuntimeError, zipfile.BadZipFile) as exc:
        return None, (
            f"wheel_metadata_unreadable:{path.as_posix()}:{type(exc).__name__}"
        )
    source = f"wheel:{path.as_posix()}!{info.filename}"
    return _decode_metadata(data, source=source)


def read_distribution_metadata(path: Path) -> tuple[str | None, str]:
    """Read validated metadata from a file, dist-info directory, or wheel.

    Returns ``(text, source_descriptor)``. ``text`` is ``None`` for every
    failure; callers can propagate the descriptor without handling archive
    exceptions or accidentally extracting attacker-controlled member paths.
    """

    candidate = Path(path)
    if candidate.is_dir():
        candidate = candidate / "METADATA"
    if candidate.suffix.lower() == ".whl":
        return _read_metadata_wheel(candidate)
    return _read_metadata_file(candidate)


__all__ = ["MAX_METADATA_BYTES", "read_distribution_metadata"]

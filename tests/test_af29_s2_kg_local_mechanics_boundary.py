from __future__ import annotations

import re
from pathlib import Path


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def test_af29_s2_core_kg_runtime_has_no_local_lock_or_quarantine_mechanics():
    forbidden_literals = (
        "os.open",
        "os.O_CREAT",
        "os.O_EXCL",
        "shutil.move",
    )
    direct_lock_constructor = re.compile(r"KGSingleWriterLock\s*\(\s*base_dir\s*=")

    offenders: list[str] = []
    for path in sorted((CORE_ROOT / "kg").rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for literal in forbidden_literals:
            if literal in text:
                offenders.append(f"{path.relative_to(CORE_ROOT)} contains {literal}")
        if direct_lock_constructor.search(text):
            offenders.append(
                f"{path.relative_to(CORE_ROOT)} constructs KGSingleWriterLock(base_dir=...)"
            )

    migrated_rest = CORE_ROOT / "api" / "kg_rebuild.py"
    assert not migrated_rest.exists()

    for path in (CORE_ROOT / "mcp" / "server.py",):
        text = path.read_text(encoding="utf-8")
        if direct_lock_constructor.search(text):
            offenders.append(
                f"{path.relative_to(CORE_ROOT)} constructs KGSingleWriterLock(base_dir=...)"
            )

    assert offenders == []

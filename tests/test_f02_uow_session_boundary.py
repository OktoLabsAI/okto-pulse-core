from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.uow_session_gate import (
    UowSessionBoundaryGate,
)


def test_f02_application_use_cases_have_no_session_escape() -> None:
    report = UowSessionBoundaryGate().run()
    assert report.status == "passed", report.evidence
    assert report.observed_value == 0
    assert report.expected_value == 0


def test_f02_gate_rejects_every_escape_shape(tmp_path: Path) -> None:
    target = tmp_path / "src" / "okto_pulse" / "core" / "application" / "use_cases"
    target.mkdir(parents=True)
    (target / "rogue.py").write_text(
        "from typing import Any\n"
        "from sqlalchemy.ext.asyncio import AsyncSession\n"
        "async def run(uow: Any):\n"
        "    first = uow.session\n"
        "    second = getattr(uow, 'session')\n"
        "    return session_of(uow), first, second, AsyncSession\n",
        encoding="utf-8",
    )

    report = UowSessionBoundaryGate().run(source_root=tmp_path)
    assert report.status == "blocking"
    symbols = {item["symbol"] for item in report.evidence["offenders"]}
    assert "sqlalchemy.ext.asyncio" in symbols
    assert "uow: Any" in symbols
    assert "uow.session" in symbols
    assert "getattr(uow, session)" in symbols
    assert "session_of" in symbols

"""F15 conformance tests for edition-neutral Core persistence contracts."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary import (
    FORBIDDEN_PERSISTENCE_EDITION_TOKENS,
    PersistenceEditionNeutralityGate,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_f15_real_core_persistence_contracts_are_edition_neutral() -> None:
    report = PersistenceEditionNeutralityGate().run(source_root=REPOSITORY_ROOT)

    assert report.status == "passed", report.evidence
    assert report.evidence["offenders"] == []
    assert report.evidence["scanned_files"]


def test_f15_gate_blocks_an_edition_specific_port(tmp_path: Path) -> None:
    port = tmp_path / "src" / "okto_pulse" / "core" / "ports" / "database.py"
    port.parent.mkdir(parents=True)
    forbidden = "post" + "gresql"
    port.write_text(f'DRIVER = "{forbidden}"\n', encoding="utf-8")

    report = PersistenceEditionNeutralityGate().run(source_root=tmp_path)

    assert report.status == "blocking"
    assert report.evidence["error"] == "edition_specific_persistence"
    assert report.evidence["offenders"]
    assert report.observed_value == report.evidence["offenders"]
    assert report.expected_value == []


def test_f15_forbidden_tokens_cover_server_engine_and_driver_choices() -> None:
    assert set(FORBIDDEN_PERSISTENCE_EDITION_TOKENS) == {
        "post" + "gres",
        "post" + "gresql",
        "async" + "pg",
    }

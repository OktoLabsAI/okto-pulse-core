"""Fail-closed gate for edition-neutral Core persistence contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .report import GateReport

FORBIDDEN_PERSISTENCE_EDITION_TOKENS: tuple[str, ...] = (
    "post" + "gres",
    "post" + "gresql",
    "async" + "pg",
)


@dataclass(frozen=True)
class PersistenceEditionFinding:
    file: str
    line: int
    token: str

    def as_dict(self) -> dict[str, object]:
        return {"file": self.file, "line": self.line, "token": self.token}


class PersistenceEditionNeutralityGate:
    gate_id = "persistence_edition_neutrality"

    def run(self, *, source_root: Path | None = None) -> GateReport:
        root = source_root or Path(__file__).resolve().parents[5]
        core_root = root / "src" / "okto_pulse" / "core"
        if not core_root.exists():
            core_root = root / "okto_pulse" / "core"
        targets = (
            core_root / "ports",
            core_root / "domain",
            core_root / "application" / "use_cases",
        )
        findings: list[PersistenceEditionFinding] = []
        scanned: list[str] = []
        for target in targets:
            if not target.exists():
                continue
            for path in sorted(target.rglob("*.py")):
                rel = path.relative_to(root).as_posix()
                scanned.append(rel)
                for line_number, line in enumerate(
                    path.read_text(encoding="utf-8").splitlines(),
                    start=1,
                ):
                    lowered = line.casefold()
                    for token in FORBIDDEN_PERSISTENCE_EDITION_TOKENS:
                        if token in lowered:
                            findings.append(
                                PersistenceEditionFinding(rel, line_number, token)
                            )
        evidence = {
            "scanned_files": scanned,
            "offenders": [finding.as_dict() for finding in findings],
        }
        if findings:
            return GateReport(
                gate_id=self.gate_id,
                subject="Core persistence contracts",
                status="blocking",
                severity="high",
                owner="okto-pulse-core/architecture",
                evidence={**evidence, "error": "edition_specific_persistence"},
                observed_value=evidence["offenders"],
                expected_value=[],
                remediation_hint=(
                    "Keep database-engine and driver choices in edition adapters; "
                    "Core contracts expose semantic persistence capabilities only."
                ),
            )
        return GateReport(
            gate_id=self.gate_id,
            subject="Core persistence contracts",
            status="passed",
            severity="low",
            owner="okto-pulse-core/architecture",
            evidence=evidence,
        )


__all__ = [
    "FORBIDDEN_PERSISTENCE_EDITION_TOKENS",
    "PersistenceEditionFinding",
    "PersistenceEditionNeutralityGate",
]

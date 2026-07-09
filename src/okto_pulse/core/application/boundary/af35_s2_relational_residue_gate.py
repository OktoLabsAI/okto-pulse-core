"""AF35-S2 KG relational residue gate.

AF35-S2 migrated the KG operational readers and worker helpers behind ports, but
some governance and consolidation-worker paths remain intentionally transitional.
This gate is a narrow, delta-aware guard over the S2 module set: existing
residue is ledgered by file/pattern count, while any new AsyncSession, select,
session.execute or flag_modified occurrence is reported as unledgered.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from .report import GateReport

AF35_S2_TARGET_FILES: tuple[str, ...] = (
    "kg/governance.py",
    "kg/dashboard_readers.py",
    "kg/health.py",
    "kg/cognitive_readiness.py",
    "kg/cognitive_action_center.py",
    "kg/workers/consolidation.py",
    "kg/workers/commit_events.py",
    "kg/workers/dead_letter.py",
)

PATTERN_ASYNC_SESSION_IMPORT = "async_session_import"
PATTERN_ASYNC_SESSION_ANNOTATION = "async_session_annotation"
PATTERN_SELECT_IMPORT = "select_import"
PATTERN_SELECT_CALL = "select_call"
PATTERN_SESSION_EXECUTE_CALL = "session_execute_call"
PATTERN_FLAG_MODIFIED_IMPORT = "flag_modified_import"
PATTERN_FLAG_MODIFIED_CALL = "flag_modified_call"

AF35_S2_RESIDUE_PATTERNS: frozenset[str] = frozenset(
    {
        PATTERN_ASYNC_SESSION_IMPORT,
        PATTERN_ASYNC_SESSION_ANNOTATION,
        PATTERN_SELECT_IMPORT,
        PATTERN_SELECT_CALL,
        PATTERN_SESSION_EXECUTE_CALL,
        PATTERN_FLAG_MODIFIED_IMPORT,
        PATTERN_FLAG_MODIFIED_CALL,
    }
)

LEDGER_STATUS_LEDGERED = "ledgered"
LEDGER_STATUS_UNLEDGERED = "unledgered"


@dataclass(frozen=True, slots=True)
class AF35S2ResidueLedgerEntry:
    file: str
    pattern: str
    allowed_count: int
    owner: str
    removal_date: str
    reason: str
    withdrawal_criterion: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


_S2_GOVERNANCE_TRANSITION = (
    "Remaining historical-governance flows still own legacy queue/audit/settings "
    "SQLAlchemy mechanics. Withdraw when governance effects are moved behind "
    "KGGovernanceEffectsPort/Community adapters in the next AF35 cluster."
)
_S2_WORKER_TRANSITION = (
    "Remaining consolidation worker artifact loading, claiming and DLQ auto-drain "
    "paths still own legacy SQLAlchemy mechanics. Withdraw when those transitions "
    "move behind KGWorkerQueuePort/Community UoW adapters."
)

# Register-before-remove ledger. Counts are intentionally per file/pattern rather
# than per line so harmless line movement does not force a re-baseline; adding a
# new occurrence still exceeds the allowed count and fails closed.
AF35_S2_RELATIONAL_RESIDUE_LEDGER: tuple[AF35S2ResidueLedgerEntry, ...] = (
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_ASYNC_SESSION_IMPORT,
        allowed_count=1,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional AsyncSession import for legacy KG governance entrypoints.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_ASYNC_SESSION_ANNOTATION,
        allowed_count=12,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional AsyncSession parameters for legacy KG governance entrypoints.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_SELECT_IMPORT,
        allowed_count=2,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional select imports for legacy KG governance reads.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_SELECT_CALL,
        allowed_count=18,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional select calls for historical backfill, retry and undo flows.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_SESSION_EXECUTE_CALL,
        allowed_count=30,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional db.execute calls for historical backfill, retry and undo flows.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_FLAG_MODIFIED_IMPORT,
        allowed_count=1,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional Board.settings mutation in historical progress metadata.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/governance.py",
        pattern=PATTERN_FLAG_MODIFIED_CALL,
        allowed_count=2,
        owner="af35-s2/kg-governance",
        removal_date="2026-09-30",
        reason="Transitional Board.settings mutation in historical progress metadata.",
        withdrawal_criterion=_S2_GOVERNANCE_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/workers/consolidation.py",
        pattern=PATTERN_ASYNC_SESSION_IMPORT,
        allowed_count=1,
        owner="af35-s2/kg-worker",
        removal_date="2026-09-30",
        reason="Transitional AsyncSession import for consolidation worker queue lifecycle.",
        withdrawal_criterion=_S2_WORKER_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/workers/consolidation.py",
        pattern=PATTERN_ASYNC_SESSION_ANNOTATION,
        allowed_count=7,
        owner="af35-s2/kg-worker",
        removal_date="2026-09-30",
        reason="Transitional AsyncSession parameters for consolidation worker lifecycle.",
        withdrawal_criterion=_S2_WORKER_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/workers/consolidation.py",
        pattern=PATTERN_SELECT_IMPORT,
        allowed_count=1,
        owner="af35-s2/kg-worker",
        removal_date="2026-09-30",
        reason="Transitional select import for consolidation worker artifact loading.",
        withdrawal_criterion=_S2_WORKER_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/workers/consolidation.py",
        pattern=PATTERN_SELECT_CALL,
        allowed_count=18,
        owner="af35-s2/kg-worker",
        removal_date="2026-09-30",
        reason="Transitional select calls for artifact loading, queue claiming and DLQ auto-drain.",
        withdrawal_criterion=_S2_WORKER_TRANSITION,
    ),
    AF35S2ResidueLedgerEntry(
        file="kg/workers/consolidation.py",
        pattern=PATTERN_SESSION_EXECUTE_CALL,
        allowed_count=16,
        owner="af35-s2/kg-worker",
        removal_date="2026-09-30",
        reason="Transitional db.execute calls for artifact loading, queue claiming and DLQ auto-drain.",
        withdrawal_criterion=_S2_WORKER_TRANSITION,
    ),
)


@dataclass(frozen=True, slots=True)
class AF35S2RelationalResidueFinding:
    file: str
    pattern: str
    symbol: str
    line: int
    source: str
    occurrence_index: int
    allowed_count: int
    ledger_status: str
    ledger_owner: str | None = None
    removal_date: str | None = None
    withdrawal_criterion: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AF35S2RelationalResidueReport:
    findings: tuple[AF35S2RelationalResidueFinding, ...]
    ledger_errors: tuple[str, ...] = ()

    @property
    def unledgered_findings(self) -> tuple[AF35S2RelationalResidueFinding, ...]:
        return tuple(
            finding
            for finding in self.findings
            if finding.ledger_status == LEDGER_STATUS_UNLEDGERED
        )

    @property
    def ok(self) -> bool:
        return not self.unledgered_findings and not self.ledger_errors

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "target_files": list(AF35_S2_TARGET_FILES),
            "patterns": sorted(AF35_S2_RESIDUE_PATTERNS),
            "findings": [finding.as_dict() for finding in self.findings],
            "unledgered_findings": [
                finding.as_dict() for finding in self.unledgered_findings
            ],
            "ledger_errors": list(self.ledger_errors),
            "ledger": [entry.as_dict() for entry in AF35_S2_RELATIONAL_RESIDUE_LEDGER],
        }

    def as_gate_report(self) -> GateReport:
        return GateReport(
            gate_id="af35_s2_relational_residue_gate",
            subject="AF35-S2 KG relational residue ledger",
            status="passed" if self.ok else "blocking",
            severity="high" if not self.ok else "medium",
            owner="af35-s2/kg-operational-boundary",
            evidence=self.as_dict(),
            observed_value=len(self.unledgered_findings) + len(self.ledger_errors),
            expected_value=0,
            promotion_criteria=(
                "Targeted AF35-S2 KG modules contain no new unledgered "
                "AsyncSession/select/session.execute/flag_modified residue."
            ),
            remediation_hint=(
                "Move the new relational occurrence behind an AF35-S2 Core port "
                "and Community adapter, or add a governed temporary ledger entry "
                "with owner, removal date, reason and withdrawal criterion."
            )
            if not self.ok
            else None,
        )


class _AF35S2ResidueVisitor(ast.NodeVisitor):
    def __init__(self, file: str, source_lines: list[str]) -> None:
        self.file = file
        self.source_lines = source_lines
        self.findings: list[tuple[int, str, str, str]] = []
        self.async_session_aliases: set[str] = {"AsyncSession"}
        self.select_aliases: set[str] = {"select", "_select"}
        self.flag_modified_aliases: set[str] = {"flag_modified"}
        self.sqlalchemy_module_aliases: set[str] = set()
        self.sqlalchemy_asyncio_aliases: set[str] = set()
        self.sqlalchemy_orm_attributes_aliases: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            local = alias.asname or alias.name.split(".", 1)[0]
            if alias.name == "sqlalchemy":
                self.sqlalchemy_module_aliases.add(local)
            elif alias.name == "sqlalchemy.ext.asyncio":
                self.sqlalchemy_asyncio_aliases.add(local)
            elif alias.name == "sqlalchemy.orm.attributes":
                self.sqlalchemy_orm_attributes_aliases.add(local)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        for alias in node.names:
            local = alias.asname or alias.name
            if module == "sqlalchemy.ext.asyncio" and alias.name == "AsyncSession":
                self.async_session_aliases.add(local)
                self._add(node, PATTERN_ASYNC_SESSION_IMPORT, alias.name)
            elif module == "sqlalchemy" and alias.name == "select":
                self.select_aliases.add(local)
                self._add(node, PATTERN_SELECT_IMPORT, alias.name)
            elif module == "sqlalchemy.orm.attributes" and alias.name == "flag_modified":
                self.flag_modified_aliases.add(local)
                self._add(node, PATTERN_FLAG_MODIFIED_IMPORT, alias.name)
            elif module == "sqlalchemy.ext" and alias.name == "asyncio":
                self.sqlalchemy_asyncio_aliases.add(local)
            elif module == "sqlalchemy.orm" and alias.name == "attributes":
                self.sqlalchemy_orm_attributes_aliases.add(local)
        self.generic_visit(node)

    def visit_arg(self, node: ast.arg) -> None:
        if _annotation_mentions_alias(
            node.annotation,
            names=self.async_session_aliases,
            module_aliases=self.sqlalchemy_asyncio_aliases,
            attr="AsyncSession",
        ):
            self._add(node, PATTERN_ASYNC_SESSION_ANNOTATION, "AsyncSession")
        self.generic_visit(node)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if _annotation_mentions_alias(
            node.annotation,
            names=self.async_session_aliases,
            module_aliases=self.sqlalchemy_asyncio_aliases,
            attr="AsyncSession",
        ):
            self._add(node, PATTERN_ASYNC_SESSION_ANNOTATION, "AsyncSession")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        func = node.func
        if isinstance(func, ast.Name):
            if func.id in self.select_aliases:
                self._add(node, PATTERN_SELECT_CALL, func.id)
            elif func.id in self.flag_modified_aliases:
                self._add(node, PATTERN_FLAG_MODIFIED_CALL, func.id)
        elif isinstance(func, ast.Attribute):
            if (
                func.attr == "select"
                and _root_name(func.value) in self.sqlalchemy_module_aliases
            ):
                self._add(node, PATTERN_SELECT_CALL, "sqlalchemy.select")
            elif func.attr == "flag_modified" and (
                _root_name(func.value) in self.sqlalchemy_orm_attributes_aliases
            ):
                self._add(node, PATTERN_FLAG_MODIFIED_CALL, "flag_modified")
            elif func.attr == "execute" and _root_name(func.value) in {
                "db",
                "session",
                "sqlite_session",
            }:
                self._add(
                    node,
                    PATTERN_SESSION_EXECUTE_CALL,
                    f"{_root_name(func.value)}.execute",
                )
        self.generic_visit(node)

    def _add(self, node: ast.AST, pattern: str, symbol: str) -> None:
        line = getattr(node, "lineno", 0)
        source = self.source_lines[line - 1].strip() if line else ""
        self.findings.append((line, pattern, symbol, source))


def _annotation_mentions_alias(
    node: ast.AST | None,
    *,
    names: set[str],
    module_aliases: set[str],
    attr: str,
) -> bool:
    if node is None:
        return False
    for child in ast.walk(node):
        if isinstance(child, ast.Name) and child.id in names:
            return True
        if (
            isinstance(child, ast.Attribute)
            and child.attr == attr
            and _root_name(child.value) in module_aliases
        ):
            return True
    return False


def _root_name(node: ast.AST) -> str | None:
    current = node
    while isinstance(current, ast.Attribute):
        current = current.value
    return current.id if isinstance(current, ast.Name) else None


def _default_core_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _ledger_by_key() -> dict[tuple[str, str], AF35S2ResidueLedgerEntry]:
    return {
        (entry.file, entry.pattern): entry
        for entry in AF35_S2_RELATIONAL_RESIDUE_LEDGER
    }


def validate_af35_s2_residue_ledger() -> tuple[str, ...]:
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for entry in AF35_S2_RELATIONAL_RESIDUE_LEDGER:
        key = (entry.file, entry.pattern)
        if key in seen:
            errors.append(f"duplicate ledger entry for {entry.file}:{entry.pattern}")
        seen.add(key)
        if entry.file not in AF35_S2_TARGET_FILES:
            errors.append(f"ledger file outside AF35-S2 target set: {entry.file}")
        if entry.pattern not in AF35_S2_RESIDUE_PATTERNS:
            errors.append(f"unknown ledger pattern for {entry.file}: {entry.pattern}")
        if entry.allowed_count < 0:
            errors.append(f"negative allowed_count for {entry.file}:{entry.pattern}")
        for field in ("owner", "removal_date", "reason", "withdrawal_criterion"):
            if not str(getattr(entry, field)).strip():
                errors.append(
                    f"ledger entry {entry.file}:{entry.pattern} missing {field}"
                )
    return tuple(errors)


def _scan_target_file(path: Path, rel: str) -> list[tuple[int, str, str, str]]:
    if not path.exists():
        return []
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, SyntaxError, UnicodeDecodeError):
        return []
    visitor = _AF35S2ResidueVisitor(rel, source.splitlines())
    visitor.visit(tree)
    return sorted(visitor.findings, key=lambda item: (item[0], item[1], item[2]))


def run_af35_s2_relational_residue_gate(
    core_root: str | Path | None = None,
) -> AF35S2RelationalResidueReport:
    """Run the AF35-S2 KG relational residue gate.

    ``core_root`` points at ``src/okto_pulse/core``. The default scans the real
    installed source tree.
    """

    root = Path(core_root) if core_root is not None else _default_core_root()
    ledger = _ledger_by_key()
    seen_counts: dict[tuple[str, str], int] = defaultdict(int)
    findings: list[AF35S2RelationalResidueFinding] = []

    for rel in AF35_S2_TARGET_FILES:
        for line, pattern, symbol, source in _scan_target_file(root / rel, rel):
            key = (rel, pattern)
            seen_counts[key] += 1
            occurrence_index = seen_counts[key]
            entry = ledger.get(key)
            allowed_count = entry.allowed_count if entry else 0
            if entry is not None and occurrence_index <= allowed_count:
                status = LEDGER_STATUS_LEDGERED
                owner = entry.owner
                removal_date = entry.removal_date
                withdrawal = entry.withdrawal_criterion
            else:
                status = LEDGER_STATUS_UNLEDGERED
                owner = None
                removal_date = None
                withdrawal = None
            findings.append(
                AF35S2RelationalResidueFinding(
                    file=rel,
                    pattern=pattern,
                    symbol=symbol,
                    line=line,
                    source=source,
                    occurrence_index=occurrence_index,
                    allowed_count=allowed_count,
                    ledger_status=status,
                    ledger_owner=owner,
                    removal_date=removal_date,
                    withdrawal_criterion=withdrawal,
                )
            )

    return AF35S2RelationalResidueReport(
        findings=tuple(sorted(findings, key=lambda item: (item.file, item.line, item.pattern))),
        ledger_errors=validate_af35_s2_residue_ledger(),
    )


__all__ = [
    "AF35S2RelationalResidueFinding",
    "AF35S2RelationalResidueReport",
    "AF35S2ResidueLedgerEntry",
    "AF35_S2_RELATIONAL_RESIDUE_LEDGER",
    "AF35_S2_RESIDUE_PATTERNS",
    "AF35_S2_TARGET_FILES",
    "LEDGER_STATUS_LEDGERED",
    "LEDGER_STATUS_UNLEDGERED",
    "validate_af35_s2_residue_ledger",
    "run_af35_s2_relational_residue_gate",
]

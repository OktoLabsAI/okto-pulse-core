"""AF35-S5 final relational gate and ledger.

This gate consolidates the AF35 S2/S3/S4 relational inventories into one
fail-closed ownership report. It does not migrate call sites; it records the
remaining governed residue and blocks new unowned AsyncSession/get_db/
get_db_for_mcp/session.execute/flag_modified coupling in the AF35 slices.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from okto_pulse.core.ports.relational_boundary import run_relational_ratchet_gate

from .af35_s2_relational_residue_gate import (
    AF35S2RelationalResidueReport,
    AF35_S2_RELATIONAL_RESIDUE_LEDGER,
    LEDGER_STATUS_UNLEDGERED,
    run_af35_s2_relational_residue_gate,
)
from .af35_s3_rest_residual_manifest import (
    AF35S3RestManifestEntry,
    AF35S3RestResidualReport,
    CLASS_ALLOWED_UOW_SEAM_API_DEPS,
    CLASS_COMMENT_OR_DOCSTRING,
    CLASS_DEFERRED_WITH_OWNER,
    CLASS_MIGRATED_CLEAN_TARGET,
    CLASS_NON_REST_FALSE_POSITIVE,
    CLASS_TEST_ONLY as S3_CLASS_TEST_ONLY,
    run_af35_s3_rest_residual_gate,
)
from .report import GateReport

AF35_S5_SCHEMA_VERSION = 1

SOURCE_AF35_S1_USE_CASES = "af35-s1-use-cases"
SOURCE_AF35_S2_KG = "af35-s2-kg"
SOURCE_AF35_S3_REST = "af35-s3-rest"
SOURCE_AF35_S4_MCP = "af35-s4-mcp"

SURFACE_APPLICATION_USE_CASES = "application_use_cases"
SURFACE_KG = "kg"
SURFACE_REST = "rest"
SURFACE_MCP = "mcp"

CLASS_ADAPTER_OWNED = "adapter_owned"
CLASS_MIGRATED_CLEAN = "migrated_clean"
CLASS_TEMPORARY_EXCEPTION = "temporary_core_exception"
CLASS_UOW_SEAM = "uow_seam"
CLASS_TEST_ONLY = "test_only"
CLASS_NON_PRODUCTIVE_REFERENCE = "non_productive_reference"
CLASS_UNOWNED = "unowned"

AF35_S5_RELATIONAL_CLASSIFICATIONS: frozenset[str] = frozenset(
    {
        CLASS_ADAPTER_OWNED,
        CLASS_MIGRATED_CLEAN,
        CLASS_TEMPORARY_EXCEPTION,
        CLASS_UOW_SEAM,
        CLASS_TEST_ONLY,
        CLASS_NON_PRODUCTIVE_REFERENCE,
        CLASS_UNOWNED,
    }
)

PATTERN_MCP_DIRECT_GET_DB_FOR_MCP = "mcp_direct_get_db_for_mcp"
PATTERN_USE_CASE_DIRECT_RELATIONAL = "use_case_direct_relational"

AF35_S5_REQUIRED_TEMPORARY_METADATA_FIELDS: frozenset[str] = frozenset(
    {"owner", "rationale", "public_surface", "removal_criterion", "evidence_ref"}
)

AF35_S5_DOC_BLOCK_BEGIN = "<!-- AF35-S5-RELATIONAL-OWNERSHIP:BEGIN -->"
AF35_S5_DOC_BLOCK_END = "<!-- AF35-S5-RELATIONAL-OWNERSHIP:END -->"
AF35_S5_SOURCE_BLOCK_BEGIN = "<!-- AF35-S5-RELATIONAL-SOURCES:BEGIN -->"
AF35_S5_SOURCE_BLOCK_END = "<!-- AF35-S5-RELATIONAL-SOURCES:END -->"


@dataclass(frozen=True, slots=True)
class AF35S5RelationalLedgerRow:
    source: str
    surface: str
    file: str
    pattern: str
    classification: str
    allowed_count: int
    observed_count: int
    owner: str = ""
    rationale: str = ""
    public_surface: str = ""
    removal_criterion: str = ""
    evidence_ref: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AF35S5RelationalFinding:
    source: str
    surface: str
    file: str
    pattern: str
    symbol: str
    line: int
    source_line: str
    classification: str
    owner: str | None
    metadata_status: str
    remediation_reason: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AF35S5StaleLedgerEntry:
    source: str
    surface: str
    file: str
    pattern: str
    classification: str
    allowed_count: int
    observed_count: int
    owner: str
    removal_criterion: str

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AF35S5McpDirectSessionEntry:
    function: str
    group: str
    owner: str
    rationale: str
    public_surface: str
    removal_criterion: str
    evidence_ref: str

    def as_row(self, *, observed_count: int) -> AF35S5RelationalLedgerRow:
        return AF35S5RelationalLedgerRow(
            source=SOURCE_AF35_S4_MCP,
            surface=SURFACE_MCP,
            file=f"mcp/server.py::{self.function}",
            pattern=PATTERN_MCP_DIRECT_GET_DB_FOR_MCP,
            classification=CLASS_TEMPORARY_EXCEPTION,
            allowed_count=1,
            observed_count=observed_count,
            owner=self.owner,
            rationale=self.rationale,
            public_surface=self.public_surface,
            removal_criterion=self.removal_criterion,
            evidence_ref=self.evidence_ref,
        )


@dataclass(frozen=True, slots=True)
class AF35S5RelationalFinalReport:
    source_root: str
    rows: tuple[AF35S5RelationalLedgerRow, ...]
    unowned_findings: tuple[AF35S5RelationalFinding, ...] = ()
    stale_ledger_entries: tuple[AF35S5StaleLedgerEntry, ...] = ()
    metadata_errors: tuple[str, ...] = ()
    upstream_errors: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return (
            not self.unowned_findings
            and not self.stale_ledger_entries
            and not self.metadata_errors
            and not self.upstream_errors
        )

    @property
    def counts_by_pattern(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for row in self.rows:
            counts[row.pattern] += row.observed_count
        for finding in self.unowned_findings:
            counts[finding.pattern] += 1
        return dict(sorted(counts.items()))

    @property
    def counts_by_classification(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for row in self.rows:
            counts[row.classification] += row.observed_count
        for finding in self.unowned_findings:
            counts[finding.classification] += 1
        return dict(sorted(counts.items()))

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "schema_version": AF35_S5_SCHEMA_VERSION,
            "source_root": self.source_root,
            "fail_closed": True,
            "taxonomy": sorted(AF35_S5_RELATIONAL_CLASSIFICATIONS),
            "counts_by_pattern": self.counts_by_pattern,
            "counts_by_classification": self.counts_by_classification,
            "ledger_rows": [row.as_dict() for row in self.rows],
            "unowned_findings": [
                finding.as_dict() for finding in self.unowned_findings
            ],
            "stale_ledger_entries": [
                entry.as_dict() for entry in self.stale_ledger_entries
            ],
            "metadata_errors": list(self.metadata_errors),
            "upstream_errors": list(self.upstream_errors),
        }

    def as_gate_report(self) -> GateReport:
        blocking_total = (
            len(self.unowned_findings)
            + len(self.stale_ledger_entries)
            + len(self.metadata_errors)
            + len(self.upstream_errors)
        )
        return GateReport(
            gate_id="af35_s5_relational_final_gate",
            subject="AF35-S5 final relational ownership ledger",
            status="passed" if self.ok else "blocking",
            severity="high" if not self.ok else "medium",
            owner="af35-s5/relational-final-gate",
            evidence=self.as_dict(),
            observed_value=blocking_total,
            expected_value=0,
            promotion_criteria=(
                "All remaining relational residue in AF35 service/KG/REST/MCP "
                "slices is classified by the closed ownership taxonomy."
            ),
            remediation_hint=(
                "Move new relational coupling behind UoW/port adapters, or add a "
                "temporary ledger row with owner, rationale, public surface, "
                "evidence and removal criterion."
            ),
        )


_AF35_S5_MCP_DIRECT_GET_DB_LEDGER: dict[str, frozenset[str]] = {
    "cards_and_task_context": frozenset(
        {
            "okto_pulse_resolve_bug_regression_scenarios",
            "okto_pulse_get_task_context",
            "okto_pulse_get_task_conclusions",
            "okto_pulse_list_cards_by_status",
        }
    ),
    "linking_archive": frozenset(
        {
            "_link_task_to_rule_internal",
            "_link_task_to_fr_internal",
            "_link_task_to_contract_internal",
            "_link_task_to_tr_internal",
            "okto_pulse_archive_tree",
            "okto_pulse_restore_tree",
        }
    ),
    "linking_requirements": frozenset(
        {
            "_link_task_to_integration_requirement_internal",
            "_link_task_to_observability_requirement_internal",
            "_link_task_to_decision_internal",
        }
    ),
    "spec_eval_knowledge_traceability": frozenset(
        {
            "_link_card_to_spec_internal",
            "okto_pulse_submit_spec_evaluation",
            "okto_pulse_list_spec_evaluations",
            "okto_pulse_get_spec_evaluation",
            "okto_pulse_delete_spec_evaluation",
            "okto_pulse_ask_spec_choice_question",
            "okto_pulse_answer_spec_question",
            "okto_pulse_get_traceability_report",
            "okto_pulse_get_spec_knowledge",
            "okto_pulse_add_spec_knowledge",
            "okto_pulse_delete_spec_knowledge",
            "okto_pulse_delete_spec_question",
        }
    ),
    "validation_amendments_default_config": frozenset(
        {
            "okto_pulse_confirm_amendment_coverage",
            "okto_pulse_create_amendment_revision",
            "okto_pulse_list_amendment_revisions",
            "okto_pulse_get_amendment_revision",
            "okto_pulse_associate_amendment_revision_artifacts",
            "okto_pulse_transition_amendment_revision",
            "_refuse_mcp_default_config_activation_if_human_skip_changes",
            "_refuse_mcp_default_config_deactivation_if_human_skip_changes",
        }
    ),
    "kg_residuals": frozenset(
        {
            "_kg_orphan_backfill_health_refusal",
            "okto_pulse_kg_migrate_schema",
            "okto_pulse_kg_tick_run_now",
        }
    ),
}

_AF35_S5_MCP_GROUP_METADATA: dict[str, dict[str, str]] = {
    "cards_and_task_context": {
        "owner": "AF35-S4 card context migration",
        "rationale": (
            "Residual card-context helpers still compose read models in the MCP "
            "wrapper after the core card CRUD migration."
        ),
        "removal_criterion": (
            "Bug-regression, task-context, task-conclusion and status-list "
            "surfaces run through card-context read models over MCP UoW."
        ),
    },
    "linking_archive": {
        "owner": "AF35-S4 linking/archive migration",
        "rationale": (
            "Requirement-link and tree archive/restore helpers span several "
            "domains and were left outside earlier family oracles."
        ),
        "removal_criterion": (
            "Requirement-linking and archive/restore operations move behind "
            "cohesive UoW use cases or explicit AF35-S5 successor records."
        ),
    },
    "linking_requirements": {
        "owner": "AF35-S4 requirement linking migration",
        "rationale": (
            "IR/OR/decision link helpers remain direct because they span "
            "structured spec children and card linkage side effects."
        ),
        "removal_criterion": (
            "Requirement-link helpers execute through cohesive structured-link "
            "use cases over MCP UoW."
        ),
    },
    "spec_eval_knowledge_traceability": {
        "owner": "AF35-S4 spec-adjacent residual migration",
        "rationale": (
            "Spec evaluation, spec Q&A, knowledge and traceability were not fully "
            "covered by the spec UoW oracle."
        ),
        "removal_criterion": (
            "Spec-adjacent residual tools delegate to existing or narrowly "
            "extended spec use cases over MCP UoW."
        ),
    },
    "validation_amendments_default_config": {
        "owner": "AF35-S4 amendment and default-config guard migration",
        "rationale": (
            "Amendment revisions and default-config human-control refusal helpers "
            "still have wrapper-level persistence."
        ),
        "removal_criterion": (
            "Amendment/default-config guard helpers use transport-free use cases "
            "or explicit AF35-S5 successor records."
        ),
    },
    "kg_residuals": {
        "owner": "AF35-S4 KG residual migration",
        "rationale": (
            "Some KG operational helpers were outside the already migrated KG "
            "health/DLQ surfaces."
        ),
        "removal_criterion": (
            "KG residual tools are either UoW-backed or explicitly ledgered into "
            "a successor AF35 cleanup."
        ),
    },
}

# Historical inventory retained for audit only. Every former opener now flows
# through ``get_uow_session_for_mcp`` and no longer participates in the active
# temporary-exception ledger.
_AF35_S5_RETIRED_MCP_DIRECT_GET_DB_LEDGER = _AF35_S5_MCP_DIRECT_GET_DB_LEDGER
_AF35_S5_RETIRED_MCP_GROUP_METADATA = _AF35_S5_MCP_GROUP_METADATA
_AF35_S5_MCP_DIRECT_GET_DB_LEDGER: dict[str, frozenset[str]] = {}
_AF35_S5_MCP_GROUP_METADATA: dict[str, dict[str, str]] = {}


def build_af35_s5_mcp_direct_session_ledger() -> tuple[AF35S5McpDirectSessionEntry, ...]:
    entries: list[AF35S5McpDirectSessionEntry] = []
    seen: set[str] = set()
    for group, functions in _AF35_S5_MCP_DIRECT_GET_DB_LEDGER.items():
        metadata = _AF35_S5_MCP_GROUP_METADATA[group]
        for function in functions:
            if function in seen:
                raise ValueError(f"duplicate MCP direct-session ledger entry: {function}")
            seen.add(function)
            entries.append(
                AF35S5McpDirectSessionEntry(
                    function=function,
                    group=group,
                    owner=metadata["owner"],
                    rationale=metadata["rationale"],
                    public_surface=f"MCP tools/{group}",
                    removal_criterion=metadata["removal_criterion"],
                    evidence_ref="AF35-S4 MCP residual inventory 2026-07-08",
                )
            )
    return tuple(sorted(entries, key=lambda entry: entry.function))


def _default_core_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _required_metadata_missing(row: AF35S5RelationalLedgerRow) -> list[str]:
    missing: list[str] = []
    if row.classification not in {
        CLASS_TEMPORARY_EXCEPTION,
        CLASS_UOW_SEAM,
        CLASS_ADAPTER_OWNED,
    }:
        return missing
    for field in sorted(AF35_S5_REQUIRED_TEMPORARY_METADATA_FIELDS):
        value = str(getattr(row, field)).strip()
        if not value or value.upper() in {"TBD", "TODO", "N/A"}:
            missing.append(field)
    return missing


def validate_af35_s5_relational_rows(
    rows: tuple[AF35S5RelationalLedgerRow, ...],
) -> tuple[tuple[str, ...], tuple[AF35S5StaleLedgerEntry, ...]]:
    errors: list[str] = []
    stale: list[AF35S5StaleLedgerEntry] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (row.source, row.surface, row.file, row.pattern)
        if key in seen:
            errors.append(
                "duplicate S5 ledger row for "
                f"{row.source}:{row.surface}:{row.file}:{row.pattern}"
            )
        seen.add(key)
        if row.classification not in AF35_S5_RELATIONAL_CLASSIFICATIONS:
            errors.append(
                f"unknown S5 classification for {row.file}:{row.pattern}: "
                f"{row.classification}"
            )
        if row.allowed_count < 0:
            errors.append(f"negative allowed_count for {row.file}:{row.pattern}")
        if row.observed_count < 0:
            errors.append(f"negative observed_count for {row.file}:{row.pattern}")
        if row.classification == CLASS_MIGRATED_CLEAN and row.allowed_count != 0:
            errors.append(
                f"migrated-clean row must allow zero for {row.file}:{row.pattern}"
            )
        for field in _required_metadata_missing(row):
            errors.append(f"S5 ledger row {row.file}:{row.pattern} missing {field}")
        if (
            row.allowed_count > row.observed_count
            and row.classification
            in {CLASS_TEMPORARY_EXCEPTION, CLASS_UOW_SEAM, CLASS_ADAPTER_OWNED}
        ):
            stale.append(
                AF35S5StaleLedgerEntry(
                    source=row.source,
                    surface=row.surface,
                    file=row.file,
                    pattern=row.pattern,
                    classification=row.classification,
                    allowed_count=row.allowed_count,
                    observed_count=row.observed_count,
                    owner=row.owner,
                    removal_criterion=row.removal_criterion,
                )
            )
    return tuple(errors), tuple(stale)


def build_af35_s5_relational_final_report(
    *,
    source_root: str,
    rows: tuple[AF35S5RelationalLedgerRow, ...],
    unowned_findings: tuple[AF35S5RelationalFinding, ...] = (),
    upstream_errors: tuple[str, ...] = (),
) -> AF35S5RelationalFinalReport:
    metadata_errors, stale = validate_af35_s5_relational_rows(rows)
    findings = list(unowned_findings)
    detailed_keys = {
        (finding.source, finding.surface, finding.file, finding.pattern)
        for finding in findings
    }
    for row in rows:
        key = (row.source, row.surface, row.file, row.pattern)
        if row.observed_count <= row.allowed_count or key in detailed_keys:
            continue
        findings.append(
            AF35S5RelationalFinding(
                source=row.source,
                surface=row.surface,
                file=row.file,
                pattern=row.pattern,
                symbol=row.pattern,
                line=0,
                source_line="",
                classification=CLASS_UNOWNED,
                owner=row.owner or None,
                metadata_status="allowance_exceeded",
                remediation_reason=(
                    "Observed relational occurrences exceed the S5 ledger "
                    "allowance for this row."
                ),
            )
        )
    return AF35S5RelationalFinalReport(
        source_root=source_root,
        rows=tuple(sorted(rows, key=lambda item: (item.source, item.surface, item.file, item.pattern))),
        unowned_findings=tuple(
            sorted(findings, key=lambda item: (item.source, item.surface, item.file, item.line, item.pattern))
        ),
        stale_ledger_entries=tuple(
            sorted(stale, key=lambda item: (item.source, item.surface, item.file, item.pattern))
        ),
        metadata_errors=tuple(sorted(metadata_errors)),
        upstream_errors=tuple(sorted(upstream_errors)),
    )


def _s2_rows(report: AF35S2RelationalResidueReport) -> list[AF35S5RelationalLedgerRow]:
    observed: dict[tuple[str, str], int] = defaultdict(int)
    for finding in report.findings:
        observed[(finding.file, finding.pattern)] += 1
    rows: list[AF35S5RelationalLedgerRow] = []
    for entry in AF35_S2_RELATIONAL_RESIDUE_LEDGER:
        rows.append(
            AF35S5RelationalLedgerRow(
                source=SOURCE_AF35_S2_KG,
                surface=SURFACE_KG,
                file=entry.file,
                pattern=entry.pattern,
                classification=CLASS_TEMPORARY_EXCEPTION,
                allowed_count=entry.allowed_count,
                observed_count=observed.get((entry.file, entry.pattern), 0),
                owner=entry.owner,
                rationale=entry.reason,
                public_surface="KG governance/workers",
                removal_criterion=entry.withdrawal_criterion,
                evidence_ref=f"AF35-S2 ledger removal_date={entry.removal_date}",
            )
        )
    return rows


def _s2_unowned_findings(
    report: AF35S2RelationalResidueReport,
) -> list[AF35S5RelationalFinding]:
    findings: list[AF35S5RelationalFinding] = []
    for finding in report.findings:
        if finding.ledger_status != LEDGER_STATUS_UNLEDGERED:
            continue
        findings.append(
            AF35S5RelationalFinding(
                source=SOURCE_AF35_S2_KG,
                surface=SURFACE_KG,
                file=finding.file,
                pattern=finding.pattern,
                symbol=finding.symbol,
                line=finding.line,
                source_line=finding.source,
                classification=CLASS_UNOWNED,
                owner=None,
                metadata_status="missing_ledger_row",
                remediation_reason=(
                    "KG relational residue is not covered by the AF35-S2/S5 "
                    "temporary exception ledger."
                ),
            )
        )
    return findings


def _s3_classification(value: str) -> str:
    if value == CLASS_MIGRATED_CLEAN_TARGET:
        return CLASS_MIGRATED_CLEAN
    if value == CLASS_DEFERRED_WITH_OWNER:
        return CLASS_TEMPORARY_EXCEPTION
    if value == CLASS_ALLOWED_UOW_SEAM_API_DEPS:
        return CLASS_UOW_SEAM
    if value == S3_CLASS_TEST_ONLY:
        return CLASS_TEST_ONLY
    if value in {CLASS_COMMENT_OR_DOCSTRING, CLASS_NON_REST_FALSE_POSITIVE}:
        return CLASS_NON_PRODUCTIVE_REFERENCE
    return CLASS_UNOWNED


def _s3_rows(report: AF35S3RestResidualReport) -> list[AF35S5RelationalLedgerRow]:
    rows: list[AF35S5RelationalLedgerRow] = []
    for raw in report.s5_ledger_rows():
        classification = _s3_classification(str(raw["classification"]))
        rows.append(
            AF35S5RelationalLedgerRow(
                source=SOURCE_AF35_S3_REST,
                surface=SURFACE_REST,
                file=str(raw["file"]),
                pattern=str(raw["pattern"]),
                classification=classification,
                allowed_count=int(raw["allowed_count"]),
                observed_count=int(raw["observed_count"]),
                owner=str(raw.get("owner") or ""),
                rationale=str(raw.get("rationale") or ""),
                public_surface="REST API",
                removal_criterion=str(raw.get("retirement_criterion") or ""),
                evidence_ref=str(raw.get("evidence_ref") or ""),
            )
        )
    return rows


def _s3_unowned_findings(
    report: AF35S3RestResidualReport,
) -> list[AF35S5RelationalFinding]:
    findings: list[AF35S5RelationalFinding] = []
    for finding in report.blocking_findings:
        findings.append(
            AF35S5RelationalFinding(
                source=SOURCE_AF35_S3_REST,
                surface=SURFACE_REST,
                file=finding.file,
                pattern=finding.pattern,
                symbol=finding.symbol,
                line=finding.line,
                source_line=finding.source,
                classification=CLASS_UNOWNED,
                owner=finding.owner,
                metadata_status=finding.status,
                remediation_reason=(
                    "REST relational residue is not covered by the AF35-S3/S5 "
                    "manifest allowance."
                ),
            )
        )
    return findings


def _callee_name(call: ast.Call) -> str | None:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def _direct_get_db_for_mcp_line(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
) -> int | None:
    for candidate in ast.walk(node):
        if not isinstance(candidate, ast.AsyncWith):
            continue
        for item in candidate.items:
            context_expr = item.context_expr
            if (
                isinstance(context_expr, ast.Call)
                and _callee_name(context_expr) == "get_db_for_mcp"
            ):
                return candidate.lineno
    return None


def _scan_mcp_direct_openers(source: str) -> dict[str, tuple[int, str]]:
    tree = ast.parse(source)
    lines = source.splitlines()
    found: dict[str, tuple[int, str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            continue
        line = _direct_get_db_for_mcp_line(node)
        if line is None:
            continue
        source_line = lines[line - 1].strip() if line <= len(lines) else ""
        found[node.name] = (line, source_line)
    return found


def _mcp_rows_and_findings(
    *,
    source: str,
    ledger: tuple[AF35S5McpDirectSessionEntry, ...],
) -> tuple[list[AF35S5RelationalLedgerRow], list[AF35S5RelationalFinding], list[str]]:
    try:
        direct_openers = _scan_mcp_direct_openers(source)
    except SyntaxError as exc:
        return [], [], [f"MCP server source parse error: {exc}"]

    by_function = {entry.function: entry for entry in ledger}
    rows = [
        entry.as_row(observed_count=1 if entry.function in direct_openers else 0)
        for entry in ledger
    ]
    findings: list[AF35S5RelationalFinding] = []
    for function, (line, source_line) in sorted(direct_openers.items()):
        if function in by_function:
            continue
        findings.append(
            AF35S5RelationalFinding(
                source=SOURCE_AF35_S4_MCP,
                surface=SURFACE_MCP,
                file=f"mcp/server.py::{function}",
                pattern=PATTERN_MCP_DIRECT_GET_DB_FOR_MCP,
                symbol="get_db_for_mcp",
                line=line,
                source_line=source_line,
                classification=CLASS_UNOWNED,
                owner=None,
                metadata_status="missing_ledger_row",
                remediation_reason=(
                    "MCP tool opens get_db_for_mcp directly without an AF35-S5 "
                    "temporary exception."
                ),
            )
        )
    return rows, findings, []


def _use_case_row_and_findings(
    root: Path,
) -> tuple[AF35S5RelationalLedgerRow, list[AF35S5RelationalFinding]]:
    report = run_relational_ratchet_gate(root=root)
    row = AF35S5RelationalLedgerRow(
        source=SOURCE_AF35_S1_USE_CASES,
        surface=SURFACE_APPLICATION_USE_CASES,
        file=str(root).replace("\\", "/"),
        pattern=PATTERN_USE_CASE_DIRECT_RELATIONAL,
        classification=CLASS_MIGRATED_CLEAN,
        allowed_count=0,
        observed_count=len(report.new_violations),
    )
    findings: list[AF35S5RelationalFinding] = []
    for violation in report.new_violations:
        findings.append(_finding_from_use_case_violation(violation))
    return row, findings


def _finding_from_use_case_violation(
    violation: Any,
) -> AF35S5RelationalFinding:
    return AF35S5RelationalFinding(
        source=SOURCE_AF35_S1_USE_CASES,
        surface=SURFACE_APPLICATION_USE_CASES,
        file=violation.file,
        pattern=PATTERN_USE_CASE_DIRECT_RELATIONAL,
        symbol=violation.symbol,
        line=violation.line,
        source_line="",
        classification=CLASS_UNOWNED,
        owner=None,
        metadata_status="ratchet_violation",
        remediation_reason=violation.remediation_hint,
    )


def run_af35_s5_relational_final_gate(
    core_root: str | Path | None = None,
    *,
    rest_manifest: tuple[AF35S3RestManifestEntry, ...] | None = None,
    rest_target_files: tuple[str, ...] | None = None,
    mcp_source: str | None = None,
    mcp_ledger: tuple[AF35S5McpDirectSessionEntry, ...] | None = None,
    use_cases_root: str | Path | None = None,
    include_s2: bool = True,
    include_s3: bool = True,
    include_mcp: bool = True,
    include_use_cases: bool = True,
) -> AF35S5RelationalFinalReport:
    """Run the final AF35 relational ownership gate.

    ``core_root`` points at ``src/okto_pulse/core``. Tests may disable individual
    slices to exercise fail-closed behavior with small synthetic source trees.
    """

    root = Path(core_root) if core_root is not None else _default_core_root()
    rows: list[AF35S5RelationalLedgerRow] = []
    findings: list[AF35S5RelationalFinding] = []
    upstream_errors: list[str] = []

    if include_s2:
        s2_report = run_af35_s2_relational_residue_gate(root)
        rows.extend(_s2_rows(s2_report))
        findings.extend(_s2_unowned_findings(s2_report))
        upstream_errors.extend(s2_report.ledger_errors)

    if include_s3:
        s3_report = run_af35_s3_rest_residual_gate(
            root,
            manifest=rest_manifest,
            target_files=rest_target_files,
        )
        rows.extend(_s3_rows(s3_report))
        findings.extend(_s3_unowned_findings(s3_report))
        upstream_errors.extend(s3_report.manifest_errors)

    if include_mcp:
        if mcp_source is None:
            mcp_path = root / "mcp" / "server.py"
            try:
                mcp_source = mcp_path.read_text(encoding="utf-8")
            except OSError as exc:
                mcp_source = ""
                upstream_errors.append(f"MCP server source read error: {exc}")
        mcp_rows, mcp_findings, mcp_errors = _mcp_rows_and_findings(
            source=mcp_source,
            ledger=(
                mcp_ledger
                if mcp_ledger is not None
                else build_af35_s5_mcp_direct_session_ledger()
            ),
        )
        rows.extend(mcp_rows)
        findings.extend(mcp_findings)
        upstream_errors.extend(mcp_errors)

    if include_use_cases:
        use_cases_path = (
            Path(use_cases_root)
            if use_cases_root is not None
            else root / "application" / "use_cases"
        )
        use_case_row, use_case_findings = _use_case_row_and_findings(use_cases_path)
        rows.append(use_case_row)
        findings.extend(use_case_findings)

    return build_af35_s5_relational_final_report(
        source_root=str(root).replace("\\", "/"),
        rows=tuple(rows),
        unowned_findings=tuple(findings),
        upstream_errors=tuple(upstream_errors),
    )


_CLASSIFICATION_DOCUMENTATION: dict[str, tuple[str, str, str]] = {
    CLASS_ADAPTER_OWNED: (
        "Edition adapters and repository implementations",
        "Concrete Community/SaaS adapters only; core must not import edition packages",
        "Replace by satisfying the same core ports with another edition adapter",
    ),
    CLASS_MIGRATED_CLEAN: (
        "Core application/use-case boundary",
        "Rows that must remain at zero direct relational occurrences",
        "Keep the count at zero; any observed occurrence is unowned",
    ),
    CLASS_NON_PRODUCTIVE_REFERENCE: (
        "Documentation and false-positive references",
        "Comments, docs or non-productive references only",
        "Remove when the reference stops being useful",
    ),
    CLASS_TEMPORARY_EXCEPTION: (
        "AF35 temporary residue owners named per row",
        "Governed KG, REST and MCP residues with owner/rationale/evidence/removal metadata",
        "Retire each row through its row-level removal criterion; stale rows fail closed",
    ),
    CLASS_TEST_ONLY: (
        "Test fixtures",
        "Tests and synthetic fixtures only",
        "Do not promote test-only residue into productive core paths",
    ),
    CLASS_UNOWNED: (
        "No valid owner",
        "None",
        "Block the change, migrate behind UoW/ports, or add a governed temporary row",
    ),
    CLASS_UOW_SEAM: (
        "AF35 REST UoW request boundary",
        "api/deps.py request UoW provider wrapper only",
        "Retire when REST request UoW wiring no longer wraps get_db",
    ),
}


def _all_classification_counts(report: AF35S5RelationalFinalReport) -> dict[str, int]:
    counts = {classification: 0 for classification in sorted(AF35_S5_RELATIONAL_CLASSIFICATIONS)}
    counts.update(report.counts_by_classification)
    return counts


def _source_matrix_rows(
    report: AF35S5RelationalFinalReport,
) -> list[tuple[str, str, str, int, int]]:
    grouped: dict[tuple[str, str, str], list[AF35S5RelationalLedgerRow]] = defaultdict(list)
    for row in report.rows:
        grouped[(row.source, row.surface, row.classification)].append(row)
    out: list[tuple[str, str, str, int, int]] = []
    for (source, surface, classification), rows in grouped.items():
        out.append(
            (
                source,
                surface,
                classification,
                sum(row.observed_count for row in rows),
                len(rows),
            )
        )
    return sorted(out)


def render_af35_s5_relational_ownership_markdown(
    report: AF35S5RelationalFinalReport,
) -> str:
    """Render the markdown block guarded by documentation drift tests."""

    counts = _all_classification_counts(report)
    lines = [
        AF35_S5_DOC_BLOCK_BEGIN,
        "| Classification | Observed count | Owner boundary | Allowed locations | Removal rule |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for classification in sorted(AF35_S5_RELATIONAL_CLASSIFICATIONS):
        owner, allowed_locations, removal_rule = _CLASSIFICATION_DOCUMENTATION[
            classification
        ]
        lines.append(
            "| "
            f"`{classification}` | {counts[classification]} | {owner} | "
            f"{allowed_locations} | {removal_rule} |"
        )
    lines.extend(
        [
            AF35_S5_DOC_BLOCK_END,
            "",
            AF35_S5_SOURCE_BLOCK_BEGIN,
            "| Source | Surface | Classification | Observed count | Ledger rows |",
            "| --- | --- | --- | ---: | ---: |",
        ]
    )
    for source, surface, classification, observed_count, ledger_rows in _source_matrix_rows(
        report
    ):
        lines.append(
            "| "
            f"`{source}` | `{surface}` | `{classification}` | "
            f"{observed_count} | {ledger_rows} |"
        )
    lines.append(AF35_S5_SOURCE_BLOCK_END)
    return "\n".join(lines)


__all__ = [
    "AF35S5McpDirectSessionEntry",
    "AF35S5RelationalFinalReport",
    "AF35S5RelationalFinding",
    "AF35S5RelationalLedgerRow",
    "AF35S5StaleLedgerEntry",
    "AF35_S5_RELATIONAL_CLASSIFICATIONS",
    "AF35_S5_REQUIRED_TEMPORARY_METADATA_FIELDS",
    "AF35_S5_SCHEMA_VERSION",
    "AF35_S5_DOC_BLOCK_BEGIN",
    "AF35_S5_DOC_BLOCK_END",
    "AF35_S5_SOURCE_BLOCK_BEGIN",
    "AF35_S5_SOURCE_BLOCK_END",
    "CLASS_ADAPTER_OWNED",
    "CLASS_MIGRATED_CLEAN",
    "CLASS_NON_PRODUCTIVE_REFERENCE",
    "CLASS_TEMPORARY_EXCEPTION",
    "CLASS_TEST_ONLY",
    "CLASS_UNOWNED",
    "CLASS_UOW_SEAM",
    "PATTERN_MCP_DIRECT_GET_DB_FOR_MCP",
    "PATTERN_USE_CASE_DIRECT_RELATIONAL",
    "build_af35_s5_mcp_direct_session_ledger",
    "build_af35_s5_relational_final_report",
    "render_af35_s5_relational_ownership_markdown",
    "run_af35_s5_relational_final_gate",
    "validate_af35_s5_relational_rows",
]

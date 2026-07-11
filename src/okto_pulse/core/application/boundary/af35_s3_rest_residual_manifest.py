"""AF35-S3 REST residual manifest and conformance gate.

AF35-S3 is the REST side of the UoW strangler.  This gate makes the current
``core/api`` relational surface explicit before the endpoint-family migrations
start: every productive ``Depends(get_db)``, ``get_db`` import,
``AsyncSession`` use, ``session.execute`` call and ``flag_modified`` occurrence
is classified by the closed S3 taxonomy.

The manifest is intentionally count-based per ``(file, pattern)``.  That keeps
line movement from forcing a re-baseline while still failing when a new residual
is introduced or when a migration removes residue without updating the ledger
that S5 will consume.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

from .report import GateReport

AF35_S3_REST_MANIFEST_SCHEMA_VERSION = 1

CLASS_MIGRATED_CLEAN_TARGET = "migrated_clean_target"
CLASS_DEFERRED_WITH_OWNER = "deferred_with_owner"
CLASS_ALLOWED_UOW_SEAM_API_DEPS = "allowed_uow_seam_api_deps"
CLASS_TEST_ONLY = "test_only"
CLASS_COMMENT_OR_DOCSTRING = "comment_or_docstring"
CLASS_NON_REST_FALSE_POSITIVE = "non_rest_false_positive"

AF35_S3_REST_RESIDUAL_TAXONOMY: frozenset[str] = frozenset(
    {
        CLASS_MIGRATED_CLEAN_TARGET,
        CLASS_DEFERRED_WITH_OWNER,
        CLASS_ALLOWED_UOW_SEAM_API_DEPS,
        CLASS_TEST_ONLY,
        CLASS_COMMENT_OR_DOCSTRING,
        CLASS_NON_REST_FALSE_POSITIVE,
    }
)

PATTERN_PRODUCTIVE_REST_RESIDUE = "productive_rest_residue"
PATTERN_GET_DB_IMPORT = "get_db_import"
PATTERN_DEPENDS_GET_DB = "depends_get_db"
PATTERN_ASYNC_SESSION_IMPORT = "async_session_import"
PATTERN_ASYNC_SESSION_ANNOTATION = "async_session_annotation"
PATTERN_SESSION_EXECUTE_CALL = "session_execute_call"
PATTERN_FLAG_MODIFIED_IMPORT = "flag_modified_import"
PATTERN_FLAG_MODIFIED_CALL = "flag_modified_call"

AF35_S3_REST_RESIDUAL_PATTERNS: frozenset[str] = frozenset(
    {
        PATTERN_PRODUCTIVE_REST_RESIDUE,
        PATTERN_GET_DB_IMPORT,
        PATTERN_DEPENDS_GET_DB,
        PATTERN_ASYNC_SESSION_IMPORT,
        PATTERN_ASYNC_SESSION_ANNOTATION,
        PATTERN_SESSION_EXECUTE_CALL,
        PATTERN_FLAG_MODIFIED_IMPORT,
        PATTERN_FLAG_MODIFIED_CALL,
    }
)

STATUS_CLASSIFIED = "classified"
STATUS_UNCLASSIFIED = "unclassified"
STATUS_EXCEEDED_MANIFEST_COUNT = "exceeded_manifest_count"

AF35_S3_CLEAN_REST_TARGET_FILES: tuple[str, ...] = (
    "api/__init__.py",
    "api/agents.py",
    "api/allowed_transitions.py",
    "api/analytics.py",
    "api/amendment_revisions.py",
    "api/architecture.py",
    "api/attachments.py",
    "api/auth_deps.py",
    "api/boards.py",
    "api/bug_cognitive_closure.py",
    "api/cards.py",
    "api/cognitive_action_center.py",
    "api/comments.py",
    "api/dead_letter.py",
    "api/default_board_config.py",
    "api/deps.py",
    "api/design_systems.py",
    "api/discovery.py",
    "api/guidelines.py",
    "api/ideations.py",
    "api/kg_canonical_debt.py",
    "api/kg_canonical_partition_integrity.py",
    "api/kg_cognitive_badges.py",
    "api/kg_cognitive_candidates.py",
    "api/kg_cognitive_candidate_commands.py",
    "api/kg_cognitive_pending.py",
    "api/kg_digest_layer_mismatch.py",
    "api/kg_events_hub.py",
    "api/kg_health.py",
    "api/kg_orphan_integrity.py",
    "api/kg_rebuild.py",
    "api/kg_routes.py",
    "api/kg_stale_canonical_parity.py",
    "api/kg_tick.py",
    "api/me.py",
    "api/metrics.py",
    "api/presets.py",
    "api/qa.py",
    "api/queue_health.py",
    "api/refinements.py",
    "api/resource_gate.py",
    "api/router.py",
    "api/screen_mockups.py",
    "api/settings.py",
    "api/specs.py",
    "api/sprints.py",
    "api/stories.py",
    "api/traceability.py",
)

_C2_COLLABORATION_OWNER = "af35-s3/c2-collaboration-rest-use-cases"
_C3_ADMIN_CATALOG_OWNER = "af35-s3/c3-admin-catalog-rest-use-cases"
_C4_OPERATIONAL_OWNER = "af35-s3/c4-operational-kg-rest-wrappers"
_API_DEPS_OWNER = "af35-s3/api-deps-uow-seam"

_C2_RATIONALE = (
    "Legacy collaboration handlers still own request-session persistence, "
    "activity logging and commit/refresh details until C2 moves them behind "
    "transport-free use cases."
)
_C3_RATIONALE = (
    "Admin/catalog REST handlers still inject the request database session while "
    "wrapping existing services. C3 owns moving these wrappers to explicit use "
    "cases or UoW-backed ports."
)
_C4_RATIONALE = (
    "Operational and KG-adjacent REST wrappers still carry transitional "
    "SQLAlchemy/session mechanics after S1/S2/S4. C4 owns either migrating them "
    "to the existing ports or leaving an explicit S5 exception."
)
_API_DEPS_RATIONALE = (
    "api/deps.py is the single allowed transitional UoW seam: it adapts FastAPI "
    "request session lifecycle to an edition-registered UnitOfWorkFactory and "
    "fails closed when no provider is configured."
)

_C2_RETIREMENT = (
    "Retire when comments, Q&A and attachments REST endpoints delegate to "
    "transport-free use cases and no handler/helper in these modules accepts "
    "AsyncSession or Depends(get_db)."
)
_C3_RETIREMENT = (
    "Retire when amendment revisions, default board config, design systems and "
    "screen mockups REST wrappers resolve persistence through explicit use cases "
    "or registered UoW/port providers."
)
_C4_RETIREMENT = (
    "Retire when KG/operational REST wrappers and S1-drift modules use AF35 "
    "ports/UoW providers, or when S5 records a final, owner-bearing exception."
)
_API_DEPS_RETIREMENT = (
    "Retire when REST UoW wiring no longer needs to wrap FastAPI get_db and the "
    "edition composition can provide request-scoped UoW without this seam."
)

_DEFERRED_PATTERN_COUNTS: dict[str, dict[str, int]] = {}

# F12 closed the final REST session seam. Any future occurrence is unclassified
# and blocks the gate; there is no zero-value allowance to keep in the ledger.
_ALLOWED_UOW_SEAM_COUNTS: dict[str, dict[str, int]] = {}


def _owner_for_deferred_file(file: str) -> tuple[str, str, str]:
    if file in {"api/comments.py", "api/qa.py", "api/attachments.py"}:
        return _C2_COLLABORATION_OWNER, _C2_RATIONALE, _C2_RETIREMENT
    if file in {
        "api/amendment_revisions.py",
        "api/default_board_config.py",
        "api/design_systems.py",
        "api/screen_mockups.py",
    }:
        return _C3_ADMIN_CATALOG_OWNER, _C3_RATIONALE, _C3_RETIREMENT
    return _C4_OPERATIONAL_OWNER, _C4_RATIONALE, _C4_RETIREMENT


@dataclass(frozen=True, slots=True)
class AF35S3RestManifestEntry:
    file: str
    pattern: str
    classification: str
    allowed_count: int
    owner: str = ""
    rationale: str = ""
    evidence_ref: str = ""
    retirement_criterion: str = ""

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AF35S3RestResidualFinding:
    file: str
    pattern: str
    symbol: str
    line: int
    source: str
    occurrence_index: int
    allowed_count: int
    classification: str | None
    status: str
    owner: str | None = None
    rationale: str | None = None
    evidence_ref: str | None = None
    retirement_criterion: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AF35S3RestResidualReport:
    findings: tuple[AF35S3RestResidualFinding, ...]
    manifest_errors: tuple[str, ...] = ()
    scanned_files: int = 0
    manifest: tuple[AF35S3RestManifestEntry, ...] = ()

    @property
    def blocking_findings(self) -> tuple[AF35S3RestResidualFinding, ...]:
        return tuple(
            finding
            for finding in self.findings
            if finding.status != STATUS_CLASSIFIED
        )

    @property
    def ok(self) -> bool:
        return not self.blocking_findings and not self.manifest_errors

    def manifest_entries(self) -> tuple[AF35S3RestManifestEntry, ...]:
        return self.manifest or build_af35_s3_rest_manifest()

    def s5_ledger_rows(self) -> list[dict[str, object]]:
        observed: dict[tuple[str, str], int] = defaultdict(int)
        for finding in self.findings:
            observed[(finding.file, finding.pattern)] += 1
        rows: list[dict[str, object]] = []
        for entry in self.manifest_entries():
            rows.append(
                {
                    **entry.as_dict(),
                    "observed_count": observed.get((entry.file, entry.pattern), 0),
                    "schema_version": AF35_S3_REST_MANIFEST_SCHEMA_VERSION,
                }
            )
        return rows

    def as_dict(self) -> dict[str, object]:
        by_classification: dict[str, int] = {}
        by_pattern: dict[str, int] = {}
        for finding in self.findings:
            if finding.classification:
                by_classification[finding.classification] = (
                    by_classification.get(finding.classification, 0) + 1
                )
            by_pattern[finding.pattern] = by_pattern.get(finding.pattern, 0) + 1
        return {
            "ok": self.ok,
            "schema_version": AF35_S3_REST_MANIFEST_SCHEMA_VERSION,
            "taxonomy": sorted(AF35_S3_REST_RESIDUAL_TAXONOMY),
            "patterns": sorted(AF35_S3_REST_RESIDUAL_PATTERNS),
            "scanned_files": self.scanned_files,
            "findings": [finding.as_dict() for finding in self.findings],
            "blocking_findings": [
                finding.as_dict() for finding in self.blocking_findings
            ],
            "manifest_errors": list(self.manifest_errors),
            "manifest": [entry.as_dict() for entry in self.manifest_entries()],
            "s5_ledger_rows": self.s5_ledger_rows(),
            "by_classification": by_classification,
            "by_pattern": by_pattern,
        }

    def as_gate_report(self) -> GateReport:
        return GateReport(
            gate_id="af35_s3_rest_residual_manifest",
            subject="AF35-S3 REST residual manifest and conformance gate",
            status="passed" if self.ok else "blocking",
            severity="high" if not self.ok else "medium",
            owner="af35-s3/rest-uow-strangler",
            evidence=self.as_dict(),
            observed_value=len(self.blocking_findings) + len(self.manifest_errors),
            expected_value=0,
            promotion_criteria=(
                "Every productive REST relational occurrence is classified by the "
                "closed S3 taxonomy and every clean target remains clean."
            ),
            remediation_hint=(
                "Move the offending REST occurrence behind a use case/UoW port, "
                "or update the S3 manifest with owner, rationale, evidence and "
                "retirement criterion before S5 consumes the ledger."
            )
            if not self.ok
            else None,
        )


class _AF35S3RestResidualVisitor(ast.NodeVisitor):
    def __init__(self, file: str, source_lines: list[str]) -> None:
        self.file = file
        self.source_lines = source_lines
        self.findings: list[tuple[int, str, str, str]] = []
        self.async_session_aliases: set[str] = {"AsyncSession"}
        self.flag_modified_aliases: set[str] = {"flag_modified"}
        self.get_db_aliases: set[str] = {"get_db"}
        self.sqlalchemy_asyncio_aliases: set[str] = set()
        self.sqlalchemy_orm_attributes_aliases: set[str] = set()
        self.database_module_aliases: set[str] = set()

    def visit_Import(self, node: ast.Import) -> None:
        for alias in node.names:
            local = alias.asname or alias.name.split(".", 1)[0]
            if alias.name == "sqlalchemy.ext.asyncio":
                self.sqlalchemy_asyncio_aliases.add(local)
            elif alias.name == "sqlalchemy.orm.attributes":
                self.sqlalchemy_orm_attributes_aliases.add(local)
            elif alias.name == "okto_pulse.core.infra.database":
                self.database_module_aliases.add(local)
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        module = node.module or ""
        for alias in node.names:
            local = alias.asname or alias.name
            if module == "sqlalchemy.ext.asyncio" and alias.name == "AsyncSession":
                self.async_session_aliases.add(local)
                self._add(node, PATTERN_ASYNC_SESSION_IMPORT, alias.name)
            elif module == "sqlalchemy.ext" and alias.name == "asyncio":
                self.sqlalchemy_asyncio_aliases.add(local)
            elif module == "sqlalchemy.orm.attributes" and alias.name == "flag_modified":
                self.flag_modified_aliases.add(local)
                self._add(node, PATTERN_FLAG_MODIFIED_IMPORT, alias.name)
            elif module == "sqlalchemy.orm" and alias.name == "attributes":
                self.sqlalchemy_orm_attributes_aliases.add(local)
            elif module == "okto_pulse.core.infra.database" and alias.name == "get_db":
                self.get_db_aliases.add(local)
                self._add(node, PATTERN_GET_DB_IMPORT, alias.name)
            elif module == "okto_pulse.core.infra" and alias.name == "database":
                self.database_module_aliases.add(local)
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
            if func.id in self.flag_modified_aliases:
                self._add(node, PATTERN_FLAG_MODIFIED_CALL, func.id)
            elif func.id == "Depends":
                for arg in node.args:
                    if isinstance(arg, ast.Name) and arg.id in self.get_db_aliases:
                        self._add(node, PATTERN_DEPENDS_GET_DB, f"Depends({arg.id})")
                    elif (
                        isinstance(arg, ast.Attribute)
                        and arg.attr == "get_db"
                        and _root_name(arg.value) in self.database_module_aliases
                    ):
                        self._add(node, PATTERN_DEPENDS_GET_DB, "Depends(get_db)")
        elif isinstance(func, ast.Attribute):
            root = _root_name(func.value)
            if func.attr == "execute" and root in {"db", "session", "sqlite_session"}:
                self._add(node, PATTERN_SESSION_EXECUTE_CALL, f"{root}.execute")
            elif (
                func.attr == "flag_modified"
                and root in self.sqlalchemy_orm_attributes_aliases
            ):
                self._add(node, PATTERN_FLAG_MODIFIED_CALL, "flag_modified")
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


def build_af35_s3_rest_manifest() -> tuple[AF35S3RestManifestEntry, ...]:
    entries: list[AF35S3RestManifestEntry] = []

    for file in AF35_S3_CLEAN_REST_TARGET_FILES:
        entries.append(
            AF35S3RestManifestEntry(
                file=file,
                pattern=PATTERN_PRODUCTIVE_REST_RESIDUE,
                classification=CLASS_MIGRATED_CLEAN_TARGET,
                allowed_count=0,
            )
        )

    for file, counts in _DEFERRED_PATTERN_COUNTS.items():
        owner, rationale, retirement = _owner_for_deferred_file(file)
        for pattern, allowed_count in counts.items():
            entries.append(
                AF35S3RestManifestEntry(
                    file=file,
                    pattern=pattern,
                    classification=CLASS_DEFERRED_WITH_OWNER,
                    allowed_count=allowed_count,
                    owner=owner,
                    rationale=rationale,
                    evidence_ref="AF35-S3-C1 AST inventory 2026-07-08",
                    retirement_criterion=retirement,
                )
            )

    for file, counts in _ALLOWED_UOW_SEAM_COUNTS.items():
        for pattern, allowed_count in counts.items():
            entries.append(
                AF35S3RestManifestEntry(
                    file=file,
                    pattern=pattern,
                    classification=CLASS_ALLOWED_UOW_SEAM_API_DEPS,
                    allowed_count=allowed_count,
                    owner=_API_DEPS_OWNER,
                    rationale=_API_DEPS_RATIONALE,
                    evidence_ref="DEC-AF35-S3-02 api/deps.py transitional UoW seam",
                    retirement_criterion=_API_DEPS_RETIREMENT,
                )
            )

    return tuple(sorted(entries, key=lambda entry: (entry.file, entry.pattern)))


def _manifest_by_key(
    manifest: tuple[AF35S3RestManifestEntry, ...],
) -> dict[tuple[str, str], AF35S3RestManifestEntry]:
    return {(entry.file, entry.pattern): entry for entry in manifest}


def validate_af35_s3_rest_manifest(
    manifest: tuple[AF35S3RestManifestEntry, ...] | None = None,
) -> tuple[str, ...]:
    entries = manifest if manifest is not None else build_af35_s3_rest_manifest()
    errors: list[str] = []
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        key = (entry.file, entry.pattern)
        if key in seen:
            errors.append(f"duplicate manifest entry for {entry.file}:{entry.pattern}")
        seen.add(key)
        if not entry.file.startswith("api/") or not entry.file.endswith(".py"):
            errors.append(f"manifest file outside REST api surface: {entry.file}")
        if entry.pattern not in AF35_S3_REST_RESIDUAL_PATTERNS:
            errors.append(f"unknown manifest pattern for {entry.file}: {entry.pattern}")
        if entry.classification not in AF35_S3_REST_RESIDUAL_TAXONOMY:
            errors.append(
                f"unknown manifest classification for {entry.file}:{entry.pattern}: "
                f"{entry.classification}"
            )
        if entry.allowed_count < 0:
            errors.append(f"negative allowed_count for {entry.file}:{entry.pattern}")
        if entry.classification == CLASS_MIGRATED_CLEAN_TARGET:
            if entry.pattern != PATTERN_PRODUCTIVE_REST_RESIDUE:
                errors.append(
                    f"migrated-clean entry must use wildcard pattern for {entry.file}"
                )
            if entry.allowed_count != 0:
                errors.append(f"migrated-clean entry must allow zero for {entry.file}")
        elif entry.classification == CLASS_ALLOWED_UOW_SEAM_API_DEPS:
            if entry.file != "api/deps.py":
                errors.append(
                    "allowed_uow_seam_api_deps is only valid for api/deps.py"
                )
        if entry.classification != CLASS_MIGRATED_CLEAN_TARGET:
            for field in ("owner", "rationale", "evidence_ref", "retirement_criterion"):
                if not str(getattr(entry, field)).strip():
                    errors.append(
                        f"manifest entry {entry.file}:{entry.pattern} missing {field}"
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
    visitor = _AF35S3RestResidualVisitor(rel, source.splitlines())
    visitor.visit(tree)
    return sorted(visitor.findings, key=lambda item: (item[0], item[1], item[2]))


def run_af35_s3_rest_residual_gate(
    core_root: str | Path | None = None,
    *,
    manifest: tuple[AF35S3RestManifestEntry, ...] | None = None,
    target_files: tuple[str, ...] | None = None,
) -> AF35S3RestResidualReport:
    """Run the AF35-S3 REST residual manifest gate.

    ``core_root`` points at ``src/okto_pulse/core``.  Tests may provide a small
    synthetic root plus ``manifest``/``target_files`` to exercise negative paths
    without importing the live FastAPI application.
    """

    root = Path(core_root) if core_root is not None else _default_core_root()
    entries = manifest if manifest is not None else build_af35_s3_rest_manifest()
    manifest_errors = list(validate_af35_s3_rest_manifest(entries))
    entry_by_key = _manifest_by_key(entries)

    if target_files is None:
        api_root = root / "api"
        files_to_scan = tuple(
            sorted(path.relative_to(root).as_posix() for path in api_root.glob("*.py"))
        )
    else:
        files_to_scan = target_files

    manifest_files = {entry.file for entry in entries}
    for rel in files_to_scan:
        if rel not in manifest_files:
            manifest_errors.append(f"REST api file missing from S3 manifest: {rel}")

    seen_counts: dict[tuple[str, str], int] = defaultdict(int)
    findings: list[AF35S3RestResidualFinding] = []
    for rel in files_to_scan:
        for line, pattern, symbol, source in _scan_target_file(root / rel, rel):
            key = (rel, pattern)
            seen_counts[key] += 1
            occurrence_index = seen_counts[key]
            entry = entry_by_key.get(key) or entry_by_key.get(
                (rel, PATTERN_PRODUCTIVE_REST_RESIDUE)
            )
            if entry is None:
                status = STATUS_UNCLASSIFIED
                allowed_count = 0
                classification = None
                owner = rationale = evidence_ref = retirement = None
            else:
                allowed_count = entry.allowed_count
                classification = entry.classification
                owner = entry.owner or None
                rationale = entry.rationale or None
                evidence_ref = entry.evidence_ref or None
                retirement = entry.retirement_criterion or None
                status = (
                    STATUS_CLASSIFIED
                    if occurrence_index <= allowed_count
                    else STATUS_EXCEEDED_MANIFEST_COUNT
                )
            findings.append(
                AF35S3RestResidualFinding(
                    file=rel,
                    pattern=pattern,
                    symbol=symbol,
                    line=line,
                    source=source,
                    occurrence_index=occurrence_index,
                    allowed_count=allowed_count,
                    classification=classification,
                    status=status,
                    owner=owner,
                    rationale=rationale,
                    evidence_ref=evidence_ref,
                    retirement_criterion=retirement,
                )
            )

    for entry in entries:
        if entry.pattern == PATTERN_PRODUCTIVE_REST_RESIDUE:
            continue
        observed = seen_counts.get((entry.file, entry.pattern), 0)
        if observed < entry.allowed_count:
            manifest_errors.append(
                "stale manifest allowance for "
                f"{entry.file}:{entry.pattern}: allowed={entry.allowed_count} "
                f"observed={observed}"
            )

    return AF35S3RestResidualReport(
        findings=tuple(
            sorted(findings, key=lambda item: (item.file, item.line, item.pattern))
        ),
        manifest_errors=tuple(manifest_errors),
        scanned_files=len(files_to_scan),
        manifest=entries,
    )


__all__ = [
    "AF35S3RestManifestEntry",
    "AF35S3RestResidualFinding",
    "AF35S3RestResidualReport",
    "AF35_S3_CLEAN_REST_TARGET_FILES",
    "AF35_S3_REST_MANIFEST_SCHEMA_VERSION",
    "AF35_S3_REST_RESIDUAL_PATTERNS",
    "AF35_S3_REST_RESIDUAL_TAXONOMY",
    "CLASS_ALLOWED_UOW_SEAM_API_DEPS",
    "CLASS_COMMENT_OR_DOCSTRING",
    "CLASS_DEFERRED_WITH_OWNER",
    "CLASS_MIGRATED_CLEAN_TARGET",
    "CLASS_NON_REST_FALSE_POSITIVE",
    "CLASS_TEST_ONLY",
    "PATTERN_ASYNC_SESSION_ANNOTATION",
    "PATTERN_ASYNC_SESSION_IMPORT",
    "PATTERN_DEPENDS_GET_DB",
    "PATTERN_FLAG_MODIFIED_CALL",
    "PATTERN_FLAG_MODIFIED_IMPORT",
    "PATTERN_GET_DB_IMPORT",
    "PATTERN_PRODUCTIVE_REST_RESIDUE",
    "PATTERN_SESSION_EXECUTE_CALL",
    "STATUS_CLASSIFIED",
    "STATUS_EXCEEDED_MANIFEST_COUNT",
    "STATUS_UNCLASSIFIED",
    "build_af35_s3_rest_manifest",
    "run_af35_s3_rest_residual_gate",
    "validate_af35_s3_rest_manifest",
]

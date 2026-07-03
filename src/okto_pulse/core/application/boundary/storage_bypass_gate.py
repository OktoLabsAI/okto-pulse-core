"""Storage-bypass anti-regression gate (SaaS Refactor spec R02, FR4 / AC4 ac_067d8d8e).

A pure, delta-aware AST gate over the core HTTP API and MCP trees
(``src/okto_pulse/core/api`` and ``src/okto_pulse/core/mcp``) that FAILS when an
attachment/resource endpoint or MCP tool reaches concrete files/URLs directly
instead of going through the registered runtime ports.

Register-before-remove governance mirrors :mod:`core_orm_import_gate`: the
:data:`STORAGE_BYPASS_ALLOWLIST` is a FROZEN literal capturing the CURRENT set of
legitimate filesystem touch-points in the core API (empty after IMP1 — the core
API has no filesystem bypass). Any occurrence in a file ABSENT from the allowlist
is a blocking violation; the allowlist may only SHRINK (ratchet).

Pure static analysis (``ast`` + ``pathlib``); the scanned code is never imported.
The concrete ``StorageProvider`` adapter (Community ``CommunityFileSystemStorage``)
legitimately owns the filesystem and lives OUTSIDE this guarded tree, so it is
never scanned here (FR4 — "fora do adapter Community").
"""

from __future__ import annotations

import ast
import copy
import hashlib
from dataclasses import dataclass, field
from pathlib import Path

# --- kinds of filesystem bypass AC4 forbids in core attachment/resource endpoints
KIND_FILE_RESPONSE = "file_response"
KIND_OPEN = "open_call"
KIND_PATHLIB_IO = "pathlib_io"
KIND_SHUTIL_IO = "shutil_io"
KIND_MCP_CONTENT_FETCH = "mcp_content_fetch"
KIND_MCP_CONCRETE_SOURCE_PARAM = "mcp_concrete_source_param"

#: ``pathlib.Path`` read/write IO methods (a ``Path(...).read_bytes()`` style call
#: is a direct filesystem read/write — the bypass the StorageProvider replaces).
_PATHLIB_IO_METHODS = frozenset(
    {"read_bytes", "read_text", "write_bytes", "write_text"}
)
#: ``shutil`` filesystem copy/move helpers.
_SHUTIL_IO_FUNCS = frozenset({"copyfile", "copy", "copy2", "copyfileobj", "move"})

#: FROZEN file -> reason ratchet (register-before-remove). EMPTY after R02 IMP1:
#: the core API has no legitimate filesystem touch-point. A NEW occurrence in any
#: file is therefore a blocking violation. Shrinks only.
STORAGE_BYPASS_ALLOWLIST: dict[str, str] = {}
STORAGE_BYPASS_OCCURRENCE_ALLOWLIST: dict[str, dict[str, str]] = {
    # Fingerprints are content-anchored. Line numbers are diagnostics only.
    "30f70245ba408cbc03a4f98a2a8efe45f470bd26ebcc1cce8209d75ac4324900": {
        "owner": "core-mcp",
        "reason": "MCP bundled prompt load; not user-supplied content ingestion.",
        "removal_criteria": "Remove when MCP instructions are provided through an injected resource provider.",
    },
    "276d7bbdeaa4e4bbdbbd39ffc261377173a71d29d091cade4e3c900160e25623": {
        "owner": "core-mcp",
        "reason": "MCP bundled resource load; not user-supplied content ingestion.",
        "removal_criteria": "Remove when MCP resources are provided through an injected resource provider.",
    },
    "e2284ae9a3a01e9b63b0c314c1e948ba535aa7d7f2f8b456ff907845045311fe": {
        "owner": "core-mcp",
        "reason": "Explicit local manifest inspection utility, not runtime attachment/content ingestion.",
        "removal_criteria": "Remove when payload budget profiles are loaded through a resource provider.",
    },
}


@dataclass(frozen=True)
class StorageBypassOccurrence:
    """One detected direct-filesystem touch-point in a core API file."""

    file: str
    line: int
    symbol: str
    kind: str
    enclosing_qualname: str
    normalized_call_ast: str
    relevant_imports: tuple[str, ...]
    fingerprint: str

    def as_dict(self) -> dict:
        return {
            "file": self.file,
            "line": self.line,
            "symbol": self.symbol,
            "kind": self.kind,
            "enclosing_qualname": self.enclosing_qualname,
            "normalized_call_ast": self.normalized_call_ast,
            "relevant_imports": list(self.relevant_imports),
            "fingerprint": self.fingerprint,
        }

    def key(self) -> str:
        return self.fingerprint


def _alias_map(tree: ast.AST) -> dict[str, str]:
    """Bind every local name to the full module/name it imports (so an aliased
    ``import shutil as sh`` or ``from starlette.responses import FileResponse`` is
    resolved back to its canonical origin)."""
    alias: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.asname:
                    alias[a.asname] = a.name
                else:
                    top = a.name.split(".", 1)[0]
                    alias.setdefault(top, top)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            for a in node.names:
                full = f"{base}.{a.name}" if base else a.name
                alias[a.asname or a.name] = full
    return alias


def _func_name(node: ast.AST) -> str | None:
    """Return the trailing name of a call target (``open`` / ``FileResponse`` /
    ``read_bytes`` / ``copyfile``) regardless of how it is qualified."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _base_name(node: ast.Attribute) -> str | None:
    """Return the root name of an attribute chain (``shutil`` in ``shutil.copyfile``,
    or ``sh`` in ``sh.copyfile``); None if the receiver is not a bare name."""
    cur: ast.AST = node.value
    while isinstance(cur, ast.Attribute):
        cur = cur.value
    return cur.id if isinstance(cur, ast.Name) else None


def _root_name(node: ast.AST) -> str | None:
    """Return the first Name in a call target chain.

    Examples:
    - ``candidate.read_text`` -> ``candidate``
    - ``Path(src).read_text`` -> ``Path``
    - ``shutil.copyfile`` -> ``shutil``
    """
    cur = node
    while isinstance(cur, ast.Attribute):
        cur = cur.value
    if isinstance(cur, ast.Call):
        return _root_name(cur.func)
    return cur.id if isinstance(cur, ast.Name) else None


def _parent_map(tree: ast.AST) -> dict[ast.AST, ast.AST]:
    return {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}


def _enclosing_qualname(parents: dict[ast.AST, ast.AST], node: ast.AST) -> str:
    names: list[str] = []
    cur: ast.AST | None = node
    while cur is not None:
        if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            names.append(cur.name)
        cur = parents.get(cur)
    return ".".join(reversed(names)) or "<module>"


def _canonical_ast_dump(node: ast.AST) -> str:
    cloned = copy.deepcopy(node)
    for child in ast.walk(cloned):
        for attr in ("lineno", "col_offset", "end_lineno", "end_col_offset"):
            if hasattr(child, attr):
                setattr(child, attr, None)
        if isinstance(child, ast.Call):
            child.keywords = sorted(child.keywords, key=lambda kw: kw.arg or "")
    return ast.dump(cloned, annotate_fields=True, include_attributes=False)


def _relevant_imports(alias: dict[str, str], call_node: ast.AST | None) -> tuple[str, ...]:
    if call_node is None:
        return ()
    root = _root_name(call_node)
    if root is None:
        return ()
    resolved = alias.get(root)
    return (f"{root}={resolved}",) if resolved is not None else ()


def _fingerprint(
    *,
    file_label: str,
    kind: str,
    symbol: str,
    normalized_call_ast: str,
    relevant_imports: tuple[str, ...],
    enclosing_qualname: str,
) -> str:
    payload = "\n".join(
        [
            f"file={file_label}",
            f"kind={kind}",
            f"symbol={symbol}",
            f"qualname={enclosing_qualname}",
            "imports=" + "\x1f".join(relevant_imports),
            f"ast={normalized_call_ast}",
        ]
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _expr_uses_name(node: ast.AST, name: str) -> bool:
    return any(isinstance(child, ast.Name) and child.id == name for child in ast.walk(node))


def _is_mcp_tool(node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
    for decorator in node.decorator_list:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Attribute) and target.attr == "tool":
            return True
    return False


def _scan_module(tree: ast.AST, file_label: str) -> list[StorageBypassOccurrence]:
    """Detect every direct-filesystem touch-point in one core API module."""
    alias = _alias_map(tree)
    parents = _parent_map(tree)
    found: dict[tuple[int, str, str, str], StorageBypassOccurrence] = {}

    def record(node: ast.AST, symbol: str, kind: str, call_node: ast.AST | None = None) -> None:
        call_for_fingerprint = call_node or node
        normalized = _canonical_ast_dump(call_for_fingerprint)
        imports = _relevant_imports(alias, call_for_fingerprint)
        qualname = _enclosing_qualname(parents, node)
        fingerprint = _fingerprint(
            file_label=file_label,
            kind=kind,
            symbol=symbol,
            normalized_call_ast=normalized,
            relevant_imports=imports,
            enclosing_qualname=qualname,
        )
        line = getattr(node, "lineno", 0)
        found.setdefault(
            (line, symbol, kind, fingerprint),
            StorageBypassOccurrence(
                file=file_label,
                line=line,
                symbol=symbol,
                kind=kind,
                enclosing_qualname=qualname,
                normalized_call_ast=normalized,
                relevant_imports=imports,
                fingerprint=fingerprint,
            ),
        )

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_mcp_tool(node):
            for arg in [*node.args.args, *node.args.kwonlyargs]:
                if arg.arg in {"file_path", "file_url"}:
                    record(arg, arg.arg, KIND_MCP_CONCRETE_SOURCE_PARAM)

        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = _func_name(func)
        if name is None:
            continue

        # FileResponse(...) — needs a concrete path; the bypass IMP1 removed.
        # Alias-aware: a literal ``FileResponse(...)`` / ``responses.FileResponse(...)``
        # OR a bare name that an import bound to FileResponse
        # (``from starlette.responses import FileResponse as FR; FR(path=...)``).
        resolved = alias.get(func.id) if isinstance(func, ast.Name) else None
        if name == "FileResponse" or (
            resolved is not None and resolved.rsplit(".", 1)[-1] == "FileResponse"
        ):
            record(node, "FileResponse", KIND_FILE_RESPONSE, node)
            continue

        # builtin open(...) — only when ``open`` is a bare name (not ``x.open``).
        if isinstance(func, ast.Name) and name == "open":
            record(node, "open", KIND_OPEN, node)
            continue

        if isinstance(func, ast.Attribute):
            # pathlib IO: ``<path>.read_bytes()`` / ``.write_text()`` / ... or a
            # ``Path(...).open()`` chain (receiver is a Path(...) construction).
            if name in _PATHLIB_IO_METHODS:
                record(node, name, KIND_PATHLIB_IO, node)
                continue
            if name == "open" and isinstance(func.value, ast.Call):
                recv = _func_name(func.value.func)
                resolved = alias.get(recv or "", recv or "")
                if recv == "Path" or resolved.endswith(".Path"):
                    record(node, "Path.open", KIND_PATHLIB_IO, node)
                    continue
            # shutil copy/move: ``shutil.copyfile(...)`` (resolve aliased base).
            if name in _SHUTIL_IO_FUNCS:
                base = _base_name(func)
                if base is not None and alias.get(base, base).split(".", 1)[0] == "shutil":
                    record(node, f"shutil.{name}", KIND_SHUTIL_IO, node)
                    continue

            if name == "get" and any(
                _expr_uses_name(arg, "file_url") for arg in [*node.args, *(kw.value for kw in node.keywords)]
            ):
                record(node, "get(file_url)", KIND_MCP_CONTENT_FETCH, node)
                continue

    return list(found.values())


@dataclass
class StorageBypassGateReport:
    """Result of running the core storage-bypass gate."""

    ok: bool
    scanned_files: int
    guarded_path: str
    occurrences: list[StorageBypassOccurrence] = field(default_factory=list)
    violations: list[StorageBypassOccurrence] = field(default_factory=list)
    allowlisted_files: list[str] = field(default_factory=list)
    allowlisted_fingerprints: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "guarded_path": self.guarded_path,
            "occurrences": [o.as_dict() for o in self.occurrences],
            "violations": [o.as_dict() for o in self.violations],
            "allowlisted_files": list(self.allowlisted_files),
            "allowlisted_fingerprints": list(self.allowlisted_fingerprints),
        }


def default_core_api_path() -> Path:
    # src/okto_pulse/core/application/boundary/storage_bypass_gate.py -> core/api
    return Path(__file__).resolve().parents[2] / "api"


def default_core_mcp_path() -> Path:
    # src/okto_pulse/core/application/boundary/storage_bypass_gate.py -> core/mcp
    return Path(__file__).resolve().parents[2] / "mcp"


def _label(path: Path) -> str:
    """Stable ``src/okto_pulse/core/...`` label — identical for the real tree and a
    mirrored ``tmp/src/okto_pulse/core/...`` fixture tree (so teeth need no real
    tree mutation)."""
    parts = path.parts
    if "src" in parts:
        return "/".join(parts[parts.index("src"):])
    return path.name


def run_storage_bypass_gate(root: str | Path | None = None) -> StorageBypassGateReport:
    """Scan every ``*.py`` under ``root`` (default: core api + mcp) for
    a direct-filesystem bypass. ``ok`` is True only when every detected occurrence
    is registered in a file or occurrence allowlist (delta-aware: a NEW bypass
    outside the allowlist makes ``ok`` False, citing file:line:kind).
    """
    bases = [Path(root)] if root is not None else [default_core_api_path(), default_core_mcp_path()]
    occurrences: list[StorageBypassOccurrence] = []
    scanned = 0
    for base in bases:
        files = sorted(base.rglob("*.py")) if base.exists() else []
        for path in files:
            if "__pycache__" in path.parts:
                continue
            scanned += 1
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            except (OSError, SyntaxError):
                continue
            occurrences.extend(_scan_module(tree, _label(path)))

    def _allowlisted(o: StorageBypassOccurrence) -> bool:
        return o.file in STORAGE_BYPASS_ALLOWLIST or o.key() in STORAGE_BYPASS_OCCURRENCE_ALLOWLIST

    violations = [o for o in occurrences if not _allowlisted(o)]
    allowlisted_hit = sorted(
        {
            o.file
            for o in occurrences
            if o.file in STORAGE_BYPASS_ALLOWLIST or o.key() in STORAGE_BYPASS_OCCURRENCE_ALLOWLIST
        }
    )
    allowlisted_fingerprints = sorted(
        {o.key() for o in occurrences if o.key() in STORAGE_BYPASS_OCCURRENCE_ALLOWLIST}
    )
    return StorageBypassGateReport(
        ok=not violations,
        scanned_files=scanned,
        guarded_path=";".join(str(base) for base in bases),
        occurrences=occurrences,
        violations=violations,
        allowlisted_files=allowlisted_hit,
        allowlisted_fingerprints=allowlisted_fingerprints,
    )


def storage_bypass_allowlist_only_shrinks(
    previous: dict[str, str], current: dict[str, str]
) -> bool:
    """True iff ``current`` is a ratchet of ``previous``: no new file, no loosened
    reason. Files may be DROPPED (a bypass removed). Mirrors
    ``core_orm_allowlist_only_shrinks``."""
    for file_label, reason in current.items():
        if file_label not in previous:
            return False
        if previous[file_label] != reason:
            return False
    return True


def storage_bypass_occurrence_allowlist_only_shrinks(
    previous: dict[str, dict[str, str]], current: dict[str, dict[str, str]]
) -> bool:
    """True iff occurrence allowlist keys/metadata only shrink.

    A new fingerprint or changed owner/reason/removal criteria is a loosening and
    must be reviewed as a new exception, not silently accepted by the ratchet.
    """
    for fingerprint, metadata in current.items():
        if fingerprint not in previous:
            return False
        if previous[fingerprint] != metadata:
            return False
    return True


__all__ = [
    "KIND_FILE_RESPONSE",
    "KIND_OPEN",
    "KIND_PATHLIB_IO",
    "KIND_SHUTIL_IO",
    "KIND_MCP_CONTENT_FETCH",
    "KIND_MCP_CONCRETE_SOURCE_PARAM",
    "STORAGE_BYPASS_ALLOWLIST",
    "STORAGE_BYPASS_OCCURRENCE_ALLOWLIST",
    "StorageBypassOccurrence",
    "StorageBypassGateReport",
    "default_core_api_path",
    "default_core_mcp_path",
    "run_storage_bypass_gate",
    "storage_bypass_allowlist_only_shrinks",
    "storage_bypass_occurrence_allowlist_only_shrinks",
]

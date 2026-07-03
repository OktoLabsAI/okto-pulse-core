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
STORAGE_BYPASS_OCCURRENCE_ALLOWLIST: dict[tuple[str, int, str, str], str] = {
    (
        "src/okto_pulse/core/mcp/server.py",
        167,
        "read_text",
        KIND_PATHLIB_IO,
    ): "MCP bundled prompt load; not user-supplied content ingestion.",
    (
        "src/okto_pulse/core/mcp/server.py",
        199,
        "read_text",
        KIND_PATHLIB_IO,
    ): "MCP bundled resource load; not user-supplied content ingestion.",
    (
        "src/okto_pulse/core/mcp/payload_budget.py",
        168,
        "read_text",
        KIND_PATHLIB_IO,
    ): "Explicit local manifest inspection utility, not runtime attachment/content ingestion.",
}


@dataclass(frozen=True)
class StorageBypassOccurrence:
    """One detected direct-filesystem touch-point in a core API file."""

    file: str
    line: int
    symbol: str
    kind: str

    def as_dict(self) -> dict:
        return {"file": self.file, "line": self.line, "symbol": self.symbol, "kind": self.kind}

    def key(self) -> tuple[str, int, str, str]:
        return (self.file, self.line, self.symbol, self.kind)


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
    found: dict[tuple[int, str], StorageBypassOccurrence] = {}

    def record(line: int, symbol: str, kind: str) -> None:
        found.setdefault((line, symbol), StorageBypassOccurrence(file_label, line, symbol, kind))

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and _is_mcp_tool(node):
            for arg in [*node.args.args, *node.args.kwonlyargs]:
                if arg.arg in {"file_path", "file_url"}:
                    record(arg.lineno, arg.arg, KIND_MCP_CONCRETE_SOURCE_PARAM)

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
            record(node.lineno, "FileResponse", KIND_FILE_RESPONSE)
            continue

        # builtin open(...) — only when ``open`` is a bare name (not ``x.open``).
        if isinstance(func, ast.Name) and name == "open":
            record(node.lineno, "open", KIND_OPEN)
            continue

        if isinstance(func, ast.Attribute):
            # pathlib IO: ``<path>.read_bytes()`` / ``.write_text()`` / ... or a
            # ``Path(...).open()`` chain (receiver is a Path(...) construction).
            if name in _PATHLIB_IO_METHODS:
                record(node.lineno, name, KIND_PATHLIB_IO)
                continue
            if name == "open" and isinstance(func.value, ast.Call):
                recv = _func_name(func.value.func)
                resolved = alias.get(recv or "", recv or "")
                if recv == "Path" or resolved.endswith(".Path"):
                    record(node.lineno, "Path.open", KIND_PATHLIB_IO)
                    continue
            # shutil copy/move: ``shutil.copyfile(...)`` (resolve aliased base).
            if name in _SHUTIL_IO_FUNCS:
                base = _base_name(func)
                if base is not None and alias.get(base, base).split(".", 1)[0] == "shutil":
                    record(node.lineno, f"shutil.{name}", KIND_SHUTIL_IO)
                    continue

            if name == "get" and any(
                _expr_uses_name(arg, "file_url") for arg in [*node.args, *(kw.value for kw in node.keywords)]
            ):
                record(node.lineno, "get(file_url)", KIND_MCP_CONTENT_FETCH)
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

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "guarded_path": self.guarded_path,
            "occurrences": [o.as_dict() for o in self.occurrences],
            "violations": [o.as_dict() for o in self.violations],
            "allowlisted_files": list(self.allowlisted_files),
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
    return StorageBypassGateReport(
        ok=not violations,
        scanned_files=scanned,
        guarded_path=";".join(str(base) for base in bases),
        occurrences=occurrences,
        violations=violations,
        allowlisted_files=allowlisted_hit,
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
]

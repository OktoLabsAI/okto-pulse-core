"""Agent credential reveal-once anti-regression gate (AF14)."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from pathlib import Path

FORBIDDEN_AGENT_RESPONSE_FIELDS = frozenset({"api_key", "secret", "token_value", "plaintext"})
PROTECTED_AGENT_RESPONSE_CLASSES = frozenset({"AgentResponse", "AgentSummary"})
ALLOWED_REVEAL_RESPONSE_CLASS = "AgentRevealResponse"
ALLOWED_REVEAL_FIELD = "reveal_once_secret"
ALLOWED_REVEAL_ENDPOINTS = frozenset({"create_agent", "regenerate_agent_key"})

KIND_RESPONSE_SECRET_FIELD = "response_secret_field"
KIND_UNSCOPED_REVEAL_RESPONSE = "unscoped_reveal_response"
KIND_PLAINTEXT_AGENT_PERSISTENCE = "plaintext_agent_persistence"
KIND_AGENT_API_SECRET_ASSIGNMENT = "agent_api_secret_assignment"
KIND_PERSISTED_SECRET_FIELD = "persisted_secret_field"


@dataclass(frozen=True)
class AgentSecretSurfaceOccurrence:
    file: str
    line: int
    symbol: str
    kind: str

    def as_dict(self) -> dict:
        return {"file": self.file, "line": self.line, "symbol": self.symbol, "kind": self.kind}


@dataclass
class AgentSecretSurfaceGateReport:
    ok: bool
    scanned_files: int
    guarded_path: str
    occurrences: list[AgentSecretSurfaceOccurrence] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "scanned_files": self.scanned_files,
            "guarded_path": self.guarded_path,
            "occurrences": [o.as_dict() for o in self.occurrences],
        }


def _label(path: Path) -> str:
    parts = path.parts
    if "src" in parts:
        return "/".join(parts[parts.index("src"):])
    return path.name


def _target_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        return _target_name(node.func)
    return None


def _is_credential_marker_call(node: ast.AST) -> bool:
    return _call_name(node) == "credential_marker"


def _route_response_model(decorator: ast.AST) -> str | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    if not (isinstance(target, ast.Attribute) and target.attr in {"get", "post", "patch"}):
        return None
    if not isinstance(decorator, ast.Call):
        return None
    for keyword in decorator.keywords:
        if keyword.arg == "response_model":
            if isinstance(keyword.value, ast.Name):
                return keyword.value.id
            if isinstance(keyword.value, ast.Subscript):
                return ast.unparse(keyword.value)
    return None


def _scan_schemas(tree: ast.AST, file_label: str) -> list[AgentSecretSurfaceOccurrence]:
    found: list[AgentSecretSurfaceOccurrence] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        if node.name in PROTECTED_AGENT_RESPONSE_CLASSES or node.name == ALLOWED_REVEAL_RESPONSE_CLASS:
            for stmt in node.body:
                if isinstance(stmt, ast.AnnAssign):
                    field_name = _target_name(stmt.target)
                    if field_name is None:
                        continue
                    forbidden = field_name in FORBIDDEN_AGENT_RESPONSE_FIELDS
                    reveal_allowed = (
                        node.name == ALLOWED_REVEAL_RESPONSE_CLASS and field_name == ALLOWED_REVEAL_FIELD
                    )
                    if forbidden and not reveal_allowed:
                        found.append(
                            AgentSecretSurfaceOccurrence(
                                file_label,
                                stmt.lineno,
                                f"{node.name}.{field_name}",
                                KIND_RESPONSE_SECRET_FIELD,
                            )
                        )
    return found


def _scan_agent_api(tree: ast.AST, file_label: str) -> list[AgentSecretSurfaceOccurrence]:
    found: list[AgentSecretSurfaceOccurrence] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                response_model = _route_response_model(decorator)
                if response_model and "AgentRevealResponse" in response_model:
                    if node.name not in ALLOWED_REVEAL_ENDPOINTS:
                        found.append(
                            AgentSecretSurfaceOccurrence(
                                file_label,
                                node.lineno,
                                f"{node.name}:{response_model}",
                                KIND_UNSCOPED_REVEAL_RESPONSE,
                            )
                        )

        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Attribute) and target.attr in FORBIDDEN_AGENT_RESPONSE_FIELDS:
                    found.append(
                        AgentSecretSurfaceOccurrence(
                            file_label,
                            node.lineno,
                            f".{target.attr}",
                            KIND_AGENT_API_SECRET_ASSIGNMENT,
                        )
                    )
    return found


def _scan_agent_service(tree: ast.AST, file_label: str) -> list[AgentSecretSurfaceOccurrence]:
    found: list[AgentSecretSurfaceOccurrence] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and _call_name(node) == "Agent":
            for keyword in node.keywords:
                if keyword.arg == "api_key" and not _is_credential_marker_call(keyword.value):
                    found.append(
                        AgentSecretSurfaceOccurrence(
                            file_label,
                            keyword.value.lineno,
                            "Agent(api_key=...)",
                            KIND_PLAINTEXT_AGENT_PERSISTENCE,
                        )
                    )
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Attribute) and target.attr == "api_key":
                    if not _is_credential_marker_call(node.value):
                        found.append(
                            AgentSecretSurfaceOccurrence(
                                file_label,
                                node.lineno,
                                "agent.api_key",
                                KIND_PLAINTEXT_AGENT_PERSISTENCE,
                            )
                        )
    return found


def _scan_agent_db(tree: ast.AST, file_label: str) -> list[AgentSecretSurfaceOccurrence]:
    found: list[AgentSecretSurfaceOccurrence] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef) or node.name != "Agent":
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign):
                continue
            field_name = _target_name(stmt.target)
            if field_name in FORBIDDEN_AGENT_RESPONSE_FIELDS and field_name != "api_key":
                found.append(
                    AgentSecretSurfaceOccurrence(
                        file_label,
                        stmt.lineno,
                        f"Agent.{field_name}",
                        KIND_PERSISTED_SECRET_FIELD,
                    )
                )
    return found


def default_agent_secret_guard_root() -> Path:
    return Path(__file__).resolve().parents[2]


def run_agent_secret_surface_gate(
    root: str | Path | None = None,
) -> AgentSecretSurfaceGateReport:
    base = Path(root) if root is not None else default_agent_secret_guard_root()
    candidates = [
        base / "models" / "schemas.py",
        base / "models" / "db.py",
        base / "api" / "agents.py",
        base / "services" / "main.py",
    ]
    occurrences: list[AgentSecretSurfaceOccurrence] = []
    scanned = 0
    for path in candidates:
        if not path.exists():
            continue
        scanned += 1
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except (OSError, SyntaxError):
            continue
        file_label = _label(path)
        if path.name == "schemas.py":
            occurrences.extend(_scan_schemas(tree, file_label))
        elif path.name == "db.py":
            occurrences.extend(_scan_agent_db(tree, file_label))
        elif path.name == "agents.py":
            occurrences.extend(_scan_agent_api(tree, file_label))
        elif path.name == "main.py":
            occurrences.extend(_scan_agent_service(tree, file_label))
    return AgentSecretSurfaceGateReport(
        ok=not occurrences,
        scanned_files=scanned,
        guarded_path=str(base),
        occurrences=occurrences,
    )


__all__ = [
    "AgentSecretSurfaceGateReport",
    "AgentSecretSurfaceOccurrence",
    "FORBIDDEN_AGENT_RESPONSE_FIELDS",
    "KIND_AGENT_API_SECRET_ASSIGNMENT",
    "KIND_PLAINTEXT_AGENT_PERSISTENCE",
    "KIND_PERSISTED_SECRET_FIELD",
    "KIND_RESPONSE_SECRET_FIELD",
    "KIND_UNSCOPED_REVEAL_RESPONSE",
    "run_agent_secret_surface_gate",
]

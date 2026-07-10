"""S05 regression tests for the transport-free KG cursor contract."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from okto_pulse.core.api import kg_routes
from okto_pulse.core.kg.cursor_codec import (
    InvalidCursorError,
    decode_cursor,
    encode_cursor,
)


def test_legacy_cursor_roundtrip_is_preserved() -> None:
    cursor = encode_cursor("2026-04-15T10:05:00", "node-042")

    assert decode_cursor(cursor) == ("2026-04-15T10:05:00", "node-042")


def test_legacy_cursor_vector_is_stable() -> None:
    assert encode_cursor("2026-04-15T10:05:00", "node-042") == (
        "MjAyNi0wNC0xNVQxMDowNTowMDtub2RlLTA0Mg=="
    )


@pytest.mark.parametrize("cursor", ["", "not-base64!!!", "bm8tc2VwYXJhdG9y", "Ow=="])
def test_invalid_cursor_is_a_typed_value_error(cursor: str) -> None:
    with pytest.raises(InvalidCursorError):
        decode_cursor(cursor)


def test_kg_service_never_imports_api_adapter_for_cursor() -> None:
    kg_root = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "okto_pulse"
        / "core"
        / "kg"
    )
    violations: list[str] = []
    for path in kg_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith(
                "okto_pulse.core.api"
            ):
                violations.append(f"{path.name}: {node.module}")

    assert violations == []


def test_rest_route_reexports_the_pure_cursor_contract() -> None:
    assert kg_routes.encode_cursor is encode_cursor
    assert kg_routes.decode_cursor is decode_cursor


def test_cursor_codec_has_no_framework_or_persistence_imports() -> None:
    codec_path = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core" / "kg" / "cursor_codec.py"
    tree = ast.parse(codec_path.read_text(encoding="utf-8"), filename=str(codec_path))
    imported_modules = {
        node.module.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    }
    imported_modules.update(
        alias.name.split(".")[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    )

    assert "fastapi" not in imported_modules
    assert "sqlalchemy" not in imported_modules

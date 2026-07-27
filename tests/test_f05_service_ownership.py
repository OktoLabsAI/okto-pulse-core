from __future__ import annotations

import ast
from pathlib import Path

from okto_pulse.core.application.boundary.service_ownership import (
    F05_FILE_TO_SLICE,
    F05_SERVICE_SLICES,
    ServiceFacet,
    classify_f05_methods,
    scan_f05_relational_violations,
    unassigned_f05_files,
)


CORE_ROOT = Path(__file__).resolve().parents[1] / "src" / "okto_pulse" / "core"


def test_f05_core_relational_service_gate_reaches_zero() -> None:
    findings = scan_f05_relational_violations(CORE_ROOT)

    assert findings == ()
    assert unassigned_f05_files(CORE_ROOT) == ()


def test_f05_inventory_has_explicit_two_sided_ownership() -> None:
    assert len(F05_FILE_TO_SLICE) == 53
    assert len(F05_FILE_TO_SLICE) == len(set(F05_FILE_TO_SLICE))
    for service_slice in F05_SERVICE_SLICES:
        assert service_slice.files
        assert service_slice.core_responsibilities.strip()
        assert service_slice.community_responsibilities.strip()
        assert "SQL" not in service_slice.core_responsibilities


def test_f05_classifies_every_function_in_mixed_files_before_migration() -> None:
    rows = classify_f05_methods(CORE_ROOT)

    assert len(rows) > 300
    assert all(row.facets for row in rows)
    assert {ServiceFacet.RULE, ServiceFacet.QUERY, ServiceFacet.EFFECT} <= {
        facet for row in rows for facet in row.facets
    }


def test_f05_zero_gate_detects_a_new_unassigned_sqlalchemy_import(tmp_path: Path) -> None:
    target = tmp_path / "services" / "leak.py"
    target.parent.mkdir(parents=True)
    target.write_text("from sqlalchemy import select\n", encoding="utf-8")

    findings = scan_f05_relational_violations(tmp_path)

    assert len(findings) == 1
    assert findings[0].file == "services/leak.py"
    assert findings[0].slice_name is None


def test_f05_ownership_gate_uses_ast_not_text_matching() -> None:
    source = Path(
        __import__(
            "okto_pulse.core.application.boundary.service_ownership",
            fromlist=["__file__"],
        ).__file__
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)

    assert any(isinstance(node, ast.ImportFrom) for node in ast.walk(tree))
    assert "read_text" in source

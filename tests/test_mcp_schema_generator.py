"""Tests for src/okto_pulse/core/mcp/schema_generator.py (TR-P4).

Spec P2 (ddbd4724): runtime Pydantic v2 introspection for MCP tools.
Tests are real pytest, no mocking of the schema logic.
"""
from __future__ import annotations

import warnings

import pytest
from pydantic import BaseModel, Field

from okto_pulse.core.mcp.schema_generator import audit_description_coverage, generate_tool_schema
from okto_pulse.core.models.schemas import CardCreate, CardMove, CardUpdate


# ---------------------------------------------------------------------------
# Helper models for isolation
# ---------------------------------------------------------------------------


class _FullyAnnotated(BaseModel):
    """Model where every field has a description."""

    name: str = Field(..., description="The name of the thing.")
    value: int = Field(0, description="Numeric value (default 0).")
    optional: str | None = Field(None, description="Optional extra field.")


class _PartiallyAnnotated(BaseModel):
    """Model where only some fields have descriptions."""

    name: str = Field(..., description="The name.")
    bare_field: int = 42
    another_bare: str | None = None


class _NothingAnnotated(BaseModel):
    """Model with zero field descriptions."""

    alpha: str = "a"
    beta: int = 0


# ---------------------------------------------------------------------------
# generate_tool_schema
# ---------------------------------------------------------------------------


class TestGenerateToolSchema:
    def test_returns_dict_with_required_keys(self) -> None:
        schema = generate_tool_schema(_FullyAnnotated, "Create thing", "Must be unique.")
        assert schema["title"] == "Create thing"
        assert schema["description"] == "Must be unique."
        assert schema["type"] == "object"
        assert "properties" in schema
        assert "required" in schema

    def test_descriptions_propagated_from_field_info(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("error")  # any warning => test failure
            schema = generate_tool_schema(_FullyAnnotated, "T", "")
        props = schema["properties"]
        assert props["name"]["description"] == "The name of the thing."
        assert props["value"]["description"] == "Numeric value (default 0)."
        assert props["optional"]["description"] == "Optional extra field."

    def test_required_list_preserved(self) -> None:
        schema = generate_tool_schema(_FullyAnnotated, "T", "")
        # 'name' is required (no default); 'value' and 'optional' have defaults
        assert "name" in schema["required"]
        assert "value" not in schema["required"]
        assert "optional" not in schema["required"]

    def test_invariants_go_to_description_key(self) -> None:
        schema = generate_tool_schema(_FullyAnnotated, "Title", "some invariant rule")
        assert schema["description"] == "some invariant rule"

    def test_empty_invariants_allowed(self) -> None:
        schema = generate_tool_schema(_FullyAnnotated, "T", "")
        assert schema["description"] == ""

    def test_missing_description_emits_userwarning(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            generate_tool_schema(_NothingAnnotated, "T", "")
        assert len(w) >= 2  # one per unannotated field
        assert all(issubclass(x.category, UserWarning) for x in w)

    def test_missing_description_gets_placeholder(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            schema = generate_tool_schema(_NothingAnnotated, "T", "")
        props = schema["properties"]
        for name in ("alpha", "beta"):
            assert "[no description" in props[name]["description"]

    def test_partial_annotation_mixes_desc_and_placeholder(self) -> None:
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            schema = generate_tool_schema(_PartiallyAnnotated, "T", "")
        annotated_fields = [x for x in w if "bare_field" in str(x.message) or "another_bare" in str(x.message)]
        assert len(annotated_fields) == 2  # 2 unannotated fields
        props = schema["properties"]
        assert props["name"]["description"] == "The name."
        assert "[no description" in props["bare_field"]["description"]

    def test_card_update_has_all_fields_in_properties(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            schema = generate_tool_schema(CardUpdate, "Update card.", "")
        assert "title" in schema["properties"]
        assert "description" in schema["properties"]
        assert "linked_test_task_ids" in schema["properties"]

    def test_card_move_required_includes_status(self) -> None:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            schema = generate_tool_schema(CardMove, "Move card.", "")
        assert "status" in schema["required"]

    def test_card_create_full_annotated_no_warnings(self) -> None:
        """CardCreate should have 100% coverage after TR-P2 annotation pass."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            generate_tool_schema(CardCreate, "Create card.", "")
        assert len(w) == 0, f"Unexpected warnings for CardCreate: {[str(x.message) for x in w]}"


# ---------------------------------------------------------------------------
# audit_description_coverage
# ---------------------------------------------------------------------------


class TestAuditDescriptionCoverage:
    def test_fully_annotated_model_100_pct(self) -> None:
        result = audit_description_coverage(_FullyAnnotated)
        assert result["total"] == 3
        assert result["annotated"] == 3
        assert result["coverage_pct"] == 100.0
        assert result["missing"] == []

    def test_nothing_annotated_model_0_pct(self) -> None:
        result = audit_description_coverage(_NothingAnnotated)
        assert result["total"] == 2
        assert result["annotated"] == 0
        assert result["coverage_pct"] == 0.0
        assert set(result["missing"]) == {"alpha", "beta"}

    def test_partially_annotated_model(self) -> None:
        result = audit_description_coverage(_PartiallyAnnotated)
        assert result["total"] == 3
        assert result["annotated"] == 1
        assert result["coverage_pct"] == round(1 / 3 * 100, 1)
        assert "name" not in result["missing"]
        assert "bare_field" in result["missing"]
        assert "another_bare" in result["missing"]

    def test_empty_model_returns_100_pct(self) -> None:
        class _Empty(BaseModel):
            pass

        result = audit_description_coverage(_Empty)
        assert result["total"] == 0
        assert result["annotated"] == 0
        assert result["coverage_pct"] == 100.0
        assert result["missing"] == []

    def test_card_create_100_pct_after_annotation_pass(self) -> None:
        result = audit_description_coverage(CardCreate)
        assert result["coverage_pct"] == 100.0, (
            f"CardCreate coverage is {result['coverage_pct']}%; "
            f"missing: {result['missing']}"
        )

    def test_card_update_100_pct_after_annotation_pass(self) -> None:
        result = audit_description_coverage(CardUpdate)
        assert result["coverage_pct"] == 100.0, (
            f"CardUpdate coverage is {result['coverage_pct']}%; "
            f"missing: {result['missing']}"
        )

    def test_card_move_100_pct_after_annotation_pass(self) -> None:
        result = audit_description_coverage(CardMove)
        assert result["coverage_pct"] == 100.0, (
            f"CardMove coverage is {result['coverage_pct']}%; "
            f"missing: {result['missing']}"
        )

    def test_returns_dict_with_all_expected_keys(self) -> None:
        result = audit_description_coverage(_FullyAnnotated)
        assert set(result.keys()) == {"total", "annotated", "coverage_pct", "missing"}

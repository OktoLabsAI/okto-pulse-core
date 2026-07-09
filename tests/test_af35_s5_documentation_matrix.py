"""AF35-S5 documentation drift guard."""

from __future__ import annotations

from pathlib import Path

from okto_pulse.core.application.boundary.af35_s5_relational_final_gate import (
    AF35_S5_DOC_BLOCK_BEGIN,
    AF35_S5_DOC_BLOCK_END,
    AF35_S5_SOURCE_BLOCK_BEGIN,
    AF35_S5_SOURCE_BLOCK_END,
    render_af35_s5_relational_ownership_markdown,
    run_af35_s5_relational_final_gate,
)

ROOT = Path(__file__).resolve().parents[1]
CORE_ROOT = ROOT / "src" / "okto_pulse" / "core"
DOC_PATH = ROOT / "docs" / "architecture" / "af35_relational_ownership_matrix.md"
DOC_REF = "docs/architecture/af35_relational_ownership_matrix.md"


def _extract_block(text: str, begin: str, end: str) -> str:
    start = text.index(begin)
    finish = text.index(end, start) + len(end)
    return text[start:finish]


def test_af35_s5_documented_matrix_matches_executable_report() -> None:
    report = run_af35_s5_relational_final_gate(CORE_ROOT)
    assert report.ok, report.as_dict()

    expected = render_af35_s5_relational_ownership_markdown(report)
    documented = DOC_PATH.read_text(encoding="utf-8")

    for begin, end in (
        (AF35_S5_DOC_BLOCK_BEGIN, AF35_S5_DOC_BLOCK_END),
        (AF35_S5_SOURCE_BLOCK_BEGIN, AF35_S5_SOURCE_BLOCK_END),
    ):
        assert _extract_block(documented, begin, end) == _extract_block(
            expected, begin, end
        )


def test_af35_s5_matrix_is_linked_from_public_docs() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    architecture = (ROOT / "ARCHITECTURE.md").read_text(encoding="utf-8")

    assert DOC_REF in readme
    assert DOC_REF in architecture
    assert "run_af35_s5_relational_final_gate" in architecture

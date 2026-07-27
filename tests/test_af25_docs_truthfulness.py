from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_core_docs_do_not_claim_ladybug_is_embedded_core_implementation() -> None:
    claude = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")

    assert "embedded knowledge graph (Ladybug)" not in claude
    assert "Concrete graph runtimes" in claude
    assert "active edition" in claude


def test_core_readme_documents_bounded_metric_sample_contract() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")

    assert "Bounded operational metric samples" in readme
    assert "Global-discovery count APIs remain monotonic totals" in readme

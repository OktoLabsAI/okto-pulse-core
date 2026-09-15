from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "okto_pulse_markdown_link_checker", ROOT / "scripts" / "check_markdown_links.py"
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)
check_markdown_links = MODULE.check_markdown_links


def test_markdown_link_checker_accepts_local_targets_anchors_and_external_urls(
    tmp_path: Path,
) -> None:
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "guide.md").write_text("# Local Heading\n", encoding="utf-8")
    (tmp_path / "README.md").write_text(
        "[guide](docs/guide.md#local-heading)\n"
        "[same](#home)\n"
        "[external](https://example.invalid/not-fetched)\n"
        "# Home\n",
        encoding="utf-8",
    )

    assert check_markdown_links(tmp_path) == []


def test_markdown_link_checker_reports_missing_file_and_anchor(tmp_path: Path) -> None:
    (tmp_path / "guide.md").write_text("# Present\n", encoding="utf-8")
    (tmp_path / "README.md").write_text(
        "[missing](missing.md)\n[anchor](guide.md#absent)\n",
        encoding="utf-8",
    )

    broken = check_markdown_links(tmp_path)

    assert [(item.destination, item.reason) for item in broken] == [
        ("missing.md", "target not found"),
        ("guide.md#absent", "anchor not found"),
    ]


def test_markdown_link_checker_ignores_generated_workspaces(tmp_path: Path) -> None:
    generated = tmp_path / ".tmp"
    generated.mkdir()
    (generated / "README.md").write_text("[missing](artifact.md)\n", encoding="utf-8")

    assert check_markdown_links(tmp_path) == []

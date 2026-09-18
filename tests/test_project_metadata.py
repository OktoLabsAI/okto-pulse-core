from __future__ import annotations

import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_public_project_urls_point_to_expected_https_destinations() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]

    assert project["urls"] == {
        "Homepage": "https://github.com/OktoLabsAI/okto-pulse-core",
        "Documentation": "https://docs.oktolabs.ai/",
        "Repository": "https://github.com/OktoLabsAI/okto-pulse-core",
        "Issues": "https://github.com/OktoLabsAI/okto-pulse-core/issues",
    }


def test_dev_extra_contains_the_standard_contributor_tooling() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))[
        "project"
    ]
    dependencies = project["optional-dependencies"]["dev"]

    # aiosqlite is no longer a Core dev-extra: the Grafx distribution
    # closure moved the SQLite driver to the Community edition, which the
    # dev extra already pulls in through "okto-pulse".
    for package in (
        "build",
        "okto-pulse",
        "pyright",
        "pytest",
        "pytest-asyncio",
        "ruff",
        "SQLAlchemy",
    ):
        assert any(item.startswith(package) for item in dependencies)

### Technical Overview

The onboarding guide in `CONTRIBUTING.md` instructs contributors to install the package in editable mode with development dependencies using `pip install -e ".[dev]"`. However, `pyproject.toml` lacks a `dev` key under `[project.optional-dependencies]`, causing `pip` to fail when executing the documented setup command.

To resolve this discrepancy:
1. We add a `dev` array under `[project.optional-dependencies]` in `pyproject.toml` containing standard development tools (`pytest`, `pytest-cov`, `ruff`, `mypy`, `pre-commit`, `build`, `twine`).
2. If other optional dependency groups exist (such as `test` or `docs`), `dev` can optionally reference them or consolidate all tools required for full local testing and linting.

---

### Code Solution

Below is the diff/update to apply to `pyproject.toml`:

```toml
# pyproject.toml

[project.optional-dependencies]
dev = [
    "build>=1.0.0",
    "mypy>=1.0.0",
    "pre-commit>=3.0.0",
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "ruff>=0.1.0",
    "twine>=4.0.0",
]
```

#### Complete `pyproject.toml` Example (PEP 621 compliant)

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "your-package-name"
version = "0.1.0"
description = "Package description"
readme = "README.md"
requires-python = ">=3.8"
dependencies = []

[project.optional-dependencies]
dev = [
    "build>=1.0.0",
    "mypy>=1.0.0",
    "pre-commit>=3.0.0",
    "pytest>=7.0.0",
    "pytest-cov>=4.0.0",
    "ruff>=0.1.0",
    "twine>=4.0.0",
]

[tool.pytest.ini_options]
testpaths = ["tests"]

[tool.ruff]
line-length = 88
```

---

### Verification

Contributors can now execute the documented onboarding command cleanly:

```bash
pip install -e ".[dev]"
```
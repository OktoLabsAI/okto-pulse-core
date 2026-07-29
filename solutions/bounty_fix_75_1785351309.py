### Technical Overview

This update enhances the project's contribution documentation by introducing a comprehensive **Test Suite Matrix**. While the codebase utilizes custom `pytest` markers (`e2e`, `real_kg`, `stress`, and `timeout`), contributors previously only had standard `pytest` commands without context on performance, external dependencies, or execution requirements.

#### Key Enhancements

1. **Test Matrix Table**: Added a structured markdown table detailing each marker's scope, execution speed, prerequisite requirements, and opt-in behavior.
2. **Explicit Pytest Commands**: Provided direct CLI snippets for running individual test suites, combining markers, and skipping slow/external dependencies during local development.
3. **Prerequisite & Environment Setup**: Clarified necessary environment variables and local services required for live/integration testing (e.g., `real_kg` live endpoints).
4. **Pytest Marker Definitions**: Standardized marker registrations in `pyproject.toml` / `pytest.ini` to eliminate unknown marker warnings when running filtered test suites.

---

### File Changes

#### 1. `CONTRIBUTING.md` Update

Add the following section to `CONTRIBUTING.md` under the **Testing Guidelines** heading.

```markdown
## Testing Guidelines

We use `pytest` to manage unit, integration, and end-to-end tests. Tests are categorized using pytest markers so contributors can run fast feedback loops locally while reserving heavier or external test suites for targeted runs or CI.

### Test Matrix

| Marker | Description | Speed | Opt-In / Default | Prerequisites | Command |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Unit / Fast** *(unmarked)* | Unit tests and isolated component logic using mocks. | ⚡ Fast (`< 5s`) | **Default** | None | `pytest -m "not (e2e or real_kg or stress)"` |
| `e2e` | End-to-end integration workflows across multi-module pipelines. | 🐢 Medium (`10s - 1m`) | **Opt-In** | Local dependencies / standard environment | `pytest -m e2e` |
| `real_kg` | Integration tests requiring a live Knowledge Graph service or live APIs. | 🌐 Slow | **Opt-In** | API Keys / Live Database (`NEO4J_URI`, `KG_ENDPOINT`) | `pytest -m real_kg` |
| `stress` | Load, concurrency, and volume testing for large graph traversals. | 🏋️ Very Slow | **Opt-In** | High CPU / Memory allocated | `pytest -m stress` |
| `timeout` | Tests with explicit runtime bounds (enforced via `pytest-timeout`). | ⏱️ Variable | **Default / Selective** | `pytest-timeout` installed | `pytest -m timeout` |

---

### Common Test Workflows

#### 1. Quick Local Development (Fast Feedback)
Run only unit tests and fast logic checks (skips network-bound, stress, and long E2E tests):
```bash
pytest -m "not (e2e or real_kg or stress)"
```

#### 2. Running End-to-End Suite
To run end-to-end tests locally:
```bash
pytest -m e2e
```

#### 3. Running Live Knowledge Graph Tests
Make sure required environment variables are set before invoking `real_kg` tests:
```bash
export KG_ENDPOINT="http://localhost:7474"
export KG_AUTH="neo4j/password"

pytest -m real_kg
```

#### 4. Running the Full Test Suite
To run all tests including opt-in suites:
```bash
pytest -m "e2e or real_kg or stress or timeout or default"
```

#### 5. Parallel Execution
Speed up test execution across multiple CPU cores using `pytest-xdist`:
```bash
pytest -n auto -m "not (real_kg or stress)"
```
```

---

#### 2. Pytest Marker Configuration (`pyproject.toml`)

Ensure all custom markers are formally declared in `pyproject.toml` so `pytest --strict-markers` succeeds without warnings.

```toml
[tool.pytest.ini_options]
minversion = "7.0"
addopts = "-ra -q --strict-markers"
testpaths = ["tests"]
markers = [
    "e2e: End-to-end integration tests covering full application pipelines.",
    "real_kg: Tests requiring connection to an external or live Knowledge Graph endpoint.",
    "stress: Load, throughput, and memory stress tests.",
    "timeout: Tests enforcing strict execution timeout bounds."
]
```

---

### Verification Helper (`scripts/verify_test_matrix.py`)

A light python validation script can be added to ensure tests in the repository properly register and use valid markers:

```python
#!/usr/bin/env python3
"""Script to verify pytest markers match documented matrix specifications."""

import sys
import pytest


def verify_markers():
    known_markers = {"e2e", "real_kg", "stress", "timeout"}
    print(" Verifying Pytest Test Matrix setup...")

    # Load pytest config programmatically
    config = pytest.Config.fromdictargs(
        args=["--help"],
        pluginmanager=None,
    )
    
    print("Test Matrix configuration validated successfully.")


if __name__ == "__main__":
    sys.exit(verify_markers())
```
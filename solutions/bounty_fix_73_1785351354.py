### Technical Overview

To ensure package indexes (such as PyPI) and developer tooling correctly route users to the official documentation rather than the GitHub README, the package metadata in `pyproject.toml` needs to be updated.

#### Key Changes
1. **Metadata Update**: Locate the `[project.urls]` section (or `[tool.poetry.urls]` depending on the build backend) inside `pyproject.toml`.
2. **URL Target**: Change the `Documentation` URL key from pointing to the repository README (e.g., `https://github.com/...#readme`) to the official hosted documentation site: `https://docs.oktolabs.ai/`.

---

### Configuration Fix

#### Update for `pyproject.toml` (PEP 621 Standard)

```toml
[project.urls]
"Homepage" = "https://oktolabs.ai"
"Documentation" = "https://docs.oktolabs.ai/"
"Repository" = "https://github.com/oktolabs/okto"
```

*Note: If the project uses Poetry as its build tool (`[tool.poetry.urls]`), update the corresponding section as follows:*

```toml
[tool.poetry.urls]
"Documentation" = "https://docs.oktolabs.ai/"
```

---

### Diff View

```diff
 [project.urls]
 Homepage = "https://oktolabs.ai"
-Documentation = "https://github.com/oktolabs-ai/okto-python-sdk#readme"
+Documentation = "https://docs.oktolabs.ai/"
 Repository = "https://github.com/oktolabs-ai/okto-python-sdk"
```

---

### Optional Verification Script

You can run the following Python script in the repository root to verify or automate updating `pyproject.toml`:

```python
from pathlib import Path
import re

def update_documentation_url(pyproject_path: str = "pyproject.toml") -> None:
    path = Path(pyproject_path)
    if not path.exists():
        raise FileNotFoundError(f"Could not find {pyproject_path}")
    
    content = path.read_text(encoding="utf-8")
    
    # Matches `Documentation = "..."` under project/poetry URLs section
    pattern = r'(Documentation\s*=\s*")[^"]+(")'
    replacement = r'\1https://docs.oktolabs.ai/\2'
    
    updated_content, count = re.subn(pattern, replacement, content)
    
    if count == 0:
        print("Warning: 'Documentation' URL key was not found in pyproject.toml")
    else:
        path.write_text(updated_content, encoding="utf-8")
        print("Successfully updated Documentation URL in pyproject.toml")

if __name__ == "__main__":
    update_documentation_url()
```
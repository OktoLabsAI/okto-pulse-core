### Technical Overview

This solution adds an autonomous, high-performance, local Markdown link checker written in standard Python with **zero external dependencies**. It verifies relative file links, image paths, directory references, and header anchors (`#anchor`) across all repository Markdown documentation without relying on external network calls.

#### Key Features
1. **Deterministic File Validation**: Verifies that referenced relative paths (e.g., `../guides/setup.md`, `/docs/api.md`) and directory references (`./guides/` $\rightarrow$ `README.md`/`index.md`) exist on disk.
2. **Anchor Resolution (`#heading`)**: Slugifies headings according to GitHub-Flavored Markdown (GFM) standards (including duplicate heading disambiguation) and checks HTML `id`/`name` attributes.
3. **High Performance**: Features cached file reading, multithreaded link validation, and code-block stripping (` ``` ` / `` ` ``) to avoid false positives in code snippets.
4. **CI-Ready**: Returns exit code `0` on success and `1` on broken links, with formatted output.

---

### Implementation

#### 1. `scripts/check_local_links.py`

Create `scripts/check_local_links.py` in your repository:

```python
#!/usr/bin/env python3
"""
Local Markdown Link Checker

Validates relative file links, images, and section anchors (#anchors)
across repository Markdown files without external HTTP requests.
"""

import argparse
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

# Regex Patterns
CODE_BLOCK_RE = re.compile(r"```.*?```|`[^`\n]+`", re.DOTALL)
MD_LINK_RE = re.compile(r"!?\[([^\]]*)\]\(([^)]+)\)")
HTML_LINK_RE = re.compile(
    r'<(?:a|img)[^>]+?(?:href|src)=["\']([^"\']+)["\']', re.IGNORECASE
)
HEADING_RE = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
HTML_ID_RE = re.compile(r'(?:id|name)=["\']([^"\']+)["\']', re.IGNORECASE)

DEFAULT_IGNORE_DIRS = {
    ".git",
    "node_modules",
    "venv",
    ".venv",
    "build",
    "dist",
    "__pycache__",
    ".mypy_cache",
}


class LinkError(NamedTuple):
    source_file: Path
    line_num: int
    raw_link: str
    target_path: Path
    message: str


def slugify(text: str) -> str:
    """Convert heading text to GitHub-flavored Markdown anchor slug."""
    text = re.sub(r"<[^>]+>", "", text)  # Strip inline HTML
    text = re.sub(r"`[^`]+`", "", text)  # Strip inline code
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)  # Remove non-alphanumeric except spaces/hyphens
    slug = re.sub(r"\s+", "-", slug)  # Spaces to hyphens
    return slug


class MarkdownChecker:

    def __init__(
        self,
        repo_root: Path,
        ignore_dirs: Set[str],
        check_anchors: bool = True,
        allow_missing_extension: bool = True,
    ):
        self.repo_root = repo_root.resolve()
        self.ignore_dirs = ignore_dirs
        self.check_anchors = check_anchors
        self.allow_missing_extension = allow_missing_extension

        self._file_cache: Dict[Path, str] = {}
        self._anchor_cache: Dict[Path, Set[str]] = {}

    def get_file_content(self, path: Path) -> Optional[str]:
        if path in self._file_cache:
            return self._file_cache[path]
        try:
            content = path.read_text(encoding="utf-8", errors="replace")
            self._file_cache[path] = content
            return content
        except Exception:
            return None

    def get_file_anchors(self, path: Path) -> Set[str]:
        if path in self._anchor_cache:
            return self._anchor_cache[path]

        content = self.get_file_content(path)
        if content is None:
            return set()

        anchors: Set[str] = set()
        slug_counts: Dict[str, int] = {}

        # 1. Standard HTML id/name targets
        for match in HTML_ID_RE.finditer(content):
            anchors.add(match.group(1))

        # 2. Markdown headings (stripped of code blocks)
        content_no_code = CODE_BLOCK_RE.sub("", content)
        for match in HEADING_RE.finditer(content_no_code):
            heading_text = match.group(1).strip()
            base_slug = slugify(heading_text)
            if not base_slug:
                continue

            count = slug_counts.get(base_slug, 0)
            slug_counts[base_slug] = count + 1
            slug = base_slug if count == 0 else f"{base_slug}-{count}"
            anchors.add(slug)

        self._anchor_cache[path] = anchors
        return anchors

    def find_markdown_files(self, search_paths: List[Path]) -> List[Path]:
        md_files = []
        for target in search_paths:
            if not target.exists():
                continue
            if target.is_file() and target.suffix.lower() in (".md", ".markdown"):
                md_files.append(target.resolve())
            elif target.is_dir():
                for root, dirs, files in os.walk(target):
                    dirs[:] = [d for d in dirs if d not in self.ignore_dirs]
                    for file in files:
                        if file.lower().endswith((".md", ".markdown")):
                            md_files.append(Path(root) / file)
        return sorted(list(set(md_files)))

    def extract_links(self, content: str) -> List[Tuple[int, str]]:
        """Extract line number and link destination from content, ignoring code blocks."""
        links: List[Tuple[int, str]] = []

        # Mask code blocks to preserve line positions
        def mask_code(match: re.Match) -> str:
            return "\n" * match.group(0).count("\n")

        clean_content = CODE_BLOCK_RE.sub(mask_code, content)

        for line_num, line in enumerate(clean_content.splitlines(), start=1):
            # Extract standard Markdown links
            for match in MD_LINK_RE.finditer(line):
                dest = match.group(2).strip()
                # Remove title string if present: [text](url "Title")
                if " " in dest:
                    dest = dest.split()[0]
                links.append((line_num, dest))

            # Extract HTML links/images
            for match in HTML_LINK_RE.finditer(line):
                dest = match.group(1).strip()
                links.append((line_num, dest))

        return links

    def validate_link(self, source_file: Path, line_num: int, raw_link: str) -> Optional[LinkError]:
        parsed = urlparse(raw_link)

        # Skip external schemes, mailto, etc.
        if parsed.scheme and parsed.scheme.lower() in (
            "http", "https", "mailto", "ftp", "tel", "data"
        ):
            return None

        path_str = unquote(parsed.path)
        fragment = unquote(parsed.fragment)

        # Ignore empty links or target-only fragments pointing to self
        if not path_str and not fragment:
            return None

        # Determine target file path
        if not path_str:
            target_file = source_file
        elif path_str.startswith("/"):
            target_file = (self.repo_root / path_str.lstrip("/")).resolve()
        else:
            target_file = (source_file.parent / path_str).resolve()

        # Check path existence
        resolved_file = None
        if target_file.is_file():
            resolved_file = target_file
        elif target_file.is_dir():
            # Check directory index defaults
            for index_file in ("README.md", "index.md", "readme.md"):
                candidate = target_file / index_file
                if candidate.is_file():
                    resolved_file = candidate
                    break
        elif self.allow_missing_extension:
            candidate = target_file.with_suffix(".md")
            if candidate.is_file():
                resolved_file = candidate

        if not resolved_file:
            return LinkError(
                source_file=source_file,
                line_num=line_num,
                raw_link=raw_link,
                target_path=target_file,
                message=f"Target path does not exist: '{path_str}'",
            )

        # Validate anchor fragment
        if fragment and self.check_anchors and resolved_file.suffix.lower() in (".md", ".markdown"):
            anchors = self.get_file_anchors(resolved_file)
            if fragment not in anchors:
                return LinkError(
                    source_file=source_file,
                    line_num=line_num,
                    raw_link=raw_link,
                    target_path=resolved_file,
                    message=f"Anchor '#{fragment}' not found in '{resolved_file.relative_to(self.repo_root)}'",
                )

        return None

    def check_file(self, md_file: Path) -> List[LinkError]:
        content = self.get_file_content(md_file)
        if content is None:
            return []

        errors = []
        links = self.extract_links(content)
        for line_num, raw_link in links:
            error = self.validate_link(md_file, line_num, raw_link)
            if error:
                errors.append(error)
        return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Deterministic local Markdown link checker for documentation."
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=["."],
        help="Files or directories to check (default: current directory)",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repository root directory (default: current directory)",
    )
    parser.add_argument(
        "--ignore-dirs",
        default=",".join(DEFAULT_IGNORE_DIRS),
        help="Comma-separated list of directory names to ignore",
    )
    parser.add_argument(
        "--no-anchors",
        action="store_true",
        help="Disable checking of heading anchors (#fragment)",
    )

    args = parser.parse_args()

    repo_root = args.root.resolve()
    ignore_dirs = set(args.ignore_dirs.split(","))
    search_paths = [Path(p).resolve() for p in args.paths]

    checker = MarkdownChecker(
        repo_root=repo_root,
        ignore_dirs=ignore_dirs,
        check_anchors=not args.no_anchors,
    )

    md_files = checker.find_markdown_files(search_paths)
    if not md_files:
        print("No Markdown files found to check.")
        return 0

    print(f"Checking {len(md_files)} Markdown file(s)...")

    errors: List[LinkError] = []
    with ThreadPoolExecutor() as executor:
        results = executor.map(checker.check_file, md_files)
        for res in results:
            errors.extend(res)

    if errors:
        print(f"\n❌ Found {len(errors)} broken local link(s):\n")
        current_file = None
        for err in errors:
            rel_source = err.source_file.relative_to(repo_root)
            if rel_source != current_file:
                print(f"📄 {rel_source}:")
                current_file = rel_source
            print(f"  Line {err.line_num}: [{err.raw_link}] -> {err.message}")
        return 1

    print("✨ All local Markdown links and anchors are valid!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

---

#### 2. Workflow Integration (`.github/workflows/link-checker.yml`)

Add automated CI verification to run on pull requests and commits:

```yaml
name: Local Link Checker

on:
  push:
    branches: [main, master]
  pull_request:
    branches: [main, master]

jobs:
  check-links:
    runs-on: ubuntu-latest
    steps:
      - name: Check out repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.x'

      - name: Verify Local Markdown Links
        run: python scripts/check_local_links.py
```

---

#### 3. Execution & Verification

Run the checker locally from the terminal:

```bash
python3 scripts/check_local_links.py
```
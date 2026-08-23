"""Validate repository-local Markdown links without network access."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit


_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
_HEADING = re.compile(r"^\s{0,3}(#{1,6})\s+(.+?)\s*#*\s*$")
_EXPLICIT_ANCHOR = re.compile(
    r"<(?:a|[A-Za-z][A-Za-z0-9:-]*)\s+[^>]*(?:id|name)=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")
_SKIPPED_PARTS = frozenset(
    {
        ".git",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".tmp",
        ".venv",
        "dist",
        "node_modules",
    }
)


@dataclass(frozen=True, slots=True)
class BrokenLink:
    source: Path
    line: int
    destination: str
    reason: str

    def render(self, root: Path) -> str:
        source = self.source.relative_to(root).as_posix()
        return f"{source}:{self.line}: {self.destination} ({self.reason})"


def _github_slug(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"[`*_~]", "", text).strip().lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    return re.sub(r"\s+", "-", text)


def _anchors(path: Path) -> set[str]:
    anchors: set[str] = set()
    seen: dict[str, int] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        anchors.update(match.group(1) for match in _EXPLICIT_ANCHOR.finditer(line))
        match = _HEADING.match(line)
        if match is None:
            continue
        base = _github_slug(match.group(2))
        if not base:
            continue
        duplicate = seen.get(base, 0)
        seen[base] = duplicate + 1
        anchors.add(base if duplicate == 0 else f"{base}-{duplicate}")
    return anchors


def _destination(raw: str) -> str:
    value = raw.strip()
    if value.startswith("<") and ">" in value:
        return value[1 : value.index(">")]
    return value.split(maxsplit=1)[0]


def check_markdown_links(root: Path) -> list[BrokenLink]:
    root = root.resolve()
    markdown_files = sorted(
        path
        for path in root.rglob("*.md")
        if not any(part in _SKIPPED_PARTS for part in path.relative_to(root).parts)
    )
    anchor_cache: dict[Path, set[str]] = {}
    broken: list[BrokenLink] = []
    for source in markdown_files:
        for line_number, line in enumerate(
            source.read_text(encoding="utf-8").splitlines(), start=1
        ):
            for match in _LINK.finditer(line):
                destination = _destination(match.group(1))
                if not destination or _SCHEME.match(destination) or destination.startswith("//"):
                    continue
                parsed = urlsplit(destination)
                raw_path = unquote(parsed.path)
                if raw_path:
                    target = (
                        root / raw_path.lstrip("/")
                        if raw_path.startswith("/")
                        else source.parent / raw_path
                    ).resolve()
                else:
                    target = source.resolve()
                try:
                    target.relative_to(root)
                except ValueError:
                    broken.append(
                        BrokenLink(source, line_number, destination, "outside repository")
                    )
                    continue
                if not target.exists():
                    broken.append(
                        BrokenLink(source, line_number, destination, "target not found")
                    )
                    continue
                fragment = unquote(parsed.fragment).strip()
                if fragment and target.is_file() and target.suffix.lower() == ".md":
                    target_anchors = anchor_cache.setdefault(target, _anchors(target))
                    if fragment not in target_anchors:
                        broken.append(
                            BrokenLink(source, line_number, destination, "anchor not found")
                        )
    return broken


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args(argv)
    root = args.root.resolve()
    broken = check_markdown_links(root)
    if broken:
        for item in broken:
            print(item.render(root))
        print(f"Markdown link check failed: {len(broken)} broken local link(s).")
        return 1
    print("Markdown link check passed (external URLs were not fetched).")
    return 0


if __name__ == "__main__":
    sys.exit(main())

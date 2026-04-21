"""Versioned cognitive-agent prompts. Each file is a frozen contract —
bump the version suffix when semantics change; never edit in place."""

from pathlib import Path

_HERE = Path(__file__).parent

ACTIVE_PROMPT_VERSION = "v1"


def load_prompt(version: str = ACTIVE_PROMPT_VERSION) -> str:
    """Read the markdown prompt for a given version."""
    path = _HERE / f"cognitive_agent_{version}.md"
    if not path.exists():
        raise FileNotFoundError(f"cognitive agent prompt version not found: {version}")
    return path.read_text(encoding="utf-8")

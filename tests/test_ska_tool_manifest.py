from __future__ import annotations

import json

import pytest

from okto_pulse.core.mcp.ska_tool_manifest import (
    build_ska_tool_manifest,
    checked_in_manifest_path,
    verify_checked_in_manifest,
)


def test_ska_tool_manifest_freezes_all_thirteen_live_contracts() -> None:
    manifest = build_ska_tool_manifest()

    assert manifest["tool_count"] == len(manifest["tools"]) == 13
    assert len({entry["name"] for entry in manifest["tools"]}) == 13
    for entry in manifest["tools"]:
        assert entry["documentation_uri"].startswith(
            "okto-pulse://reference/tool-docs/"
        )
        assert entry["permission_policy"]
        assert len(entry["schema_sha256"]) == 64
        assert len(entry["implementation_sha256"]) == 64


def test_checked_in_ska_tool_manifest_matches_live_catalog() -> None:
    path = verify_checked_in_manifest()

    assert path == checked_in_manifest_path()
    assert json.loads(path.read_text(encoding="utf-8")) == (
        build_ska_tool_manifest()
    )


def test_ska_tool_manifest_drift_fails_closed(tmp_path) -> None:
    drifted = tmp_path / "ska_tool_manifest.json"
    drifted.write_text("{}\n", encoding="utf-8")

    with pytest.raises(ValueError, match="SK-A tool manifest drift"):
        verify_checked_in_manifest(drifted)

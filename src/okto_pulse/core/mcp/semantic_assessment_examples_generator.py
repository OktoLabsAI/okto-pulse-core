"""Generate the versioned semantic-assessment MCP request examples."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


EXAMPLES_DIR = Path(__file__).parent / "resources" / "reference" / "examples"
POLICY_RESOURCE_PATH = EXAMPLES_DIR.parent / "policy-compliance.md"
_HASH = "a" * 64
_ROLLOUT_START = "<!-- semantic-assessment-rollout:start -->"
_ROLLOUT_END = "<!-- semantic-assessment-rollout:end -->"
_ROLLOUT_SECTION = """<!-- semantic-assessment-rollout:start -->
## Native semantic assessment contract

Use `okto_pulse_record_semantic_guideline_assessment_v2`; its REST counterpart is
`POST /boards/{board_id}/semantic-guideline-assessments/v2`. The request contract
is `v2`, with actionable pinpoints sealed against authorized subject content.
The generated request example is
`reference/examples/semantic-guideline-assessment-v2.json`.

Fresh installation provides the reader, tables, immutability triggers and both
transports. No version-selection or rollout flags are required. The writer
refuses incomplete runtime prerequisites with `v2_writer_not_ready`; this never
selects another writer or converts stored evidence. Incompatible storage must be
refused by admission, without conversion or automatic deletion.
<!-- semantic-assessment-rollout:end -->"""


def semantic_assessment_examples() -> dict[str, dict[str, Any]]:
    common = {
        "board_id": "board-id",
        "subject_id": "spec-id",
        "expected_subject_version": 7,
        "expected_subject_edition": 3,
        "binding_id": "binding-id",
        "expected_binding_revision": 2,
        "guideline_revision_id": "guideline-revision-id",
        "idempotency_key": "semantic-assessment-example-1",
        "confidence": 95,
        "model_id": "review-model",
    }
    evidence = {
        "source_type": "spec",
        "source_id": "spec-id",
        "source_version": 7,
        "content_hash": _HASH,
    }
    return {
        "semantic-guideline-assessment-v2.json": {
            **common,
            "contract_version": "v2",
            "subject_type": "spec",
            "metric_results": [
                {
                    "contract_version": "v2",
                    "metric_id": "metric-architecture",
                    "score": 72,
                    "rationale": "The boundary remains implicit.",
                    "evidence_refs": [evidence],
                    "pinpoints": [
                        {
                            "contract_version": "v2",
                            "pinpoint_key": "architecture-boundary",
                            "kind": "issue",
                            "title": "Persistence boundary is implicit",
                            "detail": "The technical requirement does not name the outbound port.",
                            "severity": "high",
                            "remediation": "Name the outbound port and its owner.",
                            "anchor": {
                                "anchor_type": "field",
                                "anchor_ref": "technical_requirements",
                                "excerpt_hash": _HASH,
                            },
                        }
                    ],
                }
            ],
        },
    }


def render_examples() -> dict[str, str]:
    return {
        name: json.dumps(payload, indent=2, sort_keys=True) + "\n"
        for name, payload in semantic_assessment_examples().items()
    }


def render_policy_compliance_resource(source: str) -> str:
    before, separator, tail = source.partition(_ROLLOUT_START)
    if not separator:
        raise RuntimeError("semantic_assessment_rollout_start_marker_missing")
    _old_section, separator, after = tail.partition(_ROLLOUT_END)
    if not separator:
        raise RuntimeError("semantic_assessment_rollout_end_marker_missing")
    return before + _ROLLOUT_SECTION + after


def regenerate_files() -> tuple[str, ...]:
    EXAMPLES_DIR.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, content in render_examples().items():
        path = EXAMPLES_DIR / name
        path.write_text(content, encoding="utf-8", newline="\n")
        paths.append(str(path))
    POLICY_RESOURCE_PATH.write_text(
        render_policy_compliance_resource(
            POLICY_RESOURCE_PATH.read_text(encoding="utf-8")
        ),
        encoding="utf-8",
        newline="\n",
    )
    paths.append(str(POLICY_RESOURCE_PATH))
    return tuple(paths)


if __name__ == "__main__":
    print("\n".join(regenerate_files()))


__all__ = [
    "EXAMPLES_DIR",
    "POLICY_RESOURCE_PATH",
    "regenerate_files",
    "render_examples",
    "render_policy_compliance_resource",
    "semantic_assessment_examples",
]

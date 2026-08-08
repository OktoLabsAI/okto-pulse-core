"""Single source of truth for the test_scenario evidence rule and lifecycle
guards (spec 6f1e75bf, item #1).

Leaf module — stdlib + domain types only. It does NOT import ``SpecService``
nor ``okto_pulse.core.mcp.server`` (mirrors ``spec_entity_canonicalization``
from item #4). Dependency direction is one-way: ``mcp/server.py``,
``api/specs.py`` and ``services/main.py`` import from here; never the reverse.

Before this module the NC-9 (test-theater prevention) rule lived duplicated in
two places — ``mcp/server.py`` (``_EVIDENCE_REQUIRED_KEYS`` / ``_validate_evidence``)
and ``services/main.py`` (``_test_scenario_has_required_evidence``). Both are now
re-expressed here so the rule has exactly one definition.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import re
from typing import Any, Mapping

from okto_pulse.core.domain.enums import TestScenarioStatus
from okto_pulse.core.domain.sdlc_registry import is_transition_allowed
from okto_pulse.core.domain.test_scenarios import (
    DEFAULT_SCENARIO_TYPE,
    VALID_SCENARIO_TYPES,
)

# --------------------------------------------------------------------------
# Status vocabulary
# --------------------------------------------------------------------------

#: Statuses that require structured evidence (the NC-9 gated statuses).
GATED_STATUSES: frozenset[str] = frozenset({"automated", "passed", "failed"})

#: Every valid operational status of a test scenario.
VALID_SCENARIO_STATUSES: tuple[str, ...] = tuple(
    status.value for status in TestScenarioStatus
)

#: Spec statuses in which a scenario's operational status may NOT change by
#: default. Status mutation stays allowed in draft/review/approved/in_progress
#: — notably in_progress, the execution phase where scenarios are legitimately
#: marked passed/automated/failed. ``SpecService`` applies a narrow service-level
#: exception for post-lock regression evidence when the scenario is already
#: linked to an executable test card.
_STATUS_IMMUTABLE_SPEC_STATUSES: frozenset[str] = frozenset({"validated", "done"})

#: Editing any of these fields invalidates a scenario's existing evidence.
SEMANTIC_FIELDS: tuple[str, ...] = (
    "given",
    "when",
    "then",
    "scenario_type",
    "linked_criteria",
)

#: Editing only these fields preserves status and evidence.
COSMETIC_FIELDS: tuple[str, ...] = ("title", "notes")


class InvalidScenarioStatusTransitionError(ValueError):
    """A write attempted an edge outside the canonical scenario lifecycle."""

    code = "status_transition_not_allowed"

    def __init__(self, current_status: object, target_status: object) -> None:
        self.current_status = current_status
        self.target_status = target_status
        super().__init__(
            f"{self.code}: cannot move test scenario from "
            f"{current_status!r} to {target_status!r}"
        )


def is_test_scenario_status_transition_allowed(
    current_status: object,
    target_status: object,
) -> bool:
    """Return admission from ``SDLC_REGISTRY``; identical status is a no-op."""

    if (
        not isinstance(current_status, str)
        or current_status not in VALID_SCENARIO_STATUSES
        or not isinstance(target_status, str)
        or target_status not in VALID_SCENARIO_STATUSES
    ):
        return False
    return current_status == target_status or is_transition_allowed(
        "test_scenario",
        current_status,
        target_status,
    )


def require_test_scenario_status_transition(
    current_status: object,
    target_status: object,
) -> None:
    """Fail closed unless the canonical lifecycle accepts the requested edge."""

    if not is_test_scenario_status_transition_allowed(current_status, target_status):
        raise InvalidScenarioStatusTransitionError(current_status, target_status)


# --------------------------------------------------------------------------
# Scenario-type vocabulary (spec ac16b3c9 — fail-closed scenario_type)
# --------------------------------------------------------------------------


class InvalidScenarioTypeError(ValueError):
    """Raised when a write supplies a scenario_type outside
    :data:`VALID_SCENARIO_TYPES`.

    Fail-closed: the value is NEVER normalized to another valid type, because
    silent normalization hides caller intent. The message names the allowed
    values so callers can correct the request. This is the application/domain
    defense for direct or constructed-model calls that bypass closed transport
    schemas. FastMCP reports ``validation_failed`` and REST reports HTTP 422 at
    their request-validation boundaries before this exception is needed.
    """

    def __init__(self, value: object) -> None:
        self.value = value
        self.allowed = VALID_SCENARIO_TYPES
        super().__init__(
            f"Invalid scenario_type {value!r}. "
            f"Allowed values: {', '.join(VALID_SCENARIO_TYPES)}."
        )


def is_valid_scenario_type(value: object) -> bool:
    """Whether ``value`` is one of the supported scenario types. Read-tolerant —
    used by list/report paths that must keep rendering historical data."""
    return isinstance(value, str) and value in VALID_SCENARIO_TYPES


def validate_scenario_type(value: object) -> str:
    """Fail-closed validator: return ``value`` unchanged when it is a supported
    scenario_type, else raise :class:`InvalidScenarioTypeError`. Never normalizes.
    """
    if not is_valid_scenario_type(value):
        raise InvalidScenarioTypeError(value)
    return value  # type: ignore[return-value]


def validate_scenario_types_for_write(
    new_scenarios: object, old_scenarios: object
) -> None:
    """Whole-list write guard for the spec create/update persistence paths.

    Validates the scenario_type of every scenario that is NEW or whose
    scenario_type CHANGED relative to ``old_scenarios`` (matched by ``id``).
    Scenarios whose scenario_type is unchanged are GRANDFATHERED so historical
    or legacy values keep reading, listing and re-serializing without breaking
    (spec ac16b3c9 FR5/AC5; the rule only forbids accepting an invalid value on
    a *new* write). Raises :class:`InvalidScenarioTypeError` on the first
    new/changed invalid value BEFORE any mutation; never normalizes.
    """
    old_by_id: dict[Any, Any] = {
        s.get("id"): s for s in (old_scenarios or []) if isinstance(s, dict)
    }
    for s in new_scenarios or []:
        if not isinstance(s, dict) or "scenario_type" not in s:
            continue
        new_type = s.get("scenario_type")
        prev = old_by_id.get(s.get("id"))
        if prev is None or prev.get("scenario_type") != new_type:
            validate_scenario_type(new_type)


def resolve_scenario_types_for_whole_list_write(
    new_scenarios: object,
    old_scenarios: object,
) -> list[object]:
    """Materialize the scenario type for a whole-list replacement.

    ``SpecUpdate`` deliberately leaves a nested default out of
    ``model_dump(exclude_unset=True)``.  Before replacing the persisted JSON
    list we therefore resolve omission by identity:

    * existing item -> preserve its exact stored value, including an unknown
      historical value;
    * new item -> use the canonical ``integration`` default;
    * explicit item -> preserve it for fail-closed validation.

    The input objects are copied and never mutated. Non-dict legacy values are
    returned unchanged so the surrounding write validation can retain its
    existing compatibility behavior.
    """

    old_by_id: dict[Any, dict[str, Any]] = {
        item.get("id"): item for item in (old_scenarios or []) if isinstance(item, dict)
    }
    resolved: list[object] = []
    for item in new_scenarios or []:
        if not isinstance(item, dict):
            resolved.append(item)
            continue
        candidate = dict(item)
        if "scenario_type" not in candidate:
            previous = old_by_id.get(candidate.get("id"))
            if previous is None:
                candidate["scenario_type"] = DEFAULT_SCENARIO_TYPE
            elif "scenario_type" in previous:
                candidate["scenario_type"] = previous["scenario_type"]
        resolved.append(candidate)
    return resolved


#: Required evidence keys per gated status. Each rule group is AND; a group with
#: more than one key is one-of (OR). Moved verbatim from mcp/server.py.
EVIDENCE_REQUIRED_KEYS: dict[str, tuple[tuple[str, ...], ...]] = {
    # automated: both keys required (two AND rules)
    "automated": (
        ("test_file_path",),
        ("test_function",),
    ),
    # passed/failed: last_run_at required + (output_snippet OR test_run_id)
    "passed": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of
    ),
    "failed": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of
    ),
}

# --------------------------------------------------------------------------
# Re-executable evidence contract (spec 9e0bf979)
# --------------------------------------------------------------------------

#: The authoritative evidence_class taxonomy (fr_75e54f55). One allowlist; an
#: invalid value fails closed (never normalized), mirroring scenario_type.
EVIDENCE_CLASSES: tuple[str, ...] = (
    "automated_test_pointer",
    "replay_command",
    "mcp_replay_manifest",
    "manual_checklist",
    "run_log",
    "non_replayable_justified",
)

#: Minimum fields required per evidence_class on a gated status (fr_52f084b4).
#: Same AND-of-(one-of) shape as EVIDENCE_REQUIRED_KEYS. An expected_output_snapshot
#: (expected output / success criteria) is required for EVERY class except the
#: direct automated_test_pointer; run_log / non_replayable_justified additionally
#: require non_replayable_justification (fr_0937529f).
EVIDENCE_CLASS_REQUIRED_KEYS: dict[str, tuple[tuple[str, ...], ...]] = {
    "automated_test_pointer": (
        ("test_file_path",),
        ("test_function",),
    ),
    "replay_command": (
        ("replay_command",),
        ("expected_output_snapshot",),
    ),
    "mcp_replay_manifest": (
        ("manifest_ref",),
        ("execution_attestation",),
        ("execution_receipt",),
    ),
    "manual_checklist": (
        ("manual_checklist_ref",),
        ("expected_output_snapshot",),
    ),
    "run_log": (
        ("last_run_at",),
        ("output_snippet", "test_run_id"),  # one-of: the actual log
        ("non_replayable_justification",),
        ("expected_output_snapshot",),
    ),
    "non_replayable_justified": (
        ("non_replayable_justification",),
        ("expected_output_snapshot",),
    ),
}


class InvalidEvidenceClassError(ValueError):
    """Raised when evidence supplies an evidence_class outside
    :data:`EVIDENCE_CLASSES`. Fail-closed: the value is NEVER normalized
    (spec 9e0bf979), mirroring :class:`InvalidScenarioTypeError`."""

    def __init__(self, value: object) -> None:
        self.value = value
        self.allowed = EVIDENCE_CLASSES
        super().__init__(
            f"Invalid evidence_class {value!r}. "
            f"Allowed values: {', '.join(EVIDENCE_CLASSES)}."
        )


def is_valid_evidence_class(value: object) -> bool:
    """Whether ``value`` is a supported evidence_class. Read-tolerant."""
    return isinstance(value, str) and value in EVIDENCE_CLASSES


def validate_evidence_class(value: object) -> str:
    """Fail-closed validator: return ``value`` unchanged when supported, else
    raise :class:`InvalidEvidenceClassError`. Never normalizes."""
    if not is_valid_evidence_class(value):
        raise InvalidEvidenceClassError(value)
    return value  # type: ignore[return-value]


#: Fields whose presence proves a deterministic replay already exists or is
#: cheap to produce (tr_1fd44294): an existing test (``test_file_path``), an
#: existing command/script (``replay_command``) or a deterministic MCP replay
#: manifest writable under bounded setup (``mcp_replay_manifest``). When one is
#: present, a run-log-only payload is NOT acceptable because the replayable
#: artifact is already at hand.
_CHEAP_OR_EXISTING_REPLAY_FIELDS: tuple[str, ...] = (
    "test_file_path",
    "replay_command",
    "manifest_ref",
    "mcp_replay_manifest",
)


def replay_is_cheap_or_existing(evidence: dict | None) -> bool:
    """Whether a deterministic test / command / replay manifest already exists
    or can be produced cheaply for this evidence (tr_1fd44294)."""
    if not evidence:
        return False
    return any(evidence.get(k) for k in _CHEAP_OR_EXISTING_REPLAY_FIELDS)


#: The non-replayable evidence classes — acceptable ONLY when a deterministic
#: replay genuinely does not exist and is not cheap to produce (fr_958f0c9c).
_RUN_LOG_CLASSES: frozenset[str] = frozenset({"run_log", "non_replayable_justified"})


def replayable_evidence_required(evidence: dict | None) -> bool:
    """True when a replayable artifact must be provided instead of a run log:
    the caller declared ``replay_should_exist=True`` OR a cheap/existing replay
    signal is present (fr_958f0c9c, br_078725cc, tr_1fd44294)."""
    if not evidence:
        return False
    return evidence.get("replay_should_exist") is True or replay_is_cheap_or_existing(
        evidence
    )


def infer_evidence_class(evidence: dict | None) -> str | None:
    """Best-effort classification of evidence for DISPLAY / upgrade ONLY
    (ac_8212cdbb). Never used to retroactively reject already-persisted legacy
    evidence — that path stays on the lenient per-status rules. Returns the
    explicit class when set, else infers from the present fields, else None."""
    if not evidence:
        return None
    explicit = evidence.get("evidence_class")
    if explicit:
        return str(explicit)
    if evidence.get("test_file_path") and evidence.get("test_function"):
        return "automated_test_pointer"
    if evidence.get("replay_command"):
        return "replay_command"
    if evidence.get("manifest_ref") or evidence.get("mcp_replay_manifest"):
        return "mcp_replay_manifest"
    if evidence.get("manual_checklist_ref"):
        return "manual_checklist"
    if evidence.get("last_run_at") and (
        evidence.get("output_snippet") or evidence.get("test_run_id")
    ):
        return "run_log"
    return None


# --------------------------------------------------------------------------
# Evidence V2 semantic verification
# --------------------------------------------------------------------------

EVIDENCE_V2_SCHEMA_VERSION = 2
EVIDENCE_V2_DIGEST_PREFIX = "sha256:"
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ATTESTATION_V2_KEYS = frozenset(
    {
        "schema_version",
        "run_id",
        "executed_at",
        "scenario_id",
        "scenario_sha256",
        "outcome",
        "product_runtime_exercised",
        "manifest_sha256",
        "assertions",
        "provenance",
        "attestation_sha256",
    }
)
_ASSERTION_V2_KEYS = frozenset({"name", "expected", "observed", "status", "message"})
_PROVENANCE_V2_KEYS = frozenset(
    {"producer", "producer_version", "adapter", "environment"}
)


@dataclass(frozen=True, slots=True)
class EvidenceVerificationResult:
    """Pure, transport-neutral verdict consumed by every evidence gate."""

    verified: bool
    reason_codes: tuple[str, ...] = ()
    contract_version: int | None = None
    legacy: bool = False


def _as_plain_mapping(value: object) -> dict[str, Any] | None:
    if isinstance(value, Mapping):
        return dict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="python", exclude_none=False)
        return dict(dumped) if isinstance(dumped, Mapping) else None
    return None


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def compute_execution_attestation_sha256(
    attestation: Mapping[str, Any] | object, *, manifest_ref: str
) -> str:
    """Return the deterministic Evidence V2 envelope digest.

    The exact manifest reference is included alongside the attestation body, so
    changing either the reference or any execution fact invalidates the digest.
    Concrete adapters may use filesystem/network IO to calculate
    ``manifest_sha256``; this helper intentionally performs no IO.
    """

    plain = _as_plain_mapping(attestation)
    if plain is None:
        raise TypeError("attestation must be a mapping or Pydantic model")
    unsigned = {
        key: value for key, value in plain.items() if key != "attestation_sha256"
    }
    # Pydantic materializes the optional assertion message as ``None`` while a
    # raw JSON producer may omit it. Canonicalize that one optional field so a
    # typed request round-trip cannot invalidate an otherwise identical proof.
    raw_assertions = unsigned.get("assertions")
    if isinstance(raw_assertions, list):
        unsigned["assertions"] = [
            {**dict(item), "message": dict(item).get("message")}
            if isinstance(item, Mapping)
            else item
            for item in raw_assertions
        ]
    payload = {"manifest_ref": manifest_ref, "execution_attestation": unsigned}
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{EVIDENCE_V2_DIGEST_PREFIX}{digest}"


def _semantic_acceptance_criterion(value: object) -> dict[str, str | None]:
    """Project an AC to the stable identity/text that affects test semantics.

    Task backlinks and other workflow metadata deliberately do not participate:
    linking a card must not invalidate a real execution, while changing an AC id
    or its text must.
    """

    if isinstance(value, Mapping):
        identifier = value.get("id")
        text = value.get("text")
        if text is None:
            text = value.get("description")
        if text is None:
            text = value.get("criterion")
        return {
            "id": str(identifier) if identifier not in (None, "") else None,
            "text": str(text) if text is not None else "",
        }
    return {"id": None, "text": str(value) if value is not None else ""}


def compute_test_scenario_semantic_sha256(
    *,
    board_id: str,
    spec_id: str,
    scenario: Mapping[str, Any] | object,
    acceptance_criteria: list[object] | tuple[object, ...] | None,
) -> str:
    """Hash the exact semantic contract an Evidence V2 execution proves.

    Identity, Given/When/Then, scenario type, AC links and the current AC
    identity/text projection are bound. Cosmetic title/notes and task backlinks
    are intentionally excluded, matching :data:`SEMANTIC_FIELDS`.
    """

    plain = _as_plain_mapping(scenario)
    if plain is None:
        raise TypeError("scenario must be a mapping or Pydantic model")
    scenario_id = plain.get("id")
    if not _non_empty_string(board_id) or not _non_empty_string(spec_id):
        raise ValueError("board_id and spec_id are required for scenario digest")
    if not _non_empty_string(scenario_id):
        raise ValueError("scenario id is required for scenario digest")
    linked = plain.get("linked_criteria")
    if linked is None:
        linked_values: list[str] = []
    elif isinstance(linked, (list, tuple)):
        linked_values = [str(item) for item in linked]
    else:
        raise TypeError("linked_criteria must be a list or tuple")
    payload = {
        "semantic_schema_version": 1,
        "identity": {
            "board_id": str(board_id),
            "spec_id": str(spec_id),
            "scenario_id": str(scenario_id),
        },
        "scenario": {
            "scenario_type": str(plain.get("scenario_type") or DEFAULT_SCENARIO_TYPE),
            "given": str(plain.get("given") or ""),
            "when": str(plain.get("when") or ""),
            "then": str(plain.get("then") or ""),
            "linked_criteria": linked_values,
        },
        "acceptance_criteria": [
            _semantic_acceptance_criterion(item) for item in (acceptance_criteria or ())
        ],
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"{EVIDENCE_V2_DIGEST_PREFIX}{digest}"


def _non_empty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_aware_iso_datetime(value: object) -> bool:
    if not _non_empty_string(value):
        return False
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None and parsed.utcoffset() is not None


def _json_values_equal(expected: object, observed: object) -> bool:
    try:
        return _canonical_json(expected) == _canonical_json(observed)
    except (TypeError, ValueError):
        return False


def verify_mcp_replay_evidence_v2(
    status: str,
    evidence: Mapping[str, Any] | object | None,
    *,
    scenario_id: str | None = None,
    scenario_sha256: str | None = None,
) -> EvidenceVerificationResult:
    """Semantically verify a canonical MCP replay attestation.

    This is deliberately fail-closed.  Merely carrying a manifest, an output
    string or a caller-supplied boolean is insufficient: the product runtime
    must have been exercised, the outcome must match the scenario status, each
    assertion must be internally coherent, and both manifest/attestation
    digests plus producer provenance must be present. Historical
    ``mcp_replay_manifest`` values remain readable but return
    ``legacy_mcp_replay_manifest_unverified``.
    """

    ev = _as_plain_mapping(evidence) if evidence is not None else None
    if ev is None:
        return EvidenceVerificationResult(
            False, ("evidence_v2.evidence_required",), EVIDENCE_V2_SCHEMA_VERSION
        )

    manifest_ref = ev.get("manifest_ref")
    attestation = _as_plain_mapping(ev.get("execution_attestation"))
    execution_receipt = ev.get("execution_receipt")
    legacy_value = ev.get("mcp_replay_manifest")
    has_legacy = legacy_value not in (None, "", {})
    if not _non_empty_string(manifest_ref) or attestation is None:
        reasons: list[str] = []
        if not _non_empty_string(manifest_ref):
            reasons.append("evidence_v2.manifest_ref_required")
        if attestation is None:
            reasons.append("evidence_v2.execution_attestation_required")
        if has_legacy:
            reasons.append("evidence_v2.legacy_mcp_replay_manifest_unverified")
        return EvidenceVerificationResult(
            False,
            tuple(reasons),
            EVIDENCE_V2_SCHEMA_VERSION,
            legacy=has_legacy,
        )

    reasons = []
    # This is deliberately only a structural check.  A receipt is opaque to
    # CORE and must be authenticated by the registered edition verifier before
    # any write.  A public SHA over caller-controlled fields is not authority.
    if not _non_empty_string(execution_receipt):
        reasons.append("evidence_v2.execution_receipt_required")
    if has_legacy:
        if (
            not isinstance(legacy_value, str)
            or legacy_value.strip() != manifest_ref.strip()
        ):
            reasons.append("evidence_v2.ambiguous_legacy_manifest")

    unexpected = sorted(set(attestation) - _ATTESTATION_V2_KEYS)
    if unexpected:
        reasons.append(
            "evidence_v2.unexpected_attestation_fields:" + ",".join(unexpected)
        )
    if attestation.get("schema_version") != EVIDENCE_V2_SCHEMA_VERSION:
        reasons.append("evidence_v2.schema_version_must_be_2")
    if not _non_empty_string(attestation.get("run_id")):
        reasons.append("evidence_v2.run_id_required")
    if not _is_aware_iso_datetime(attestation.get("executed_at")):
        reasons.append("evidence_v2.executed_at_timezone_required")
    attested_scenario_id = attestation.get("scenario_id")
    if not _non_empty_string(attested_scenario_id):
        reasons.append("evidence_v2.scenario_id_required")
    elif scenario_id is not None and attested_scenario_id != scenario_id:
        reasons.append("evidence_v2.scenario_binding_mismatch")
    attested_scenario_sha256 = attestation.get("scenario_sha256")
    if not isinstance(attested_scenario_sha256, str) or not _SHA256_RE.fullmatch(
        attested_scenario_sha256
    ):
        reasons.append("evidence_v2.scenario_sha256_invalid")
    elif scenario_sha256 is not None and attested_scenario_sha256 != scenario_sha256:
        reasons.append("evidence_v2.scenario_semantic_binding_mismatch")
    if attestation.get("product_runtime_exercised") is not True:
        reasons.append("evidence_v2.product_runtime_not_exercised")

    manifest_sha256 = attestation.get("manifest_sha256")
    if not isinstance(manifest_sha256, str) or not _SHA256_RE.fullmatch(
        manifest_sha256
    ):
        reasons.append("evidence_v2.manifest_sha256_invalid")

    provenance = _as_plain_mapping(attestation.get("provenance"))
    if provenance is None:
        reasons.append("evidence_v2.provenance_required")
    else:
        unexpected_provenance = sorted(set(provenance) - _PROVENANCE_V2_KEYS)
        if unexpected_provenance:
            reasons.append(
                "evidence_v2.unexpected_provenance_fields:"
                + ",".join(unexpected_provenance)
            )
        for field in sorted(_PROVENANCE_V2_KEYS):
            if not _non_empty_string(provenance.get(field)):
                reasons.append(f"evidence_v2.provenance_{field}_required")

    assertions = attestation.get("assertions")
    if not isinstance(assertions, list) or not assertions:
        reasons.append("evidence_v2.assertions_required")
        assertion_results: list[bool] = []
    else:
        assertion_results = []
        for index, raw_assertion in enumerate(assertions):
            assertion = _as_plain_mapping(raw_assertion)
            prefix = f"evidence_v2.assertion[{index}]"
            if assertion is None:
                reasons.append(f"{prefix}.object_required")
                assertion_results.append(False)
                continue
            unexpected_assertion = sorted(set(assertion) - _ASSERTION_V2_KEYS)
            if unexpected_assertion:
                reasons.append(
                    f"{prefix}.unexpected_fields:" + ",".join(unexpected_assertion)
                )
            if not _non_empty_string(assertion.get("name")):
                reasons.append(f"{prefix}.name_required")
            has_expected = "expected" in assertion
            has_observed = "observed" in assertion
            if not has_expected:
                reasons.append(f"{prefix}.expected_required")
            if not has_observed:
                reasons.append(f"{prefix}.observed_required")
            values_match = (
                _json_values_equal(assertion.get("expected"), assertion.get("observed"))
                if has_expected and has_observed
                else False
            )
            assertion_status = assertion.get("status")
            if assertion_status not in {"passed", "failed"}:
                reasons.append(f"{prefix}.status_invalid")
                assertion_results.append(False)
            elif assertion_status == "passed" and not values_match:
                reasons.append(f"{prefix}.observed_expected_mismatch")
                assertion_results.append(False)
            elif assertion_status == "failed" and values_match:
                reasons.append(f"{prefix}.failed_but_values_match")
                assertion_results.append(False)
            else:
                assertion_results.append(assertion_status == "passed")

    outcome = attestation.get("outcome")
    if outcome not in {"passed", "failed"}:
        reasons.append("evidence_v2.outcome_invalid")
    elif status in {"passed", "automated"}:
        if outcome != "passed":
            reasons.append("evidence_v2.outcome_status_mismatch")
        if assertion_results and not all(assertion_results):
            reasons.append("evidence_v2.passed_requires_all_assertions_passed")
    elif status == "failed":
        if outcome != "failed":
            reasons.append("evidence_v2.outcome_status_mismatch")
        if assertion_results and all(assertion_results):
            reasons.append("evidence_v2.failed_requires_failed_assertion")

    supplied_attestation_sha256 = attestation.get("attestation_sha256")
    if not isinstance(supplied_attestation_sha256, str) or not _SHA256_RE.fullmatch(
        supplied_attestation_sha256
    ):
        reasons.append("evidence_v2.attestation_sha256_invalid")
    else:
        expected_attestation_sha256 = compute_execution_attestation_sha256(
            attestation, manifest_ref=str(manifest_ref)
        )
        if supplied_attestation_sha256 != expected_attestation_sha256:
            reasons.append("evidence_v2.attestation_sha256_mismatch")

    return EvidenceVerificationResult(
        verified=not reasons,
        reason_codes=tuple(reasons),
        contract_version=EVIDENCE_V2_SCHEMA_VERSION,
        legacy=False,
    )


class StatusNotMutableError(ValueError):
    """Raised when a scenario status mutation is attempted while the spec is in a
    status that forbids it by default (``validated`` or ``done``).

    Intentionally distinct from ``SpecService._require_spec_unlocked`` (the
    content-lock), which triggers on an active *passed validation* rather than on
    status and would therefore wrongly block ``in_progress``. The service layer
    may allow post-lock regression evidence after a linked test card is already
    executable; arbitrary scenario status edits still fail with this error.
    """

    def __init__(self, spec_status: str) -> None:
        self.spec_status = spec_status
        super().__init__(
            f"Cannot change test scenario status while spec is '{spec_status}'. "
            "Scenario status is mutable only in draft/review/approved/in_progress, "
            "unless the target scenario is already linked to an executable test card "
            "for post-lock regression evidence."
        )


def _apply_evidence_rules(
    rules: tuple[tuple[str, ...], ...], evidence: dict | None
) -> tuple[bool, list[str]]:
    """Apply an AND-of-(one-of) rule set to an evidence dict → ``(ok, missing)``.
    A single-key group is required; a multi-key group is satisfied by any one
    key. Empty ``rules`` is always valid."""
    if not rules:
        return True, []
    if not evidence:
        flat: list[str] = [
            group[0] if len(group) == 1 else " or ".join(group) for group in rules
        ]
        return False, flat
    missing: list[str] = []
    for group in rules:
        if len(group) == 1:
            if not evidence.get(group[0]):
                missing.append(group[0])
        elif not any(evidence.get(k) for k in group):
            missing.append(" or ".join(group))
    return (len(missing) == 0, missing)


#: Hint appended when an unclassed gated write is rejected so the caller knows
#: the supported escape hatch (declare a replayable class) instead of guessing.
_DECLARE_CLASS_HINT = (
    "or set evidence_class to replay_command/mcp_replay_manifest/manual_checklist/"
    "run_log/non_replayable_justified with its required fields"
)


def _validate_unclassed_gated_write(
    status: str, evidence: dict | None
) -> tuple[bool, list[str]]:
    """Write-side gate for a NEW gated write that omits evidence_class
    (spec 9e0bf979).

    Only the legacy direct automated-test-pointer shape (``test_file_path`` +
    ``test_function``) is grandfathered without an explicit class. Any other
    shape — notably run-log-like (``last_run_at`` + ``output_snippet``/
    ``test_run_id``) — must carry replayable-quality fields
    (``expected_output_snapshot`` + ``non_replayable_justification``),
    equivalent to declaring ``run_log``/``non_replayable_justified``, so a weak
    run-log payload can no longer pass a new validation silently (fr_0937529f,
    br_078725cc). Already-persisted legacy evidence is unaffected: this runs
    only when ``for_write=True``."""
    ev = evidence or {}
    if ev.get("test_file_path") and ev.get("test_function"):
        return True, []  # legacy automated_test_pointer flow
    if status == "automated":
        # automated has no run-log substitute — it needs the test pointer.
        return _apply_evidence_rules(EVIDENCE_REQUIRED_KEYS["automated"], ev)
    # passed/failed without a pointer: must be replayable-grade run-log evidence.
    ok, missing = _apply_evidence_rules(
        (
            ("last_run_at",),
            ("output_snippet", "test_run_id"),
            ("expected_output_snapshot",),
            ("non_replayable_justification",),
        ),
        ev,
    )
    if not ok:
        missing = [*missing, _DECLARE_CLASS_HINT]
    return ok, missing


def validate_test_scenario_evidence(
    status: str,
    evidence: dict | None,
    *,
    for_write: bool = False,
    scenario_id: str | None = None,
) -> tuple[bool, list[str]]:
    """Return ``(ok, missing_keys)``; empty ``missing_keys`` means valid.

    Composition (spec 9e0bf979):

    * MCP replay evidence is always verified against the semantic V2 contract
      (runtime, assertions, binding, digests and provenance); its legacy alias
      remains readable but unverified;
    * every other EXPLICIT, valid ``evidence_class`` is validated against that
      class's minimum fields in BOTH read and write contexts;
    * without an explicit class, ``for_write=True`` applies the write-side gate
      (:func:`_validate_unclassed_gated_write`: only a direct test pointer is
      grandfathered, run-log-like must be replayable-grade), while the default
      read context keeps the legacy per-status rules so previously persisted
      evidence stays valid and readable (ac_8212cdbb).

    An invalid ``evidence_class`` value always fails closed (never normalized).
    Each rule group is AND; a multi-key group is one-of (OR).
    """
    if status not in GATED_STATUSES:
        return True, []

    explicit_class: str | None = None
    if evidence:
        raw_class = evidence.get("evidence_class")
        if raw_class not in (None, ""):
            if not is_valid_evidence_class(raw_class):
                return False, [
                    f"evidence_class must be one of {', '.join(EVIDENCE_CLASSES)}"
                ]
            explicit_class = str(raw_class)

    claims_mcp_replay = bool(
        evidence
        and (
            explicit_class == "mcp_replay_manifest"
            or evidence.get("manifest_ref") is not None
            or evidence.get("execution_attestation") is not None
            or evidence.get("mcp_replay_manifest") is not None
        )
    )
    if claims_mcp_replay:
        if explicit_class not in (None, "mcp_replay_manifest"):
            return False, ["evidence_v2.evidence_class_conflicts_with_manifest"]
        verdict = verify_mcp_replay_evidence_v2(
            status, evidence, scenario_id=scenario_id
        )
        return verdict.verified, list(verdict.reason_codes)

    if explicit_class is not None:
        if explicit_class in _RUN_LOG_CLASSES and replayable_evidence_required(
            evidence
        ):
            # fr_958f0c9c / br_078725cc: a run-log / non-replayable class is NOT
            # acceptable when a deterministic replay should exist or is cheap/
            # existing — demand a replayable class rather than a weak log.
            return False, [
                "replayable_evidence_required: replay is cheap/existing or "
                "replay_should_exist=true — use evidence_class "
                "automated_test_pointer/replay_command/mcp_replay_manifest/manual_checklist"
            ]
        return _apply_evidence_rules(
            EVIDENCE_CLASS_REQUIRED_KEYS[explicit_class], evidence
        )
    if for_write:
        return _validate_unclassed_gated_write(status, evidence)
    return _apply_evidence_rules(EVIDENCE_REQUIRED_KEYS.get(status) or (), evidence)


def scenario_has_required_evidence(
    scenario: dict[str, Any], *, for_write: bool = False
) -> bool:
    """Whether a scenario carries the evidence its status requires.

    Reads ``evidence`` (or the legacy ``latest_evidence`` key). Re-expressed on
    top of :func:`validate_test_scenario_evidence` so the rule has one source.
    A non-gated status always passes.
    """
    status = scenario.get("status")
    if status not in GATED_STATUSES:
        return True
    evidence = scenario.get("evidence") or scenario.get("latest_evidence")
    if not isinstance(evidence, dict):
        return False
    ok, _missing = validate_test_scenario_evidence(
        str(status),
        evidence,
        for_write=for_write,
        scenario_id=str(scenario.get("id")) if scenario.get("id") is not None else None,
    )
    return ok


def scenario_has_authenticated_required_evidence(
    *,
    board_id: str,
    spec_id: str,
    scenario: dict[str, Any],
    acceptance_criteria: list[object],
) -> bool:
    """Authenticate Evidence V2 while preserving structural legacy evidence.

    Evidence V2 must resolve through the registered edition verifier and bind
    the scenario's current semantic digest.  The verifier deliberately receives
    no actor constraint because downstream reviewers may consume proof issued
    by another actor; actor equality is enforced when evidence is written.
    """

    if not scenario_has_required_evidence(scenario):
        return False
    evidence = scenario.get("evidence") or scenario.get("latest_evidence")
    claims_v2 = bool(
        isinstance(evidence, dict)
        and (
            evidence.get("manifest_ref") is not None
            or evidence.get("execution_attestation") is not None
            or evidence.get("execution_receipt") is not None
        )
    )
    if not claims_v2:
        return True

    from okto_pulse.core.ports.test_evidence import (
        resolve_test_evidence_write_verifier,
    )

    verifier = resolve_test_evidence_write_verifier()
    if verifier is None or not isinstance(evidence, dict):
        return False
    try:
        scenario_sha256 = compute_test_scenario_semantic_sha256(
            board_id=board_id,
            spec_id=spec_id,
            scenario=scenario,
            acceptance_criteria=acceptance_criteria,
        )
        verification = verifier.verify(
            board_id=board_id,
            spec_id=spec_id,
            scenario_id=str(scenario.get("id") or ""),
            scenario_sha256=scenario_sha256,
            status=str(scenario.get("status") or ""),
            actor_id=None,
            evidence=evidence,
        )
    except (TypeError, ValueError):
        return False
    return verification.verified


def reexecutable_evidence_reference(scenario: dict[str, Any]) -> str:
    """Return a bounded audit reference for valid replayable scenario evidence.

    This helper does not authenticate edition-owned Evidence V2 receipts; callers
    that consume evidence for a gate must first run their authenticated consumer
    check.  It only projects evidence that already satisfies the shared structural
    contract, keeping amendment coverage aligned with the scenario lifecycle
    taxonomy instead of recognizing a smaller legacy subset.
    """

    if str(scenario.get("status") or "").lower() not in GATED_STATUSES:
        return ""
    if not scenario_has_required_evidence(scenario):
        return ""
    evidence = scenario.get("evidence") or scenario.get("latest_evidence")
    if not isinstance(evidence, dict):
        return ""

    test_file_path = str(evidence.get("test_file_path") or "").strip()
    test_function = str(evidence.get("test_function") or "").strip()
    if test_file_path and test_function:
        return f"{test_file_path}::{test_function}"

    test_run_id = str(evidence.get("test_run_id") or "").strip()
    if test_run_id:
        return f"test_run:{test_run_id}"

    evidence_class = infer_evidence_class(evidence)
    replay_field = {
        "replay_command": "replay_command",
        "mcp_replay_manifest": "manifest_ref",
        "manual_checklist": "manual_checklist_ref",
    }.get(str(evidence_class or ""))
    if replay_field is None:
        return ""
    replay_value = str(evidence.get(replay_field) or "").strip()
    if not replay_value:
        return ""
    digest = hashlib.sha256(replay_value.encode("utf-8")).hexdigest()
    return f"{evidence_class}:sha256:{digest}"


def require_test_scenario_status_mutable(spec_status: str | None) -> None:
    """Raise :class:`StatusNotMutableError` when ``spec_status`` forbids scenario
    status mutation (``validated`` or ``done``); otherwise return ``None``.

    This is a STATUS-based guard for the operational status path. It is NOT the
    content-lock (``_require_spec_unlocked``) — using that here would block
    ``in_progress`` and break marking scenarios passed during execution.

    ``spec_status`` may be a plain string or a ``SpecStatus`` enum; the enum's
    ``.value`` is used for the comparison.
    """
    value = getattr(spec_status, "value", spec_status)
    if value in _STATUS_IMMUTABLE_SPEC_STATUSES:
        raise StatusNotMutableError(str(value))


def evidence_invalidated_by_semantic_edit(changed_fields: object) -> bool:
    """Whether any of ``changed_fields`` is a SEMANTIC field (so existing
    evidence must be reset). Cosmetic-only edits return ``False``.
    """
    if not changed_fields:
        return False
    semantic = set(SEMANTIC_FIELDS)
    return any(field in semantic for field in changed_fields)

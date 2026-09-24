"""Pre-cutover authority evidence and exact parity gate; not live grants.

Compatibility evidence must outlive removal of the old registry. In particular,
an all-denied document with owner review is not interchangeable with an ordinary
all-denied policy: deleting obsolete fields must not resolve that review.
"""

from dataclasses import dataclass
from copy import deepcopy

from okto_pulse.core.domain import historical_permission_policy_v034 as historical
from okto_pulse.core.domain.permission_migration_review import PermissionMigrationReview
from okto_pulse.core.ports.historical_archive_authority import (
    HistoricalArchivePresetFacts,
    _resolve_agent_v034,
)
from okto_pulse.core.ports.permission_policy import (
    PermissionSet,
    flatten_permission_flags,
    registered_permission_flags,
)

SOURCE_VERSION = "permission-authority/v0.3.4"
SOURCE_BLOB = "74101618064a1e50a1e9e11c012f7ff6f1f8f7a9"
_FLAGS = tuple(sorted(historical.ALL_FLAGS))
_RETIRED_FEATURE_FLAGS = tuple(sorted((
    "kg.operations.integrity.backfill",
    "kg.operations.queue.read",
    "kg.operations.queue.reprocess",
    "runtime.settings.read",
    "runtime.settings.write",
    "kg.operations.historical.read",
    "kg.operations.settings.read",
    "kg.operations.settings.write",
    "kg.operations.historical.start",
    "kg.operations.historical.cancel",
    "kg.operations.integrity.read",
    "kg.operations.integrity.reconcile",
    "kg.operations.schema.migrate",
    "kg.operations.rebuild.preflight", "kg.operations.rebuild.confirm", "kg.operations.rebuild.run",
    "kg.operations.global_recovery.preflight", "kg.operations.global_recovery.confirm", "kg.operations.global_recovery.read",
    "kg.operations.global_recovery.cancel", "kg.operations.global_recovery.resume", "kg.operations.global_recovery.run",
    "kg.operations.quarantine.restore", "kg.operations.global_outbox.read", "kg.operations.global_outbox.reprocess",
    "kg.operations.global_outbox.verify", "kg.operations.tick.run",
    *(path for path in _FLAGS if path.startswith("sprint.")),
)))


def retired_feature_permission_flags() -> tuple[str, ...]:
    """Closed Sprint/maintenance retirement, never inferred from registry drift.

    The edition may apply this policy only after preserving authority evidence.
    Any other missing or added live flag fails the complete registry parity gate.
    """
    validate_permission_retirement_registry(_RETIRED_FEATURE_FLAGS)
    return _RETIRED_FEATURE_FLAGS


@dataclass(frozen=True, slots=True)
class PermissionRetirementAuthority:
    decisions: tuple[tuple[str, bool], ...]
    owner_review_required: bool
    review_reason: str | None

    def __post_init__(self) -> None:
        if (type(self.decisions) is not tuple
                or any(type(pair) is not tuple or len(pair) != 2
                    or type(pair[0]) is not str or type(pair[1]) is not bool
                    for pair in self.decisions)
                or tuple(path for path, _ in self.decisions) != _FLAGS
                or type(self.owner_review_required) is not bool
                or (self.owner_review_required and (type(self.review_reason) is not str or not self.review_reason))
                or (not self.owner_review_required and self.review_reason is not None)
                or (self.owner_review_required and any(allowed for _, allowed in self.decisions))):
            raise ValueError("permission_retirement_authority_invalid")

    def document(self) -> dict:
        return {"format": SOURCE_VERSION, "source_blob": SOURCE_BLOB,
            "decisions": dict(self.decisions), "owner_review_required": self.owner_review_required,
            "review_reason": self.review_reason}


def parse_permission_retirement_authority(value: object) -> PermissionRetirementAuthority:
    if (type(value) is not dict or set(value) != {"format", "source_blob", "decisions", "owner_review_required", "review_reason"}
            or value["format"] != SOURCE_VERSION or value["source_blob"] != SOURCE_BLOB
            or type(value["decisions"]) is not dict
            or any(type(key) is not str for key in value["decisions"])):
        raise ValueError("permission_retirement_authority_invalid")
    return PermissionRetirementAuthority(tuple(sorted(value["decisions"].items())),
        value["owner_review_required"], value["review_reason"])


def capture_permission_retirement_authority(
    *, agent_flags: object, legacy_permissions: object, preset_id: str | None,
    presets: tuple[HistoricalArchivePresetFacts, ...], board_overrides: object,
) -> PermissionRetirementAuthority:
    permissions = _resolve_agent_v034(agent_flags=agent_flags,
        legacy_permissions=legacy_permissions, preset_id=preset_id,
        presets=presets, board_overrides=board_overrides)
    return PermissionRetirementAuthority(tuple((path, permissions.has(path)) for path in _FLAGS),
        permissions.owner_review_required, permissions.review_reason)


def capture_permission_migration_review(
    *, layer: str, flags: object, source_sha256: str, checkpoint_sha256: str,
    preset_id: str | None = None,
) -> PermissionMigrationReview | None:
    """Classify an old layer in isolation, retaining only its own review reason.

    Invalid ancestry remains an ancestry error; it is not flattened into an
    agent override. A valid layer never inherits a marker from another layer.
    """
    if layer == "agent":
        result = _resolve_agent_v034(agent_flags=flags, legacy_permissions=None, preset_id=preset_id,
            presets=(HistoricalArchivePresetFacts(preset_id, historical.PERMISSION_REGISTRY),) if preset_id else (),
            board_overrides=None)
    elif layer == "preset":
        result = historical.resolve_permission_preset_lineage("source", (
            historical.PermissionPresetLineageNode("source", flags),))
        if not result.owner_review_required:
            result = historical.resolve_permissions(None, result.flags, None)
    elif layer == "board":
        result = historical.resolve_permissions(None, None, flags)
    else:
        raise ValueError("permission_migration_review_layer_invalid")
    if not result.owner_review_required:
        return None
    return PermissionMigrationReview(layer, result.review_reason, source_sha256, checkpoint_sha256)


class PermissionRetirementParityError(ValueError):
    def __init__(self, changed_flags: tuple[str, ...], *, review_changed: bool):
        self.changed_flags = changed_flags
        self.review_changed = review_changed
        super().__init__("permission_retirement_authority_changed")


def require_permission_retirement_parity(
    source: PermissionRetirementAuthority, candidate: PermissionSet, *, retired_flags: tuple[str, ...],
) -> None:
    """Require the whole surviving registry and review signal to be unchanged.

    The caller cannot select only convenient flags for comparison. The declared
    retirement must explain exactly the live registry difference; introductions
    require a separate versioned decision. This gate does not edit candidate
    policy, release a review, or authorize any operation.
    """
    remaining = validate_permission_retirement_registry(retired_flags)
    changed = tuple(path for path, allowed in source.decisions
        if path in remaining and candidate.has(path) is not allowed)
    review_changed = (candidate.owner_review_required is not source.owner_review_required
        or candidate.review_reason != source.review_reason)
    if changed or review_changed:
        raise PermissionRetirementParityError(changed, review_changed=review_changed)


def validate_permission_retirement_registry(retired_flags: tuple[str, ...]) -> frozenset[str]:
    """Validate the complete retirement even when a database has no agents."""
    if (type(retired_flags) is not tuple or any(type(path) is not str for path in retired_flags)
            or len(set(retired_flags)) != len(retired_flags) or not set(retired_flags) <= set(_FLAGS)):
        raise ValueError("permission_retirement_flags_invalid")
    remaining = set(_FLAGS) - set(retired_flags)
    if set(flatten_permission_flags(registered_permission_flags())) != remaining:
        raise ValueError("permission_retirement_registry_mismatch")
    return frozenset(remaining)


@dataclass(frozen=True, slots=True)
class RetiredPermissionDocument:
    document: object
    removed_paths: tuple[str, ...]


def retire_permission_document(document: object, *, retired_flags: tuple[str, ...]) -> RetiredPermissionDocument:
    """Remove declared obsolete leaves without normalizing surviving policy.

    Unknown extension siblings remain untouched. A malformed scalar branch may
    disappear only when every registered descendant was retired. The caller must
    retain the source document, install its review classification and verify
    effective authority before persisting this candidate.
    """
    remaining = validate_permission_retirement_registry(retired_flags)
    retired = set(retired_flags)
    source_paths = set(_FLAGS)
    working = deepcopy(document)
    removed = []

    def visit(value: object, path: str) -> bool:
        if path in retired:
            removed.append(path)
            return True
        prefix = f"{path}." if path else ""
        descendants = {flag for flag in source_paths if flag.startswith(prefix)}
        if not descendants.intersection(retired):
            return False
        if not isinstance(value, dict):
            if path and not descendants.intersection(remaining):
                removed.append(path)
                return True
            return False
        changed = False
        for key, child in tuple(value.items()):
            if not isinstance(key, str):
                raise ValueError("permission_retirement_document_invalid")
            if "." in key:
                continue  # A literal extension key is not a registered path.
            if visit(child, f"{prefix}{key}"):
                del value[key]
                changed = True
        return bool(path) and changed and not value

    visit(working, "")
    return RetiredPermissionDocument(working, tuple(sorted(removed)))

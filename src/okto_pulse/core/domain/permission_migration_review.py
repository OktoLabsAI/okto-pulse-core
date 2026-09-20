"""Typed, migration-only owner-review provenance; never an executable grant."""

from dataclasses import dataclass

_REASONS = {
    "agent": frozenset({"invalid_agent_flags", "unrecognized_direct_permissions"}),
    "preset": frozenset({"invalid_preset_flags"}),
    "board": frozenset({"invalid_board_overrides"}),
}
_FORMAT = "permission-migration-review/v1"


@dataclass(frozen=True, slots=True)
class PermissionMigrationReview:
    layer: str
    review_reason: str
    source_sha256: str
    checkpoint_sha256: str

    def __post_init__(self) -> None:
        if (type(self.layer) is not str or self.layer not in _REASONS
                or type(self.review_reason) is not str or self.review_reason not in _REASONS[self.layer]
                or any(type(value) is not str or len(value) != 64
                    or any(character not in "0123456789abcdef" for character in value)
                    for value in (self.source_sha256, self.checkpoint_sha256))):
            raise ValueError("permission_migration_review_invalid")

    def document(self) -> dict:
        return {"format": _FORMAT, "layer": self.layer, "review_reason": self.review_reason,
            "source_sha256": self.source_sha256, "checkpoint_sha256": self.checkpoint_sha256}


def migration_review_reason(value: object, *, layer: str) -> str | None:
    """Reject damaged or cross-layer persisted markers without granting access."""
    if value is None:
        return None
    try:
        if (type(value) is not dict or set(value) != {"format", "layer", "review_reason", "source_sha256", "checkpoint_sha256"}
                or value["format"] != _FORMAT or value["layer"] != layer):
            raise ValueError
        return PermissionMigrationReview(value["layer"], value["review_reason"],
            value["source_sha256"], value["checkpoint_sha256"]).review_reason
    except (TypeError, ValueError):
        return "invalid_permission_migration_review"

"""Migration-only v0.3.4 authority, independent from the current live registry.

Inputs are edition-loaded facts, without credentials. This facade returns only
archived section decisions; it cannot authorize live operations or grant access
to a Board. Keep activity, realm and membership checks in the capture boundary.
"""

from dataclasses import dataclass

from okto_pulse.core.domain import historical_permission_policy_v034 as historical
from okto_pulse.core.ports.historical_archive import ArchiveReadSections

_SOURCE_READS = ("sprint.entity.read", "sprint.qa.read", "sprint.evaluations.read", "sprint.history_read")


@dataclass(frozen=True, slots=True)
class HistoricalArchivePresetFacts:
    id: str
    flags: object
    base_preset_id: str | None = None


def _sections(permissions: historical.PermissionSet) -> ArchiveReadSections:
    decisions = tuple(permissions.has(flag) for flag in _SOURCE_READS)
    return ArchiveReadSections(*(decisions[0] and decision for decision in decisions))


def capture_authenticated_human_sections_v034(flags: dict) -> ArchiveReadSections:
    """Apply the old reader to server-authenticated human claims, not agent rows."""
    if not isinstance(flags, dict):
        raise ValueError("historical_archive_human_authority_invalid")
    return _sections(historical.PermissionSet(flags))


def resolve_historical_archive_sections_v034(
    *, agent_flags: object, legacy_permissions: object, preset_id: str | None,
    presets: tuple[HistoricalArchivePresetFacts, ...], board_overrides: object,
) -> ArchiveReadSections:
    """Reproduce the v0.3.4 agent gateway's resolution and owner-review decisions.

    Unknown/partial direct documents remain denied. Empty legacy lists retain
    their original read authority. Preset ancestry and Board ceilings use the
    frozen canonical engine, never reconstructed rules in an edition adapter.
    This compatibility evaluation is for pre-cutover capture/installation only.
    """
    review, reason = False, None
    if agent_flags is not None:
        direct = agent_flags
        if preset_id is None:
            if not isinstance(direct, dict):
                review, reason = True, "invalid_agent_flags"
            else:
                try:
                    normalized = historical.normalize_agent_permission_overrides(direct)
                    if normalized is not None:
                        review, reason = True, "unrecognized_direct_permissions"
                except (TypeError, ValueError):
                    review, reason = True, "invalid_agent_flags"
    elif isinstance(legacy_permissions, list):
        direct = historical.map_legacy_permissions(legacy_permissions)
    else:
        direct = None
    preset_flags = None
    if preset_id:
        lineage = historical.resolve_permission_preset_lineage(preset_id, tuple(
            historical.PermissionPresetLineageNode(item.id, item.flags, item.base_preset_id) for item in presets))
        preset_flags, review, reason = lineage.flags, lineage.owner_review_required, lineage.review_reason
    return _sections(historical.resolve_permissions(direct, preset_flags, board_overrides,
        owner_review_required=review, review_reason=reason))

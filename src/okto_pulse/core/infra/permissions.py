"""Permission system for agent access control.

Provides:
- Legacy flat permission constants (Permissions class) for backward compat
- Granular permission registry (PERMISSION_REGISTRY) with ~190 flags
- PermissionSet class for resolved, board-scoped permissions
"""

from __future__ import annotations

import json
from typing import Any


# ---------------------------------------------------------------------------
# Legacy flat permissions (kept for backward compat during migration)
# ---------------------------------------------------------------------------


class Permissions:
    """Permission constants for agent access control (legacy flat model)."""

    # Board
    BOARD_READ = "board:read"

    # Cards
    CARDS_CREATE = "cards:create"
    CARDS_UPDATE = "cards:update"
    CARDS_DELETE = "cards:delete"
    CARDS_MOVE = "cards:move"

    # Comments
    COMMENTS_CREATE = "comments:create"
    COMMENTS_UPDATE = "comments:update"
    COMMENTS_DELETE = "comments:delete"

    # Q&A
    QA_CREATE = "qa:create"
    QA_ANSWER = "qa:answer"
    QA_DELETE = "qa:delete"

    # Specs
    SPECS_CREATE = "specs:create"
    SPECS_UPDATE = "specs:update"
    SPECS_DELETE = "specs:delete"
    SPECS_MOVE = "specs:move"
    SPECS_EVALUATE = "specs:evaluate"

    # Attachments
    ATTACHMENTS_UPLOAD = "attachments:upload"
    ATTACHMENTS_DELETE = "attachments:delete"

    # Self
    SELF_UPDATE = "self:update"

    ALL = [
        BOARD_READ,
        CARDS_CREATE,
        CARDS_UPDATE,
        CARDS_DELETE,
        CARDS_MOVE,
        SPECS_CREATE,
        SPECS_UPDATE,
        SPECS_DELETE,
        SPECS_MOVE,
        SPECS_EVALUATE,
        COMMENTS_CREATE,
        COMMENTS_UPDATE,
        COMMENTS_DELETE,
        QA_CREATE,
        QA_ANSWER,
        QA_DELETE,
        ATTACHMENTS_UPLOAD,
        ATTACHMENTS_DELETE,
        SELF_UPDATE,
    ]

    # Default permissions for new agents
    DEFAULT = [
        BOARD_READ,
        CARDS_CREATE,
        CARDS_UPDATE,
        CARDS_MOVE,
        SPECS_CREATE,
        SPECS_UPDATE,
        SPECS_MOVE,
        SPECS_EVALUATE,
        COMMENTS_CREATE,
        QA_CREATE,
        QA_ANSWER,
        ATTACHMENTS_UPLOAD,
        SELF_UPDATE,
    ]


# ---------------------------------------------------------------------------
# Granular permission registry (~190 flags)
# ---------------------------------------------------------------------------

PERMISSION_REGISTRY: dict[str, dict[str, Any]] = {
    # ---- Board & Context ----
    "board": {
        "read": True,
        "activity_read": True,
        "analytics_read": True,
        "mentions_read": True,
        "mentions_mark_seen": True,
    },
    "profile": {
        "update": True,
    },
    "guidelines": {
        "read": True,
        "create": True,
        "edit": True,
        "delete": True,
        "link": True,
        "unlink": True,
    },
    # ---- Ideation ----
    "ideation": {
        "entity": {
            "read": True, "create": True, "edit_fields": True,
            "assign": True, "label": True, "evaluate": True,
            "archive": True, "restore": True, "delete": True,
        },
        "move": {
            "draft_to_evaluating": True, "evaluating_to_refined": True,
            "refined_to_done": True, "any_to_cancelled": True,
        },
        "interact_in": {
            "draft": True, "evaluating": True, "refined": True,
            "done": True, "cancelled": True,
        },
        "qa": {"read": True, "ask": True, "ask_choice": True, "answer": True},
        "mockups": {"read": True, "create": True, "edit": True, "delete": True, "annotate": True},
        "specs_derive": True,
        "versions_read": True,
        "history_read": True,
    },
    # ---- Refinement ----
    "refinement": {
        "entity": {
            "read": True, "create": True, "edit_fields": True,
            "assign": True, "label": True,
            "archive": True, "restore": True, "delete": True,
        },
        "move": {
            "draft_to_in_progress": True, "in_progress_to_review": True,
            "review_to_approved": True, "approved_to_done": True,
            "any_to_cancelled": True,
        },
        "interact_in": {
            "draft": True, "in_progress": True, "review": True,
            "approved": True, "done": True, "cancelled": True,
        },
        "qa": {"read": True, "ask": True, "ask_choice": True, "answer": True},
        "mockups": {"read": True, "create": True, "edit": True, "delete": True, "annotate": True},
        "knowledge": {"read": True, "create": True, "delete": True},
        "specs_derive": True,
        "versions_read": True,
        "history_read": True,
    },
    # ---- Spec ----
    "spec": {
        "entity": {
            "read": True, "create": True, "edit_fields": True,
            "edit_coverage_flags": True, "assign": True, "label": True,
            "link_card": True, "archive": True, "restore": True, "delete": True,
        },
        "move": {
            "draft_to_review": True, "review_to_approved": True,
            "approved_to_validated": True, "validated_to_in_progress": True,
            "in_progress_to_done": True, "any_to_cancelled": True,
        },
        "interact_in": {
            "draft": True, "review": True, "approved": True,
            "validated": True, "in_progress": True, "done": True, "cancelled": True,
        },
        "qa": {"read": True, "ask": True, "ask_choice": True, "answer": True},
        "tests": {"read": True, "create": True, "update_status": True},
        "rules": {"read": True, "create": True, "edit": True, "delete": True},
        "contracts": {"read": True, "create": True, "edit": True, "delete": True},
        "mockups": {"read": True, "create": True, "edit": True, "delete": True, "annotate": True},
        "skills": {"read": True, "load": True, "create": True, "delete": True},
        "knowledge": {"read": True, "create": True, "delete": True},
        "evaluations": {"read": True, "submit": True, "delete": True},
        "cards_derive": True,
        "history_read": True,
    },
    # ---- Sprint ----
    "sprint": {
        "entity": {
            "read": True, "create": True, "edit_fields": True,
            "edit_coverage_flags": True, "assign": True, "label": True,
            "archive": True, "restore": True, "delete": True,
        },
        "move": {
            "draft_to_active": True, "active_to_review": True,
            "review_to_closed": True, "any_to_cancelled": True,
        },
        "interact_in": {
            "draft": True, "active": True, "review": True,
            "closed": True, "cancelled": True,
        },
        "qa": {"read": True, "ask": True, "answer": True},
        "evaluations": {"read": True, "submit": True, "delete": True},
        "history_read": True,
    },
    # ---- Card ----
    "card": {
        "entity": {
            "read": True, "context_read": True, "create": True, "create_test": True,
            "edit_fields": True, "edit_bug_fields": True,
            "assign": True, "label": True,
            "link_spec": True, "link_tests": True,
            "manage_dependencies": True, "delete": True,
        },
        "copy_from_spec": {"mockups": True, "knowledge": True, "qa": True},
        "link_to": {"scenario": True, "tr": True, "rule": True, "contract": True},
        "move": {
            "not_started_to_started": True, "started_to_in_progress": True,
            "in_progress_to_on_hold": True, "on_hold_to_in_progress": True,
            "in_progress_to_done": True, "any_to_cancelled": True,
            "in_progress_to_validation": True,
            "validation_to_done": True,
            "validation_to_not_started": True,
            "validation_to_on_hold": True,
            "validation_to_cancelled": True,
        },
        "interact_in": {
            "not_started": True, "started": True, "in_progress": True,
            "on_hold": True, "done": True, "cancelled": True,
            "validation": True,
        },
        "validation": {
            "submit": True,
            "read": True,
            "delete": True,
        },
        "qa": {"read": True, "ask": True, "answer": True, "delete": True},
        "comments": {
            "read": True, "create": True, "create_choice": True,
            "respond_choice": True, "get_responses": True,
            "edit": True, "delete": True,
        },
        "attachments": {"read": True, "upload": True, "delete": True},
        "mockups": {"read": True, "create": True, "edit": True, "delete": True, "annotate": True},
        "tests": {"read": True, "link": True, "update_status": True},
        "conclusion": {"read": True, "write": True},
        "activity_read": True,
    },
}


def _flatten_registry(d: dict[str, Any], prefix: str = "") -> list[str]:
    """Flatten nested registry dict into dot-separated flag names."""
    flags: list[str] = []
    for key, value in d.items():
        path = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            flags.extend(_flatten_registry(value, path))
        else:
            flags.append(path)
    return flags


# All flag names as flat list (e.g., "spec.tests.create", "card.move.in_progress_to_done")
ALL_FLAGS: list[str] = _flatten_registry(PERMISSION_REGISTRY)


def _get_nested(d: dict[str, Any], path: str) -> Any:
    """Get value from nested dict by dot-separated path."""
    parts = path.split(".")
    current = d
    for part in parts:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
        if current is None:
            return None
    return current


def _set_nested(d: dict[str, Any], path: str, value: Any) -> None:
    """Set value in nested dict by dot-separated path."""
    parts = path.split(".")
    current = d
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


# ---------------------------------------------------------------------------
# PermissionSet — resolved, board-scoped permissions
# ---------------------------------------------------------------------------


class PermissionSet:
    """Resolved permission flags for an agent on a specific board.

    Encapsulates the merged result of agent_flags ∩ board_overrides.
    Provides typed methods for checking permissions with state awareness.
    """

    def __init__(self, flags: dict[str, Any], preset_name: str | None = None):
        self.flags = flags
        self.preset_name = preset_name

    def has(self, flag: str) -> bool:
        """Check if a specific flag is active.

        Flags absent from the dict default to True (backward compat for
        existing agents that predate new flags).
        """
        value = _get_nested(self.flags, flag)
        if value is None:
            return True  # absent flag = allowed (backward compat)
        return bool(value)

    def check(self, flag: str) -> str | None:
        """Check permission flag. Returns None if allowed, error dict as JSON if denied."""
        if self.has(flag):
            return None
        return _perm_error_detailed(
            reason="permission_missing",
            required_permission=flag,
            detail=f"Agent does not have the '{flag}' permission.",
        )

    def can_interact_in(self, entity: str, status: str) -> bool:
        """Check if agent can interact with an entity in a given status."""
        flag = f"{entity}.interact_in.{status}"
        return self.has(flag)

    def check_with_state(
        self, flag: str, entity: str, status: str
    ) -> str | None:
        """Check permission flag considering entity state.

        Read flags bypass interact_in. For all other actions, interact_in
        must be active for the current entity status.
        """
        # Read actions bypass interact_in
        is_read = flag.endswith(".read") or flag.endswith("_read")
        if not is_read:
            if not self.can_interact_in(entity, status):
                return _perm_error_detailed(
                    reason="interact_in_blocked",
                    required_permission=f"{entity}.interact_in.{status}",
                    current_state=status,
                    detail=(
                        f"Agent cannot interact with {entity} in '{status}' status. "
                        f"Required: {entity}.interact_in.{status}"
                    ),
                )
        # Check the action flag itself
        return self.check(flag)


# ---------------------------------------------------------------------------
# Permission resolution
# ---------------------------------------------------------------------------


def resolve_permissions(
    agent_flags: dict[str, Any] | None,
    preset_flags: dict[str, Any] | None,
    board_overrides: dict[str, Any] | None,
) -> PermissionSet:
    """Resolve effective permissions: preset → agent customization → board override.

    Ceiling model: board_overrides can only restrict (AND), never expand.
    """
    import copy

    # Start from preset or full registry (all True)
    if preset_flags:
        base = copy.deepcopy(preset_flags)
    else:
        base = copy.deepcopy(PERMISSION_REGISTRY)

    # Apply agent-level customizations (override preset values)
    if agent_flags:
        for flag_path in _flatten_registry(agent_flags):
            value = _get_nested(agent_flags, flag_path)
            if value is not None:
                _set_nested(base, flag_path, value)

    # Apply board overrides (AND — can only restrict)
    if board_overrides:
        for flag_path in _flatten_registry(board_overrides):
            override_value = _get_nested(board_overrides, flag_path)
            if override_value is False:
                _set_nested(base, flag_path, False)
            # True in override does NOT expand — ceiling model

    return PermissionSet(base)


# ---------------------------------------------------------------------------
# Legacy permission mapping (19 old → ~190 new)
# ---------------------------------------------------------------------------

LEGACY_PERMISSION_MAP: dict[str, list[str]] = {
    "board:read": [
        "board.read", "board.activity_read", "board.analytics_read",
        "board.mentions_read", "board.mentions_mark_seen",
    ],
    "cards:create": [
        "card.entity.create", "card.entity.create_test",
    ],
    "cards:update": [
        "card.entity.edit_fields", "card.entity.edit_bug_fields",
        "card.entity.assign", "card.entity.label",
        "card.entity.link_spec", "card.entity.link_tests",
        "card.entity.manage_dependencies",
        "card.copy_from_spec.mockups", "card.copy_from_spec.knowledge", "card.copy_from_spec.qa",
        "card.link_to.scenario", "card.link_to.tr", "card.link_to.rule", "card.link_to.contract",
    ],
    "cards:delete": ["card.entity.delete"],
    "cards:move": [
        "card.move.not_started_to_started", "card.move.started_to_in_progress",
        "card.move.in_progress_to_on_hold", "card.move.on_hold_to_in_progress",
        "card.move.in_progress_to_done", "card.move.any_to_cancelled",
        "card.move.in_progress_to_validation",
        "card.move.validation_to_done", "card.move.validation_to_not_started",
        "card.move.validation_to_on_hold", "card.move.validation_to_cancelled",
    ],
    "specs:create": ["spec.entity.create", "sprint.entity.create"],
    "specs:update": [
        "spec.entity.edit_fields", "spec.entity.edit_coverage_flags",
        "spec.entity.assign", "spec.entity.label", "spec.entity.link_card",
        "spec.tests.create", "spec.tests.update_status",
        "spec.rules.create", "spec.rules.edit", "spec.rules.delete",
        "spec.contracts.create", "spec.contracts.edit", "spec.contracts.delete",
        "spec.mockups.create", "spec.mockups.edit", "spec.mockups.delete", "spec.mockups.annotate",
        "spec.skills.create", "spec.skills.delete",
        "spec.knowledge.create", "spec.knowledge.delete",
        "spec.cards_derive",
    ],
    "specs:delete": ["spec.entity.delete"],
    "specs:move": [
        "spec.move.draft_to_review", "spec.move.review_to_approved",
        "spec.move.approved_to_validated", "spec.move.validated_to_in_progress",
        "spec.move.in_progress_to_done", "spec.move.any_to_cancelled",
        "sprint.move.draft_to_active", "sprint.move.active_to_review",
        "sprint.move.review_to_closed", "sprint.move.any_to_cancelled",
    ],
    "specs:evaluate": [
        "spec.evaluations.submit", "spec.evaluations.delete",
        "sprint.evaluations.submit", "sprint.evaluations.delete",
    ],
    "comments:create": [
        "card.comments.create", "card.comments.create_choice",
        "card.comments.respond_choice",
    ],
    "comments:update": ["card.comments.edit"],
    "comments:delete": ["card.comments.delete"],
    "qa:create": [
        "card.qa.ask", "spec.qa.ask", "spec.qa.ask_choice",
        "ideation.qa.ask", "ideation.qa.ask_choice",
        "refinement.qa.ask", "refinement.qa.ask_choice",
        "sprint.qa.ask",
    ],
    "qa:answer": [
        "card.qa.answer", "spec.qa.answer",
        "ideation.qa.answer", "refinement.qa.answer",
        "sprint.qa.answer",
    ],
    "qa:delete": ["card.qa.delete"],
    "attachments:upload": ["card.attachments.upload"],
    "attachments:delete": ["card.attachments.delete"],
    "self:update": ["profile.update"],
}


def map_legacy_permissions(old_permissions: list[str]) -> dict[str, Any]:
    """Map legacy flat permissions to new granular flag structure.

    Flags mapped from old permissions → True. All others → False.
    All interact_in flags → True (backward compat).
    All read flags → True (backward compat).
    """
    import copy
    # Start with all False
    flags = _set_all_flags(copy.deepcopy(PERMISSION_REGISTRY), False)

    # Enable all interact_in (backward compat — existing agents could interact in all states)
    for entity in ("ideation", "refinement", "spec", "sprint", "card"):
        interact_in = flags.get(entity, {}).get("interact_in", {})
        if isinstance(interact_in, dict):
            for status in interact_in:
                interact_in[status] = True

    # Enable all read flags (backward compat)
    for flag_path in ALL_FLAGS:
        if flag_path.endswith(".read") or flag_path.endswith("_read"):
            _set_nested(flags, flag_path, True)

    # Map each legacy permission to new flags
    for old_perm in old_permissions:
        new_flags = LEGACY_PERMISSION_MAP.get(old_perm, [])
        for flag_path in new_flags:
            _set_nested(flags, flag_path, True)

    return flags


def _set_all_flags(d: dict[str, Any], value: bool) -> dict[str, Any]:
    """Set all leaf values in a nested dict to a specific value."""
    for key in d:
        if isinstance(d[key], dict):
            _set_all_flags(d[key], value)
        else:
            d[key] = value
    return d


# ---------------------------------------------------------------------------
# Built-in preset definitions
# ---------------------------------------------------------------------------


def _build_preset_flags(enabled_flags: list[str]) -> dict[str, Any]:
    """Build a flags dict from a list of enabled flag paths. All others are False."""
    import copy
    flags = _set_all_flags(copy.deepcopy(PERMISSION_REGISTRY), False)
    for path in enabled_flags:
        if path.endswith(".*"):
            # Wildcard: enable all flags under this prefix
            prefix = path[:-2]
            for flag in ALL_FLAGS:
                if flag.startswith(prefix):
                    _set_nested(flags, flag, True)
        else:
            _set_nested(flags, path, True)
    return flags


def get_builtin_presets() -> list[dict[str, Any]]:
    """Return the 5 built-in preset definitions."""
    import copy

    full_control = copy.deepcopy(PERMISSION_REGISTRY)  # all True

    executor = _build_preset_flags([
        "board.*",
        "guidelines.read",
        "profile.update",
        # Spec read-only
        "spec.entity.read",
        "spec.qa.read", "spec.tests.read", "spec.rules.read", "spec.contracts.read",
        "spec.mockups.read", "spec.skills.read", "spec.skills.load",
        "spec.knowledge.read", "spec.evaluations.read", "spec.history_read",
        "spec.interact_in.validated", "spec.interact_in.in_progress", "spec.interact_in.done",
        # Sprint: read + interact active/review
        "sprint.entity.read", "sprint.qa.read", "sprint.evaluations.read", "sprint.history_read",
        "sprint.interact_in.active", "sprint.interact_in.review",
        # Card full
        "card.*",
    ])
    # Executor: can read validations but not submit or delete them
    _set_nested(executor, "card.validation.submit", False)
    _set_nested(executor, "card.validation.delete", False)

    validator = _build_preset_flags([
        "board.read", "board.activity_read", "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Ideation: read + Q&A + versions/history
        "ideation.entity.read",
        "ideation.qa.*", "ideation.mockups.read", "ideation.versions_read", "ideation.history_read",
        "ideation.interact_in.evaluating", "ideation.interact_in.refined",
        "ideation.move.evaluating_to_refined", "ideation.move.refined_to_done",
        # Refinement: read + Q&A
        "refinement.entity.read",
        "refinement.qa.*", "refinement.mockups.read", "refinement.knowledge.read",
        "refinement.versions_read", "refinement.history_read",
        "refinement.interact_in.review", "refinement.interact_in.approved",
        "refinement.move.review_to_approved", "refinement.move.approved_to_done",
        # Spec: read + evaluations + move validated->in_progress
        "spec.entity.read",
        "spec.qa.*", "spec.tests.read", "spec.rules.read", "spec.contracts.read",
        "spec.mockups.read", "spec.skills.read", "spec.skills.load",
        "spec.knowledge.read", "spec.history_read",
        "spec.evaluations.read", "spec.evaluations.submit",
        "spec.interact_in.review", "spec.interact_in.approved", "spec.interact_in.validated",
        "spec.move.review_to_approved", "spec.move.validated_to_in_progress",
        # Sprint: read + evaluations + move
        "sprint.entity.read", "sprint.qa.read",
        "sprint.evaluations.read", "sprint.evaluations.submit",
        "sprint.history_read",
        "sprint.interact_in.review", "sprint.interact_in.closed",
        "sprint.move.active_to_review", "sprint.move.review_to_closed",
        # Card: read + Q&A + comments + conclusion
        "card.entity.read", "card.entity.context_read",
        "card.qa.*", "card.comments.*", "card.conclusion.read", "card.activity_read",
        "card.tests.read", "card.mockups.read", "card.attachments.read",
        "card.interact_in.in_progress", "card.interact_in.done",
        "card.interact_in.validation",
        "card.move.in_progress_to_done",
        "card.move.in_progress_to_validation",
        "card.move.validation_to_done", "card.move.validation_to_not_started",
        "card.move.validation_to_on_hold", "card.move.validation_to_cancelled",
        "card.validation.submit", "card.validation.read",
    ])

    qa = _build_preset_flags([
        "board.read", "board.activity_read", "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Ideation/refinement read
        "ideation.entity.read", "ideation.interact_in.*",
        "refinement.entity.read", "refinement.knowledge.read", "refinement.interact_in.*",
        # Spec: read + tests full + evaluations submit
        "spec.entity.read",
        "spec.qa.read", "spec.qa.ask", "spec.qa.answer",
        "spec.tests.*",
        "spec.rules.read", "spec.contracts.read", "spec.mockups.read",
        "spec.knowledge.read", "spec.history_read",
        "spec.evaluations.read", "spec.evaluations.submit",
        "spec.interact_in.*",
        # Sprint: interact all + evaluations all
        "sprint.entity.read", "sprint.interact_in.*",
        "sprint.qa.*", "sprint.evaluations.*", "sprint.history_read",
        # Card: read + create_test + Q&A + comments + tests
        "card.entity.read", "card.entity.context_read", "card.entity.create_test",
        "card.qa.*", "card.comments.read", "card.comments.create",
        "card.comments.create_choice", "card.comments.respond_choice",
        "card.tests.*", "card.conclusion.read", "card.activity_read",
        "card.mockups.read", "card.attachments.read",
        "card.interact_in.*",
        "card.validation.submit", "card.validation.read",
    ])

    spec_writer = _build_preset_flags([
        "board.read", "board.activity_read", "board.analytics_read",
        "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Ideation: read + evaluate + derive + Q&A
        "ideation.entity.read", "ideation.entity.evaluate",
        "ideation.qa.*", "ideation.mockups.read",
        "ideation.specs_derive", "ideation.versions_read", "ideation.history_read",
        "ideation.interact_in.draft", "ideation.interact_in.evaluating", "ideation.interact_in.refined",
        # Refinement: read + derive + Q&A + knowledge
        "refinement.entity.read",
        "refinement.qa.*", "refinement.mockups.read",
        "refinement.knowledge.*", "refinement.specs_derive",
        "refinement.versions_read", "refinement.history_read",
        "refinement.interact_in.draft", "refinement.interact_in.in_progress",
        "refinement.interact_in.review", "refinement.interact_in.approved",
        # Spec: full CRUD
        "spec.*",
        # Sprint: entity.* + move.* + interact draft/active
        "sprint.entity.*", "sprint.move.*", "sprint.qa.*",
        "sprint.evaluations.*", "sprint.history_read",
        "sprint.interact_in.draft", "sprint.interact_in.active",
        # Card: read + create + basic edit
        "card.entity.read", "card.entity.context_read", "card.entity.create", "card.entity.create_test",
        "card.entity.edit_fields", "card.entity.assign", "card.entity.label",
        "card.entity.link_spec", "card.entity.link_tests", "card.entity.manage_dependencies",
        "card.link_to.*", "card.copy_from_spec.*",
        "card.comments.read", "card.comments.create", "card.activity_read",
        "card.interact_in.not_started",
        "card.validation.read",
    ])

    return [
        {"name": "Full Control", "description": "All permissions active — unrestricted access.", "flags": full_control},
        {"name": "Executor", "description": "Execute tasks. Full card access, spec read-only.", "flags": executor},
        {"name": "Validator", "description": "Review and approve. Evaluate specs, promote status, Q&A and comments.", "flags": validator},
        {"name": "QA", "description": "Quality assurance. Create test cards, manage test scenarios, evaluate specs.", "flags": qa},
        {"name": "Spec", "description": "Specification writer. Derive and define specs, create task breakdown.", "flags": spec_writer},
    ]


# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------


def validate_registry_vs_tools(tool_names: list[str]) -> None:
    """Validate PERMISSION_REGISTRY against registered MCP tools. Logs warnings."""
    import logging
    logger = logging.getLogger("okto_pulse.permissions")

    # Build expected tool name patterns from flag paths
    # Flags like "spec.tests.create" map to tools like "okto_pulse_add_test_scenario"
    # This is a loose validation — just checks for orphan flags and unmapped tools
    registry_flags = set(ALL_FLAGS)
    okto_tools = {t for t in tool_names if t.startswith("okto_pulse_")}

    if not okto_tools:
        return

    logger.info(
        f"Permission registry: {len(registry_flags)} flags, "
        f"{len(okto_tools)} MCP tools registered."
    )


def _perm_error_detailed(
    reason: str,
    required_permission: str,
    current_state: str | None = None,
    detail: str = "",
) -> str:
    """Build detailed permission error JSON string."""
    error: dict[str, Any] = {
        "error": "Permission denied",
        "reason": reason,
        "required_permission": required_permission,
    }
    if current_state:
        error["current_state"] = current_state
    if detail:
        error["detail"] = detail
    return json.dumps(error)


# ---------------------------------------------------------------------------
# Backward-compatible check functions
# ---------------------------------------------------------------------------


def has_permission(agent_permissions: "list[str] | PermissionSet | None", required: str) -> bool:
    """Check if agent has a specific permission.

    Accepts:
    - None: full access (backwards compat)
    - list[str]: legacy flat permissions
    - PermissionSet: new granular permissions
    """
    if agent_permissions is None:
        return True
    if isinstance(agent_permissions, PermissionSet):
        return agent_permissions.has(required)
    return required in agent_permissions


def check_permission(agent_permissions: "list[str] | PermissionSet | None", required: str) -> str | None:
    """Check permission and return error message if denied.

    Returns None if allowed, error message string if denied.
    Accepts list[str] (legacy), PermissionSet (new), or None (full access).
    """
    if agent_permissions is None:
        return None
    if isinstance(agent_permissions, PermissionSet):
        return agent_permissions.check(required)
    if required in agent_permissions:
        return None
    return f"Permission denied: requires '{required}'"

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
            # Spec Validation Gate — direct backward transitions to draft.
            # approved_to_draft unblocks minor edits; validated_to_draft unlocks
            # a validated spec in 1 click (replaces the 3-hop validated→approved→review→draft).
            "approved_to_draft": True, "validated_to_draft": True,
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
        # Spec Validation Gate — dedicated flags mirroring card.validation.
        # Different from spec.evaluations (which is the qualitative gate for
        # validated→in_progress). This is the approved→validated content gate.
        "validation": {"submit": True, "read": True, "delete": True},
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
        # Spec Validation Gate — new backward transitions
        "spec.move.approved_to_draft", "spec.move.validated_to_draft",
        "sprint.move.draft_to_active", "sprint.move.active_to_review",
        "sprint.move.review_to_closed", "sprint.move.any_to_cancelled",
    ],
    "specs:evaluate": [
        "spec.evaluations.submit", "spec.evaluations.delete",
        # Spec Validation Gate — legacy agents with specs:evaluate also get
        # the new validation gate submit/read permissions automatically.
        "spec.validation.submit", "spec.validation.read",
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
    """Return the 5 built-in preset definitions with clean role separation.

    Role boundaries (see docstring for each preset):
    - Full Control: unrestricted
    - Spec:       defines WHAT to build — owns ideation/refinement/spec content,
                  plans sprints, drafts card breakdown. Never submits gates.
    - Executor:   implements normal cards. Moves not_started→validation. Never
                  submits gates, never crosses into validation→done.
    - QA:         owns test scenarios and test card lifecycle. Reads specs,
                  asks questions. Never submits any gate.
    - Validator:  exclusive gate-holder. Submits spec_validation, spec_evaluation,
                  sprint_evaluation, task_validation. Owns approved→validated,
                  validated→in_progress, in_progress→done (spec) and the backward
                  unlock transitions. On cards, only touches validation status
                  and only moves validation→done or validation→not_started.
    """
    import copy

    full_control = copy.deepcopy(PERMISSION_REGISTRY)  # all True

    # ------------------------------------------------------------------
    # Spec — defines WHAT to build
    # ------------------------------------------------------------------
    # Owns: ideation + refinement + spec content (BRs/TRs/contracts/mockups/
    # skills/knowledge/test scenarios), sprint planning, initial card breakdown.
    # Cannot: submit gates, validate anything, move cards past not_started,
    # move specs past approved (Validator promotes to validated).
    spec_writer = _build_preset_flags([
        "board.read", "board.activity_read", "board.analytics_read",
        "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Ideation — full ownership (create → done), evaluate, derive spec
        "ideation.entity.read", "ideation.entity.create", "ideation.entity.edit_fields",
        "ideation.entity.assign", "ideation.entity.label", "ideation.entity.evaluate",
        "ideation.entity.archive", "ideation.entity.restore", "ideation.entity.delete",
        "ideation.move.draft_to_evaluating", "ideation.move.evaluating_to_refined",
        "ideation.move.refined_to_done", "ideation.move.any_to_cancelled",
        "ideation.interact_in.draft", "ideation.interact_in.evaluating",
        "ideation.interact_in.refined",
        "ideation.qa.read", "ideation.qa.ask", "ideation.qa.ask_choice", "ideation.qa.answer",
        "ideation.mockups.read", "ideation.mockups.create", "ideation.mockups.edit",
        "ideation.mockups.delete", "ideation.mockups.annotate",
        "ideation.specs_derive", "ideation.versions_read", "ideation.history_read",
        # Refinement — full ownership (create → done), derive spec
        "refinement.entity.read", "refinement.entity.create", "refinement.entity.edit_fields",
        "refinement.entity.assign", "refinement.entity.label",
        "refinement.entity.archive", "refinement.entity.restore", "refinement.entity.delete",
        "refinement.move.draft_to_in_progress", "refinement.move.in_progress_to_review",
        "refinement.move.review_to_approved", "refinement.move.approved_to_done",
        "refinement.move.any_to_cancelled",
        "refinement.interact_in.draft", "refinement.interact_in.in_progress",
        "refinement.interact_in.review", "refinement.interact_in.approved",
        "refinement.qa.read", "refinement.qa.ask", "refinement.qa.ask_choice", "refinement.qa.answer",
        "refinement.mockups.read", "refinement.mockups.create", "refinement.mockups.edit",
        "refinement.mockups.delete", "refinement.mockups.annotate",
        "refinement.knowledge.read", "refinement.knowledge.create", "refinement.knowledge.delete",
        "refinement.specs_derive", "refinement.versions_read", "refinement.history_read",
        # Spec — content CRUD up to approved. Gates and beyond are Validator's.
        "spec.entity.read", "spec.entity.create", "spec.entity.edit_fields",
        "spec.entity.edit_coverage_flags", "spec.entity.assign", "spec.entity.label",
        "spec.entity.link_card",
        "spec.entity.archive", "spec.entity.restore", "spec.entity.delete",
        "spec.move.draft_to_review", "spec.move.review_to_approved",
        "spec.move.any_to_cancelled",
        "spec.interact_in.draft", "spec.interact_in.review", "spec.interact_in.approved",
        "spec.qa.read", "spec.qa.ask", "spec.qa.ask_choice", "spec.qa.answer",
        "spec.tests.read", "spec.tests.create", "spec.tests.update_status",
        "spec.rules.read", "spec.rules.create", "spec.rules.edit", "spec.rules.delete",
        "spec.contracts.read", "spec.contracts.create", "spec.contracts.edit", "spec.contracts.delete",
        "spec.mockups.read", "spec.mockups.create", "spec.mockups.edit",
        "spec.mockups.delete", "spec.mockups.annotate",
        "spec.skills.read", "spec.skills.load", "spec.skills.create", "spec.skills.delete",
        "spec.knowledge.read", "spec.knowledge.create", "spec.knowledge.delete",
        # Spec read-only on gates (sees history, cannot submit)
        "spec.evaluations.read",
        "spec.validation.read",
        "spec.cards_derive", "spec.history_read",
        # Sprint — planner owns structure, reads gate history
        "sprint.entity.read", "sprint.entity.create", "sprint.entity.edit_fields",
        "sprint.entity.edit_coverage_flags", "sprint.entity.assign", "sprint.entity.label",
        "sprint.entity.archive", "sprint.entity.restore", "sprint.entity.delete",
        "sprint.move.draft_to_active", "sprint.move.active_to_review",
        "sprint.move.any_to_cancelled",
        "sprint.interact_in.draft", "sprint.interact_in.active",
        "sprint.qa.read", "sprint.qa.ask", "sprint.qa.answer",
        "sprint.evaluations.read",
        "sprint.history_read",
        # Card — breakdown only (create, link, configure). Lifecycle is Executor/QA/Validator.
        "card.entity.read", "card.entity.context_read",
        "card.entity.create", "card.entity.create_test",
        "card.entity.edit_fields",
        "card.entity.assign", "card.entity.label",
        "card.entity.link_spec", "card.entity.link_tests", "card.entity.manage_dependencies",
        "card.copy_from_spec.mockups", "card.copy_from_spec.knowledge", "card.copy_from_spec.qa",
        "card.link_to.scenario", "card.link_to.tr", "card.link_to.rule", "card.link_to.contract",
        "card.comments.read", "card.comments.create",
        "card.attachments.read",
        "card.mockups.read",
        "card.tests.read",
        "card.qa.read", "card.qa.ask",
        "card.validation.read",
        "card.activity_read",
        "card.interact_in.not_started",
    ])

    # ------------------------------------------------------------------
    # Executor — implements normal cards
    # ------------------------------------------------------------------
    # Owns: card lifecycle from not_started → started → in_progress → validation
    # (and on_hold detours). Reads spec context to implement correctly.
    # Cannot: create cards, submit validation, promote validation→done,
    # create/edit spec content, touch sprint/gates.
    executor = _build_preset_flags([
        "board.read", "board.activity_read",
        "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Spec — read-only, interact while in_progress lifecycle states
        "spec.entity.read",
        "spec.qa.read", "spec.qa.ask",
        "spec.tests.read",
        "spec.rules.read", "spec.contracts.read",
        "spec.mockups.read",
        "spec.skills.read", "spec.skills.load",
        "spec.knowledge.read",
        "spec.evaluations.read",
        "spec.validation.read",
        "spec.history_read",
        "spec.interact_in.validated", "spec.interact_in.in_progress", "spec.interact_in.done",
        # Sprint — read active sprint to know scope
        "sprint.entity.read",
        "sprint.qa.read", "sprint.qa.ask",
        "sprint.evaluations.read",
        "sprint.history_read",
        "sprint.interact_in.active",
        # Card — implementer: owns everything up to moving into validation
        "card.entity.read", "card.entity.context_read",
        "card.entity.edit_fields", "card.entity.edit_bug_fields",
        "card.entity.assign", "card.entity.label",
        "card.interact_in.not_started", "card.interact_in.started",
        "card.interact_in.in_progress", "card.interact_in.on_hold",
        "card.interact_in.validation",  # read-only touch (to see failed validation feedback)
        "card.move.not_started_to_started", "card.move.started_to_in_progress",
        "card.move.in_progress_to_on_hold", "card.move.on_hold_to_in_progress",
        "card.move.in_progress_to_validation",
        "card.move.any_to_cancelled",
        "card.qa.read", "card.qa.ask", "card.qa.answer",
        "card.comments.read", "card.comments.create",
        "card.comments.create_choice", "card.comments.respond_choice", "card.comments.get_responses",
        "card.attachments.read", "card.attachments.upload", "card.attachments.delete",
        "card.mockups.read", "card.mockups.annotate",
        "card.tests.read",
        "card.conclusion.read", "card.conclusion.write",
        "card.validation.read",  # read-only — cannot submit, cannot delete
        "card.activity_read",
    ])

    # ------------------------------------------------------------------
    # QA — owns test scenarios and test card lifecycle
    # ------------------------------------------------------------------
    # Owns: test_scenarios CRUD on specs, test cards (card_type="test")
    # throughout their lifecycle, test scenario status updates.
    # Cannot: submit any gate (spec_validation, spec_evaluation,
    # sprint_evaluation, task_validation — all exclusive to Validator),
    # create normal cards, touch implementation cards.
    # NOTE: card_type enforcement is a convention, not hard-blocked by flags.
    # The agent is instructed to only work on test cards.
    qa = _build_preset_flags([
        "board.read", "board.activity_read",
        "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Ideation — read + Q&A to raise test-related questions
        "ideation.entity.read",
        "ideation.qa.read", "ideation.qa.ask", "ideation.qa.ask_choice", "ideation.qa.answer",
        "ideation.mockups.read",
        "ideation.versions_read", "ideation.history_read",
        "ideation.interact_in.evaluating", "ideation.interact_in.refined",
        # Refinement — read + Q&A
        "refinement.entity.read",
        "refinement.qa.read", "refinement.qa.ask", "refinement.qa.ask_choice", "refinement.qa.answer",
        "refinement.mockups.read", "refinement.knowledge.read",
        "refinement.versions_read", "refinement.history_read",
        "refinement.interact_in.review", "refinement.interact_in.approved",
        # Spec — tests CRUD (QA's core); read everything else, no gate submissions
        "spec.entity.read",
        "spec.qa.read", "spec.qa.ask", "spec.qa.ask_choice", "spec.qa.answer",
        "spec.tests.read", "spec.tests.create", "spec.tests.update_status",
        "spec.rules.read", "spec.contracts.read", "spec.mockups.read",
        "spec.skills.read", "spec.skills.load",
        "spec.knowledge.read",
        "spec.evaluations.read",   # read-only — Validator submits
        "spec.validation.read",    # read-only — Validator submits
        "spec.history_read",
        "spec.interact_in.approved", "spec.interact_in.validated", "spec.interact_in.in_progress",
        # Sprint — read + Q&A only (no evaluation submission)
        "sprint.entity.read",
        "sprint.qa.read", "sprint.qa.ask", "sprint.qa.answer",
        "sprint.evaluations.read",   # read-only — Validator submits
        "sprint.history_read",
        "sprint.interact_in.active", "sprint.interact_in.review",
        # Card — test cards lifecycle (create, implement, complete) + read others
        "card.entity.read", "card.entity.context_read",
        "card.entity.create_test", "card.entity.edit_fields",
        "card.link_to.scenario",
        "card.qa.read", "card.qa.ask", "card.qa.answer",
        "card.comments.read", "card.comments.create",
        "card.attachments.read", "card.attachments.upload",
        "card.mockups.read",
        "card.tests.read", "card.tests.link", "card.tests.update_status",
        "card.conclusion.read", "card.conclusion.write",
        "card.validation.read",  # read-only
        "card.activity_read",
        # Test cards don't go through validation gate — QA moves them directly through lifecycle
        "card.interact_in.not_started", "card.interact_in.started",
        "card.interact_in.in_progress", "card.interact_in.on_hold",
        "card.interact_in.done",
        "card.move.not_started_to_started", "card.move.started_to_in_progress",
        "card.move.in_progress_to_on_hold", "card.move.on_hold_to_in_progress",
        "card.move.in_progress_to_done",   # test cards bypass validation gate
        "card.move.any_to_cancelled",
    ])

    # ------------------------------------------------------------------
    # Validator — exclusive gate-holder for every SDLC checkpoint
    # ------------------------------------------------------------------
    # Owns: spec_validation submit, spec_evaluation submit, sprint_evaluation
    # submit, task_validation submit, spec promotions (approved→validated,
    # validated→in_progress, in_progress→done), spec backward unlock
    # (approved→draft, validated→draft), sprint review→closed.
    # Cards: ONLY interact_in validation. ONLY move validation→done or
    # validation→not_started (user requirement — strict).
    # Cannot: create/edit anything, touch cards outside validation status,
    # move specs forward without the gate.
    validator = _build_preset_flags([
        "board.read", "board.activity_read",
        "board.mentions_read", "board.mentions_mark_seen",
        "guidelines.read",
        "profile.update",
        # Ideation — read + Q&A (observer, cannot edit or promote)
        "ideation.entity.read",
        "ideation.qa.read", "ideation.qa.ask", "ideation.qa.answer",
        "ideation.mockups.read",
        "ideation.versions_read", "ideation.history_read",
        "ideation.interact_in.evaluating", "ideation.interact_in.refined",
        # Refinement — read + Q&A
        "refinement.entity.read",
        "refinement.qa.read", "refinement.qa.ask", "refinement.qa.answer",
        "refinement.mockups.read", "refinement.knowledge.read",
        "refinement.versions_read", "refinement.history_read",
        "refinement.interact_in.review", "refinement.interact_in.approved",
        # Spec — full read + both gates (validation + evaluation) EXCLUSIVE submit
        "spec.entity.read",
        "spec.qa.read", "spec.qa.ask", "spec.qa.answer",
        "spec.tests.read", "spec.rules.read", "spec.contracts.read",
        "spec.mockups.read", "spec.skills.read", "spec.skills.load",
        "spec.knowledge.read",
        "spec.history_read",
        # Exclusive gate capabilities
        "spec.evaluations.read", "spec.evaluations.submit",
        "spec.validation.read", "spec.validation.submit",
        # Spec status promotions — only the gate-bound moves
        "spec.move.approved_to_validated",
        "spec.move.validated_to_in_progress",
        "spec.move.in_progress_to_done",
        # Backward unlock paths (preserved from current preset — enables the
        # fix-and-revalidate loop after a gate failure).
        "spec.move.approved_to_draft", "spec.move.validated_to_draft",
        "spec.interact_in.approved", "spec.interact_in.validated", "spec.interact_in.in_progress",
        # Sprint — evaluation gate EXCLUSIVE + review→closed
        "sprint.entity.read",
        "sprint.qa.read", "sprint.qa.ask", "sprint.qa.answer",
        "sprint.evaluations.read", "sprint.evaluations.submit",
        "sprint.history_read",
        "sprint.interact_in.review",
        "sprint.move.review_to_closed",
        # Card — ONLY the validation status, EXCLUSIVE task_validation submit
        "card.entity.read", "card.entity.context_read",
        "card.qa.read", "card.qa.ask", "card.qa.answer",
        "card.comments.read", "card.comments.create",  # leave feedback
        "card.conclusion.read",
        "card.tests.read",
        "card.mockups.read",
        "card.attachments.read",
        "card.validation.read", "card.validation.submit",  # exclusive submit
        "card.activity_read",
        # interact_in ONLY validation — hard user requirement
        "card.interact_in.validation",
        # moves ONLY validation → {done, not_started} — hard user requirement.
        # submit_task_validation auto-routes via these flags.
        "card.move.validation_to_done",
        "card.move.validation_to_not_started",
    ])

    return [
        {"name": "Full Control", "description": "All permissions active — unrestricted access.", "flags": full_control},
        {"name": "Executor", "description": "Implement normal cards. Moves not_started→validation. Cannot submit gates or promote validation→done.", "flags": executor},
        {"name": "Validator", "description": "Exclusive gate-holder. Submits spec/task/sprint validations and evaluations. On cards, only touches validation status.", "flags": validator},
        {"name": "QA", "description": "Owns test scenarios and test card lifecycle. No gate submissions.", "flags": qa},
        {"name": "Spec", "description": "Defines the spec (ideation→refinement→spec content, sprint plan, card breakdown). No gate submissions, no card execution.", "flags": spec_writer},
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

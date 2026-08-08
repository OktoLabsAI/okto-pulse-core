"""Board-level governance settings resolution."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from okto_pulse.core.models.schemas import BoardSettings
from okto_pulse.core.ports.domain_event_delivery import (
    get_domain_event_fact_reader,
)
from okto_pulse.core.services.governance_observability import (
    METRIC_QA_SELF_ANSWER_DENIED,
    build_qa_self_answer_denied_details as _build_qa_self_answer_denied_details,
)


GOVERNANCE_SETTING_KEYS = (
    "allow_agent_self_answering",
    "require_full_context_for_critical_actions",
)
LEGACY_ABSENT_SETTING_KEYS = (
    "skip_task_requirement_link_gate_global",
    "reviewer_separation_mode",
)
QA_SELF_ANSWER_DENIED_ACTION = "qa_self_answer_denied"
QA_SELF_ANSWER_DENIED_METRIC = METRIC_QA_SELF_ANSWER_DENIED
SELF_ANSWERING_NOT_ALLOWED_REASON = "self_answering_not_allowed"


@dataclass(frozen=True)
class BoardGovernanceSettings:
    """Effective board governance policy resolved from stored settings."""

    allow_agent_self_answering: bool
    require_full_context_for_critical_actions: bool
    qa_require_role_separation: bool
    settings: dict[str, Any]


class QASelfAnsweringNotAllowedError(ValueError):
    """Raised when same-principal Q&A answering is not explicitly enabled."""

    reason = SELF_ANSWERING_NOT_ALLOWED_REASON

    def __init__(self) -> None:
        super().__init__(
            "self_answering_not_allowed: the same principal who asked this "
            "question cannot answer it unless allow_agent_self_answering is "
            "enabled on the board."
        )


class BoardGovernanceService:
    """Resolve effective board governance settings with safe defaults.

    Legacy ``qa_require_role_separation`` is retained as input compatibility,
    but it never grants the canonical positive opt-in for self-answering.
    """

    def __init__(self, db: object | None = None):
        self.db = db

    @staticmethod
    def normalize_settings(
        settings: dict[str, Any] | BoardSettings | None,
        *,
        read_tolerant: bool = False,
    ) -> dict[str, Any]:
        """Normalize a settings payload against :class:`BoardSettings`.

        ``read_tolerant`` is OPT-IN and belongs to READ paths only. A
        persisted/tampered ``impact_evidence_mode`` must never fail-close a
        move (AC-9 of SK-B2-S1), so read callers degrade it to the ``off``
        default exactly like ``resolve_impact_evidence_mode``. WRITE callers
        (board create/update, default-config templates) keep the strict
        parse: an out-of-enum value there is an authoring error and must be
        rejected, never silently disabled.
        """

        if isinstance(settings, BoardSettings):
            return settings.model_dump(mode="json")
        raw = dict(settings or {})
        if read_tolerant:
            from okto_pulse.core.services.impact_evidence import (
                IMPACT_EVIDENCE_MODES,
            )

            mode = raw.get("impact_evidence_mode")
            if (
                mode is not None
                and str(mode).strip().lower() not in IMPACT_EVIDENCE_MODES
            ):
                raw.pop("impact_evidence_mode", None)
        return BoardSettings.model_validate(raw).model_dump(mode="json")

    @classmethod
    def from_settings(
        cls,
        settings: dict[str, Any] | BoardSettings | None,
    ) -> BoardGovernanceSettings:
        normalized = cls.normalize_settings(settings, read_tolerant=True)
        return BoardGovernanceSettings(
            allow_agent_self_answering=bool(
                normalized.get("allow_agent_self_answering", False)
            ),
            require_full_context_for_critical_actions=bool(
                normalized.get("require_full_context_for_critical_actions", True)
            ),
            qa_require_role_separation=bool(
                normalized.get("qa_require_role_separation", False)
            ),
            settings=normalized,
        )

    @classmethod
    def authorize_qa_answer(
        cls,
        settings: dict[str, Any] | BoardSettings | None,
        *,
        asked_by: str | None,
        answered_by: str,
    ) -> None:
        """Authorize a Q&A answer against the canonical self-answering policy."""
        resolved = cls.from_settings(settings)
        if asked_by == answered_by and not resolved.allow_agent_self_answering:
            raise QASelfAnsweringNotAllowedError()

    @classmethod
    def merge_settings_patch(
        cls,
        current: dict[str, Any] | BoardSettings | None,
        patch: dict[str, Any] | BoardSettings | None,
    ) -> dict[str, Any]:
        current_raw = (
            current.model_dump(mode="json")
            if isinstance(current, BoardSettings)
            else dict(current or {})
        )
        patch_raw = (
            patch.model_dump(mode="json", exclude_unset=True)
            if isinstance(patch, BoardSettings)
            else dict(patch or {})
        )
        preserve_absent = {
            key
            for key in LEGACY_ABSENT_SETTING_KEYS
            if key not in current_raw and key not in patch_raw
        }
        # A patch merges PERSISTED state (read) with AUTHORED keys (write).
        # Tolerance applies only to the persisted half: a value the author is
        # actually writing stays strict, so a typo is rejected instead of
        # silently disabling governance, while a previously tampered value
        # never blocks an unrelated settings edit.
        normalized = cls.normalize_settings(
            {**current_raw, **patch_raw},
            read_tolerant=("impact_evidence_mode" not in patch_raw),
        )
        for key in preserve_absent:
            normalized.pop(key, None)
        return normalized

    async def resolve(self, board_id: str) -> BoardGovernanceSettings:
        if self.db is None:
            raise RuntimeError("BoardGovernanceService.resolve requires a read context")
        settings = await get_domain_event_fact_reader().load_board_settings(
            self.db,
            board_id=board_id,
        )
        if settings is None:
            raise ValueError("board_not_found")
        return self.from_settings(settings)

    @classmethod
    def changed_governance_settings(
        cls,
        previous: dict[str, Any] | BoardSettings | None,
        current: dict[str, Any] | BoardSettings | None,
    ) -> list[tuple[str, bool, bool]]:
        previous_effective = cls.from_settings(previous)
        current_effective = cls.from_settings(current)
        changes: list[tuple[str, bool, bool]] = []
        for key in GOVERNANCE_SETTING_KEYS:
            old_value = bool(getattr(previous_effective, key))
            new_value = bool(getattr(current_effective, key))
            if old_value != new_value:
                changes.append((key, old_value, new_value))
        return changes


def build_qa_self_answer_denied_details(
    *,
    board_id: str,
    actor_id: str,
    entity_type: str,
    question_id: str,
    surface: str,
) -> dict[str, Any]:
    """Return the exact safe audit/metric payload for Q&A self-answer denials."""
    return _build_qa_self_answer_denied_details(
        board_id=board_id,
        actor_id=actor_id,
        entity_type=entity_type,
        question_id=question_id,
        surface=surface,
    )

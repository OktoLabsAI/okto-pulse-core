"""Service layer for business logic."""

import hashlib
import os
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import flag_modified

from okto_pulse.core.infra.config import get_settings
from okto_pulse.core.infra.storage import get_storage_provider
from okto_pulse.core.models.db import (
    ActivityLog,
    Agent,
    AgentBoard,
    Attachment,
    Board,
    BoardGuideline,
    BoardShare,
    Card,
    CardDependency,
    CardStatus,
    CardType,
    Comment,
    Guideline,
    Ideation,
    IdeationComplexity,
    IdeationHistory,
    IdeationQAItem,
    IdeationStatus,
    PermissionPreset,
    QAItem,
    Refinement,
    RefinementHistory,
    RefinementKnowledgeBase,
    RefinementQAItem,
    RefinementSnapshot,
    RefinementStatus,
    Spec,
    SpecHistory,
    SpecKnowledgeBase,
    SpecQAItem,
    SpecSkill,
    SpecStatus,
    Sprint,
    SprintHistory,
    SprintQAItem,
    SprintStatus,
)
from okto_pulse.core.models.schemas import (
    AgentCreate,
    AgentUpdate,
    BoardCreate,
    BoardShareCreate,
    BoardShareUpdate,
    BoardUpdate,
    CardCreate,
    CardMove,
    CardUpdate,
    CommentCreate,
    CommentUpdate,
    GuidelineCreate,
    GuidelineUpdate,
    IdeationCreate,
    IdeationMove,
    IdeationQAAnswer,
    IdeationQACreate,
    IdeationUpdate,
    QACreate,
    QAAnswer,
    RefinementCreate,
    RefinementKnowledgeCreate,
    RefinementMove,
    RefinementQAAnswer,
    RefinementQACreate,
    RefinementUpdate,
    SpecCreate,
    SpecKnowledgeCreate,
    SpecKnowledgeUpdate,
    SpecMove,
    SpecQAAnswer,
    SpecQACreate,
    SpecSkillCreate,
    SpecSkillUpdate,
    SpecUpdate,
    SpecValidationSubmit,
    SprintCreate,
    SprintMove,
    SprintUpdate,
)

settings = get_settings()


# ---------------------------------------------------------------------------
# Spec Validation Gate — exception and lock helper
# ---------------------------------------------------------------------------


class SpecLockedError(Exception):
    """Raised when a content-edit operation is attempted on a locked spec.

    A spec is locked when its current_validation_id points to a validation
    record with outcome='success'. To edit, the spec must be moved back to
    draft or approved (any backward transition from validated/in_progress/done),
    which atomically clears current_validation_id but preserves validations history.
    """

    def __init__(self, spec_id: str, current_validation_id: str | None = None, message: str | None = None):
        self.spec_id = spec_id
        self.current_validation_id = current_validation_id
        self.message = message or (
            "Spec is locked because validation passed. "
            "Move the spec back to draft or approved to edit (validation will be cleared, history preserved)."
        )
        super().__init__(self.message)


async def _require_spec_unlocked(db: AsyncSession, spec_id: str) -> None:
    """Raise SpecLockedError if spec has an active passed validation.

    Called at the top of every content-edit method on SpecService to enforce
    the Spec Validation Gate content lock. Skips silently when spec doesn't
    exist (caller handles that) or when no validation is active.
    """
    spec = await db.get(Spec, spec_id)
    if not spec:
        return
    current_id = getattr(spec, "current_validation_id", None)
    if not current_id:
        return
    validations = getattr(spec, "validations", None) or []
    current = next((v for v in validations if v.get("id") == current_id), None)
    if current and current.get("outcome") == "success":
        raise SpecLockedError(spec_id=spec_id, current_validation_id=current_id)


# ---------------------------------------------------------------------------
# Artifact propagation utility
# ---------------------------------------------------------------------------


def _filter_mockups(
    mockups: list[dict] | None,
    mockup_ids: list[str] | None,
) -> list[dict]:
    """Filter and copy mockups, adding origin_id for traceability."""
    if not mockups:
        return []
    source = mockups if mockup_ids is None else [m for m in mockups if m.get("id") in mockup_ids]
    copied = []
    for m in source:
        new_m = dict(m)
        new_m["origin_id"] = m.get("id")
        new_m["id"] = f"sm_{hashlib.md5(f'{m.get("id")}{id(new_m)}'.encode()).hexdigest()[:8]}"
        copied.append(new_m)
    return copied


def _compile_qa_context(qa_items: list) -> str | None:
    """Compile answered Q&A items into a context section."""
    answered = [qa for qa in (qa_items or []) if getattr(qa, "answer", None) or (isinstance(qa, dict) and qa.get("answer"))]
    if not answered:
        return None
    lines = []
    for qa in answered:
        q = getattr(qa, "question", None) or qa.get("question", "")
        a = getattr(qa, "answer", None) or qa.get("answer", "")
        lines.append(f"**Q:** {q}\n**A:** {a}")
    return "## Q&A Decisions\n" + "\n\n".join(lines)


async def propagate_artifacts(
    db: AsyncSession,
    source_mockups: list[dict] | None,
    source_qa_items: list | None,
    source_knowledge_bases: list | None,
    target_entity: Any,
    target_kb_class: type | None,
    user_id: str,
    mockup_ids: list[str] | None = None,
    kb_ids: list[str] | None = None,
) -> None:
    """Propagate mockups, KBs and Q&A from a parent entity to a target entity.

    - Mockups: copied as JSON with origin_id. Default=all, filter by mockup_ids.
    - KBs: copied as new DB rows with origin_id field. Default=all, filter by kb_ids.
    - Q&A: compiled into context (appended, not replaced).
    - Existing artifacts on target are preserved (additive, not replacement).
    """
    # Propagate mockups
    copied_mockups = _filter_mockups(source_mockups, mockup_ids)
    if copied_mockups:
        existing = target_entity.screen_mockups or []
        target_entity.screen_mockups = existing + copied_mockups

    # Propagate knowledge bases (DB rows) — accepts ORM objects or dicts
    if target_kb_class and source_knowledge_bases:
        kbs = source_knowledge_bases if kb_ids is None else [
            kb for kb in source_knowledge_bases
            if (kb.get("id") if isinstance(kb, dict) else getattr(kb, "id", None)) in kb_ids
        ]
        # Determine FK field name from target_kb_class table
        target_id_field = None
        for col in ["spec_id", "refinement_id"]:
            if hasattr(target_kb_class, col):
                target_id_field = col
                break
        if target_id_field:
            for kb in kbs:
                _get = (lambda k: kb.get(k)) if isinstance(kb, dict) else (lambda k: getattr(kb, k, None))
                new_kb = target_kb_class(
                    **{target_id_field: target_entity.id},
                    title=_get("title"),
                    description=f"[propagated from parent] {_get('description') or ''}".strip(),
                    content=_get("content"),
                    mime_type=_get("mime_type") or "text/markdown",
                    created_by=user_id,
                )
                db.add(new_kb)
            await db.flush()

    # Propagate Q&A items as proper QA rows on the target entity
    if source_qa_items:
        from okto_pulse.core.models.db import SpecQAItem, RefinementQAItem
        # Determine target QA class based on entity type
        target_qa_class = None
        target_fk_field = None
        if hasattr(target_entity, "spec_id") or target_entity.__tablename__ == "specs":
            target_qa_class = SpecQAItem
            target_fk_field = "spec_id"
        elif hasattr(target_entity, "refinement_id") or (hasattr(target_entity, "__tablename__") and target_entity.__tablename__ == "refinements"):
            target_qa_class = RefinementQAItem
            target_fk_field = "refinement_id"

        if target_qa_class and target_fk_field:
            for qa in source_qa_items:
                _get = (lambda k: qa.get(k)) if isinstance(qa, dict) else (lambda k: getattr(qa, k, None))
                # Only copy ANSWERED Q&A items. Choice questions (choice/
                # single_choice/multi_choice) store the answer in `selected`
                # and leave `answer` as None — the original `if not answer`
                # silently dropped every choice-type response, so derived
                # entities lost the decisions made on the parent. Treat the
                # item as answered when EITHER `answer` OR `selected` is set.
                answer = _get("answer")
                selected = _get("selected")
                has_selection = bool(selected) and len(selected) > 0
                if not answer and not has_selection:
                    continue
                new_qa = target_qa_class(
                    **{target_fk_field: target_entity.id},
                    question=_get("question") or "",
                    question_type=_get("question_type") or "text",
                    choices=_get("choices"),
                    allow_free_text=_get("allow_free_text") or False,
                    answer=answer,
                    selected=selected,
                    asked_by=_get("asked_by") or user_id,
                    answered_by=_get("answered_by"),
                )
                db.add(new_qa)
            await db.flush()


async def resolve_actor_name(db: AsyncSession, user_id: str, board_id: str) -> str:
    """Resolve a user/agent ID to a friendly display name."""
    agent = await db.get(Agent, user_id)
    if agent:
        return agent.name
    board = await db.get(Board, board_id)
    if board and board.owner_id == user_id:
        return "Owner"
    if user_id == "dev-user":
        return "Owner"
    return user_id[:20]


class BoardService:
    """Service for board operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_board(self, user_id: str, data: BoardCreate, realm_id: str | None = None) -> Board:
        """Create a new board."""
        board = Board(
            name=data.name,
            description=data.description,
            owner_id=user_id,
            realm_id=realm_id,
        )
        self.db.add(board)
        await self.db.flush()
        actor_name = await resolve_actor_name(self.db, user_id, board.id)
        await self._log_activity(
            board_id=board.id,
            action="board_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"name": data.name},
        )
        return board

    async def get_board(self, board_id: str, user_id: str | None = None) -> Board | None:
        """Get a board by ID with all relationships."""
        query = (
            select(Board)
            .options(selectinload(Board.cards).selectinload(Card.attachments))
            .options(selectinload(Board.cards).selectinload(Card.qa_items))
            .options(selectinload(Board.cards).selectinload(Card.comments))
            .where(Board.id == board_id)
        )
        if user_id:
            query = query.where(Board.owner_id == user_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_boards(
        self, user_id: str, offset: int = 0, limit: int = 20, realm_id: str | None = None,
        view: str = "my",
    ) -> tuple[list[Board], int]:
        """List boards for a user.

        view: "my" (owned), "shared" (shared with user), "all" (union)
        """
        from okto_pulse.core.models.db import BoardShare

        filters = []
        if realm_id:
            filters.append(Board.realm_id == realm_id)

        if view == "shared":
            # Boards shared with the user (not owned)
            base = (
                select(Board)
                .join(BoardShare, BoardShare.board_id == Board.id)
                .where(BoardShare.user_id == user_id, *filters)
            )
            count_base = (
                select(func.count())
                .select_from(Board)
                .join(BoardShare, BoardShare.board_id == Board.id)
                .where(BoardShare.user_id == user_id, *filters)
            )
        elif view == "all":
            # Owned OR shared
            owned = select(Board.id).where(Board.owner_id == user_id, *filters)
            shared = (
                select(Board.id)
                .join(BoardShare, BoardShare.board_id == Board.id)
                .where(BoardShare.user_id == user_id, *filters)
            )
            combined_ids = owned.union(shared).subquery()
            base = select(Board).where(Board.id.in_(select(combined_ids)))
            count_base = select(func.count()).select_from(Board).where(Board.id.in_(select(combined_ids)))
        else:
            # "my" - owned boards only
            base = select(Board).where(Board.owner_id == user_id, *filters)
            count_base = select(func.count()).select_from(Board).where(Board.owner_id == user_id, *filters)

        total = (await self.db.execute(count_base)).scalar() or 0
        query = base.order_by(Board.updated_at.desc()).offset(offset).limit(limit)
        result = await self.db.execute(query)
        boards = list(result.scalars().all())
        return boards, total

    async def update_board(self, board_id: str, user_id: str, data: BoardUpdate) -> Board | None:
        """Update a board."""
        board = await self.get_board(board_id, user_id)
        if not board:
            return None

        update_data = data.model_dump(exclude_unset=True)
        # Serialize settings if present
        if "settings" in update_data and update_data["settings"] is not None:
            update_data["settings"] = (
                update_data["settings"].model_dump()
                if hasattr(update_data["settings"], "model_dump")
                else update_data["settings"]
            )
        for key, value in update_data.items():
            setattr(board, key, value)
            if key == "settings":
                flag_modified(board, "settings")

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="board_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details=update_data,
        )
        return board

    async def delete_board(self, board_id: str, user_id: str) -> bool:
        """Delete a board."""
        board = await self.get_board(board_id, user_id)
        if not board:
            return False

        await self.db.delete(board)
        return True

    async def _log_activity(
        self,
        board_id: str,
        action: str,
        actor_type: str,
        actor_id: str,
        actor_name: str,
        card_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Log an activity."""
        log = ActivityLog(
            board_id=board_id,
            card_id=card_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            details=details,
        )
        self.db.add(log)


class CardService:
    """Service for card operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_card(
        self, board_id: str, user_id: str, data: CardCreate, skip_ownership_check: bool = False
    ) -> Card | None:
        """Create a new card in a board."""
        if skip_ownership_check:
            # Just verify the board exists (for MCP agents)
            board_query = select(Board).where(Board.id == board_id)
        else:
            # Check if board exists and user owns it (for REST API)
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
        result = await self.db.execute(board_query)
        if not result.scalar_one_or_none():
            return None

        # --- Bug card validations (before spec check, since spec is auto-resolved) ---
        card_type_val = getattr(data, "card_type", "normal") or "normal"
        if card_type_val == "bug":
            if not data.origin_task_id:
                raise ValueError("origin_task_id is required for bug cards")

            # Validate origin task exists
            origin_task = await self.db.get(Card, data.origin_task_id)
            if not origin_task:
                raise ValueError("Origin task not found")

            # Validate origin task has a spec
            if not origin_task.spec_id:
                raise ValueError(
                    "Origin task has no linked spec — bug cards require a spec-linked task"
                )

            # Auto-resolve spec_id from origin task
            data.spec_id = origin_task.spec_id

            # Validate required bug fields
            if not data.severity:
                raise ValueError("severity is required for bug cards (critical, major, minor)")
            if not data.expected_behavior:
                raise ValueError("expected_behavior is required for bug cards")
            if not data.observed_behavior:
                raise ValueError("observed_behavior is required for bug cards")

            # Bug cards must start as not_started — they must go through
            # the move_card workflow to reach in_progress/done (which enforces
            # test task linkage). Prevent bypassing via create with status=done.
            if data.status not in (CardStatus.NOT_STARTED, CardStatus.STARTED):
                raise ValueError(
                    "Bug cards can only be created with status 'not_started' or 'started'. "
                    "Use move_card to advance status — this enforces test task linkage requirements."
                )

        # Enforce: every card must be linked to a spec
        if not data.spec_id:
            raise ValueError(
                "Every task must be linked to a spec. Provide spec_id when creating a card. "
                "If this task is not related to any spec, create a spec first."
            )

        # --- Test card validations ---
        if card_type_val == "test":
            if not data.test_scenario_ids:
                raise ValueError(
                    "test_scenario_ids is required for test cards and must contain at least one scenario ID"
                )

        # Enforce: spec status rules for card creation
        # - Normal tasks: spec must be 'approved' or 'in_progress'
        # - Bug cards: also allowed when spec is 'done'
        # - Test cards: also allowed when spec is 'validated'
        spec = await self.db.get(Spec, data.spec_id)
        if not spec:
            raise ValueError(f"Spec '{data.spec_id}' not found")

        if card_type_val == "bug":
            allowed_statuses = {SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
            status_msg = "'approved', 'in_progress', or 'done'"
        elif card_type_val == "test":
            allowed_statuses = {SpecStatus.APPROVED, SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
            status_msg = "'approved', 'validated', 'in_progress', or 'done'"
        else:
            allowed_statuses = {SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
            status_msg = "'approved', 'in_progress', or 'done'"

        if spec.status not in allowed_statuses:
            raise ValueError(
                f"{card_type_val.capitalize()} cards can only be created for specs in {status_msg} status. "
                f"Spec '{spec.title}' is currently '{spec.status.value}'."
            )

        # Validate test_scenario_ids against spec for test cards
        if card_type_val == "test" and data.test_scenario_ids:
            spec_scenario_ids = {s["id"] for s in (spec.test_scenarios or [])}
            invalid_ids = [sid for sid in data.test_scenario_ids if sid not in spec_scenario_ids]
            if invalid_ids:
                raise ValueError(
                    f"Test scenario(s) not found in spec '{spec.title}': {invalid_ids}. "
                    f"Available scenarios: {sorted(spec_scenario_ids)}"
                )

        # Get max position for the status column
        pos_query = (
            select(func.max(Card.position))
            .where(Card.board_id == board_id, Card.status == data.status)
        )
        max_pos = (await self.db.execute(pos_query)).scalar() or -1

        card = Card(
            board_id=board_id,
            spec_id=data.spec_id,
            title=data.title,
            description=data.description,
            details=data.details,
            status=data.status,
            priority=data.priority,
            position=max_pos + 1,
            assignee_id=data.assignee_id,
            created_by=user_id,
            due_date=data.due_date,
            labels=data.labels,
            test_scenario_ids=data.test_scenario_ids,
            card_type=card_type_val,
            origin_task_id=getattr(data, "origin_task_id", None),
            severity=getattr(data, "severity", None),
            expected_behavior=getattr(data, "expected_behavior", None),
            observed_behavior=getattr(data, "observed_behavior", None),
            steps_to_reproduce=getattr(data, "steps_to_reproduce", None),
            action_plan=getattr(data, "action_plan", None),
        )
        self.db.add(card)
        await self.db.flush()

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import CardCreated

        await event_publish(
            CardCreated(
                board_id=board_id,
                actor_id=user_id,
                card_id=card.id,
                spec_id=card.spec_id,
                sprint_id=card.sprint_id,
                card_type=card_type_val,
                priority=data.priority.value,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            card_id=card.id,
            action="card_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "status": data.status.value},
        )
        return card

    async def get_card(self, card_id: str) -> Card | None:
        """Get a card by ID with all relationships."""
        query = (
            select(Card)
            .options(selectinload(Card.attachments))
            .options(selectinload(Card.qa_items))
            .options(selectinload(Card.comments))
            .where(Card.id == card_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def update_card(self, card_id: str, user_id: str, data: CardUpdate) -> Card | None:
        """Update a card."""
        card = await self.get_card(card_id)
        if not card:
            return None

        if getattr(card, "archived", False):
            raise ValueError(
                "This card is archived. Restore it first using restore_tree before making changes."
            )

        update_data = data.model_dump(exclude_unset=True)

        # Serialize screen_mockups if present
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]

        card_json_fields = {"labels", "test_scenario_ids", "conclusions", "screen_mockups"}
        for key, value in update_data.items():
            setattr(card, key, value)
            if key in card_json_fields:
                flag_modified(card, key)

        actor_name = await resolve_actor_name(self.db, user_id, card.board_id)
        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="card_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details=update_data,
        )
        return card

    # ---- Dependency methods ----

    async def add_dependency(
        self, card_id: str, depends_on_id: str
    ) -> CardDependency | None:
        """Add a dependency. Returns None if circular."""
        if card_id == depends_on_id:
            return None
        # Check circular
        if await self._would_create_cycle(card_id, depends_on_id):
            return None
        dep = CardDependency(card_id=card_id, depends_on_id=depends_on_id)
        self.db.add(dep)
        await self.db.flush()
        return dep

    async def remove_dependency(self, card_id: str, depends_on_id: str) -> bool:
        stmt = delete(CardDependency).where(
            CardDependency.card_id == card_id,
            CardDependency.depends_on_id == depends_on_id,
        )
        result = await self.db.execute(stmt)
        return result.rowcount > 0

    async def get_dependencies(self, card_id: str) -> list[Card]:
        """Get cards that this card depends on."""
        query = (
            select(Card)
            .join(CardDependency, CardDependency.depends_on_id == Card.id)
            .where(CardDependency.card_id == card_id)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_dependents(self, card_id: str) -> list[Card]:
        """Get cards that depend on this card."""
        query = (
            select(Card)
            .join(CardDependency, CardDependency.card_id == Card.id)
            .where(CardDependency.depends_on_id == card_id)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def check_dependencies_met(self, card_id: str) -> tuple[bool, list[str]]:
        """Check if all dependencies are met (done or cancelled).
        Returns (all_met, list_of_blocking_card_titles).
        """
        deps = await self.get_dependencies(card_id)
        blocking = [
            d.title for d in deps
            if d.status not in (CardStatus.DONE, CardStatus.CANCELLED)
        ]
        return len(blocking) == 0, blocking

    async def _would_create_cycle(self, card_id: str, new_dep_id: str) -> bool:
        """Check if adding card_id -> new_dep_id would create a cycle.
        A cycle exists if new_dep_id (directly or transitively) depends on card_id.
        """
        visited: set[str] = set()
        stack = [new_dep_id]
        while stack:
            current = stack.pop()
            if current == card_id:
                return True
            if current in visited:
                continue
            visited.add(current)
            # Get what 'current' depends on
            query = select(CardDependency.depends_on_id).where(
                CardDependency.card_id == current
            )
            result = await self.db.execute(query)
            for (dep_id,) in result.all():
                stack.append(dep_id)
        return False

    # ---- Status progression order ----
    _STATUS_ORDER = {
        CardStatus.NOT_STARTED: 0,
        CardStatus.STARTED: 1,
        CardStatus.IN_PROGRESS: 2,
        CardStatus.VALIDATION: 2,  # same level as in_progress — lateral move into gate
        CardStatus.ON_HOLD: 2,  # same level — lateral move
        CardStatus.DONE: 3,
        CardStatus.CANCELLED: 3,
    }

    # ---- Task Validation Gate ----

    def _resolve_validation_config(
        self, card: Card, spec: "Spec | None", sprint: "Sprint | None", board_settings: dict
    ) -> dict:
        """Resolve validation gate config from hierarchy: sprint → spec → board.

        Returns dict with: required (bool), min_confidence, min_completeness, max_drift, resolved_from.
        """
        # Defaults from board settings
        board_required = board_settings.get("require_task_validation", False)
        board_min_conf = board_settings.get("min_confidence", 70)
        board_min_comp = board_settings.get("min_completeness", 80)
        board_max_drift = board_settings.get("max_drift", 50)

        # Spec overrides
        spec_required = getattr(spec, "require_task_validation", None) if spec else None
        spec_min_conf = getattr(spec, "validation_min_confidence", None) if spec else None
        spec_min_comp = getattr(spec, "validation_min_completeness", None) if spec else None
        spec_max_drift = getattr(spec, "validation_max_drift", None) if spec else None

        # Sprint overrides
        spr_required = getattr(sprint, "require_task_validation", None) if sprint else None
        spr_min_conf = getattr(sprint, "validation_min_confidence", None) if sprint else None
        spr_min_comp = getattr(sprint, "validation_min_completeness", None) if sprint else None
        spr_max_drift = getattr(sprint, "validation_max_drift", None) if sprint else None

        # Resolve with null-coalescing: sprint ?? spec ?? board
        def _coalesce(*vals, default):
            for v in vals:
                if v is not None:
                    return v
            return default

        required = _coalesce(spr_required, spec_required, board_required, default=False)
        resolved_from = "board"
        if spr_required is not None:
            resolved_from = "sprint"
        elif spec_required is not None:
            resolved_from = "spec"

        return {
            "required": bool(required),
            "min_confidence": _coalesce(spr_min_conf, spec_min_conf, board_min_conf, default=70),
            "min_completeness": _coalesce(spr_min_comp, spec_min_comp, board_min_comp, default=80),
            "max_drift": _coalesce(spr_max_drift, spec_max_drift, board_max_drift, default=50),
            "resolved_from": resolved_from,
        }

    async def submit_task_validation(
        self,
        card_id: str,
        reviewer_id: str,
        reviewer_name: str,
        data: dict,
    ) -> dict:
        """Submit a task validation for a card in 'validation' status.

        Executes threshold check, computes outcome, persists validation,
        and routes card (success→done, failed→not_started).
        """
        import uuid as _uuid

        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")

        if card.status != CardStatus.VALIDATION:
            raise ValueError(
                f"Card is not in 'validation' status (currently '{card.status.value}'). "
                f"Only cards in 'validation' status can receive validations."
            )

        if getattr(card, "card_type", CardType.NORMAL) == CardType.TEST:
            raise ValueError("Card type 'test' is not subject to validation gate.")

        # Resolve thresholds from hierarchy
        board = await self.db.get(Board, card.board_id)
        board_settings = board.settings or {} if board else {}
        spec = await self.db.get(Spec, card.spec_id) if card.spec_id else None
        sprint = await self.db.get(Sprint, card.sprint_id) if card.sprint_id else None
        config = self._resolve_validation_config(card, spec, sprint, board_settings)

        # Extract scores
        confidence = data["confidence"]
        completeness = data["estimated_completeness"]
        drift = data["estimated_drift"]
        recommendation = data["recommendation"]

        # Threshold check
        violations = []
        if confidence < config["min_confidence"]:
            violations.append(f"confidence {confidence} < min {config['min_confidence']}")
        if completeness < config["min_completeness"]:
            violations.append(f"completeness {completeness} < min {config['min_completeness']}")
        if drift > config["max_drift"]:
            violations.append(f"drift {drift} > max {config['max_drift']}")

        # Compute outcome
        if violations or recommendation == "reject":
            outcome = "failed"
        else:
            outcome = "success"

        # Build validation entry.
        # Dual naming: we persist BOTH the legacy names (estimated_*, outcome, reviewer_id,
        # general_justification) and the clean frontend-compatible names (completeness, drift,
        # verdict, evaluator_id, summary). This keeps backward compat for any downstream code
        # that reads the legacy names while allowing the IDE ValidationsTab (which reads the
        # clean names) to render correctly. Going forward, consumers should prefer the clean
        # names; the legacy aliases can be removed in a future cleanup.
        validation_id = f"val_{_uuid.uuid4().hex[:8]}"
        _general = data["general_justification"].strip()
        validation = {
            "id": validation_id,
            "card_id": card_id,
            "board_id": card.board_id,
            # Reviewer — legacy name + clean alias for frontend
            "reviewer_id": reviewer_id,
            "evaluator_id": reviewer_id,
            # Confidence
            "confidence": confidence,
            "confidence_justification": data["confidence_justification"].strip(),
            # Completeness — legacy estimated_* + clean name
            "estimated_completeness": completeness,
            "completeness": completeness,
            "completeness_justification": data["completeness_justification"].strip(),
            # Drift — legacy estimated_* + clean name
            "estimated_drift": drift,
            "drift": drift,
            "drift_justification": data["drift_justification"].strip(),
            # General justification — legacy + frontend "summary" alias
            "general_justification": _general,
            "summary": _general,
            # Recommendation + outcome — legacy "outcome" + frontend "verdict" alias
            "recommendation": recommendation,
            "outcome": outcome,
            "verdict": "pass" if outcome == "success" else "fail",
            "threshold_violations": violations,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Persist validation (append-only)
        validations = list(card.validations or [])
        validations.append(validation)
        card.validations = validations
        flag_modified(card, "validations")

        # Auto-populate conclusion when outcome=success. The Conclusion tab has always
        # expected completeness, drift, justifications and a "text" describing what was
        # done. When the card auto-routes from validation→done, we derive a conclusion
        # entry from the validation scores and general_justification so the tab is not
        # left empty. Users can still add additional conclusion entries manually if they
        # want more detail.
        if outcome == "success":
            conclusions_list = list(card.conclusions or [])
            conclusions_list.append({
                "text": _general,
                "author_id": reviewer_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completeness": completeness,
                "completeness_justification": data["completeness_justification"].strip(),
                "drift": drift,
                "drift_justification": data["drift_justification"].strip(),
                "source": "task_validation",
                "validation_id": validation_id,
            })
            card.conclusions = conclusions_list
            flag_modified(card, "conclusions")

        # Route card based on outcome (atomic with validation persist)
        if outcome == "success":
            card.status = CardStatus.DONE
        else:
            card.status = CardStatus.NOT_STARTED

        # Auto-position at end of target column
        pos_query = (
            select(func.max(Card.position))
            .where(Card.board_id == card.board_id, Card.status == card.status)
        )
        max_pos = (await self.db.execute(pos_query)).scalar() or -1
        card.position = max_pos + 1

        # Activity log
        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="validation_submitted",
            actor_type="agent",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            details={
                "validation_id": validation_id,
                "outcome": outcome,
                "recommendation": recommendation,
                "confidence": confidence,
                "estimated_completeness": completeness,
                "estimated_drift": drift,
                "threshold_violations": violations,
                "card_title": card.title,
            },
        )

        return {
            **validation,
            "card_status": card.status.value,
            "resolved_thresholds": config,
        }

    async def list_task_validations(self, card_id: str) -> list[dict]:
        """List all validations for a card in reverse chronological order."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        validations = list(card.validations or [])
        validations.reverse()
        return validations

    async def get_task_validation(self, card_id: str, validation_id: str) -> dict | None:
        """Get a single validation by ID."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        for v in (card.validations or []):
            if v.get("id") == validation_id:
                return v
        return None

    async def delete_task_validation(self, card_id: str, validation_id: str, user_id: str) -> bool:
        """Delete a validation entry. Requires card.validation.delete permission."""
        card = await self.get_card(card_id)
        if not card:
            raise ValueError("Card not found")
        validations = list(card.validations or [])
        new_validations = [v for v in validations if v.get("id") != validation_id]
        if len(new_validations) == len(validations):
            return False
        card.validations = new_validations
        flag_modified(card, "validations")
        return True

    # ---- Coverage gate functions (used by SpecService.move_spec) ----

    async def check_test_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every test scenario has at least one linked card of type TEST."""
        skip_global = (board.settings or {}).get("skip_test_coverage_global", False) if board else False
        if spec.skip_test_coverage or skip_global:
            return
        scenarios = list(spec.test_scenarios or [])
        if not scenarios:
            return
        # Collect all card IDs from linked_task_ids across scenarios
        all_card_ids: set[str] = set()
        for s in scenarios:
            for cid in (s.get("linked_task_ids") or []):
                all_card_ids.add(cid)
        # Batch query to get card_type for all linked cards
        test_card_ids: set[str] = set()
        if all_card_ids:
            result = await self.db.execute(
                select(Card.id, Card.card_type).where(Card.id.in_(all_card_ids))
            )
            for cid, ctype in result.all():
                if ctype == CardType.TEST:
                    test_card_ids.add(cid)
        # Check each scenario has at least one TEST card
        unlinked = []
        for s in scenarios:
            task_ids = s.get("linked_task_ids") or []
            has_test = any(tid in test_card_ids for tid in task_ids)
            if not has_test:
                unlinked.append(s)
        if unlinked:
            titles = ", ".join(f'"{s["title"]}"' for s in unlinked[:3])
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} test scenario(s) "
                f"in spec '{spec.title}' have no linked test cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Create test cards (card_type='test') with test_scenario_ids "
                f"for each uncovered scenario. Only cards of type 'test' count for coverage. "
                f"Alternatively, enable 'skip test coverage' on the spec or board."
            )

    async def check_rules_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every FR has a BR and every BR has a linked task."""
        skip_global = (board.settings or {}).get("skip_rules_coverage_global", False) if board else False
        if getattr(spec, "skip_rules_coverage", False) or skip_global:
            return
        frs = list(spec.functional_requirements or [])
        brs = list(spec.business_rules or [])
        if not frs:
            return
        # Check FR → BR coverage
        covered_fr_indices: set[int] = set()
        for br in brs:
            if isinstance(br, dict):
                for ref in (br.get("linked_requirements") or []):
                    ref_str = str(ref)
                    try:
                        idx_num = int(ref_str)
                        if 0 <= idx_num < len(frs):
                            covered_fr_indices.add(idx_num)
                            continue
                    except (ValueError, TypeError):
                        pass
                    for fi, fr_text in enumerate(frs):
                        if ref_str in fr_text or fr_text in ref_str:
                            covered_fr_indices.add(fi)
                            break
        uncovered = [(i, fr) for i, fr in enumerate(frs) if i not in covered_fr_indices]
        if uncovered:
            previews = ", ".join(
                f'"FR{i}: {fr[:40]}..."' if len(fr) > 40 else f'"FR{i}: {fr}"'
                for i, fr in uncovered[:3]
            )
            suffix = f" and {len(uncovered) - 3} more" if len(uncovered) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(uncovered)} functional requirement(s) "
                f"in spec '{spec.title}' have no linked business rules "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Create business rules with linked_requirements "
                f"for each uncovered FR. "
                f"Alternatively, enable 'skip rules coverage' on the spec or board."
            )
        # Check BR → Task coverage
        unlinked_rules = [
            br for br in brs
            if isinstance(br, dict) and not br.get("linked_task_ids")
        ]
        if unlinked_rules:
            titles = ", ".join(
                f'"{br.get("title", br.get("id", "?"))}"'
                for br in unlinked_rules[:3]
            )
            suffix = f" and {len(unlinked_rules) - 3} more" if len(unlinked_rules) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked_rules)} business rule(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each business rule via "
                f"okto_pulse_link_task_to_rule. "
                f"Alternatively, enable 'skip rules coverage' on the spec or board."
            )

    async def check_trs_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every structured TR has a linked task."""
        skip_global = (board.settings or {}).get("skip_trs_coverage_global", False) if board else False
        if getattr(spec, "skip_trs_coverage", False) or skip_global:
            return
        trs = list(spec.technical_requirements or [])
        structured_trs = [tr for tr in trs if isinstance(tr, dict) and tr.get("id")]
        if not structured_trs:
            return
        unlinked_trs = [tr for tr in structured_trs if not tr.get("linked_task_ids")]
        if unlinked_trs:
            previews = ", ".join(
                f'"{tr.get("text", tr.get("id", "?"))[:40]}"'
                for tr in unlinked_trs[:3]
            )
            suffix = f" and {len(unlinked_trs) - 3} more" if len(unlinked_trs) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked_trs)} technical requirement(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each TR via "
                f"okto_pulse_link_task_to_tr. "
                f"Alternatively, enable 'skip TRs coverage' on the spec or board."
            )

    async def check_contract_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every API contract has a linked task."""
        skip_global = (board.settings or {}).get("skip_contract_coverage_global", False) if board else False
        if getattr(spec, "skip_contract_coverage", False) or skip_global:
            return
        contracts = list(spec.api_contracts or [])
        if not contracts:
            return
        unlinked = [c for c in contracts if not c.get("linked_task_ids")]
        if unlinked:
            previews = ", ".join(
                f'"{c.get("method", "?")} {c.get("path", "?")}"'
                for c in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} API contract(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({previews}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each API contract via "
                f"okto_pulse_link_task_to_contract. "
                f"Alternatively, enable 'skip contract coverage' on the spec or board."
            )

    async def check_decisions_coverage(self, spec: "Spec", board: "Board | None") -> None:
        """Check that every active Decision has a linked task (OPT-IN).

        Specs and boards default `skip_decisions_coverage=True`, so this is a
        no-op unless the user explicitly enables the gate. Only `active`
        decisions are checked — `superseded` and `revoked` are historical and
        don't need linkage.
        """
        skip_global = (board.settings or {}).get("skip_decisions_coverage_global", False) if board else False
        # Default True at both levels — if either says skip, skip.
        skip_spec = getattr(spec, "skip_decisions_coverage", True)
        if skip_spec or skip_global:
            return
        decisions = list(spec.decisions or [])
        active = [d for d in decisions if isinstance(d, dict) and d.get("status", "active") == "active"]
        if not active:
            return
        unlinked = [d for d in active if not d.get("linked_task_ids")]
        if unlinked:
            titles = ", ".join(
                f'"{d.get("title", d.get("id", "?"))}"'
                for d in unlinked[:3]
            )
            suffix = f" and {len(unlinked) - 3} more" if len(unlinked) > 3 else ""
            raise ValueError(
                f"Cannot validate spec: {len(unlinked)} Decision(s) "
                f"in spec '{spec.title}' have no linked task cards "
                f"({titles}{suffix}). "
                f"REQUIRED ACTION: Link task cards to each Decision via "
                f"okto_pulse_link_task_to_decision. "
                f"Alternatively, enable 'skip decisions coverage' on the spec or board."
            )

    async def move_card(
        self, card_id: str, user_id: str, data: CardMove, actor_name: str | None = None
    ) -> Card | None:
        """Move a card to a different column/position. Blocks if dependencies not met.

        Moving to 'done' requires a conclusion text. The conclusion is appended
        to the card's conclusions list (supports multiple cycles).
        """
        card = await self.get_card(card_id)
        if not card:
            return None

        if getattr(card, "archived", False):
            raise ValueError(
                "This card is archived. Restore it first using restore_tree before making changes."
            )

        old_status = card.status
        old_position = card.position

        # Load board settings for governance
        board = await self.db.get(Board, card.board_id)
        board_settings = board.settings or {} if board else {}
        skip_global = board_settings.get("skip_test_coverage_global", False)

        # Block forward moves based on card_type and spec status.
        # Uses level comparison: spec must have reached the minimum required status.
        # Once a spec reaches IN_PROGRESS or DONE, cards can advance freely.
        old_level = self._STATUS_ORDER.get(old_status, 0)
        new_level = self._STATUS_ORDER.get(data.status, 0)
        if new_level > old_level and card.spec_id:
            spec_for_status = await self.db.get(Spec, card.spec_id)
            if spec_for_status:
                from okto_pulse.core.services.main import SpecService
                spec_level = SpecService._STATUS_ORDER.get(spec_for_status.status, 0)
                card_type = getattr(card, "card_type", CardType.NORMAL)
                if card_type == CardType.TEST:
                    # Test cards can start when spec >= validated (level 3)
                    min_spec_level = SpecService._STATUS_ORDER.get(SpecStatus.VALIDATED, 3)
                else:
                    # Normal and bug cards can start when spec >= in_progress (level 4)
                    min_spec_level = SpecService._STATUS_ORDER.get(SpecStatus.IN_PROGRESS, 4)
                if spec_level < min_spec_level:
                    raise ValueError(
                        f"Cannot move card forward: spec '{spec_for_status.title}' must be at least "
                        f"'{SpecStatus.VALIDATED.value if card_type == CardType.TEST else SpecStatus.IN_PROGRESS.value}' "
                        f"(currently '{spec_for_status.status.value}'). "
                        f"Move the spec forward before starting work on its cards."
                    )

        # Sprint gate: if spec has sprints, card must have sprint_id and sprint must be active
        if new_level > old_level and card.spec_id:
            spec_for_sprint = await self.db.get(Spec, card.spec_id)
            if spec_for_sprint:
                sprint_count_q = select(func.count()).select_from(Sprint).where(
                    Sprint.spec_id == card.spec_id, Sprint.archived.is_(False),
                )
                sprint_count = (await self.db.execute(sprint_count_q)).scalar() or 0
                if sprint_count > 0:
                    if not card.sprint_id:
                        raise ValueError(
                            "This spec uses sprints. Card must be assigned to a sprint before advancing. "
                            "Use okto_pulse_update_card or assign_tasks_to_sprint to assign it."
                        )
                    sprint_obj = await self.db.get(Sprint, card.sprint_id)
                    if sprint_obj and sprint_obj.status != SprintStatus.ACTIVE:
                        raise ValueError(
                            f"Card's sprint '{sprint_obj.title}' is not active "
                            f"(status: '{sprint_obj.status.value}'). "
                            f"Only cards in active sprints can advance."
                        )

        # --- Task Validation Gate: block in_progress→done when gate active ---
        if (
            data.status == CardStatus.DONE
            and old_status in (CardStatus.IN_PROGRESS, CardStatus.STARTED, CardStatus.NOT_STARTED)
            and getattr(card, "card_type", CardType.NORMAL) != CardType.TEST
        ):
            spec_for_gate = await self.db.get(Spec, card.spec_id) if card.spec_id else None
            sprint_for_gate = await self.db.get(Sprint, card.sprint_id) if card.sprint_id else None
            gate_config = self._resolve_validation_config(
                card, spec_for_gate, sprint_for_gate, board_settings
            )
            if gate_config["required"]:
                raise ValueError(
                    "Validation gate is active. Move card to 'validation' status first. "
                    "A reviewer must submit a task validation before the card can move to 'done'. "
                    "Use move_card(status='validation') then submit_task_validation."
                )

        # Block Done on test cards if linked scenarios not updated
        if data.status == CardStatus.DONE and card.spec_id and card.test_scenario_ids:
            spec = spec if 'spec' in dir() else await self.db.get(Spec, card.spec_id)
            if spec and not skip_global:
                all_scenarios = {s["id"]: s for s in (spec.test_scenarios or [])}
                stale = []
                for sid in (card.test_scenario_ids or []):
                    sc = all_scenarios.get(sid)
                    if sc and sc.get("status") in ("draft", "ready"):
                        stale.append(sc.get("title", sid))
                if stale:
                    titles = ", ".join(f'"{t}"' for t in stale[:3])
                    suffix = f" and {len(stale) - 3} more" if len(stale) > 3 else ""
                    raise ValueError(
                        f"Cannot complete this test card: {len(stale)} linked scenario(s) "
                        f"still have status 'draft' or 'ready' ({titles}{suffix}). "
                        f"Update scenario statuses to 'automated' or 'passed' using "
                        f"okto_pulse_update_test_scenario_status before completing the card."
                    )

        # --- Bug card: block in_progress/done without properly linked test tasks ---
        # Gate triggers when moving TO in_progress or done FROM a status before in_progress
        # (i.e. not_started or started). Once in_progress is reached, the gate was already passed.
        if (
            data.status in (CardStatus.IN_PROGRESS, CardStatus.DONE)
            and old_level < self._STATUS_ORDER.get(CardStatus.IN_PROGRESS, 2)
            and getattr(card, "card_type", CardType.NORMAL) == CardType.BUG
        ):
            linked_tests = card.linked_test_task_ids or []
            if not linked_tests:
                raise ValueError(
                    "Bug card requires at least 1 new test task linked before moving to in_progress. "
                    "REQUIRED STEPS: "
                    "(1) Create a new test scenario on the spec using okto_pulse_add_test_scenario, "
                    "(2) Create a test task card with spec_id and test_scenario_ids using okto_pulse_create_card, "
                    "(3) Link the test task to this bug using okto_pulse_update_card with linked_test_task_ids, "
                    "(4) Then retry moving this bug card to in_progress."
                )

            # Validate each linked test task
            bug_created = card.created_at
            spec_for_bug = await self.db.get(Spec, card.spec_id) if card.spec_id else None
            all_scenarios = {s["id"]: s for s in (spec_for_bug.test_scenarios or [])} if spec_for_bug else {}

            for test_task_id in linked_tests:
                test_task = await self.db.get(Card, test_task_id)
                if not test_task:
                    raise ValueError(
                        f"Linked test task '{test_task_id}' not found. "
                        f"Remove it from linked_test_task_ids using okto_pulse_update_card "
                        f"and link a valid test task instead."
                    )

                # Validate test task is of type TEST
                if getattr(test_task, "card_type", "normal") != CardType.TEST:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' is not a test card "
                        f"(type: {getattr(test_task, 'card_type', 'normal')}). "
                        f"Bug cards require linked test cards of type 'test'."
                    )

                # Validate test task has test_scenario_ids
                if not test_task.test_scenario_ids:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' has no test_scenario_ids. "
                        f"A test task must be linked to at least one test scenario. "
                        f"Use okto_pulse_link_task_to_scenario to link the test task to a scenario, "
                        f"or create a new test task with test_scenario_ids set."
                    )

                # Validate test task belongs to the same spec
                if test_task.spec_id != card.spec_id:
                    raise ValueError(
                        f"Linked test task '{test_task.title}' belongs to spec '{test_task.spec_id}' "
                        f"but this bug belongs to spec '{card.spec_id}'. "
                        f"Test tasks must belong to the same spec as the bug card."
                    )

                # Validate scenarios exist in spec and were created AFTER the bug
                for sid in test_task.test_scenario_ids:
                    sc = all_scenarios.get(sid)
                    if not sc:
                        raise ValueError(
                            f"Test scenario '{sid}' referenced by test task '{test_task.title}' "
                            f"does not exist in spec '{spec_for_bug.title if spec_for_bug else card.spec_id}'. "
                            f"The scenario may have been deleted. Create a new test scenario "
                            f"using okto_pulse_add_test_scenario and update the test task."
                        )
                    sc_created = sc.get("created_at", "")
                    if sc_created and bug_created and sc_created < bug_created.isoformat():
                        raise ValueError(
                            f"Test scenario '{sc.get('title', sid)}' was created before this bug card. "
                            f"Only NEW test scenarios (created after the bug) count for unblocking. "
                            f"Create a new test scenario using okto_pulse_add_test_scenario "
                            f"that specifically covers the bug's observed behavior."
                        )

        # Require conclusion when moving to Done
        if data.status == CardStatus.DONE:
            if not data.conclusion or not data.conclusion.strip():
                raise ValueError(
                    "A conclusion is required when moving a card to Done. "
                    "The conclusion must be a detailed summary including: "
                    "(1) what was done — specific changes and files modified, "
                    "(2) technical decisions and reasoning, "
                    "(3) what was tested and results, "
                    "(4) any side effects or follow-ups. "
                    "Provide the conclusion in the 'conclusion' parameter."
                )
            # Validate completeness (0-100)
            if data.completeness is None:
                raise ValueError(
                    "completeness (0-100) is required when moving a card to Done. "
                    "It indicates how much of the planned work was actually implemented. "
                    "100 = fully complete, 0 = nothing delivered."
                )
            if not (0 <= data.completeness <= 100):
                raise ValueError("completeness must be between 0 and 100.")
            if not data.completeness_justification or not data.completeness_justification.strip():
                raise ValueError(
                    "completeness_justification is required when moving a card to Done. "
                    "Explain why the completeness score is what it is."
                )
            # Validate drift (0-100)
            if data.drift is None:
                raise ValueError(
                    "drift (0-100) is required when moving a card to Done. "
                    "It indicates how much the implementation deviated from the original plan. "
                    "0 = no deviation, 100 = completely different from plan."
                )
            if not (0 <= data.drift <= 100):
                raise ValueError("drift must be between 0 and 100.")
            if not data.drift_justification or not data.drift_justification.strip():
                raise ValueError(
                    "drift_justification is required when moving a card to Done. "
                    "Explain what caused the deviation from the original plan."
                )
            # Append conclusion
            conclusions = list(card.conclusions or [])
            conclusions.append({
                "text": data.conclusion.strip(),
                "author_id": user_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completeness": data.completeness,
                "completeness_justification": data.completeness_justification.strip(),
                "drift": data.drift,
                "drift_justification": data.drift_justification.strip(),
            })
            card.conclusions = conclusions
            flag_modified(card, "conclusions")

        # Block forward moves if dependencies not met
        if new_level > old_level:
            deps_met, blocking = await self.check_dependencies_met(card_id)
            if not deps_met:
                raise ValueError(
                    f"Dependências não concluídas: {', '.join(blocking)}"
                )

        card.status = data.status
        if data.position is not None:
            card.position = data.position
        else:
            # Move to end of new column
            pos_query = (
                select(func.max(Card.position))
                .where(Card.board_id == card.board_id, Card.status == data.status)
            )
            max_pos = (await self.db.execute(pos_query)).scalar() or -1
            card.position = max_pos + 1

        # Auto-rollback: if card cancelled and spec is validated → revert to approved
        if data.status == CardStatus.CANCELLED and card.spec_id:
            spec_for_rollback = await self.db.get(Spec, card.spec_id)
            if spec_for_rollback and spec_for_rollback.status == SpecStatus.VALIDATED:
                spec_for_rollback.status = SpecStatus.APPROVED
                if spec_for_rollback.evaluations:
                    for ev in spec_for_rollback.evaluations:
                        ev["stale"] = True
                    flag_modified(spec_for_rollback, "evaluations")
                rollback_name = actor_name or await resolve_actor_name(self.db, user_id, card.board_id)
                spec_service = SpecService(self.db)
                await spec_service._record_history(
                    spec_id=card.spec_id, action="status_changed",
                    actor_id=user_id, actor_name=rollback_name,
                    changes=[{"field": "status", "old": "validated", "new": "approved"}],
                    summary=f"Auto-rollback: card '{card.title}' cancelled — spec reverted for revalidation",
                    version=spec_for_rollback.version,
                )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, card.board_id)

        # Emit CardMoved + optional CardCancelled / CardRestored so downstream
        # handlers (e.g. KG decay on cancel) can react.
        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                CardCancelled,
                CardMoved,
                CardRestored,
            )

            await event_publish(
                CardMoved(
                    board_id=card.board_id,
                    actor_id=user_id,
                    card_id=card.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )
            if data.status == CardStatus.CANCELLED:
                await event_publish(
                    CardCancelled(
                        board_id=card.board_id,
                        actor_id=user_id,
                        card_id=card.id,
                        previous_status=old_status.value,
                    ),
                    session=self.db,
                )
            elif old_status == CardStatus.CANCELLED:
                await event_publish(
                    CardRestored(
                        board_id=card.board_id,
                        actor_id=user_id,
                        card_id=card.id,
                        to_status=data.status.value,
                    ),
                    session=self.db,
                )

        await self._log_activity(
            board_id=card.board_id,
            card_id=card_id,
            action="card_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "from_status": old_status.value,
                "to_status": data.status.value,
                "from_position": old_position,
                "to_position": card.position,
            },
        )
        return card

    async def delete_card(self, card_id: str, user_id: str) -> bool:
        """Delete a card."""
        card = await self.get_card(card_id)
        if not card:
            return False

        board_id = card.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(card)

        await self._log_activity(
            board_id=board_id,
            card_id=card_id,
            action="card_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
        )
        return True

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class AgentService:
    """Service for agent operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    @staticmethod
    def generate_api_key() -> str:
        """Generate a secure API key."""
        return f"dash_{secrets.token_hex(24)}"

    @staticmethod
    def hash_api_key(key: str) -> str:
        """Hash an API key for storage."""
        return hashlib.sha256(key.encode()).hexdigest()

    async def create_agent(
        self, user_id: str, data: AgentCreate
    ) -> tuple[Agent, str]:
        """Create a new global agent (no board_id).

        If preset_id is provided, agent.permission_flags is initialised from
        that preset's flags so the agent immediately reflects the preset.
        Otherwise, permission_flags defaults to a deep copy of the full
        registry (all True), giving new agents full access by default.
        """
        import copy
        from okto_pulse.core.infra.permissions import PERMISSION_REGISTRY

        api_key = self.generate_api_key()

        flags: dict | None = data.permission_flags
        preset_id = data.preset_id
        if preset_id and flags is None:
            preset = await self.db.get(PermissionPreset, preset_id)
            if preset and preset.flags:
                flags = copy.deepcopy(preset.flags)
        if flags is None:
            flags = copy.deepcopy(PERMISSION_REGISTRY)

        agent = Agent(
            name=data.name,
            description=data.description,
            objective=data.objective,
            api_key=api_key,
            api_key_hash=self.hash_api_key(api_key),
            permissions=data.permissions,
            preset_id=preset_id,
            permission_flags=flags,
            created_by=user_id,
        )
        self.db.add(agent)
        await self.db.flush()
        return agent, api_key

    async def get_agent(self, agent_id: str) -> Agent | None:
        """Get an agent by ID."""
        query = select(Agent).where(Agent.id == agent_id)
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def get_agent_by_key(self, api_key: str) -> Agent | None:
        """Get an agent by API key."""
        key_hash = self.hash_api_key(api_key)
        query = select(Agent).where(Agent.api_key_hash == key_hash, Agent.is_active == True)
        result = await self.db.execute(query)
        agent = result.scalar_one_or_none()
        if agent:
            agent.last_used_at = datetime.now(timezone.utc)
        return agent

    async def list_agents_for_user(self, user_id: str) -> list[Agent]:
        """List all agents owned by a user."""
        query = select(Agent).where(Agent.created_by == user_id).order_by(Agent.created_at)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def list_agents_for_board(self, board_id: str) -> list[Agent]:
        """List all agents that have access to a board (via junction)."""
        query = (
            select(Agent)
            .join(AgentBoard, AgentBoard.agent_id == Agent.id)
            .where(AgentBoard.board_id == board_id)
            .order_by(Agent.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def list_agents(self, board_id: str) -> list[Agent]:
        """Backward-compat alias for list_agents_for_board."""
        return await self.list_agents_for_board(board_id)

    async def agent_has_board_access(self, agent_id: str, board_id: str) -> bool:
        """Check if an agent has access to a board."""
        query = select(AgentBoard).where(
            AgentBoard.agent_id == agent_id,
            AgentBoard.board_id == board_id,
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none() is not None

    async def grant_board_access(
        self, agent_id: str, board_id: str, granted_by: str
    ) -> AgentBoard:
        """Grant an agent access to a board."""
        grant = AgentBoard(
            agent_id=agent_id,
            board_id=board_id,
            granted_by=granted_by,
        )
        self.db.add(grant)
        await self.db.flush()
        return grant

    async def revoke_board_access(self, agent_id: str, board_id: str) -> bool:
        """Revoke an agent's access to a board."""
        query = delete(AgentBoard).where(
            AgentBoard.agent_id == agent_id,
            AgentBoard.board_id == board_id,
        )
        result = await self.db.execute(query)
        return result.rowcount > 0

    async def update_board_overrides(
        self, agent_id: str, board_id: str, permission_overrides: dict | None
    ) -> AgentBoard | None:
        """Update permission overrides for an agent on a specific board."""
        query = select(AgentBoard).where(
            AgentBoard.agent_id == agent_id,
            AgentBoard.board_id == board_id,
        )
        result = await self.db.execute(query)
        ab = result.scalar_one_or_none()
        if not ab:
            return None
        ab.permission_overrides = permission_overrides
        return ab

    async def list_boards_for_agent(self, agent_id: str) -> list[Board]:
        """List all boards an agent has access to."""
        query = (
            select(Board)
            .join(AgentBoard, AgentBoard.board_id == Board.id)
            .where(AgentBoard.agent_id == agent_id)
            .order_by(Board.name)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_agent(self, agent_id: str, data: AgentUpdate) -> Agent | None:
        """Update an agent.

        Special handling:
        - If `preset_id` is set (and `permission_flags` is NOT in the same
          payload), agent.permission_flags is reset from the preset's flags.
          This makes selecting a preset in the UI behave intuitively: the
          agent's effective permissions immediately match the preset.
        - If `preset_id` is explicitly cleared (None), permission_flags is
          reset to the full registry (all True) — i.e. "Full Control".
        """
        import copy
        from sqlalchemy.orm.attributes import flag_modified
        from okto_pulse.core.infra.permissions import PERMISSION_REGISTRY

        agent = await self.get_agent(agent_id)
        if not agent:
            return None

        update_data = data.model_dump(exclude_unset=True)

        preset_id_in_payload = "preset_id" in update_data
        flags_in_payload = "permission_flags" in update_data

        for key, value in update_data.items():
            setattr(agent, key, value)

        if preset_id_in_payload and not flags_in_payload:
            new_preset_id = update_data.get("preset_id")
            if new_preset_id:
                preset = await self.db.get(PermissionPreset, new_preset_id)
                if preset and preset.flags:
                    agent.permission_flags = copy.deepcopy(preset.flags)
                    flag_modified(agent, "permission_flags")
            else:
                agent.permission_flags = copy.deepcopy(PERMISSION_REGISTRY)
                flag_modified(agent, "permission_flags")
        elif flags_in_payload:
            flag_modified(agent, "permission_flags")

        return agent

    async def regenerate_key(self, agent_id: str) -> tuple[Agent | None, str | None]:
        """Regenerate an agent's API key."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return None, None

        new_key = self.generate_api_key()
        agent.api_key = new_key
        agent.api_key_hash = self.hash_api_key(new_key)
        return agent, new_key

    async def delete_agent(self, agent_id: str) -> bool:
        """Delete an agent."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return False
        await self.db.delete(agent)
        return True


class AttachmentService:
    """Service for attachment operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def upload_attachment(
        self,
        card_id: str,
        user_id: str,
        filename: str,
        content: bytes,
        mime_type: str,
    ) -> Attachment | None:
        """Upload a file attachment."""
        # Verify card exists
        card = await self.db.get(Card, card_id)
        if not card:
            return None

        # Delegate to the registered storage provider
        storage = get_storage_provider()
        file_path = await storage.save(card.board_id, filename, content)
        unique_name = Path(file_path).name

        attachment = Attachment(
            card_id=card_id,
            filename=unique_name,
            original_filename=filename,
            mime_type=mime_type,
            size=len(content),
            path=file_path,
            uploaded_by=user_id,
        )
        self.db.add(attachment)
        await self.db.flush()
        return attachment

    async def get_attachment(self, attachment_id: str) -> Attachment | None:
        """Get an attachment by ID."""
        return await self.db.get(Attachment, attachment_id)

    async def delete_attachment(self, attachment_id: str) -> bool:
        """Delete an attachment."""
        attachment = await self.get_attachment(attachment_id)
        if not attachment:
            return False

        # Delete file via storage provider
        storage = get_storage_provider()
        await storage.delete(attachment.path)

        await self.db.delete(attachment)
        return True


class QAService:
    """Service for Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(
        self, card_id: str, user_id: str, data: QACreate
    ) -> QAItem | None:
        """Create a Q&A question."""
        card = await self.db.get(Card, card_id)
        if not card:
            return None

        qa = QAItem(
            card_id=card_id,
            question=data.question,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self, qa_id: str, user_id: str, data: QAAnswer
    ) -> QAItem | None:
        """Answer a Q&A question."""
        qa = await self.db.get(QAItem, qa_id)
        if not qa:
            return None

        qa.answer = data.answer
        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(QAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class CommentService:
    """Service for comment operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_comment(
        self, card_id: str, user_id: str, data: CommentCreate
    ) -> Comment | None:
        """Create a comment (text or choice board)."""
        card = await self.db.get(Card, card_id)
        if not card:
            return None

        comment = Comment(
            card_id=card_id,
            content=data.content,
            author_id=user_id,
            comment_type=data.comment_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            responses=[],
            allow_free_text=data.allow_free_text,
        )
        self.db.add(comment)
        await self.db.flush()
        return comment

    async def respond_to_choice(
        self, comment_id: str, responder_id: str, responder_name: str,
        selected: list[str], free_text: str | None = None,
    ) -> Comment | None:
        """Add a response to a choice board comment."""
        comment = await self.db.get(Comment, comment_id)
        if not comment or comment.comment_type == "text":
            return None

        # Validate selected options exist
        valid_ids = {c["id"] for c in (comment.choices or [])}
        for sel in selected:
            if sel not in valid_ids:
                return None

        # Single-choice: only one selection allowed
        if comment.comment_type == "choice" and len(selected) > 1:
            selected = selected[:1]

        responses = list(comment.responses or [])
        # Replace existing response from same responder
        responses = [r for r in responses if r.get("responder_id") != responder_id]
        responses.append({
            "responder_id": responder_id,
            "responder_name": responder_name,
            "selected": selected,
            "free_text": free_text,
        })
        comment.responses = responses
        await self.db.flush()
        return comment

    async def update_comment(
        self, comment_id: str, user_id: str, data: CommentUpdate
    ) -> Comment | None:
        """Update a comment."""
        comment = await self.db.get(Comment, comment_id)
        if not comment or comment.author_id != user_id:
            return None

        comment.content = data.content
        return comment

    async def delete_comment(self, comment_id: str, user_id: str) -> bool:
        """Delete a comment."""
        comment = await self.db.get(Comment, comment_id)
        if not comment or comment.author_id != user_id:
            return False
        await self.db.delete(comment)
        return True


async def _validate_spec_linked_refs(
    db: AsyncSession,
    current_spec: Any,
    update_data: dict[str, Any],
) -> None:
    """Reject orphan references in linked_* fields before they hit the DB.

    Computes the *final* state of each spec collection (incoming value when
    the field is in `update_data`, otherwise the current persisted value)
    and validates that every `linked_*` reference points to an existing
    target:

    - linked_criteria (test_scenarios → AC):
        Must be a 0-based string index "0".."N-1" OR the exact AC text.
        AC labels like "AC1" are rejected — the SpecModal coverage widget
        does not recognise them and they would silently appear uncovered.

    - linked_requirements (business_rules + api_contracts → FR):
        Same rule — index "0".."N-1" OR exact FR text. Anything else
        (including "FR1" labels) is rejected.

    - linked_rules (api_contracts → BR):
        Must match an existing business_rule.id in the same spec.

    - linked_task_ids (test_scenarios + business_rules + api_contracts +
      structured_trs → Card):
        Each id must resolve to an existing Card row in the DB.

    Raises ValueError with all offenders enumerated so the caller can fix
    them in one round-trip instead of one-by-one.
    """
    def _final(field: str, default: Any):
        if field in update_data:
            return update_data[field] if update_data[field] is not None else default
        return getattr(current_spec, field, None) or default

    final_frs: list[str] = list(_final("functional_requirements", []) or [])
    final_acs: list[str] = list(_final("acceptance_criteria", []) or [])
    final_brs: list[dict] = [
        b if isinstance(b, dict) else b.model_dump()
        for b in (_final("business_rules", []) or [])
    ]
    final_contracts: list[dict] = [
        c if isinstance(c, dict) else c.model_dump()
        for c in (_final("api_contracts", []) or [])
    ]
    final_scenarios: list[dict] = [
        s if isinstance(s, dict) else s.model_dump()
        for s in (_final("test_scenarios", []) or [])
    ]
    final_decisions: list[dict] = [
        d if isinstance(d, dict) else d.model_dump()
        for d in (_final("decisions", []) or [])
    ]
    final_trs_raw: list = list(_final("technical_requirements", []) or [])
    final_trs_structured: list[dict] = []
    for tr in final_trs_raw:
        if isinstance(tr, dict) and tr.get("id"):
            final_trs_structured.append(tr)
        elif hasattr(tr, "model_dump") and getattr(tr, "id", None):
            final_trs_structured.append(tr.model_dump())

    valid_fr_indices = {str(i) for i in range(len(final_frs))}
    valid_ac_indices = {str(i) for i in range(len(final_acs))}
    valid_fr_texts = set(final_frs)
    valid_ac_texts = set(final_acs)
    valid_br_ids = {br.get("id") for br in final_brs if br.get("id")}

    errors: list[str] = []

    _DIM_TARGET = {"requirements": "FR", "criteria": "AC"}
    def _check_index_or_text(refs: list[str], valid_indices: set, valid_texts: set, dim: str, owner_label: str):
        target = _DIM_TARGET.get(dim, dim.upper()[:2])
        for ref in refs or []:
            ref_str = str(ref)
            if ref_str in valid_indices or ref_str in valid_texts:
                continue
            max_idx = max(0, len(valid_indices) - 1)
            errors.append(
                f"{owner_label}: linked_{dim} reference '{ref_str}' is not a valid 0-based index "
                f"(0..{max_idx}) nor matches any existing {target} text."
            )

    # business_rules.linked_requirements → FR
    for br in final_brs:
        owner = f"BR '{br.get('id') or br.get('title') or '?'}'"
        _check_index_or_text(br.get("linked_requirements") or [], valid_fr_indices, valid_fr_texts, "requirements", owner)

    # api_contracts.linked_requirements → FR
    # api_contracts.linked_rules → BR.id
    for ct in final_contracts:
        owner = f"Contract '{ct.get('id') or (ct.get('method', '?') + ' ' + ct.get('path', '?'))}'"
        _check_index_or_text(ct.get("linked_requirements") or [], valid_fr_indices, valid_fr_texts, "requirements", owner)
        for ref in ct.get("linked_rules") or []:
            if str(ref) not in valid_br_ids:
                errors.append(
                    f"{owner}: linked_rules reference '{ref}' does not match any business_rule.id "
                    f"in the spec (valid: {sorted(valid_br_ids) or 'none'})."
                )

    # test_scenarios.linked_criteria → AC
    for sc in final_scenarios:
        owner = f"Scenario '{sc.get('id') or sc.get('title') or '?'}'"
        _check_index_or_text(sc.get("linked_criteria") or [], valid_ac_indices, valid_ac_texts, "criteria", owner)

    # decisions.linked_requirements → FR  +  supersedes_decision_id → Decision.id
    valid_decision_ids = {d.get("id") for d in final_decisions if d.get("id")}
    for dec in final_decisions:
        owner = f"Decision '{dec.get('id') or dec.get('title') or '?'}'"
        _check_index_or_text(
            dec.get("linked_requirements") or [],
            valid_fr_indices, valid_fr_texts, "requirements", owner,
        )
        sup = dec.get("supersedes_decision_id")
        if sup and sup not in valid_decision_ids:
            errors.append(
                f"{owner}: supersedes_decision_id '{sup}' does not match any decision.id "
                f"in the spec (valid: {sorted(valid_decision_ids) or 'none'})."
            )

    # linked_task_ids → Card.id (DB existence check). Collect all in one batch.
    all_task_ids: set[str] = set()
    task_owners: dict[str, list[str]] = {}
    for sc in final_scenarios:
        owner = f"Scenario '{sc.get('id') or sc.get('title') or '?'}'"
        for tid in sc.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for br in final_brs:
        owner = f"BR '{br.get('id') or br.get('title') or '?'}'"
        for tid in br.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for ct in final_contracts:
        owner = f"Contract '{ct.get('id') or '?'}'"
        for tid in ct.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for tr in final_trs_structured:
        owner = f"TR '{tr.get('id')}'"
        for tid in tr.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)
    for dec in final_decisions:
        owner = f"Decision '{dec.get('id') or dec.get('title') or '?'}'"
        for tid in dec.get("linked_task_ids") or []:
            all_task_ids.add(tid)
            task_owners.setdefault(tid, []).append(owner)

    if all_task_ids:
        existing_ids: set[str] = set()
        result = await db.execute(select(Card.id).where(Card.id.in_(all_task_ids)))
        for (cid,) in result.all():
            existing_ids.add(cid)
        for missing in all_task_ids - existing_ids:
            owners = ", ".join(task_owners.get(missing, []))
            errors.append(
                f"linked_task_ids reference card '{missing}' that does not exist in the database. "
                f"Referenced by: {owners}."
            )

    if errors:
        joined = "; ".join(errors[:10])
        more = f" (and {len(errors) - 10} more)" if len(errors) > 10 else ""
        raise ValueError(
            f"Cannot update spec: {len(errors)} orphan link reference(s) found. {joined}{more}. "
            f"Use 0-based string indices (\"0\", \"1\", ...) for FR/AC, the BR.id for linked_rules, "
            f"and an existing Card.id for linked_task_ids."
        )


class SpecService:
    """Service for spec operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ---- Status progression order ----
    _STATUS_ORDER = {
        SpecStatus.DRAFT: 0,
        SpecStatus.REVIEW: 1,
        SpecStatus.APPROVED: 2,
        SpecStatus.VALIDATED: 3,
        SpecStatus.IN_PROGRESS: 4,
        SpecStatus.DONE: 5,
        SpecStatus.CANCELLED: 5,
    }

    async def _record_history(
        self,
        spec_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for a spec."""
        entry = SpecHistory(
            spec_id=spec_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        self.db.add(entry)

    async def list_history(self, spec_id: str, limit: int = 50) -> list[SpecHistory]:
        """List history entries for a spec, newest first."""
        query = (
            select(SpecHistory)
            .where(SpecHistory.spec_id == spec_id)
            .order_by(SpecHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            # Normalize enum values
            if hasattr(old_val, 'value'):
                old_val = old_val.value
            if hasattr(new_val, 'value'):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_spec(
        self, board_id: str, user_id: str, data: SpecCreate, skip_ownership_check: bool = False
    ) -> Spec | None:
        """Create a new spec in a board."""
        if skip_ownership_check:
            board_query = select(Board).where(Board.id == board_id)
        else:
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
        result = await self.db.execute(board_query)
        if not result.scalar_one_or_none():
            return None

        spec = Spec(
            board_id=board_id,
            title=data.title,
            description=data.description,
            context=data.context,
            functional_requirements=data.functional_requirements,
            technical_requirements=data.technical_requirements,
            acceptance_criteria=data.acceptance_criteria,
            test_scenarios=[s.model_dump() for s in data.test_scenarios] if data.test_scenarios else None,
            status=data.status,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels,
            ideation_id=data.ideation_id,
            refinement_id=data.refinement_id,
        )
        self.db.add(spec)
        await self.db.flush()

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import SpecCreated

        spec_source: str = "manual"
        origin_id: str | None = None
        if data.refinement_id:
            spec_source = "derived_refinement"
            origin_id = data.refinement_id
        elif data.ideation_id:
            spec_source = "derived_ideation"
            origin_id = data.ideation_id

        await event_publish(
            SpecCreated(
                board_id=board_id,
                actor_id=user_id,
                spec_id=spec.id,
                source=spec_source,
                origin_id=origin_id,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="spec_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "spec_id": spec.id},
        )
        await self._record_history(
            spec_id=spec.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Spec created: {data.title}", version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": data.status.value},
                *([{"field": "functional_requirements", "old": None, "new": data.functional_requirements}] if data.functional_requirements else []),
                *([{"field": "technical_requirements", "old": None, "new": data.technical_requirements}] if data.technical_requirements else []),
                *([{"field": "acceptance_criteria", "old": None, "new": data.acceptance_criteria}] if data.acceptance_criteria else []),
            ],
        )
        return spec

    async def get_spec(self, spec_id: str) -> Spec | None:
        """Get a spec by ID with its cards, skills, and knowledge bases."""
        query = (
            select(Spec)
            .options(selectinload(Spec.cards))
            .options(selectinload(Spec.skills))
            .options(selectinload(Spec.knowledge_bases))
            .options(selectinload(Spec.qa_items))
            .where(Spec.id == spec_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_specs(self, board_id: str, status_filter: str | None = None, include_archived: bool = False) -> list[Spec]:
        """List specs for a board, optionally filtered by status."""
        query = select(Spec).where(Spec.board_id == board_id)
        if status_filter:
            query = query.where(Spec.status == SpecStatus(status_filter))
        if not include_archived:
            query = query.where(Spec.archived == False)
        query = query.order_by(Spec.updated_at.desc())
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_spec(self, spec_id: str, user_id: str, data: SpecUpdate) -> Spec | None:
        """Update a spec. Bumps version on content changes. Records field-level diffs.

        Enforces the Spec Validation Gate content lock: if the spec has an active
        validation with outcome='success', raises SpecLockedError. All content tools
        (business rules, contracts, scenarios, mockups, knowledge, skills) flow
        through this method via SpecUpdate, so applying the lock check here covers
        the whole surface in one place.

        Also enforces referential integrity for `linked_*` fields: any
        `linked_criteria`/`linked_requirements`/`linked_rules`/`linked_task_ids`
        that points to a non-existent target raises ValueError before any write.
        """
        await _require_spec_unlocked(self.db, spec_id)

        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError("This spec is archived. Restore it first before making changes.")

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {
            "functional_requirements", "technical_requirements",
            "acceptance_criteria", "context", "description",
        }
        # Note: test_scenarios changes do NOT bump version — they are tracked
        # via activity logs. Only spec requirement/criteria changes bump version.
        bumps_version = bool(content_fields & update_data.keys())

        # Capture old values for diff
        old_data = {k: getattr(spec, k) for k in update_data.keys()}

        # Serialize test_scenarios, screen_mockups, business_rules, api_contracts, decisions if present
        for json_list_field in ("test_scenarios", "screen_mockups", "business_rules", "api_contracts", "decisions"):
            if json_list_field in update_data and update_data[json_list_field] is not None:
                update_data[json_list_field] = [
                    s.model_dump() if hasattr(s, "model_dump") else s
                    for s in update_data[json_list_field]
                ]

        # Validate referential integrity of all `linked_*` fields BEFORE
        # mutating the spec. The validator computes the final state of each
        # collection (incoming value OR current state if untouched) and
        # rejects orphan references with a precise error message.
        await _validate_spec_linked_refs(self.db, spec, update_data)

        json_fields = {"test_scenarios", "screen_mockups", "business_rules", "api_contracts", "decisions", "functional_requirements", "technical_requirements", "acceptance_criteria", "labels"}
        for key, value in update_data.items():
            setattr(spec, key, value)
            if key in json_fields:
                flag_modified(spec, key)

        old_version = spec.version
        if bumps_version:
            spec.version += 1

        # Compute diffs
        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        if bumps_version:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecVersionBumped

            changed_struct_fields = sorted(content_fields & update_data.keys())
            await event_publish(
                SpecVersionBumped(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    old_version=old_version,
                    new_version=spec.version,
                    changed_fields=changed_struct_fields,
                ),
                session=self.db,
            )

        actor_name = await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"spec_id": spec_id, "version": spec.version, "fields": list(update_data.keys())},
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                spec_id=spec_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=spec.version,
                summary=f"Updated: {changed_fields}",
            )
        return spec

    # ---- Spec state machine ----
    # Direct APPROVED→DRAFT and VALIDATED→DRAFT transitions added for the Spec
    # Validation Gate: editing a validated spec requires one click/call, not three
    # hops (validated→approved→review→draft). Both transitions trigger the backward
    # clear of current_validation_id in move_spec().
    _SPEC_TRANSITIONS = {
        SpecStatus.DRAFT: [SpecStatus.REVIEW, SpecStatus.CANCELLED],
        SpecStatus.REVIEW: [SpecStatus.DRAFT, SpecStatus.APPROVED, SpecStatus.CANCELLED],
        SpecStatus.APPROVED: [SpecStatus.REVIEW, SpecStatus.VALIDATED, SpecStatus.DRAFT, SpecStatus.CANCELLED],
        SpecStatus.VALIDATED: [SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DRAFT, SpecStatus.CANCELLED],
        SpecStatus.IN_PROGRESS: [SpecStatus.VALIDATED, SpecStatus.DONE, SpecStatus.CANCELLED],
        SpecStatus.DONE: [SpecStatus.DRAFT],
        SpecStatus.CANCELLED: [SpecStatus.DRAFT],
    }

    # Statuses from which a backward move clears current_validation_id.
    # Any move from {validated, in_progress, done} to {draft, review, approved}
    # unlocks content editing but preserves spec.validations history.
    _SPEC_LOCKED_STATUSES = frozenset(
        {SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE}
    )
    _SPEC_EDITABLE_STATUSES = frozenset(
        {SpecStatus.DRAFT, SpecStatus.REVIEW, SpecStatus.APPROVED}
    )

    async def move_spec(
        self, spec_id: str, user_id: str, data: SpecMove, actor_name: str | None = None
    ) -> Spec | None:
        """Move a spec to a different status.

        Enforces a strict state machine. Coverage gates run on approved→validated.
        Qualitative validation runs on validated→in_progress.
        Moving to 'done' requires full test coverage and task completion.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError("This spec is archived. Restore it first before changing status.")

        # Enforce state machine transitions
        allowed = self._SPEC_TRANSITIONS.get(spec.status, [])
        if data.status not in allowed:
            allowed_values = [s.value for s in allowed]
            raise ValueError(
                f"Cannot move spec from '{spec.status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_values}"
            )

        # Load board for settings
        board = await self.db.get(Board, spec.board_id)

        # Enforce coverage gates when moving to validated
        if data.status == SpecStatus.VALIDATED:
            card_service = CardService(self.db)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)

            # Spec Validation Gate: when enabled, the only path to validated is via
            # submit_spec_validation (which runs the semantic gate). Direct move_spec
            # from approved→validated is blocked so users/agents cannot bypass the
            # quality check. Backward transitions from validated/in_progress/done→
            # draft/review/approved are intentionally unaffected (they preserve the
            # unlock flow).
            board_settings = (board.settings or {}) if board else {}
            if (
                spec.status == SpecStatus.APPROVED
                and board_settings.get("require_spec_validation", False)
            ):
                raise ValueError(
                    "Spec Validation Gate is enabled on this board. Direct "
                    "approved→validated is blocked — submit a spec validation "
                    "via okto_pulse_submit_spec_validation (or the IDE Validate "
                    "button) to go through the semantic quality gate."
                )

        # Re-execute coverage gates + qualitative validation when moving to in_progress
        if data.status == SpecStatus.IN_PROGRESS and spec.status == SpecStatus.VALIDATED:
            card_service = CardService(self.db)
            await card_service.check_test_coverage(spec, board)
            await card_service.check_rules_coverage(spec, board)
            await card_service.check_trs_coverage(spec, board)
            await card_service.check_contract_coverage(spec, board)

            # Qualitative validation gate
            auto_validate = (board.settings or {}).get("auto_validate", False) if board else False
            skip_qualitative = getattr(spec, "skip_qualitative_validation", False)
            if not auto_validate and not skip_qualitative:
                evaluations = [e for e in (spec.evaluations or []) if not e.get("stale")]
                approvals = [e for e in evaluations if e.get("recommendation") == "approve"]
                rejections = [e for e in evaluations if e.get("recommendation") == "reject"]
                if rejections:
                    reject_names = ", ".join(
                        e.get("evaluator_name", e.get("evaluator_id", "?")) for e in rejections
                    )
                    raise ValueError(
                        f"Cannot move spec to 'in_progress': {len(rejections)} evaluation(s) "
                        f"with 'reject' recommendation exist (by: {reject_names}). "
                        f"Remove or replace the rejecting evaluations before proceeding."
                    )
                if not approvals:
                    raise ValueError(
                        "Cannot move spec to 'in_progress': no evaluation with "
                        "'approve' recommendation found. At least one approval is required. "
                        "Submit an evaluation via okto_pulse_submit_spec_evaluation."
                    )
                threshold = (
                    getattr(spec, "validation_threshold", None)
                    or (board.settings or {}).get("validation_threshold_global", 70) if board else 70
                )
                avg_score = sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
                if avg_score < threshold:
                    raise ValueError(
                        f"Cannot move spec to 'in_progress': average approval score "
                        f"({avg_score:.0f}) is below threshold ({threshold}). "
                        f"Submit additional evaluations with higher scores or lower the threshold."
                    )

        # Enforce test coverage when moving to Done
        skip_global = (board.settings or {}).get("skip_test_coverage_global", False) if board else False
        if data.status == SpecStatus.DONE and not spec.skip_test_coverage and not skip_global:
            criteria = spec.acceptance_criteria or []
            scenarios = spec.test_scenarios or []
            if criteria:
                uncovered = []
                for i, c in enumerate(criteria):
                    covering = [s for s in scenarios if c in (s.get("linked_criteria") or [])]
                    if not covering:
                        uncovered.append(f"[{i}] {c[:80]}...")
                if uncovered:
                    raise ValueError(
                        f"Cannot move spec to 'done': {len(uncovered)} acceptance criteria lack test scenarios. "
                        f"Uncovered: {'; '.join(uncovered[:5])}"
                        f"{f' (and {len(uncovered) - 5} more)' if len(uncovered) > 5 else ''}. "
                        f"Create test scenarios for all criteria, or set skip_test_coverage flag in the spec."
                    )

        # Sprint done gate: all sprints must be closed|cancelled (min 1 closed)
        if data.status == SpecStatus.DONE:
            sprints_q = select(Sprint).where(
                Sprint.spec_id == spec_id, Sprint.archived.is_(False),
            )
            sprints_result = await self.db.execute(sprints_q)
            spec_sprints = list(sprints_result.scalars().all())
            if spec_sprints:
                pending = [
                    s for s in spec_sprints
                    if s.status not in (SprintStatus.CLOSED, SprintStatus.CANCELLED)
                ]
                has_closed = any(s.status == SprintStatus.CLOSED for s in spec_sprints)
                if pending:
                    sprint_list = "; ".join(
                        f"'{s.title}' ({s.status.value})" for s in pending[:5]
                    )
                    raise ValueError(
                        f"Cannot move spec to 'done': {len(pending)} sprint(s) are not closed or cancelled. "
                        f"Pending: {sprint_list}. Close or cancel all sprints first."
                    )
                if not has_closed:
                    raise ValueError(
                        "Cannot move spec to 'done': at least 1 sprint must be closed "
                        "(all are cancelled). Close at least one sprint."
                    )

        # Enforce all linked tasks (non-bug) must be done/cancelled before spec can be done
        if data.status == SpecStatus.DONE:
            linked_tasks_q = select(Card).where(
                Card.spec_id == spec_id,
                Card.card_type == CardType.NORMAL,
                Card.archived.is_(False),
                Card.status.notin_([CardStatus.DONE, CardStatus.CANCELLED]),
            )
            result = await self.db.execute(linked_tasks_q)
            pending_tasks = result.scalars().all()
            if pending_tasks:
                task_list = "; ".join(
                    f"'{t.title}' ({t.status.value})" for t in pending_tasks[:5]
                )
                extra = f" (and {len(pending_tasks) - 5} more)" if len(pending_tasks) > 5 else ""
                raise ValueError(
                    f"Cannot move spec to 'done': {len(pending_tasks)} linked task(s) are not yet done or cancelled. "
                    f"Pending: {task_list}{extra}. "
                    f"Complete or cancel all linked tasks before finalizing the spec."
                )

        old_status = spec.status
        spec.status = data.status

        # Spec Validation Gate: any backward transition from validated/in_progress/done
        # to an editable status (draft/review/approved) clears current_validation_id,
        # releasing the content lock. spec.validations array is preserved intact.
        if (
            old_status in self._SPEC_LOCKED_STATUSES
            and data.status in self._SPEC_EDITABLE_STATUSES
            and getattr(spec, "current_validation_id", None) is not None
        ):
            spec.current_validation_id = None

        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import SpecMoved

            await event_publish(
                SpecMoved(
                    board_id=spec.board_id,
                    actor_id=user_id,
                    spec_id=spec.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, spec.board_id)
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "spec_id": spec_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
            },
        )
        await self._record_history(
            spec_id=spec_id, action="status_changed", actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=f"Status: {old_status.value} → {data.status.value}",
            version=spec.version,
        )
        return spec

    async def delete_spec(self, spec_id: str, user_id: str) -> bool:
        """Delete a spec. Unlinks cards but doesn't delete them."""
        spec = await self.get_spec(spec_id)
        if not spec:
            return False

        # Unlink cards
        await self.db.execute(
            update(Card).where(Card.spec_id == spec_id).values(spec_id=None)
        )

        board_id = spec.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(spec)

        await self._log_activity(
            board_id=board_id,
            action="spec_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"spec_id": spec_id},
        )
        return True

    async def link_card(self, spec_id: str, card_id: str) -> bool:
        """Link an existing card to a spec. Spec must be in 'approved', 'in_progress', or 'done' status."""
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return False
        if spec.status not in (SpecStatus.APPROVED, SpecStatus.VALIDATED, SpecStatus.IN_PROGRESS, SpecStatus.DONE):
            raise ValueError(f"Cards can only be linked to a spec in 'approved', 'validated', 'in_progress', or 'done' status (current: '{spec.status.value}')")
        card = await self.db.get(Card, card_id)
        if not card or card.board_id != spec.board_id:
            return False
        card.spec_id = spec_id
        return True

    async def unlink_card(self, card_id: str) -> bool:
        """Unlink a card from its spec."""
        card = await self.db.get(Card, card_id)
        if not card or not card.spec_id:
            return False
        card.spec_id = None
        return True

    # ---- Spec Validation Gate ----

    @staticmethod
    def _resolve_spec_validation_config(board: Board | None) -> dict[str, Any]:
        """Resolve Spec Validation Gate thresholds from board settings.

        Defaults are more rigorous than the Task Validation Gate (70/80/50)
        because poor spec quality has amplified downstream cost.
        """
        settings = (board.settings if board else None) or {}
        return {
            "require_spec_validation": bool(settings.get("require_spec_validation", False)),
            "min_spec_completeness": int(settings.get("min_spec_completeness", 80)),
            "min_spec_assertiveness": int(settings.get("min_spec_assertiveness", 80)),
            "max_spec_ambiguity": int(settings.get("max_spec_ambiguity", 30)),
        }

    async def submit_spec_validation(
        self,
        spec_id: str,
        reviewer_id: str,
        reviewer_name: str,
        data: dict,
    ) -> dict:
        """Submit a Spec Validation Gate record for a spec in 'approved' status.

        Mirrors CardService.submit_task_validation: runs coverage gates as
        pre-requisite, computes outcome atomically, appends to spec.validations
        array (append-only history), sets current_validation_id, and on success
        atomically moves spec.status to validated.

        Outcome rule: failed if any threshold violated OR recommendation=reject;
        success only if ALL thresholds ok AND recommendation=approve.
        """
        import uuid as _uuid

        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")

        if spec.status != SpecStatus.APPROVED:
            raise ValueError(
                f"Spec must be in 'approved' status to receive validation "
                f"(current: '{spec.status.value}')."
            )

        board = await self.db.get(Board, spec.board_id)
        config = self._resolve_spec_validation_config(board)
        if not config["require_spec_validation"]:
            raise ValueError(
                "This board does not require spec validation. "
                "Enable 'require_spec_validation' in board settings first."
            )

        # Run coverage gates as pre-requisite — reuses existing CardService checks.
        card_service = CardService(self.db)
        await card_service.check_test_coverage(spec, board)
        await card_service.check_rules_coverage(spec, board)
        await card_service.check_trs_coverage(spec, board)
        await card_service.check_contract_coverage(spec, board)
        # Decisions coverage is OPT-IN — no-op when skip_decisions_coverage=True
        # (spec or board). See check_decisions_coverage for details.
        await card_service.check_decisions_coverage(spec, board)

        # Extract and validate inputs
        completeness = int(data["completeness"])
        assertiveness = int(data["assertiveness"])
        ambiguity = int(data["ambiguity"])
        recommendation = data["recommendation"]
        if recommendation not in ("approve", "reject"):
            raise ValueError("recommendation must be 'approve' or 'reject'")
        for name, score in (
            ("completeness", completeness),
            ("assertiveness", assertiveness),
            ("ambiguity", ambiguity),
        ):
            if not (0 <= score <= 100):
                raise ValueError(f"{name} must be between 0 and 100")

        # Threshold check (ambiguity is max_drift-style — lower is better)
        violations: list[str] = []
        if completeness < config["min_spec_completeness"]:
            violations.append(f"completeness {completeness} < min {config['min_spec_completeness']}")
        if assertiveness < config["min_spec_assertiveness"]:
            violations.append(f"assertiveness {assertiveness} < min {config['min_spec_assertiveness']}")
        if ambiguity > config["max_spec_ambiguity"]:
            violations.append(f"ambiguity {ambiguity} > max {config['max_spec_ambiguity']}")

        # Compute outcome: failed if any violation OR reject; success only if
        # all thresholds ok AND approve.
        if violations or recommendation == "reject":
            outcome = "failed"
        else:
            outcome = "success"

        # Build validation record (id <= 32 chars: "val_" + 8 hex = 12 chars)
        validation_id = f"val_{_uuid.uuid4().hex[:8]}"
        resolved_thresholds = {
            "min_spec_completeness": config["min_spec_completeness"],
            "min_spec_assertiveness": config["min_spec_assertiveness"],
            "max_spec_ambiguity": config["max_spec_ambiguity"],
        }
        validation = {
            "id": validation_id,
            "spec_id": spec_id,
            "board_id": spec.board_id,
            "reviewer_id": reviewer_id,
            "reviewer_name": reviewer_name,
            "completeness": completeness,
            "completeness_justification": data["completeness_justification"].strip(),
            "assertiveness": assertiveness,
            "assertiveness_justification": data["assertiveness_justification"].strip(),
            "ambiguity": ambiguity,
            "ambiguity_justification": data["ambiguity_justification"].strip(),
            "general_justification": data["general_justification"].strip(),
            "recommendation": recommendation,
            "outcome": outcome,
            "threshold_violations": violations,
            "resolved_thresholds": resolved_thresholds,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Append-only: never overwrite history. flag_modified is required for JSONB.
        validations = list(spec.validations or [])
        validations.append(validation)
        spec.validations = validations
        flag_modified(spec, "validations")
        spec.current_validation_id = validation_id

        # Atomic state transition on success — same transaction as the persist.
        old_status = spec.status
        if outcome == "success":
            spec.status = SpecStatus.VALIDATED

        # Activity log
        await self._log_activity(
            board_id=spec.board_id,
            action="spec_validation_submitted",
            actor_type="agent" if reviewer_name and "agent" in reviewer_name.lower() else "user",
            actor_id=reviewer_id,
            actor_name=reviewer_name,
            details={
                "spec_id": spec_id,
                "validation_id": validation_id,
                "outcome": outcome,
                "recommendation": recommendation,
                "completeness": completeness,
                "assertiveness": assertiveness,
                "ambiguity": ambiguity,
                "threshold_violations": violations,
                "from_status": old_status.value,
                "to_status": spec.status.value,
            },
        )

        return {
            **validation,
            "spec_status": spec.status.value,
            "active": True,
        }

    async def list_spec_validations(self, spec_id: str) -> dict[str, Any]:
        """List all spec validations in reverse chronological order.

        Returns a dict with current_validation_id and validations list where
        each record has an 'active' flag indicating if it's the current pointer.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            raise ValueError("Spec not found")

        validations = list(spec.validations or [])
        current_id = getattr(spec, "current_validation_id", None)

        # Reverse chronological order + mark active
        result_list = []
        for v in reversed(validations):
            result_list.append({**v, "active": v.get("id") == current_id})

        return {
            "current_validation_id": current_id,
            "validations": result_list,
        }

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class SpecQAService:
    """Service for spec Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(self, spec_id: str, user_id: str, data: SpecQACreate) -> SpecQAItem | None:
        """Create a question on a spec (text or choice)."""
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return None
        qa = SpecQAItem(
            spec_id=spec_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(self, qa_id: str, user_id: str, data: SpecQAAnswer) -> SpecQAItem | None:
        """Answer a spec Q&A question (text or choice selection).
        Mirrors IdeationQAService.answer_question — accepts `single_choice`
        as alias of `choice`, and only commits when something was persisted.
        """
        qa = await self.db.get(SpecQAItem, qa_id)
        if not qa:
            return None

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type in ("choice", "single_choice") and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def list_qa(self, spec_id: str) -> list[SpecQAItem]:
        """List all Q&A items for a spec."""
        query = (
            select(SpecQAItem)
            .where(SpecQAItem.spec_id == spec_id)
            .order_by(SpecQAItem.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(SpecQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class SpecSkillService:
    """Service for spec skill operations — follows the 3-level loading pattern."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_skill(self, spec_id: str, user_id: str, data: SpecSkillCreate) -> SpecSkill | None:
        """Create a skill on a spec."""
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return None
        skill = SpecSkill(
            spec_id=spec_id,
            skill_id=data.skill_id,
            name=data.name,
            description=data.description,
            type=data.type,
            version=data.version,
            tags=data.tags,
            sections=[s.model_dump() for s in data.sections] if data.sections else None,
            created_by=user_id,
        )
        self.db.add(skill)
        await self.db.flush()
        return skill

    async def get_skill(self, spec_id: str, skill_id: str) -> SpecSkill | None:
        """Get a skill by spec_id and skill_id slug."""
        query = select(SpecSkill).where(
            SpecSkill.spec_id == spec_id, SpecSkill.skill_id == skill_id
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_skills(self, spec_id: str) -> list[SpecSkill]:
        """List all skills for a spec (RETRIEVE level)."""
        query = (
            select(SpecSkill)
            .where(SpecSkill.spec_id == spec_id)
            .order_by(SpecSkill.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_skill(self, spec_id: str, skill_id: str, data: SpecSkillUpdate) -> SpecSkill | None:
        """Update a skill."""
        skill = await self.get_skill(spec_id, skill_id)
        if not skill:
            return None
        update_data = data.model_dump(exclude_unset=True)
        if "sections" in update_data and update_data["sections"] is not None:
            update_data["sections"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["sections"]
            ]
        for key, value in update_data.items():
            setattr(skill, key, value)
        return skill

    async def delete_skill(self, spec_id: str, skill_id: str) -> bool:
        """Delete a skill."""
        skill = await self.get_skill(spec_id, skill_id)
        if not skill:
            return False
        await self.db.delete(skill)
        return True


class SpecKnowledgeService:
    """Service for spec knowledge base operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_knowledge(self, spec_id: str, user_id: str, data: SpecKnowledgeCreate) -> SpecKnowledgeBase | None:
        """Create a knowledge base item on a spec."""
        spec = await self.db.get(Spec, spec_id)
        if not spec:
            return None
        kb = SpecKnowledgeBase(
            spec_id=spec_id,
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            created_by=user_id,
        )
        self.db.add(kb)
        await self.db.flush()
        return kb

    async def get_knowledge(self, knowledge_id: str) -> SpecKnowledgeBase | None:
        """Get a knowledge base item by ID."""
        return await self.db.get(SpecKnowledgeBase, knowledge_id)

    async def list_knowledge(self, spec_id: str) -> list[SpecKnowledgeBase]:
        """List all knowledge base items for a spec."""
        query = (
            select(SpecKnowledgeBase)
            .where(SpecKnowledgeBase.spec_id == spec_id)
            .order_by(SpecKnowledgeBase.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_knowledge(self, knowledge_id: str, data: SpecKnowledgeUpdate) -> SpecKnowledgeBase | None:
        """Update a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return None
        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(kb, key, value)
        return kb

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        await self.db.delete(kb)
        return True


class ShareService:
    """Service for board sharing operations."""

    VALID_PERMISSIONS = ("viewer", "editor", "admin")

    def __init__(self, db: AsyncSession):
        self.db = db

    async def share_board(
        self, board_id: str, owner_id: str, realm_id: str, data: BoardShareCreate
    ) -> BoardShare | None:
        """Share a board with another user. Only owner/admin can share."""
        # Check board exists and caller is owner or admin
        if not await self._can_manage_shares(board_id, owner_id):
            return None

        if data.user_id == owner_id:
            return None  # Can't share with yourself

        share = BoardShare(
            board_id=board_id,
            user_id=data.user_id,
            realm_id=realm_id,
            permission=data.permission,
            shared_by=owner_id,
        )
        self.db.add(share)
        await self.db.flush()
        return share

    async def list_shares(self, board_id: str) -> list[BoardShare]:
        """List all shares for a board."""
        query = (
            select(BoardShare)
            .where(BoardShare.board_id == board_id)
            .order_by(BoardShare.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_share(
        self, share_id: str, caller_id: str, data: BoardShareUpdate
    ) -> BoardShare | None:
        """Update a share permission. Only owner/admin can update."""
        share = await self.db.get(BoardShare, share_id)
        if not share:
            return None

        if not await self._can_manage_shares(share.board_id, caller_id):
            return None

        share.permission = data.permission
        return share

    async def revoke_share(self, share_id: str, caller_id: str) -> bool:
        """Revoke a share. Owner/admin can revoke, or user can leave."""
        share = await self.db.get(BoardShare, share_id)
        if not share:
            return False

        # Allow if caller is the shared user (leaving) or can manage shares
        if share.user_id != caller_id and not await self._can_manage_shares(share.board_id, caller_id):
            return False

        await self.db.delete(share)
        return True

    async def get_user_permission(self, board_id: str, user_id: str) -> str | None:
        """Get a user's permission level for a board. Returns None if no access."""
        # Check if owner
        board = await self.db.get(Board, board_id)
        if not board:
            return None
        if board.owner_id == user_id:
            return "owner"

        # Check shares
        query = select(BoardShare).where(
            BoardShare.board_id == board_id,
            BoardShare.user_id == user_id,
        )
        result = await self.db.execute(query)
        share = result.scalar_one_or_none()
        return share.permission if share else None

    async def _can_manage_shares(self, board_id: str, user_id: str) -> bool:
        """Check if user is owner or admin of the board."""
        board = await self.db.get(Board, board_id)
        if not board:
            return False
        if board.owner_id == user_id:
            return True

        # Check if admin via share
        query = select(BoardShare).where(
            BoardShare.board_id == board_id,
            BoardShare.user_id == user_id,
            BoardShare.permission == "admin",
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none() is not None


class IdeationService:
    """Service for ideation operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    _STATUS_ORDER = {
        IdeationStatus.DRAFT: 0,
        IdeationStatus.REVIEW: 1,
        IdeationStatus.APPROVED: 2,
        IdeationStatus.EVALUATING: 3,
        IdeationStatus.DONE: 4,
        IdeationStatus.CANCELLED: 4,
    }

    async def _record_history(
        self,
        ideation_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for an ideation."""
        entry = IdeationHistory(
            ideation_id=ideation_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        self.db.add(entry)

    async def list_history(self, ideation_id: str, limit: int = 50) -> list[IdeationHistory]:
        """List history entries for an ideation, newest first."""
        query = (
            select(IdeationHistory)
            .where(IdeationHistory.ideation_id == ideation_id)
            .order_by(IdeationHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            if hasattr(old_val, 'value'):
                old_val = old_val.value
            if hasattr(new_val, 'value'):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_ideation(
        self, board_id: str, user_id: str, data: IdeationCreate, skip_ownership_check: bool = False
    ) -> Ideation | None:
        """Create a new ideation in a board."""
        if skip_ownership_check:
            board_query = select(Board).where(Board.id == board_id)
        else:
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
        result = await self.db.execute(board_query)
        if not result.scalar_one_or_none():
            return None

        ideation = Ideation(
            board_id=board_id,
            title=data.title,
            description=data.description,
            problem_statement=data.problem_statement,
            proposed_approach=data.proposed_approach,
            scope_assessment=data.scope_assessment,
            complexity=IdeationComplexity(data.complexity) if data.complexity else None,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels,
        )
        self.db.add(ideation)
        await self.db.flush()

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="ideation_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "ideation_id": ideation.id},
        )
        await self._record_history(
            ideation_id=ideation.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Ideation created: {data.title}", version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": IdeationStatus.DRAFT.value},
                *([{"field": "problem_statement", "old": None, "new": data.problem_statement}] if data.problem_statement else []),
                *([{"field": "proposed_approach", "old": None, "new": data.proposed_approach}] if data.proposed_approach else []),
            ],
        )
        return ideation

    async def get_ideation(self, ideation_id: str) -> Ideation | None:
        """Get an ideation by ID with refinements, specs, and qa_items."""
        query = (
            select(Ideation)
            .options(selectinload(Ideation.refinements))
            .options(selectinload(Ideation.specs))
            .options(selectinload(Ideation.qa_items))
            .where(Ideation.id == ideation_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_ideations(self, board_id: str, status_filter: str | None = None, include_archived: bool = False) -> list[Ideation]:
        """List ideations for a board, optionally filtered by status."""
        query = select(Ideation).where(Ideation.board_id == board_id)
        if status_filter:
            query = query.where(Ideation.status == IdeationStatus(status_filter))
        if not include_archived:
            query = query.where(Ideation.archived == False)
        query = query.order_by(Ideation.updated_at.desc())
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_ideation(self, ideation_id: str, user_id: str, data: IdeationUpdate) -> Ideation | None:
        """Update an ideation. Bumps version on content changes. Records field-level diffs.

        Only allowed in Draft status — all other statuses are read-only.
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError("This ideation is archived. Restore it first before making changes.")

        if ideation.status != IdeationStatus.DRAFT:
            raise ValueError(
                f"Cannot edit ideation in '{ideation.status.value}' status. "
                f"Move it back to 'draft' to make changes."
            )

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {
            "description", "problem_statement", "proposed_approach",
            "scope_assessment",
        }
        bumps_version = bool(content_fields & update_data.keys())

        old_data = {k: getattr(ideation, k) for k in update_data.keys()}

        # Serialize screen_mockups if present
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]

        ideation_json_fields = {"scope_assessment", "labels", "screen_mockups"}
        for key, value in update_data.items():
            if key == "complexity" and value is not None:
                setattr(ideation, key, IdeationComplexity(value))
            else:
                setattr(ideation, key, value)
            if key in ideation_json_fields:
                flag_modified(ideation, key)

        if bumps_version:
            ideation.version += 1

        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id, "version": ideation.version, "fields": list(update_data.keys())},
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                ideation_id=ideation_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=ideation.version,
                summary=f"Updated: {changed_fields}",
            )
        return ideation

    # Allowed ideation transitions:
    # Draft → Review, Cancelled
    # Review → Draft, Approved, Cancelled
    # Approved → Review, Evaluating, Cancelled
    # Evaluating → Approved, Done, Cancelled
    # Done → Draft (new version)
    _IDEATION_TRANSITIONS: dict[IdeationStatus, list[IdeationStatus]] = {
        IdeationStatus.DRAFT: [IdeationStatus.REVIEW, IdeationStatus.CANCELLED],
        IdeationStatus.REVIEW: [IdeationStatus.DRAFT, IdeationStatus.APPROVED, IdeationStatus.CANCELLED],
        IdeationStatus.APPROVED: [IdeationStatus.REVIEW, IdeationStatus.EVALUATING, IdeationStatus.CANCELLED],
        IdeationStatus.EVALUATING: [IdeationStatus.APPROVED, IdeationStatus.DONE, IdeationStatus.CANCELLED],
        IdeationStatus.DONE: [IdeationStatus.DRAFT],
        IdeationStatus.CANCELLED: [],
    }

    async def move_ideation(
        self, ideation_id: str, user_id: str, data: IdeationMove, actor_name: str | None = None
    ) -> Ideation | None:
        """Move an ideation to a different status.

        Enforces transition rules:
        - Draft → Review → Approved → Evaluating → Done
        - Done → Draft (creates new version)
        - Any (except Done) → Cancelled
        - Evaluation can only happen in Evaluating status
        - Editing only allowed in Draft
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if getattr(ideation, "archived", False):
            raise ValueError("This ideation is archived. Restore it first before changing status.")

        old_status = ideation.status
        allowed = self._IDEATION_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = ", ".join(s.value for s in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move ideation from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, ideation.board_id)

        # Snapshot on done
        if data.status == IdeationStatus.DONE:
            await self._create_snapshot(ideation, user_id)

        # Version bump on back-to-draft from done
        if data.status == IdeationStatus.DRAFT and old_status == IdeationStatus.DONE:
            ideation.version += 1

        ideation.status = data.status

        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "ideation_id": ideation_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": ideation.version,
            },
        )
        summary = f"Status: {old_status.value} → {data.status.value}"
        if data.status == IdeationStatus.DONE:
            summary += f" (snapshot v{ideation.version} created)"
        elif data.status == IdeationStatus.DRAFT and old_status == IdeationStatus.DONE:
            summary += f" (new iteration v{ideation.version})"

        await self._record_history(
            ideation_id=ideation_id, action="status_changed", actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=summary,
            version=ideation.version,
        )
        return ideation

    async def _create_snapshot(self, ideation: "Ideation", user_id: str) -> "IdeationSnapshot":
        """Create an immutable snapshot of the ideation's current state."""
        from okto_pulse.core.models.db import IdeationSnapshot

        qa_snapshot = []
        for qa in (ideation.qa_items or []):
            qa_snapshot.append({
                "question": qa.question,
                "question_type": qa.question_type,
                "choices": qa.choices,
                "answer": qa.answer,
                "selected": qa.selected,
                "asked_by": qa.asked_by,
                "answered_by": qa.answered_by,
            })

        snapshot = IdeationSnapshot(
            ideation_id=ideation.id,
            version=ideation.version,
            title=ideation.title,
            description=ideation.description,
            problem_statement=ideation.problem_statement,
            proposed_approach=ideation.proposed_approach,
            scope_assessment=ideation.scope_assessment,
            complexity=ideation.complexity.value if ideation.complexity else None,
            labels=ideation.labels,
            qa_snapshot=qa_snapshot if qa_snapshot else None,
            created_by=user_id,
        )
        self.db.add(snapshot)
        await self.db.flush()
        return snapshot

    async def list_snapshots(self, ideation_id: str) -> list:
        """List all snapshots for an ideation."""
        from okto_pulse.core.models.db import IdeationSnapshot
        query = (
            select(IdeationSnapshot)
            .where(IdeationSnapshot.ideation_id == ideation_id)
            .order_by(IdeationSnapshot.version.desc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_snapshot(self, ideation_id: str, version: int):
        """Get a specific version snapshot."""
        from okto_pulse.core.models.db import IdeationSnapshot
        query = select(IdeationSnapshot).where(
            IdeationSnapshot.ideation_id == ideation_id,
            IdeationSnapshot.version == version,
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def delete_ideation(self, ideation_id: str, user_id: str) -> bool:
        """Delete an ideation."""
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return False

        board_id = ideation.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(ideation)

        await self._log_activity(
            board_id=board_id,
            action="ideation_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id},
        )
        return True

    async def evaluate_complexity(self, ideation_id: str, user_id: str) -> Ideation | None:
        """Evaluate and set complexity based on scope_assessment.

        Only allowed in Evaluating status.

        Rules:
        - domains >= 3 OR ambiguity >= 3 OR dependencies >= 3 -> large
        - any >= 2 -> medium
        - else -> small
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.EVALUATING:
            raise ValueError(
                f"Evaluation can only be performed in 'evaluating' status (current: '{ideation.status.value}'). "
                f"Move the ideation to 'evaluating' first."
            )

        scope = ideation.scope_assessment or {}
        domains = scope.get("domains", 1)
        ambiguity = scope.get("ambiguity", 1)
        dependencies = scope.get("dependencies", 1)

        if domains >= 3 or ambiguity >= 3 or dependencies >= 3:
            new_complexity = IdeationComplexity.LARGE
        elif domains >= 2 or ambiguity >= 2 or dependencies >= 2:
            new_complexity = IdeationComplexity.MEDIUM
        else:
            new_complexity = IdeationComplexity.SMALL

        old_complexity = ideation.complexity
        ideation.complexity = new_complexity

        actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
        await self._record_history(
            ideation_id=ideation_id, action="complexity_evaluated", actor_id=user_id, actor_name=actor_name,
            changes=[{"field": "complexity", "old": old_complexity.value if old_complexity else None, "new": new_complexity.value}],
            summary=f"Complexity evaluated: {new_complexity.value}",
            version=ideation.version,
        )
        await self._log_activity(
            board_id=ideation.board_id,
            action="ideation_complexity_evaluated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"ideation_id": ideation_id, "complexity": new_complexity.value},
        )
        return ideation

    async def derive_spec(
        self, ideation_id: str, user_id: str, skip_ownership_check: bool = False,
        mockup_ids: list[str] | None = None, kb_ids: list[str] | None = None,
    ) -> Spec | None:
        """Create a Spec draft linked to an ideation.

        Compiles context from the ideation's problem statement, proposed approach,
        scope assessment, and Q&A history. Artifacts (mockups, KBs) are automatically
        propagated. Use mockup_ids/kb_ids to select specific ones.

        Only allowed when ideation status is 'done'.
        """
        ideation = await self.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.DONE:
            raise ValueError("Spec can only be created from a 'done' ideation")

        if ideation.complexity and ideation.complexity != IdeationComplexity.SMALL:
            raise ValueError(
                f"Ideation has complexity '{ideation.complexity.value}' — "
                "create refinements first, then derive specs from refinements"
            )

        # Compile rich context from ideation data
        context_parts: list[str] = []
        if ideation.problem_statement:
            context_parts.append(f"## Problem Statement\n{ideation.problem_statement}")
        if ideation.proposed_approach:
            context_parts.append(f"## Proposed Approach\n{ideation.proposed_approach}")
        if ideation.scope_assessment:
            sa = ideation.scope_assessment
            context_parts.append(
                f"## Scope Assessment\n"
                f"- Domains: {sa.get('domains', '?')}/5\n"
                f"- Ambiguity: {sa.get('ambiguity', '?')}/5\n"
                f"- Dependencies: {sa.get('dependencies', '?')}/5\n"
                f"- Complexity: {ideation.complexity.value if ideation.complexity else 'not evaluated'}"
            )
        context = "\n\n".join(context_parts) if context_parts else ideation.description

        # Snapshot Q&A before flush (eager-loaded collections expire after flush)
        snapshot_qa = list(ideation.qa_items or [])

        spec_data = SpecCreate(
            title=ideation.title,
            description=ideation.description,
            context=context,
            ideation_id=ideation_id,
            labels=ideation.labels,
        )
        spec_service = SpecService(self.db)
        spec = await spec_service.create_spec(
            ideation.board_id, user_id, spec_data, skip_ownership_check=skip_ownership_check
        )
        if spec:
            # Propagate mockups and Q&A from ideation to spec
            await propagate_artifacts(
                db=self.db,
                source_mockups=ideation.screen_mockups,
                source_qa_items=snapshot_qa,
                source_knowledge_bases=None,
                target_entity=spec,
                target_kb_class=SpecKnowledgeBase,
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=kb_ids,
            )

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import IdeationDerivedToSpec

            await event_publish(
                IdeationDerivedToSpec(
                    board_id=ideation.board_id,
                    actor_id=user_id,
                    ideation_id=ideation_id,
                    spec_id=spec.id,
                ),
                session=self.db,
            )

            actor_name = await resolve_actor_name(self.db, user_id, ideation.board_id)
            await self._record_history(
                ideation_id=ideation_id, action="spec_draft_created", actor_id=user_id, actor_name=actor_name,
                changes=[{"field": "spec", "old": None, "new": spec.id}],
                summary=f"Spec draft created: {spec.title} (requirements to be defined)",
                version=ideation.version,
            )
        return spec

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class IdeationQAService:
    """Service for ideation Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(self, ideation_id: str, user_id: str, data: IdeationQACreate) -> IdeationQAItem | None:
        """Create a question on an ideation (text or choice)."""
        ideation = await self.db.get(Ideation, ideation_id)
        if not ideation:
            return None
        qa = IdeationQAItem(
            ideation_id=ideation_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(self, qa_id: str, user_id: str, data: IdeationQAAnswer) -> IdeationQAItem | None:
        """Answer an ideation Q&A question (text or choice selection).

        Accepts `question_type in {"choice","single_choice","multi_choice"}`
        — `single_choice` is treated as an alias of `choice`. Only commits
        `answered_at`/`answered_by` when something was actually persisted,
        otherwise returns None so the route surfaces a 404 instead of a
        false-positive 200 (which caused the "toast says saved but the
        question flips back to unanswered" UX bug).
        """
        qa = await self.db.get(IdeationQAItem, qa_id)
        if not qa:
            return None

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type in ("choice", "single_choice") and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True
        elif qa.question_type not in choice_types and data.answer == "":
            # Explicit clear of a free-text answer.
            qa.answer = None

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def list_qa(self, ideation_id: str) -> list[IdeationQAItem]:
        """List all Q&A items for an ideation."""
        query = (
            select(IdeationQAItem)
            .where(IdeationQAItem.ideation_id == ideation_id)
            .order_by(IdeationQAItem.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(IdeationQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class RefinementService:
    """Service for refinement operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    _STATUS_ORDER = {
        RefinementStatus.DRAFT: 0,
        RefinementStatus.REVIEW: 1,
        RefinementStatus.APPROVED: 2,
        RefinementStatus.DONE: 3,
        RefinementStatus.CANCELLED: 3,
    }

    async def _record_history(
        self,
        refinement_id: str,
        action: str,
        actor_id: str,
        actor_name: str,
        actor_type: str = "user",
        changes: list[dict] | None = None,
        summary: str | None = None,
        version: int | None = None,
    ) -> None:
        """Record a history entry for a refinement."""
        entry = RefinementHistory(
            refinement_id=refinement_id,
            action=action,
            actor_type=actor_type,
            actor_id=actor_id,
            actor_name=actor_name,
            changes=changes,
            summary=summary,
            version=version,
        )
        self.db.add(entry)

    async def list_history(self, refinement_id: str, limit: int = 50) -> list[RefinementHistory]:
        """List history entries for a refinement, newest first."""
        query = (
            select(RefinementHistory)
            .where(RefinementHistory.refinement_id == refinement_id)
            .order_by(RefinementHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    @staticmethod
    def _compute_diff(old_data: dict, new_data: dict, fields: list[str]) -> list[dict]:
        """Compute field-level diffs between old and new data."""
        changes = []
        for field in fields:
            old_val = old_data.get(field)
            new_val = new_data.get(field)
            if hasattr(old_val, 'value'):
                old_val = old_val.value
            if hasattr(new_val, 'value'):
                new_val = new_val.value
            if old_val != new_val:
                changes.append({"field": field, "old": old_val, "new": new_val})
        return changes

    async def create_refinement(
        self, ideation_id: str, user_id: str, data: RefinementCreate, skip_ownership_check: bool = False
    ) -> Refinement | None:
        """Create a new refinement for a done ideation.

        The ideation must be in 'done' status (snapshotted) before refinements
        can be created from it — same governance as spec derivation.

        If description is not provided, compiles context from the ideation
        (problem statement, approach, scope, Q&A) as starting point.
        """
        ideation_service = IdeationService(self.db)
        ideation = await ideation_service.get_ideation(ideation_id)
        if not ideation:
            return None

        if ideation.status != IdeationStatus.DONE:
            raise ValueError("Refinements can only be created from a 'done' ideation")

        board_id = ideation.board_id
        if not skip_ownership_check:
            board_query = select(Board).where(Board.id == board_id, Board.owner_id == user_id)
            result = await self.db.execute(board_query)
            if not result.scalar_one_or_none():
                return None

        # Compile context from ideation if description not provided
        description = data.description
        if not description:
            context_parts: list[str] = []
            if ideation.problem_statement:
                context_parts.append(f"## Problem Statement\n{ideation.problem_statement}")
            if ideation.proposed_approach:
                context_parts.append(f"## Proposed Approach\n{ideation.proposed_approach}")
            if ideation.scope_assessment:
                sa = ideation.scope_assessment
                context_parts.append(
                    f"## Scope Assessment\n"
                    f"- Domains: {sa.get('domains', '?')}/5\n"
                    f"- Ambiguity: {sa.get('ambiguity', '?')}/5\n"
                    f"- Dependencies: {sa.get('dependencies', '?')}/5\n"
                    f"- Complexity: {ideation.complexity.value if ideation.complexity else 'not evaluated'}"
                )
            qa_items = [qa for qa in (ideation.qa_items or []) if qa.answer]
            if qa_items:
                qa_lines = [f"**Q:** {qa.question}\n**A:** {qa.answer}" for qa in qa_items]
                context_parts.append(f"## Q&A Decisions\n" + "\n\n".join(qa_lines))
            description = "\n\n".join(context_parts) if context_parts else None

        # Parse optional mockup/kb filters from data (if present)
        prop_mockup_ids = getattr(data, "mockup_ids", None)
        prop_kb_ids = getattr(data, "kb_ids", None)

        refinement = Refinement(
            ideation_id=ideation_id,
            board_id=board_id,
            title=data.title,
            description=description,
            in_scope=data.in_scope,
            out_of_scope=data.out_of_scope,
            analysis=data.analysis,
            decisions=data.decisions,
            screen_mockups=data.screen_mockups,  # manual mockups only; propagation adds via propagate_artifacts
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels or ideation.labels,
        )
        self.db.add(refinement)
        await self.db.flush()

        # Propagate artifacts from ideation (mockups, KBs, Q&A)
        await propagate_artifacts(
            db=self.db,
            source_mockups=ideation.screen_mockups,
            source_qa_items=ideation.qa_items,
            source_knowledge_bases=None,  # Ideations don't have KBs
            target_entity=refinement,
            target_kb_class=RefinementKnowledgeBase,
            user_id=user_id,
            mockup_ids=prop_mockup_ids,
            kb_ids=prop_kb_ids,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id,
            action="refinement_created",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"title": data.title, "refinement_id": refinement.id, "ideation_id": ideation_id},
        )
        await self._record_history(
            refinement_id=refinement.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Refinement created: {data.title}", version=1,
            changes=[
                {"field": "title", "old": None, "new": data.title},
                {"field": "status", "old": None, "new": RefinementStatus.DRAFT.value},
                *([{"field": "in_scope", "old": None, "new": data.in_scope}] if data.in_scope else []),
                *([{"field": "out_of_scope", "old": None, "new": data.out_of_scope}] if data.out_of_scope else []),
                *([{"field": "analysis", "old": None, "new": data.analysis}] if data.analysis else []),
                *([{"field": "decisions", "old": None, "new": data.decisions}] if data.decisions else []),
            ],
        )
        return refinement

    async def get_refinement(self, refinement_id: str) -> Refinement | None:
        """Get a refinement by ID with specs, knowledge_bases, and qa_items."""
        query = (
            select(Refinement)
            .options(selectinload(Refinement.specs))
            .options(selectinload(Refinement.knowledge_bases))
            .options(selectinload(Refinement.qa_items))
            .where(Refinement.id == refinement_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_refinements(self, ideation_id: str, status_filter: str | None = None, include_archived: bool = False) -> list[Refinement]:
        """List refinements for an ideation, optionally filtered by status."""
        query = select(Refinement).where(Refinement.ideation_id == ideation_id)
        if status_filter:
            query = query.where(Refinement.status == RefinementStatus(status_filter))
        if not include_archived:
            query = query.where(Refinement.archived == False)
        query = query.order_by(Refinement.updated_at.desc())
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_refinement(self, refinement_id: str, user_id: str, data: RefinementUpdate) -> Refinement | None:
        """Update a refinement. Bumps version on content changes. Records field-level diffs.

        Only allowed in Draft status — all other statuses are read-only.
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if getattr(refinement, "archived", False):
            raise ValueError("This refinement is archived. Restore it first before making changes.")

        if refinement.status != RefinementStatus.DRAFT:
            raise ValueError(
                f"Cannot edit refinement in '{refinement.status.value}' status. "
                f"Move it back to 'draft' to make changes."
            )

        update_data = data.model_dump(exclude_unset=True)
        content_fields = {"description", "scope", "analysis", "decisions"}
        bumps_version = bool(content_fields & update_data.keys())

        old_data = {k: getattr(refinement, k) for k in update_data.keys()}

        # Serialize screen_mockups if present
        if "screen_mockups" in update_data and update_data["screen_mockups"] is not None:
            update_data["screen_mockups"] = [
                s.model_dump() if hasattr(s, "model_dump") else s
                for s in update_data["screen_mockups"]
            ]

        refinement_json_fields = {"in_scope", "out_scope", "labels", "screen_mockups"}
        for key, value in update_data.items():
            setattr(refinement, key, value)
            if key in refinement_json_fields:
                flag_modified(refinement, key)

        if bumps_version:
            refinement.version += 1

        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

        actor_name = await resolve_actor_name(self.db, user_id, refinement.board_id)
        await self._log_activity(
            board_id=refinement.board_id,
            action="refinement_updated",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"refinement_id": refinement_id, "version": refinement.version, "fields": list(update_data.keys())},
        )
        if changes:
            changed_fields = ", ".join(c["field"] for c in changes)
            await self._record_history(
                refinement_id=refinement_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=refinement.version,
                summary=f"Updated: {changed_fields}",
            )
        return refinement

    # Allowed refinement transitions:
    # Draft → Review, Cancelled
    # Review → Draft, Approved, Cancelled
    # Approved → Review, Done, Cancelled
    # Done → Draft (new version)
    _REFINEMENT_TRANSITIONS: dict[RefinementStatus, list[RefinementStatus]] = {
        RefinementStatus.DRAFT: [RefinementStatus.REVIEW, RefinementStatus.CANCELLED],
        RefinementStatus.REVIEW: [RefinementStatus.DRAFT, RefinementStatus.APPROVED, RefinementStatus.CANCELLED],
        RefinementStatus.APPROVED: [RefinementStatus.REVIEW, RefinementStatus.DONE, RefinementStatus.CANCELLED],
        RefinementStatus.DONE: [RefinementStatus.DRAFT],
        RefinementStatus.CANCELLED: [],
    }

    async def move_refinement(
        self, refinement_id: str, user_id: str, data: RefinementMove, actor_name: str | None = None
    ) -> Refinement | None:
        """Move a refinement to a different status.

        Enforces transition rules:
        - Draft → Review → Approved → Done
        - Done → Draft (creates new version)
        - Any (except Done) → Cancelled
        - Editing only allowed in Draft
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if getattr(refinement, "archived", False):
            raise ValueError("This refinement is archived. Restore it first before changing status.")

        old_status = refinement.status
        allowed = self._REFINEMENT_TRANSITIONS.get(old_status, [])
        if data.status not in allowed:
            allowed_str = ", ".join(s.value for s in allowed) if allowed else "none"
            raise ValueError(
                f"Cannot move refinement from '{old_status.value}' to '{data.status.value}'. "
                f"Allowed transitions: {allowed_str}."
            )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, refinement.board_id)

        # Snapshot on done
        if data.status == RefinementStatus.DONE:
            await self._create_snapshot(refinement, user_id)

        # Version bump on back-to-draft from done
        if data.status == RefinementStatus.DRAFT and old_status == RefinementStatus.DONE:
            refinement.version += 1

        refinement.status = data.status

        await self._log_activity(
            board_id=refinement.board_id,
            action="refinement_moved",
            actor_type="user",
            actor_id=user_id,
            actor_name=resolved_name,
            details={
                "refinement_id": refinement_id,
                "from_status": old_status.value,
                "to_status": data.status.value,
                "version": refinement.version,
            },
        )
        summary = f"Status: {old_status.value} \u2192 {data.status.value}"
        if data.status == RefinementStatus.DONE:
            summary += f" (snapshot v{refinement.version} created)"
        elif data.status == RefinementStatus.DRAFT and old_status == RefinementStatus.DONE:
            summary += f" (new iteration v{refinement.version})"

        await self._record_history(
            refinement_id=refinement_id, action="status_changed", actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=summary,
            version=refinement.version,
        )
        return refinement

    async def _create_snapshot(self, refinement: "Refinement", user_id: str) -> "RefinementSnapshot":
        """Create an immutable snapshot of the refinement's current state."""
        qa_snapshot = []
        for qa in (refinement.qa_items or []):
            qa_snapshot.append({
                "question": qa.question,
                "question_type": qa.question_type,
                "choices": qa.choices,
                "answer": qa.answer,
                "selected": qa.selected,
                "asked_by": qa.asked_by,
                "answered_by": qa.answered_by,
            })

        snapshot = RefinementSnapshot(
            refinement_id=refinement.id,
            version=refinement.version,
            title=refinement.title,
            description=refinement.description,
            in_scope=refinement.in_scope,
            out_of_scope=refinement.out_of_scope,
            analysis=refinement.analysis,
            decisions=refinement.decisions,
            labels=refinement.labels,
            qa_snapshot=qa_snapshot if qa_snapshot else None,
            created_by=user_id,
        )
        self.db.add(snapshot)
        await self.db.flush()
        return snapshot

    async def list_snapshots(self, refinement_id: str) -> list:
        """List all snapshots for a refinement."""
        query = (
            select(RefinementSnapshot)
            .where(RefinementSnapshot.refinement_id == refinement_id)
            .order_by(RefinementSnapshot.version.desc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def get_snapshot(self, refinement_id: str, version: int):
        """Get a specific version snapshot."""
        query = select(RefinementSnapshot).where(
            RefinementSnapshot.refinement_id == refinement_id,
            RefinementSnapshot.version == version,
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def delete_refinement(self, refinement_id: str, user_id: str) -> bool:
        """Delete a refinement."""
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return False

        board_id = refinement.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(refinement)

        await self._log_activity(
            board_id=board_id,
            action="refinement_deleted",
            actor_type="user",
            actor_id=user_id,
            actor_name=actor_name,
            details={"refinement_id": refinement_id},
        )
        return True

    async def derive_spec(
        self, refinement_id: str, user_id: str, skip_ownership_check: bool = False,
        mockup_ids: list[str] | None = None, kb_ids: list[str] | None = None,
    ) -> Spec | None:
        """Create a Spec draft linked to a refinement.

        Artifacts (mockups, KBs) are automatically propagated. Use mockup_ids/kb_ids
        to select specific ones. Compiles context from the refinement's scope, analysis, decisions,
        technical_requirements, acceptance_criteria) are left empty — they must be
        filled by the agent or human through deliberate analysis.

        Only allowed when refinement status is 'done'.
        """
        refinement = await self.get_refinement(refinement_id)
        if not refinement:
            return None

        if refinement.status != RefinementStatus.DONE:
            raise ValueError("Spec can only be created from a 'done' refinement")

        # Compile rich context from refinement data
        context_parts: list[str] = []
        if refinement.in_scope:
            scope_text = "\n".join(f"- {s}" for s in refinement.in_scope)
            context_parts.append(f"## In Scope\n{scope_text}")
        if refinement.out_of_scope:
            out_text = "\n".join(f"- {s}" for s in refinement.out_of_scope)
            context_parts.append(f"## Out of Scope\n{out_text}")
        if refinement.analysis:
            context_parts.append(f"## Analysis\n{refinement.analysis}")
        if refinement.decisions:
            decisions_text = "\n".join(f"- {d}" for d in refinement.decisions)
            context_parts.append(f"## Decisions\n{decisions_text}")
        context = "\n\n".join(context_parts) if context_parts else refinement.description

        # Snapshot artifact data BEFORE create_spec — flush() in create_spec
        # expires all session objects, making eagerly-loaded collections inaccessible.
        snapshot_qa = list(refinement.qa_items or [])
        snapshot_mockups = list(refinement.screen_mockups or [])
        snapshot_kbs = [
            {"title": kb.title, "description": kb.description, "content": kb.content,
             "mime_type": getattr(kb, "mime_type", "text/markdown"), "id": kb.id}
            for kb in (refinement.knowledge_bases or [])
        ]

        spec_data = SpecCreate(
            title=refinement.title,
            description=refinement.description,
            context=context,
            ideation_id=refinement.ideation_id,
            refinement_id=refinement_id,
            labels=refinement.labels,
        )
        spec_service = SpecService(self.db)
        spec = await spec_service.create_spec(
            refinement.board_id, user_id, spec_data, skip_ownership_check=skip_ownership_check
        )
        if spec:
            # Propagate artifacts using pre-flush snapshots
            await propagate_artifacts(
                db=self.db,
                source_mockups=snapshot_mockups,
                source_qa_items=snapshot_qa,
                source_knowledge_bases=snapshot_kbs,
                target_entity=spec,
                target_kb_class=SpecKnowledgeBase,
                user_id=user_id,
                mockup_ids=mockup_ids,
                kb_ids=kb_ids,
            )

            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import RefinementDerivedToSpec

            await event_publish(
                RefinementDerivedToSpec(
                    board_id=refinement.board_id,
                    actor_id=user_id,
                    refinement_id=refinement_id,
                    spec_id=spec.id,
                ),
                session=self.db,
            )

            actor_name = await resolve_actor_name(self.db, user_id, refinement.board_id)
            await self._record_history(
                refinement_id=refinement_id, action="spec_draft_created", actor_id=user_id, actor_name=actor_name,
                changes=[{"field": "spec", "old": None, "new": spec.id}],
                summary=f"Spec draft created: {spec.title} (requirements to be defined)",
                version=refinement.version,
            )
        return spec

    async def _log_activity(self, **kwargs: Any) -> None:
        """Log an activity."""
        log = ActivityLog(**kwargs)
        self.db.add(log)


class RefinementQAService:
    """Service for refinement Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(self, refinement_id: str, user_id: str, data: RefinementQACreate) -> RefinementQAItem | None:
        """Create a question on a refinement (text or choice)."""
        refinement = await self.db.get(Refinement, refinement_id)
        if not refinement:
            return None
        qa = RefinementQAItem(
            refinement_id=refinement_id,
            question=data.question,
            question_type=data.question_type or "text",
            choices=[c.model_dump() for c in data.choices] if data.choices else None,
            allow_free_text=data.allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(self, qa_id: str, user_id: str, data: RefinementQAAnswer) -> RefinementQAItem | None:
        """Answer a refinement Q&A question (text or choice selection).
        Mirrors IdeationQAService.answer_question — accepts `single_choice`
        as alias of `choice`, and only commits when something was persisted.
        """
        qa = await self.db.get(RefinementQAItem, qa_id)
        if not qa:
            return None

        saved_something = False
        choice_types = ("choice", "single_choice", "multi_choice")
        if qa.question_type in choice_types and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type in ("choice", "single_choice") and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected
            saved_something = True

        if data.answer:
            qa.answer = data.answer
            saved_something = True

        if not saved_something:
            return None

        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        return qa

    async def list_qa(self, refinement_id: str) -> list[RefinementQAItem]:
        """List all Q&A items for a refinement."""
        query = (
            select(RefinementQAItem)
            .where(RefinementQAItem.refinement_id == refinement_id)
            .order_by(RefinementQAItem.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(RefinementQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True


class RefinementKnowledgeService:
    """Service for refinement knowledge base operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_knowledge(self, refinement_id: str, user_id: str, data: RefinementKnowledgeCreate) -> RefinementKnowledgeBase | None:
        """Create a knowledge base item on a refinement."""
        refinement = await self.db.get(Refinement, refinement_id)
        if not refinement:
            return None
        kb = RefinementKnowledgeBase(
            refinement_id=refinement_id,
            title=data.title,
            description=data.description,
            content=data.content,
            mime_type=data.mime_type,
            created_by=user_id,
        )
        self.db.add(kb)
        await self.db.flush()
        return kb

    async def get_knowledge(self, knowledge_id: str) -> RefinementKnowledgeBase | None:
        """Get a knowledge base item by ID."""
        return await self.db.get(RefinementKnowledgeBase, knowledge_id)

    async def list_knowledge(self, refinement_id: str) -> list[RefinementKnowledgeBase]:
        """List all knowledge base items for a refinement."""
        query = (
            select(RefinementKnowledgeBase)
            .where(RefinementKnowledgeBase.refinement_id == refinement_id)
            .order_by(RefinementKnowledgeBase.created_at)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_knowledge(self, knowledge_id: str) -> bool:
        """Delete a knowledge base item."""
        kb = await self.get_knowledge(knowledge_id)
        if not kb:
            return False
        await self.db.delete(kb)
        return True


class GuidelineService:
    """Service for guideline operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_guideline(self, owner_id: str, data: GuidelineCreate) -> Guideline:
        """Create a new guideline."""
        guideline = Guideline(
            title=data.title,
            content=data.content,
            tags=data.tags,
            scope=data.scope,
            board_id=data.board_id,
            owner_id=owner_id,
        )
        self.db.add(guideline)
        await self.db.flush()
        return guideline

    async def get_guideline(self, guideline_id: str) -> Guideline | None:
        """Get a guideline by ID."""
        return await self.db.get(Guideline, guideline_id)

    async def list_guidelines(
        self, owner_id: str, offset: int = 0, limit: int = 50, tag: str | None = None,
    ) -> list[Guideline]:
        """List global guidelines for an owner, optionally filtered by tag."""
        query = (
            select(Guideline)
            .where(Guideline.owner_id == owner_id, Guideline.scope == "global")
            .order_by(Guideline.created_at.desc())
        )
        if tag:
            query = query.where(Guideline.tags.contains([tag]))
        query = query.offset(offset).limit(limit)
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_guideline(
        self, guideline_id: str, owner_id: str, data: GuidelineUpdate,
    ) -> Guideline | None:
        """Update a guideline."""
        guideline = await self.get_guideline(guideline_id)
        if not guideline or guideline.owner_id != owner_id:
            return None
        changed = False
        if data.title is not None and data.title != guideline.title:
            guideline.title = data.title
            changed = True
        if data.content is not None and data.content != guideline.content:
            guideline.content = data.content
            changed = True
        if data.tags is not None:
            guideline.tags = data.tags
            flag_modified(guideline, "tags")
            changed = True
        if changed and guideline.scope == "global":
            guideline.version = (guideline.version or 1) + 1
        await self.db.flush()
        return guideline

    async def delete_guideline(self, guideline_id: str, owner_id: str) -> bool:
        """Delete a guideline."""
        guideline = await self.get_guideline(guideline_id)
        if not guideline or guideline.owner_id != owner_id:
            return False
        await self.db.delete(guideline)
        return True

    async def get_board_guidelines(self, board_id: str) -> list[dict]:
        """Get all guidelines for a board — linked globals + inline, sorted by priority."""
        # Linked global guidelines
        linked_query = (
            select(Guideline, BoardGuideline.priority)
            .join(BoardGuideline, BoardGuideline.guideline_id == Guideline.id)
            .where(BoardGuideline.board_id == board_id)
        )
        linked_result = await self.db.execute(linked_query)
        linked_rows = linked_result.all()

        # Inline (board-scoped) guidelines
        inline_query = (
            select(Guideline)
            .where(Guideline.board_id == board_id, Guideline.scope == "inline")
            .order_by(Guideline.created_at)
        )
        inline_result = await self.db.execute(inline_query)
        inline_rows = inline_result.scalars().all()

        items: list[dict] = []
        for guideline, priority in linked_rows:
            items.append({
                "id": guideline.id,
                "guideline": {
                    "id": guideline.id,
                    "title": guideline.title,
                    "content": guideline.content,
                    "tags": guideline.tags,
                    "scope": guideline.scope,
                    "board_id": guideline.board_id,
                    "owner_id": guideline.owner_id,
                    "created_at": guideline.created_at.isoformat() if guideline.created_at else None,
                    "version": guideline.version or 1,
                    "updated_at": guideline.updated_at.isoformat() if guideline.updated_at else None,
                },
                "priority": priority,
                "scope": guideline.scope,
            })
        for guideline in inline_rows:
            items.append({
                "id": guideline.id,
                "guideline": {
                    "id": guideline.id,
                    "title": guideline.title,
                    "content": guideline.content,
                    "tags": guideline.tags,
                    "scope": guideline.scope,
                    "board_id": guideline.board_id,
                    "owner_id": guideline.owner_id,
                    "created_at": guideline.created_at.isoformat() if guideline.created_at else None,
                    "updated_at": guideline.updated_at.isoformat() if guideline.updated_at else None,
                },
                "priority": 0,
                "scope": "inline",
            })

        items.sort(key=lambda x: x["priority"])
        return items

    async def link_guideline_to_board(
        self, board_id: str, guideline_id: str, priority: int = 0,
    ) -> BoardGuideline:
        """Link a global guideline to a board."""
        link = BoardGuideline(
            board_id=board_id,
            guideline_id=guideline_id,
            priority=priority,
        )
        self.db.add(link)
        await self.db.flush()
        return link

    async def unlink_guideline_from_board(self, board_id: str, guideline_id: str) -> bool:
        """Unlink a guideline from a board."""
        query = select(BoardGuideline).where(
            BoardGuideline.board_id == board_id,
            BoardGuideline.guideline_id == guideline_id,
        )
        result = await self.db.execute(query)
        link = result.scalar_one_or_none()
        if not link:
            return False
        await self.db.delete(link)
        return True

    async def update_priority(self, board_id: str, guideline_id: str, priority: int) -> bool:
        """Update the priority of a linked guideline."""
        query = select(BoardGuideline).where(
            BoardGuideline.board_id == board_id,
            BoardGuideline.guideline_id == guideline_id,
        )
        result = await self.db.execute(query)
        link = result.scalar_one_or_none()
        if not link:
            return False
        link.priority = priority
        await self.db.flush()
        return True


# ============================================================================
# Archive Service
# ============================================================================


class ArchiveService:
    """Service for archiving and restoring entity trees."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def _resolve_tree(
        self, entity_type: str, entity_id: str
    ) -> dict[str, list]:
        """Resolve the full descendant tree from a given entity.
        Returns {ideations: [...], refinements: [...], specs: [...], cards: [...]}.
        """
        from okto_pulse.core.models.db import Ideation, Refinement, Spec, Card

        tree: dict[str, list] = {"ideations": [], "refinements": [], "specs": [], "cards": []}

        if entity_type == "ideation":
            ideation = await self.db.get(Ideation, entity_id)
            if not ideation:
                raise ValueError("Ideation not found")
            tree["ideations"].append(ideation)

            # Refinements from this ideation
            q = select(Refinement).where(Refinement.ideation_id == entity_id)
            refinements = list((await self.db.execute(q)).scalars().all())
            tree["refinements"].extend(refinements)

            # Specs from refinements + direct from ideation
            ref_ids = [r.id for r in refinements]
            spec_q = select(Spec).where(
                (Spec.ideation_id == entity_id) | (Spec.refinement_id.in_(ref_ids) if ref_ids else False)
            )
            specs = list((await self.db.execute(spec_q)).scalars().all())
            tree["specs"].extend(specs)

        elif entity_type == "refinement":
            refinement = await self.db.get(Refinement, entity_id)
            if not refinement:
                raise ValueError("Refinement not found")
            tree["refinements"].append(refinement)

            spec_q = select(Spec).where(Spec.refinement_id == entity_id)
            specs = list((await self.db.execute(spec_q)).scalars().all())
            tree["specs"].extend(specs)

        elif entity_type == "spec":
            spec = await self.db.get(Spec, entity_id)
            if not spec:
                raise ValueError("Spec not found")
            tree["specs"].append(spec)

        else:
            raise ValueError(f"Invalid entity_type: {entity_type}. Must be ideation, refinement, or spec.")

        # Cards from all specs in tree
        spec_ids = [s.id for s in tree["specs"]]
        if spec_ids:
            card_q = select(Card).where(Card.spec_id.in_(spec_ids))
            cards = list((await self.db.execute(card_q)).scalars().all())
            tree["cards"].extend(cards)

            # Bug cards linked to these cards via origin_task_id
            card_ids = [c.id for c in cards]
            if card_ids:
                bug_q = select(Card).where(
                    Card.origin_task_id.in_(card_ids),
                    Card.id.notin_(card_ids),  # avoid duplicates
                )
                bugs = list((await self.db.execute(bug_q)).scalars().all())
                tree["cards"].extend(bugs)

        return tree

    async def archive_tree(self, entity_type: str, entity_id: str) -> dict[str, int]:
        """Archive an entity and all its descendants."""
        tree = await self._resolve_tree(entity_type, entity_id)

        counts = {"ideations": 0, "refinements": 0, "specs": 0, "cards": 0}

        for ideation in tree["ideations"]:
            if not ideation.archived:
                ideation.pre_archive_status = ideation.status.value if hasattr(ideation.status, "value") else str(ideation.status)
                ideation.archived = True
                counts["ideations"] += 1

        for refinement in tree["refinements"]:
            if not refinement.archived:
                refinement.pre_archive_status = refinement.status.value if hasattr(refinement.status, "value") else str(refinement.status)
                refinement.archived = True
                counts["refinements"] += 1

        for spec in tree["specs"]:
            if not spec.archived:
                spec.pre_archive_status = spec.status.value if hasattr(spec.status, "value") else str(spec.status)
                spec.archived = True
                counts["specs"] += 1

        for card in tree["cards"]:
            if not card.archived:
                card.pre_archive_status = card.status.value if hasattr(card.status, "value") else str(card.status)
                card.archived = True
                counts["cards"] += 1

        await self.db.flush()
        return counts

    async def restore_tree(self, entity_type: str, entity_id: str) -> dict[str, int]:
        """Restore an archived entity and all its descendants."""
        from okto_pulse.core.models.db import (
            IdeationStatus, RefinementStatus, SpecStatus, CardStatus,
        )

        tree = await self._resolve_tree(entity_type, entity_id)

        counts = {"ideations": 0, "refinements": 0, "specs": 0, "cards": 0}

        for ideation in tree["ideations"]:
            if ideation.archived:
                if ideation.pre_archive_status:
                    try:
                        ideation.status = IdeationStatus(ideation.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                ideation.archived = False
                ideation.pre_archive_status = None
                counts["ideations"] += 1

        for refinement in tree["refinements"]:
            if refinement.archived:
                if refinement.pre_archive_status:
                    try:
                        refinement.status = RefinementStatus(refinement.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                refinement.archived = False
                refinement.pre_archive_status = None
                counts["refinements"] += 1

        for spec in tree["specs"]:
            if spec.archived:
                if spec.pre_archive_status:
                    try:
                        spec.status = SpecStatus(spec.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                spec.archived = False
                spec.pre_archive_status = None
                counts["specs"] += 1

        for card in tree["cards"]:
            if card.archived:
                if card.pre_archive_status:
                    try:
                        card.status = CardStatus(card.pre_archive_status)
                    except (ValueError, KeyError):
                        pass
                card.archived = False
                card.pre_archive_status = None
                counts["cards"] += 1

        await self.db.flush()
        return counts


# ============================================================================
# SPRINT SERVICE
# ============================================================================


class SprintService:
    """Service for sprint operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    _SPRINT_TRANSITIONS = {
        SprintStatus.DRAFT: [SprintStatus.ACTIVE, SprintStatus.CANCELLED],
        SprintStatus.ACTIVE: [SprintStatus.DRAFT, SprintStatus.REVIEW, SprintStatus.CANCELLED],
        SprintStatus.REVIEW: [SprintStatus.ACTIVE, SprintStatus.CLOSED, SprintStatus.CANCELLED],
        SprintStatus.CLOSED: [SprintStatus.DRAFT],
        SprintStatus.CANCELLED: [SprintStatus.DRAFT],
    }

    async def _record_history(
        self, sprint_id: str, action: str, actor_id: str, actor_name: str,
        actor_type: str = "user", changes: list[dict] | None = None,
        summary: str | None = None, version: int | None = None,
    ) -> None:
        entry = SprintHistory(
            sprint_id=sprint_id, action=action, actor_type=actor_type,
            actor_id=actor_id, actor_name=actor_name,
            changes=changes, summary=summary, version=version,
        )
        self.db.add(entry)

    async def _log_activity(self, **kwargs: Any) -> None:
        log = ActivityLog(**kwargs)
        self.db.add(log)

    async def create_sprint(
        self, board_id: str, user_id: str, data: SprintCreate,
        skip_ownership_check: bool = False,
    ) -> Sprint | None:
        """Create a new sprint for a spec."""
        spec = await self.db.get(Spec, data.spec_id)
        if not spec or spec.board_id != board_id:
            return None
        if not skip_ownership_check:
            board = await self.db.get(Board, board_id)
            if not board or board.owner_id != user_id:
                return None

        # Validate scoped IDs exist in spec
        if data.test_scenario_ids:
            spec_ts_ids = {s.get("id") for s in (spec.test_scenarios or [])}
            invalid = set(data.test_scenario_ids) - spec_ts_ids
            if invalid:
                raise ValueError(f"Test scenario IDs not found in spec: {invalid}")
        if data.business_rule_ids:
            spec_br_ids = {r.get("id") for r in (spec.business_rules or [])}
            invalid = set(data.business_rule_ids) - spec_br_ids
            if invalid:
                raise ValueError(f"Business rule IDs not found in spec: {invalid}")

        sprint = Sprint(
            board_id=board_id, spec_id=data.spec_id,
            title=data.title, description=data.description,
            objective=data.objective,
            expected_outcome=data.expected_outcome,
            spec_version=spec.version,
            test_scenario_ids=data.test_scenario_ids,
            business_rule_ids=data.business_rule_ids,
            start_date=data.start_date, end_date=data.end_date,
            labels=data.labels, created_by=user_id,
        )
        self.db.add(sprint)
        await self.db.flush()

        from okto_pulse.core.events import publish as event_publish
        from okto_pulse.core.events.types import SprintCreated as SprintCreatedEvent

        await event_publish(
            SprintCreatedEvent(
                board_id=board_id,
                actor_id=user_id,
                sprint_id=sprint.id,
                spec_id=data.spec_id,
            ),
            session=self.db,
        )

        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self._log_activity(
            board_id=board_id, action="sprint_created",
            actor_type="user", actor_id=user_id, actor_name=actor_name,
            details={"title": data.title, "sprint_id": sprint.id, "spec_id": data.spec_id},
        )
        await self._record_history(
            sprint_id=sprint.id, action="created", actor_id=user_id, actor_name=actor_name,
            summary=f"Sprint created: {data.title}", version=1,
        )
        return sprint

    async def get_sprint(self, sprint_id: str) -> Sprint | None:
        """Get a sprint by ID with cards, Q&A, and history."""
        query = (
            select(Sprint)
            .options(selectinload(Sprint.cards))
            .options(selectinload(Sprint.qa_items))
            .options(selectinload(Sprint.history))
            .where(Sprint.id == sprint_id)
        )
        result = await self.db.execute(query)
        return result.scalar_one_or_none()

    async def list_sprints(
        self, spec_id: str, include_archived: bool = False,
    ) -> list[Sprint]:
        """List sprints for a spec."""
        query = select(Sprint).where(Sprint.spec_id == spec_id)
        if not include_archived:
            query = query.where(Sprint.archived == False)
        query = query.order_by(Sprint.created_at.asc())
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def list_board_sprints(
        self, board_id: str, status_filter: str | None = None,
        spec_id: str | None = None,
        include_archived: bool = False,
    ) -> list[Sprint]:
        """List all sprints for a board, optionally filtered by status and/or spec."""
        from sqlalchemy.orm import selectinload
        query = select(Sprint).where(Sprint.board_id == board_id)
        if status_filter:
            query = query.where(Sprint.status == SprintStatus(status_filter))
        if spec_id:
            query = query.where(Sprint.spec_id == spec_id)
        if not include_archived:
            query = query.where(Sprint.archived == False)
        query = query.options(selectinload(Sprint.spec))
        query = query.order_by(Sprint.updated_at.desc())
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def update_sprint(
        self, sprint_id: str, user_id: str, data: SprintUpdate,
    ) -> Sprint | None:
        """Update a sprint. Bumps version on content changes."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return None
        if sprint.archived:
            raise ValueError("This sprint is archived. Restore it first.")

        update_data = data.model_dump(exclude_unset=True)
        old_data = {k: getattr(sprint, k) for k in update_data.keys()}

        # Validate scoped IDs if changed
        if "test_scenario_ids" in update_data and update_data["test_scenario_ids"] is not None:
            spec = await self.db.get(Spec, sprint.spec_id)
            if spec:
                spec_ts_ids = {s.get("id") for s in (spec.test_scenarios or [])}
                invalid = set(update_data["test_scenario_ids"]) - spec_ts_ids
                if invalid:
                    raise ValueError(f"Test scenario IDs not found in spec: {invalid}")
        if "business_rule_ids" in update_data and update_data["business_rule_ids"] is not None:
            spec = spec if "test_scenario_ids" in update_data else await self.db.get(Spec, sprint.spec_id)
            if spec:
                spec_br_ids = {r.get("id") for r in (spec.business_rules or [])}
                invalid = set(update_data["business_rule_ids"]) - spec_br_ids
                if invalid:
                    raise ValueError(f"Business rule IDs not found in spec: {invalid}")

        content_fields = {"title", "description", "test_scenario_ids", "business_rule_ids"}
        bumps_version = bool(content_fields & update_data.keys())

        json_fields = {"test_scenario_ids", "business_rule_ids", "labels"}
        for key, value in update_data.items():
            setattr(sprint, key, value)
            if key in json_fields:
                flag_modified(sprint, key)

        if bumps_version:
            sprint.version += 1

        actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
        await self._log_activity(
            board_id=sprint.board_id, action="sprint_updated",
            actor_type="user", actor_id=user_id, actor_name=actor_name,
            details={"sprint_id": sprint_id, "version": sprint.version, "fields": list(update_data.keys())},
        )
        changes = SpecService._compute_diff(old_data, update_data, list(update_data.keys()))
        if changes:
            await self._record_history(
                sprint_id=sprint_id, action="updated", actor_id=user_id, actor_name=actor_name,
                changes=changes, version=sprint.version,
                summary=f"Updated: {', '.join(c['field'] for c in changes)}",
            )
        return sprint

    async def move_sprint(
        self, sprint_id: str, user_id: str, data: SprintMove,
        actor_name: str | None = None,
    ) -> Sprint | None:
        """Move a sprint to a different status with gates."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return None
        if sprint.archived:
            raise ValueError("This sprint is archived. Restore it first.")

        allowed = self._SPRINT_TRANSITIONS.get(sprint.status, [])
        if data.status not in allowed:
            allowed_values = [s.value for s in allowed]
            raise ValueError(
                f"Cannot move sprint from '{sprint.status.value}' to '{data.status.value}'. "
                f"Allowed: {allowed_values}"
            )

        spec = await self.db.get(Spec, sprint.spec_id)
        board = await self.db.get(Board, sprint.board_id) if spec else None

        # Gate: draft → active requires at least 1 card assigned
        if data.status == SprintStatus.ACTIVE:
            cards_q = select(func.count()).select_from(Card).where(
                Card.sprint_id == sprint_id, Card.archived.is_(False),
            )
            card_count = (await self.db.execute(cards_q)).scalar() or 0
            if card_count == 0:
                raise ValueError(
                    "Cannot activate sprint: no cards assigned. "
                    "Assign at least one card to this sprint before activating."
                )

        # Gate: active → review requires scoped test coverage check
        if data.status == SprintStatus.REVIEW:
            skip_tc = sprint.skip_test_coverage or (
                (board.settings or {}).get("skip_test_coverage_global", False) if board else False
            )
            if not skip_tc and spec and sprint.test_scenario_ids:
                scenarios = spec.test_scenarios or []
                scoped = [s for s in scenarios if s.get("id") in (sprint.test_scenario_ids or [])]
                not_covered = [s for s in scoped if s.get("status") != "passed"]
                if not_covered:
                    names = "; ".join(s.get("title", s.get("id", "?"))[:60] for s in not_covered[:5])
                    raise ValueError(
                        f"Cannot submit sprint for review: {len(not_covered)} scoped test scenario(s) "
                        f"not passed. Pending: {names}"
                        f"{f' (and {len(not_covered) - 5} more)' if len(not_covered) > 5 else ''}."
                    )

        # Gate: review → closed requires evaluation
        if data.status == SprintStatus.CLOSED:
            skip_qual = sprint.skip_qualitative_validation
            if not skip_qual:
                evaluations = [e for e in (sprint.evaluations or []) if not e.get("stale")]
                approvals = [e for e in evaluations if e.get("recommendation") == "approve"]
                rejections = [e for e in evaluations if e.get("recommendation") == "reject"]
                if rejections:
                    names = ", ".join(e.get("evaluator_name", "?") for e in rejections)
                    raise ValueError(
                        f"Cannot close sprint: {len(rejections)} evaluation(s) with 'reject' "
                        f"recommendation (by: {names}). Remove or replace rejections."
                    )
                if not approvals:
                    raise ValueError(
                        "Cannot close sprint: no evaluation with 'approve' recommendation. "
                        "Submit an evaluation before closing."
                    )
                threshold = (
                    sprint.validation_threshold
                    or (board.settings or {}).get("validation_threshold_global", 70) if board else 70
                )
                avg_score = sum(e.get("overall_score", 0) for e in approvals) / len(approvals)
                if avg_score < threshold:
                    raise ValueError(
                        f"Cannot close sprint: average approval score ({avg_score:.0f}) "
                        f"is below threshold ({threshold})."
                    )

        old_status = sprint.status
        sprint.status = data.status

        if old_status != data.status:
            from okto_pulse.core.events import publish as event_publish
            from okto_pulse.core.events.types import (
                SprintClosed as SprintClosedEvent,
                SprintMoved as SprintMovedEvent,
            )

            await event_publish(
                SprintMovedEvent(
                    board_id=sprint.board_id,
                    actor_id=user_id,
                    sprint_id=sprint.id,
                    from_status=old_status.value,
                    to_status=data.status.value,
                ),
                session=self.db,
            )
            if data.status == SprintStatus.CLOSED:
                await event_publish(
                    SprintClosedEvent(
                        board_id=sprint.board_id,
                        actor_id=user_id,
                        sprint_id=sprint.id,
                    ),
                    session=self.db,
                )

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, sprint.board_id)
        await self._log_activity(
            board_id=sprint.board_id, action="sprint_moved",
            actor_type="user", actor_id=user_id, actor_name=resolved_name,
            details={
                "sprint_id": sprint_id, "spec_id": sprint.spec_id,
                "from_status": old_status.value, "to_status": data.status.value,
            },
        )
        await self._record_history(
            sprint_id=sprint_id, action="status_changed",
            actor_id=user_id, actor_name=resolved_name,
            changes=[{"field": "status", "old": old_status.value, "new": data.status.value}],
            summary=f"Status: {old_status.value} → {data.status.value}",
            version=sprint.version,
        )
        return sprint

    async def delete_sprint(self, sprint_id: str, user_id: str) -> bool:
        """Delete a sprint. Unlinks cards but doesn't delete them."""
        sprint = await self.get_sprint(sprint_id)
        if not sprint:
            return False
        await self.db.execute(
            update(Card).where(Card.sprint_id == sprint_id).values(sprint_id=None)
        )
        board_id = sprint.board_id
        actor_name = await resolve_actor_name(self.db, user_id, board_id)
        await self.db.delete(sprint)
        await self._log_activity(
            board_id=board_id, action="sprint_deleted",
            actor_type="user", actor_id=user_id, actor_name=actor_name,
            details={"sprint_id": sprint_id},
        )
        return True

    async def assign_tasks(
        self, sprint_id: str, card_ids: list[str], user_id: str,
    ) -> int:
        """Assign cards to a sprint. Cards must belong to the same spec."""
        sprint = await self.db.get(Sprint, sprint_id)
        if not sprint:
            raise ValueError("Sprint not found")
        assigned = 0
        for card_id in card_ids:
            card = await self.db.get(Card, card_id)
            if not card:
                continue
            if card.spec_id != sprint.spec_id:
                raise ValueError(
                    f"Card '{card.title}' belongs to a different spec. "
                    f"Sprint spec: {sprint.spec_id}, card spec: {card.spec_id}"
                )
            card.sprint_id = sprint_id
            assigned += 1
        if assigned:
            actor_name = await resolve_actor_name(self.db, user_id, sprint.board_id)
            await self._log_activity(
                board_id=sprint.board_id, action="sprint_tasks_assigned",
                actor_type="user", actor_id=user_id, actor_name=actor_name,
                details={"sprint_id": sprint_id, "card_ids": card_ids, "count": assigned},
            )
            await self._record_history(
                sprint_id=sprint_id, action="tasks_assigned",
                actor_id=user_id, actor_name=actor_name,
                changes=[{"field": "cards", "added": card_ids, "count": assigned}],
                summary=f"Assigned {assigned} card(s) to sprint",
                version=sprint.version,
            )
        return assigned

    async def submit_evaluation(
        self, sprint_id: str, user_id: str, evaluation: dict,
    ) -> Sprint | None:
        """Submit a qualitative evaluation for a sprint."""
        sprint = await self.db.get(Sprint, sprint_id)
        if not sprint:
            return None
        if sprint.status != SprintStatus.REVIEW:
            raise ValueError(
                f"Evaluations can only be submitted for sprints in 'review' status "
                f"(current: '{sprint.status.value}')"
            )
        import uuid as _uuid
        eval_entry = {
            "id": f"eval_{_uuid.uuid4().hex[:8]}",
            "evaluator_id": user_id,
            "evaluator_name": await resolve_actor_name(self.db, user_id, sprint.board_id),
            "evaluator_type": "user",
            **evaluation,
            "stale": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        evals = list(sprint.evaluations or [])
        evals.append(eval_entry)
        sprint.evaluations = evals
        flag_modified(sprint, "evaluations")

        await self._log_activity(
            board_id=sprint.board_id, action="sprint_evaluation_submitted",
            actor_type="user", actor_id=user_id, actor_name=eval_entry["evaluator_name"],
            details={"sprint_id": sprint_id, "evaluation_id": eval_entry["id"], "score": evaluation.get("overall_score")},
        )
        await self._record_history(
            sprint_id=sprint_id, action="evaluation_submitted",
            actor_id=user_id, actor_name=eval_entry["evaluator_name"],
            changes=[{
                "field": "evaluations",
                "evaluation_id": eval_entry["id"],
                "recommendation": evaluation.get("recommendation"),
                "overall_score": evaluation.get("overall_score"),
            }],
            summary=f"Evaluation submitted: {evaluation.get('recommendation')} (score: {evaluation.get('overall_score')})",
            version=sprint.version,
        )
        return sprint

    async def list_history(self, sprint_id: str, limit: int = 50) -> list[SprintHistory]:
        query = (
            select(SprintHistory)
            .where(SprintHistory.sprint_id == sprint_id)
            .order_by(SprintHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def suggest_sprints(
        self, spec_id: str, threshold: int = 8,
    ) -> list[dict]:
        """Suggest sprint breakdown for a spec based on FRs, test scenarios, and dependencies.

        Algorithm:
        1. Group cards by linked FRs (via test_scenario_ids → linked_criteria).
        2. Consider card dependencies (dependent cards in same or later sprint).
        3. Distribute into N sprints where N = ceil(total_cards / threshold).
        4. Each sprint gets the test_scenario_ids and business_rule_ids for its cards.
        Returns suggestions without creating anything.
        """
        import math

        spec = await self.db.get(Spec, spec_id)
        if not spec:
            raise ValueError("Spec not found")

        cards_q = select(Card).where(
            Card.spec_id == spec_id, Card.archived.is_(False),
            Card.status.notin_([CardStatus.DONE, CardStatus.CANCELLED]),
        )
        result = await self.db.execute(cards_q)
        cards = list(result.scalars().all())

        if not cards:
            return []

        # Build FR→cards mapping via test_scenario_ids → linked_criteria
        scenarios = {s.get("id"): s for s in (spec.test_scenarios or [])}
        rules = {r.get("id"): r for r in (spec.business_rules or [])}
        fr_groups: dict[str, list[Card]] = {}
        ungrouped: list[Card] = []

        for card in cards:
            linked_frs: set[str] = set()
            for ts_id in (card.test_scenario_ids or []):
                sc = scenarios.get(ts_id)
                if sc:
                    for crit in (sc.get("linked_criteria") or []):
                        linked_frs.add(crit)
            if linked_frs:
                primary_fr = sorted(linked_frs)[0]
                fr_groups.setdefault(primary_fr, []).append(card)
            else:
                ungrouped.append(card)

        # Build dependency graph
        deps_q = select(CardDependency).where(
            CardDependency.card_id.in_([c.id for c in cards])
        )
        deps_result = await self.db.execute(deps_q)
        dependencies = list(deps_result.scalars().all())
        dep_map: dict[str, set[str]] = {}
        for d in dependencies:
            dep_map.setdefault(d.card_id, set()).add(d.depends_on_id)

        # Flatten groups into ordered buckets
        all_groups = list(fr_groups.values())
        if ungrouped:
            all_groups.append(ungrouped)

        # Determine number of sprints
        total = len(cards)
        n_sprints = max(1, math.ceil(total / threshold))

        # Distribute groups across sprints
        suggested: list[list[Card]] = [[] for _ in range(n_sprints)]
        group_idx = 0
        for group in all_groups:
            target = group_idx % n_sprints
            suggested[target].extend(group)
            group_idx += 1

        # Ensure dependency ordering: if card A depends on B, B must be in same or earlier sprint
        card_sprint_map: dict[str, int] = {}
        for si, sprint_cards in enumerate(suggested):
            for c in sprint_cards:
                card_sprint_map[c.id] = si

        # Adjust: move cards earlier if their dependencies are in later sprints
        changed = True
        iterations = 0
        while changed and iterations < 10:
            changed = False
            iterations += 1
            for card_id, card_deps in dep_map.items():
                if card_id not in card_sprint_map:
                    continue
                card_si = card_sprint_map[card_id]
                for dep_id in card_deps:
                    dep_si = card_sprint_map.get(dep_id)
                    if dep_si is not None and dep_si > card_si:
                        # Move dependency to same sprint as dependent card
                        card_sprint_map[dep_id] = card_si
                        changed = True

        # Rebuild sprints from adjusted map
        final: list[list[Card]] = [[] for _ in range(n_sprints)]
        for card in cards:
            si = card_sprint_map.get(card.id, 0)
            final[si].append(card)

        # Build suggestion output
        suggestions = []
        for i, sprint_cards in enumerate(final):
            if not sprint_cards:
                continue
            # Collect scoped test scenario and BR IDs
            ts_ids: set[str] = set()
            br_ids: set[str] = set()
            for c in sprint_cards:
                for ts_id in (c.test_scenario_ids or []):
                    ts_ids.add(ts_id)
                    sc = scenarios.get(ts_id)
                    if sc:
                        for linked in (sc.get("linked_criteria") or []):
                            # Find BRs that reference this FR
                            for r in (spec.business_rules or []):
                                if linked in (r.get("linked_requirements") or []):
                                    br_ids.add(r.get("id"))

            suggestions.append({
                "title": f"Sprint {i + 1}",
                "description": f"Auto-suggested sprint ({len(sprint_cards)} tasks)",
                "card_ids": [c.id for c in sprint_cards],
                "card_titles": [c.title for c in sprint_cards],
                "test_scenario_ids": sorted(ts_ids) if ts_ids else None,
                "business_rule_ids": sorted(br_ids) if br_ids else None,
            })

        return suggestions


class SprintQAService:
    """Service for sprint Q&A operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    async def create_question(
        self, sprint_id: str, user_id: str, question: str,
        question_type: str = "text", choices: list | None = None,
        allow_free_text: bool = False,
    ) -> SprintQAItem | None:
        sprint = await self.db.get(Sprint, sprint_id)
        if not sprint:
            return None
        qa = SprintQAItem(
            sprint_id=sprint_id, question=question,
            question_type=question_type or "text",
            choices=choices, allow_free_text=allow_free_text,
            asked_by=user_id,
        )
        self.db.add(qa)
        await self.db.flush()
        return qa

    async def answer_question(
        self, qa_id: str, user_id: str, answer: str | None = None,
        selected: list[str] | None = None,
    ) -> SprintQAItem | None:
        qa = await self.db.get(SprintQAItem, qa_id)
        if not qa:
            return None
        qa.answer = answer
        qa.selected = selected
        qa.answered_by = user_id
        qa.answered_at = datetime.now(timezone.utc)
        if selected is not None:
            flag_modified(qa, "selected")
        return qa

    async def list_qa(self, sprint_id: str) -> list[SprintQAItem]:
        query = (
            select(SprintQAItem)
            .where(SprintQAItem.sprint_id == sprint_id)
            .order_by(SprintQAItem.created_at.asc())
        )
        result = await self.db.execute(query)
        return list(result.scalars().all())

    async def delete_question(self, qa_id: str) -> bool:
        """Delete a Q&A item."""
        qa = await self.db.get(SprintQAItem, qa_id)
        if not qa:
            return False
        await self.db.delete(qa)
        return True

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
)

settings = get_settings()


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

        # Enforce: spec status rules for card creation
        # - Normal tasks: spec must be 'approved' or 'in_progress'
        # - Bug cards: also allowed when spec is 'done'
        spec = await self.db.get(Spec, data.spec_id)
        if not spec:
            raise ValueError(f"Spec '{data.spec_id}' not found")
        is_bug = data.card_type == "bug"
        allowed_statuses = {SpecStatus.APPROVED, SpecStatus.IN_PROGRESS}
        if is_bug:
            allowed_statuses.add(SpecStatus.DONE)
        if spec.status not in allowed_statuses:
            if is_bug:
                raise ValueError(
                    f"Bug cards can only be created for specs in 'approved', 'in_progress', or 'done' status. "
                    f"Spec '{spec.title}' is currently '{spec.status.value}'."
                )
            raise ValueError(
                f"Task cards can only be created for specs in 'approved' or 'in_progress' status. "
                f"Spec '{spec.title}' is currently '{spec.status.value}'. "
                f"Move the spec to 'approved' first."
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
        CardStatus.ON_HOLD: 2,  # same level — lateral move
        CardStatus.DONE: 3,
        CardStatus.CANCELLED: 3,
    }

    async def move_card(
        self, card_id: str, user_id: str, data: CardMove, actor_name: str | None = None
    ) -> Card | None:
        """Move a card to a different column/position. Blocks if dependencies not met.

        Moving to 'done' requires a conclusion text. The conclusion is appended
        to the card's conclusions list (supports multiple cycles).
        Moving forward (started/in_progress) requires all test scenarios in the
        spec to have linked task cards, unless skip_test_coverage is set.
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

        # Block forward moves unless spec is in_progress
        old_level = self._STATUS_ORDER.get(old_status, 0)
        new_level = self._STATUS_ORDER.get(data.status, 0)
        if new_level > old_level and card.spec_id:
            spec_for_status = await self.db.get(Spec, card.spec_id)
            if spec_for_status and spec_for_status.status != SpecStatus.IN_PROGRESS:
                raise ValueError(
                    f"Cannot move card forward: spec '{spec_for_status.title}' must be 'in_progress' "
                    f"(currently '{spec_for_status.status.value}'). "
                    f"Move the spec to 'in_progress' before starting work on its cards."
                )

        # Block forward moves if spec test scenarios lack linked tasks
        if new_level > old_level and card.spec_id:
            spec = await self.db.get(Spec, card.spec_id)
            if spec and not spec.skip_test_coverage and not skip_global:
                scenarios = list(spec.test_scenarios or [])
                if scenarios:
                    unlinked = [
                        s for s in scenarios if not s.get("linked_task_ids")
                    ]
                    if unlinked:
                        titles = ", ".join(
                            f'"{s["title"]}"' for s in unlinked[:3]
                        )
                        suffix = (
                            f" and {len(unlinked) - 3} more"
                            if len(unlinked) > 3
                            else ""
                        )
                        raise ValueError(
                            f"Cannot start this card: {len(unlinked)} test scenario(s) "
                            f"in spec '{spec.title}' have no linked task cards "
                            f"({titles}{suffix}). "
                            f"REQUIRED ACTION: For each test scenario, you must: "
                            f"(1) create a test card with spec_id and test_scenario_ids "
                            f"via okto_pulse_create_card (this auto-links bidirectionally), "
                            f"(2) verify with okto_pulse_list_test_scenarios that every "
                            f"scenario shows linked_task_ids. Only then can cards be started. "
                            f"Alternatively, enable 'skip test coverage' on the spec or "
                            f"the board-level global override in the IDE."
                        )

        # Block forward moves if FRs lack business rules coverage
        skip_rules_global = board_settings.get("skip_rules_coverage_global", False)
        if new_level > old_level and card.spec_id:
            spec = spec if 'spec' in dir() else await self.db.get(Spec, card.spec_id)
            if spec and not getattr(spec, "skip_rules_coverage", False) and not skip_rules_global:
                frs = list(spec.functional_requirements or [])
                brs = list(spec.business_rules or [])
                if frs:
                    # Collect FR indices covered by at least one business rule.
                    # linked_requirements can be numeric indices ("0") or full FR text.
                    covered_fr_indices: set[int] = set()
                    for br in brs:
                        if isinstance(br, dict):
                            for ref in (br.get("linked_requirements") or []):
                                ref_str = str(ref)
                                # Try as numeric index
                                try:
                                    idx_num = int(ref_str)
                                    if 0 <= idx_num < len(frs):
                                        covered_fr_indices.add(idx_num)
                                        continue
                                except (ValueError, TypeError):
                                    pass
                                # Try matching by FR text content
                                for fi, fr_text in enumerate(frs):
                                    if ref_str in fr_text or fr_text in ref_str:
                                        covered_fr_indices.add(fi)
                                        break
                    uncovered = [
                        (i, fr) for i, fr in enumerate(frs) if i not in covered_fr_indices
                    ]
                    if uncovered:
                        previews = ", ".join(
                            f'"FR{i}: {fr[:40]}..."' if len(fr) > 40 else f'"FR{i}: {fr}"'
                            for i, fr in uncovered[:3]
                        )
                        suffix = f" and {len(uncovered) - 3} more" if len(uncovered) > 3 else ""
                        raise ValueError(
                            f"Cannot start this card: {len(uncovered)} functional requirement(s) "
                            f"in spec '{spec.title}' have no linked business rules "
                            f"({previews}{suffix}). "
                            f"REQUIRED ACTION: For each uncovered FR, you must: "
                            f"(1) create a business rule with linked_requirements referencing "
                            f"the FR index via okto_pulse_add_business_rule, "
                            f"(2) verify with okto_pulse_list_business_rules that every FR "
                            f"has at least one linked rule. Only then can cards be started. "
                            f"Alternatively, enable 'skip rules coverage' on the spec or "
                            f"the board-level global override in the IDE."
                        )

                    # Check that all BRs have linked tasks (mirrors test scenario task linkage)
                    unlinked_rules = [
                        br for br in brs
                        if isinstance(br, dict) and not br.get("linked_task_ids")
                    ]
                    if unlinked_rules:
                        titles = ", ".join(
                            f'"{br.get("title", br.get("id", "?"))}"'
                            for br in unlinked_rules[:3]
                        )
                        suffix = (
                            f" and {len(unlinked_rules) - 3} more"
                            if len(unlinked_rules) > 3 else ""
                        )
                        raise ValueError(
                            f"Cannot start this card: {len(unlinked_rules)} business rule(s) "
                            f"in spec '{spec.title}' have no linked task cards "
                            f"({titles}{suffix}). "
                            f"REQUIRED ACTION: For each business rule, you must: "
                            f"(1) create an implementation card with spec_id via okto_pulse_create_card, "
                            f"(2) link the card to the rule via okto_pulse_link_task_to_rule. "
                            f"Only then can cards be started. "
                            f"Alternatively, enable 'skip rules coverage' on the spec or "
                            f"the board-level global override in the IDE."
                        )

        # Block forward moves if TRs lack linked tasks
        skip_trs_global = board_settings.get("skip_trs_coverage_global", False)
        if new_level > old_level and card.spec_id:
            spec = spec if 'spec' in dir() else await self.db.get(Spec, card.spec_id)
            if spec and not getattr(spec, "skip_trs_coverage", False) and not skip_trs_global:
                trs = list(spec.technical_requirements or [])
                # Only check structured TRs (dicts with id), skip legacy strings
                structured_trs = [tr for tr in trs if isinstance(tr, dict) and tr.get("id")]
                if structured_trs:
                    unlinked_trs = [
                        tr for tr in structured_trs if not tr.get("linked_task_ids")
                    ]
                    if unlinked_trs:
                        previews = ", ".join(
                            f'"{tr.get("text", tr.get("id", "?"))[:40]}"'
                            for tr in unlinked_trs[:3]
                        )
                        suffix = f" and {len(unlinked_trs) - 3} more" if len(unlinked_trs) > 3 else ""
                        raise ValueError(
                            f"Cannot start this card: {len(unlinked_trs)} technical requirement(s) "
                            f"in spec '{spec.title}' have no linked task cards "
                            f"({previews}{suffix}). "
                            f"REQUIRED ACTION: For each TR, you must: "
                            f"(1) create an implementation card with spec_id via okto_pulse_create_card, "
                            f"(2) link the card to the TR via okto_pulse_link_task_to_tr. "
                            f"Only then can cards be started. "
                            f"Alternatively, enable 'skip TRs coverage' on the spec or "
                            f"the board-level global override in the IDE."
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

        # --- Bug card: block in_progress without properly linked test tasks ---
        if (
            data.status == CardStatus.IN_PROGRESS
            and getattr(card, "card_type", "normal") == "bug"
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

        resolved_name = actor_name or await resolve_actor_name(self.db, user_id, card.board_id)
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
        """Create a new global agent (no board_id)."""
        api_key = self.generate_api_key()
        agent = Agent(
            name=data.name,
            description=data.description,
            objective=data.objective,
            api_key=api_key,
            api_key_hash=self.hash_api_key(api_key),
            permissions=data.permissions,
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
        """Update an agent."""
        agent = await self.get_agent(agent_id)
        if not agent:
            return None

        update_data = data.model_dump(exclude_unset=True)
        for key, value in update_data.items():
            setattr(agent, key, value)
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


class SpecService:
    """Service for spec operations."""

    def __init__(self, db: AsyncSession):
        self.db = db

    # ---- Status progression order ----
    _STATUS_ORDER = {
        SpecStatus.DRAFT: 0,
        SpecStatus.REVIEW: 1,
        SpecStatus.APPROVED: 2,
        SpecStatus.IN_PROGRESS: 3,
        SpecStatus.DONE: 4,
        SpecStatus.CANCELLED: 4,
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
        """Update a spec. Bumps version on content changes. Records field-level diffs."""
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

        # Serialize test_scenarios, screen_mockups, business_rules, api_contracts if present
        for json_list_field in ("test_scenarios", "screen_mockups", "business_rules", "api_contracts"):
            if json_list_field in update_data and update_data[json_list_field] is not None:
                update_data[json_list_field] = [
                    s.model_dump() if hasattr(s, "model_dump") else s
                    for s in update_data[json_list_field]
                ]

        json_fields = {"test_scenarios", "screen_mockups", "business_rules", "api_contracts", "functional_requirements", "technical_requirements", "acceptance_criteria", "labels"}
        for key, value in update_data.items():
            setattr(spec, key, value)
            if key in json_fields:
                flag_modified(spec, key)

        if bumps_version:
            spec.version += 1

        # Compute diffs
        changes = self._compute_diff(old_data, update_data, list(update_data.keys()))

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

    async def move_spec(
        self, spec_id: str, user_id: str, data: SpecMove, actor_name: str | None = None
    ) -> Spec | None:
        """Move a spec to a different status.

        Moving to 'done' requires full test coverage (every acceptance criterion
        must have at least one test scenario) unless skip_test_coverage is set.
        """
        spec = await self.get_spec(spec_id)
        if not spec:
            return None

        if getattr(spec, "archived", False):
            raise ValueError("This spec is archived. Restore it first before changing status.")

        # Enforce test coverage when moving to Done
        board = await self.db.get(Board, spec.board_id)
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
        if spec.status not in (SpecStatus.APPROVED, SpecStatus.IN_PROGRESS, SpecStatus.DONE):
            raise ValueError(f"Cards can only be linked to a spec in 'approved', 'in_progress', or 'done' status (current: '{spec.status.value}')")
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
        """Answer a spec Q&A question (text or choice selection)."""
        qa = await self.db.get(SpecQAItem, qa_id)
        if not qa:
            return None

        if qa.question_type in ("choice", "multi_choice") and data.selected:
            # Validate selected options
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type == "choice" and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected

        if data.answer:
            qa.answer = data.answer

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
        self, ideation_id: str, user_id: str, skip_ownership_check: bool = False
    ) -> Spec | None:
        """Create a Spec draft linked to an ideation.

        Compiles context from the ideation's problem statement, proposed approach,
        scope assessment, and Q&A history. Structured fields (functional_requirements,
        technical_requirements, acceptance_criteria) are left empty — they must be
        filled by the agent or human through deliberate analysis.

        Only allowed when ideation status is 'done' — ensures the ideation has been
        fully reviewed and snapshotted before specs are created from it.
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
        # Include answered Q&A as context
        qa_items = [qa for qa in (ideation.qa_items or []) if qa.answer]
        if qa_items:
            qa_lines = []
            for qa in qa_items:
                qa_lines.append(f"**Q:** {qa.question}\n**A:** {qa.answer}")
            context_parts.append(f"## Q&A Decisions\n" + "\n\n".join(qa_lines))

        context = "\n\n".join(context_parts) if context_parts else ideation.description

        spec_data = SpecCreate(
            title=ideation.title,
            description=ideation.description,
            context=context,
            ideation_id=ideation_id,
            labels=ideation.labels,
            screen_mockups=ideation.screen_mockups,
        )
        spec_service = SpecService(self.db)
        spec = await spec_service.create_spec(
            ideation.board_id, user_id, spec_data, skip_ownership_check=skip_ownership_check
        )
        if spec:
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
        """Answer an ideation Q&A question (text or choice selection)."""
        qa = await self.db.get(IdeationQAItem, qa_id)
        if not qa:
            return None

        if qa.question_type in ("choice", "multi_choice") and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type == "choice" and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected

        if data.answer:
            qa.answer = data.answer

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

        refinement = Refinement(
            ideation_id=ideation_id,
            board_id=board_id,
            title=data.title,
            description=description,
            in_scope=data.in_scope,
            out_of_scope=data.out_of_scope,
            analysis=data.analysis,
            decisions=data.decisions,
            screen_mockups=data.screen_mockups or ideation.screen_mockups,
            assignee_id=data.assignee_id,
            created_by=user_id,
            labels=data.labels or ideation.labels,
        )
        self.db.add(refinement)
        await self.db.flush()

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
        self, refinement_id: str, user_id: str, skip_ownership_check: bool = False
    ) -> Spec | None:
        """Create a Spec draft linked to a refinement.

        Compiles context from the refinement's scope, analysis, decisions,
        and Q&A history. Structured fields (functional_requirements,
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
        # Include answered Q&A as context
        qa_items = [qa for qa in (refinement.qa_items or []) if qa.answer]
        if qa_items:
            qa_lines = []
            for qa in qa_items:
                qa_lines.append(f"**Q:** {qa.question}\n**A:** {qa.answer}")
            context_parts.append(f"## Q&A Decisions\n" + "\n\n".join(qa_lines))

        context = "\n\n".join(context_parts) if context_parts else refinement.description

        # Merge mockups: refinement's own + ideation's (if not already included)
        merged_mockups = list(refinement.screen_mockups or [])
        if refinement.ideation_id:
            ideation_service = IdeationService(self.db)
            parent_ideation = await ideation_service.get_ideation(refinement.ideation_id)
            if parent_ideation and parent_ideation.screen_mockups:
                existing_ids = {m.get("id") for m in merged_mockups}
                for m in parent_ideation.screen_mockups:
                    if m.get("id") not in existing_ids:
                        merged_mockups.append(m)

        spec_data = SpecCreate(
            title=refinement.title,
            description=refinement.description,
            context=context,
            ideation_id=refinement.ideation_id,
            refinement_id=refinement_id,
            labels=refinement.labels,
            screen_mockups=merged_mockups or None,
        )
        spec_service = SpecService(self.db)
        spec = await spec_service.create_spec(
            refinement.board_id, user_id, spec_data, skip_ownership_check=skip_ownership_check
        )
        if spec:
            # Propagate knowledge bases from refinement to spec
            for kb in (refinement.knowledge_bases or []):
                spec_kb = SpecKnowledgeBase(
                    spec_id=spec.id,
                    title=kb.title,
                    description=kb.description,
                    content=kb.content,
                    mime_type=kb.mime_type,
                    created_by=user_id,
                )
                self.db.add(spec_kb)

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
        """Answer a refinement Q&A question (text or choice selection)."""
        qa = await self.db.get(RefinementQAItem, qa_id)
        if not qa:
            return None

        if qa.question_type in ("choice", "multi_choice") and data.selected:
            valid_ids = {c["id"] for c in (qa.choices or [])}
            for sel in data.selected:
                if sel not in valid_ids:
                    return None
            if qa.question_type == "choice" and len(data.selected) > 1:
                data.selected = data.selected[:1]
            qa.selected = data.selected

        if data.answer:
            qa.answer = data.answer

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

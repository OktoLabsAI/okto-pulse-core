"""Test-only SQLAlchemy adapter for Core application records."""

from __future__ import annotations

import copy
from typing import Any, Mapping
from weakref import WeakKeyDictionary

from sqlalchemy import and_, event, false, func, or_, select, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import selectinload

import sqlalchemy_test_models as models
from okto_pulse.core.ports.application_persistence import (
    ApplicationFilter,
    ApplicationGroupCount,
    ApplicationGroupCountQuery,
    ApplicationQuery,
    ApplicationRecord,
    ApplicationRecordConflictError,
)


_ENTITY_CLASSES = {
    "activity_log": models.ActivityLog,
    "agent": models.Agent,
    "agent_board": models.AgentBoard,
    "agent_seen_item": models.AgentSeenItem,
    "amendment_hotfix_revision": models.AmendmentHotfixRevision,
    "architecture_design": models.ArchitectureDesign,
    "attachment": models.Attachment,
    "board": models.Board,
    "board_guideline": models.BoardGuideline,
    "board_share": models.BoardShare,
    "card": models.Card,
    "card_dependency": models.CardDependency,
    "comment": models.Comment,
    "guideline": models.Guideline,
    "ideation": models.Ideation,
    "ideation_history": models.IdeationHistory,
    "ideation_knowledge_base": models.IdeationKnowledgeBase,
    "ideation_qa_item": models.IdeationQAItem,
    "ideation_snapshot": models.IdeationSnapshot,
    "permission_preset": models.PermissionPreset,
    "qa_item": models.QAItem,
    "refinement": models.Refinement,
    "refinement_history": models.RefinementHistory,
    "refinement_knowledge_base": models.RefinementKnowledgeBase,
    "refinement_qa_item": models.RefinementQAItem,
    "refinement_snapshot": models.RefinementSnapshot,
    "spec": models.Spec,
    "spec_history": models.SpecHistory,
    "spec_knowledge_base": models.SpecKnowledgeBase,
    "spec_qa_item": models.SpecQAItem,
    "sprint": models.Sprint,
    "sprint_history": models.SprintHistory,
    "sprint_qa_item": models.SprintQAItem,
    "story": models.Story,
    "story_ideation_link": models.StoryIdeationLink,
    "topic": models.Topic,
}
_CLASS_ENTITIES = {value: key for key, value in _ENTITY_CLASSES.items()}

_DIRECT_COMMIT_RECORDS_KEY = "okto_pulse.application_persistence.direct_commit_records"
_DIRECT_COMMIT_LISTENER_KEY = "okto_pulse.application_persistence.direct_commit_listener"


def _synchronize_records_before_direct_commit(sync_session: Any) -> None:
    entries = sync_session.info.get(_DIRECT_COMMIT_RECORDS_KEY, ())
    for record, row in tuple(entries):
        if not record.dirty_fields:
            continue
        if row not in sync_session or row in sync_session.deleted:
            continue
        for field_name in tuple(record.dirty_fields):
            setattr(row, field_name, copy.deepcopy(record.values[field_name]))
        record.dirty_fields.clear()


def _register_direct_commit_record(
    context: Any,
    record: ApplicationRecord,
    row: Any,
) -> None:
    sync_session = context.sync_session
    entries = sync_session.info.setdefault(_DIRECT_COMMIT_RECORDS_KEY, [])
    if all(existing is not record for existing, _ in entries):
        entries.append((record, row))
    if not sync_session.info.get(_DIRECT_COMMIT_LISTENER_KEY):
        event.listen(
            sync_session,
            "before_commit",
            _synchronize_records_before_direct_commit,
        )
        sync_session.info[_DIRECT_COMMIT_LISTENER_KEY] = True


def _model(entity: str):
    try:
        return _ENTITY_CLASSES[entity]
    except KeyError as exc:
        raise ValueError(f"unsupported_application_entity:{entity}") from exc


def _predicate(model: Any, item: ApplicationFilter):
    if model is models.Ideation and item.field == "derivation_pending":
        active_refinement_exists = (
            select(models.Refinement.id)
            .where(
                models.Refinement.board_id == models.Ideation.board_id,
                models.Refinement.ideation_id == models.Ideation.id,
                models.Refinement.archived == false(),
                models.Refinement.status != "cancelled",
            )
            .exists()
        )
        active_direct_spec_exists = (
            select(models.Spec.id)
            .where(
                models.Spec.board_id == models.Ideation.board_id,
                models.Spec.ideation_id == models.Ideation.id,
                models.Spec.refinement_id.is_(None),
                models.Spec.archived == false(),
                models.Spec.status != "cancelled",
            )
            .exists()
        )
        pending = func.coalesce(
            and_(
                models.Ideation.status == "done",
                or_(
                    and_(
                        models.Ideation.complexity.in_(("medium", "large")),
                        ~active_refinement_exists,
                    ),
                    and_(
                        models.Ideation.complexity == "small",
                        ~active_direct_spec_exists,
                    ),
                ),
            ),
            false(),
        )
        if item.operator == "is_true" or (
            item.operator == "eq" and item.value is True
        ):
            return pending
        if item.operator == "is_false" or (
            item.operator == "eq" and item.value is False
        ):
            return ~pending
        raise ValueError(f"unsupported_application_operator:{item.operator}")
    if model is models.Refinement and item.field == "derivation_pending":
        active_spec_exists = (
            select(models.Spec.id)
            .where(
                models.Spec.board_id == models.Refinement.board_id,
                models.Spec.refinement_id == models.Refinement.id,
                models.Spec.archived == false(),
                models.Spec.status != "cancelled",
            )
            .exists()
        )
        pending = and_(
            models.Refinement.status == "done",
            ~active_spec_exists,
        )
        if item.operator == "is_true" or (
            item.operator == "eq" and item.value is True
        ):
            return pending
        if item.operator == "is_false" or (
            item.operator == "eq" and item.value is False
        ):
            return ~pending
        raise ValueError(f"unsupported_application_operator:{item.operator}")
    if model is models.Story and item.field == "linked":
        link_exists = (
            select(models.StoryIdeationLink.id)
            .where(models.StoryIdeationLink.story_id == models.Story.id)
            .exists()
        )
        if item.operator == "is_true" or (
            item.operator == "eq" and item.value is True
        ):
            return link_exists
        if item.operator == "is_false" or (
            item.operator == "eq" and item.value is False
        ):
            return ~link_exists
        raise ValueError(f"unsupported_application_operator:{item.operator}")
    if model is models.Story and item.field == "converted":
        is_converted = models.Story.status == "converted"
        if item.operator == "is_true" or (
            item.operator == "eq" and item.value is True
        ):
            return is_converted
        if item.operator == "is_false" or (
            item.operator == "eq" and item.value is False
        ):
            return ~is_converted
        raise ValueError(f"unsupported_application_operator:{item.operator}")
    column = getattr(model, item.field)
    if item.operator == "eq":
        return column == item.value
    if item.operator == "ne":
        return column != item.value
    if item.operator == "in":
        return column.in_(tuple(item.value))
    if item.operator == "not_in":
        return column.notin_(tuple(item.value))
    if item.operator == "gte":
        return column >= item.value
    if item.operator == "lte":
        return column <= item.value
    if item.operator == "gt":
        return column > item.value
    if item.operator == "lt":
        return column < item.value
    if item.operator == "is_true":
        return column.is_(True)
    if item.operator == "is_false":
        return column.is_(False)
    if item.operator == "is_none":
        return column.is_(None)
    if item.operator == "not_none":
        return column.is_not(None)
    if item.operator == "contains":
        return column.contains(item.value)
    if item.operator == "ilike":
        return column.ilike(item.value)
    raise ValueError(f"unsupported_application_operator:{item.operator}")


def _load_option(model: Any, path: str):
    parts = path.split(".")
    relationship = getattr(model, parts[0])
    option = selectinload(relationship)
    related_model = relationship.property.mapper.class_
    for part in parts[1:]:
        relationship = getattr(related_model, part)
        option = option.selectinload(relationship)
        related_model = relationship.property.mapper.class_
    return option


def _relationship_includes(includes: tuple[str, ...], name: str) -> tuple[str, ...]:
    nested: list[str] = []
    for path in includes:
        head, separator, tail = path.partition(".")
        if head == name:
            nested.append(tail if separator else "")
    return tuple(item for item in nested if item)


def _record(entity: str, row: Any, includes: tuple[str, ...] = ()) -> ApplicationRecord:
    values = {
        column.key: copy.deepcopy(getattr(row, column.key))
        for column in row.__table__.columns
    }
    if entity == "architecture_design":
        parent_type = values.get("parent_type")
        values["parent_id"] = values.get(f"{parent_type}_id")
    top_level = {path.split(".", 1)[0] for path in includes}
    mapper = row.__mapper__
    for name in top_level:
        relationship = mapper.relationships.get(name)
        if relationship is None:
            continue
        if name not in row.__dict__:
            raise RuntimeError(f"application_include_not_loaded:{entity}.{name}")
        related = getattr(row, name)
        related_entity = _CLASS_ENTITIES[relationship.mapper.class_]
        nested = _relationship_includes(includes, name)
        if relationship.uselist:
            values[name] = [_record(related_entity, item, nested) for item in related]
        else:
            values[name] = (
                _record(related_entity, related, nested) if related is not None else None
            )
    return ApplicationRecord(entity=entity, values=values)


class TestSqlAlchemyApplicationPersistence:
    __test__ = False

    def __init__(self) -> None:
        self._tracked: WeakKeyDictionary[Any, list[ApplicationRecord]] = (
            WeakKeyDictionary()
        )

    def _track(
        self,
        context: Any,
        record: ApplicationRecord,
        row: Any,
    ) -> ApplicationRecord:
        records = self._tracked.setdefault(context, [])
        if all(existing is not record for existing in records):
            records.append(record)
        _register_direct_commit_record(context, record, row)
        return record

    def _clear_tracking(self, context: Any) -> None:
        self._tracked.pop(context, None)
        context.sync_session.info.pop(_DIRECT_COMMIT_RECORDS_KEY, None)

    async def list(
        self, context: Any, query: ApplicationQuery
    ) -> tuple[ApplicationRecord, ...]:
        model = _model(query.entity)
        statement = select(model)
        if query.filters:
            statement = statement.where(
                *(_predicate(model, item) for item in query.filters)
            )
        if query.any_filters:
            statement = statement.where(
                or_(*(_predicate(model, item) for item in query.any_filters))
            )
        if query.any_groups:
            statement = statement.where(
                or_(
                    *(and_(*(_predicate(model, item) for item in group)) for group in query.any_groups)
                )
            )
        if query.includes:
            statement = statement.options(
                *(_load_option(model, path) for path in query.includes)
            )
        for field_name, descending in query.order_by:
            column = getattr(model, field_name)
            statement = statement.order_by(
                column.desc() if descending else column.asc()
            )
        if query.offset:
            statement = statement.offset(query.offset)
        if query.limit is not None:
            statement = statement.limit(query.limit)
        rows = (
            await context.execute(statement.execution_options(populate_existing=True))
        ).scalars().all()
        return tuple(
            self._track(context, _record(query.entity, row, query.includes), row)
            for row in rows
        )

    async def count(self, context: Any, query: ApplicationQuery) -> int:
        model = _model(query.entity)
        statement = select(func.count()).select_from(model)
        if query.filters:
            statement = statement.where(
                *(_predicate(model, item) for item in query.filters)
            )
        if query.any_filters:
            statement = statement.where(
                or_(*(_predicate(model, item) for item in query.any_filters))
            )
        if query.any_groups:
            statement = statement.where(
                or_(
                    *(and_(*(_predicate(model, item) for item in group)) for group in query.any_groups)
                )
            )
        result = await context.execute(statement)
        return int(result.scalar_one())

    async def group_count(
        self, context: Any, query: ApplicationGroupCountQuery
    ) -> tuple[ApplicationGroupCount, ...]:
        """Test-port parity for catalogued server-side aggregates."""
        model = _model(query.entity)
        if not query.group_by:
            raise ValueError("application_group_count_fields_required")
        group_columns = tuple(getattr(model, field) for field in query.group_by)
        statement = select(
            *group_columns,
            func.count().label("count"),
        ).select_from(model)
        if query.filters:
            statement = statement.where(
                *(_predicate(model, item) for item in query.filters)
            )
        for dimension in query.disjunctions:
            if not dimension:
                raise ValueError("application_group_count_disjunction_empty")
            if any(not branch for branch in dimension):
                raise ValueError("application_group_count_branch_empty")
            statement = statement.where(
                or_(
                    *(
                        and_(*(_predicate(model, item) for item in branch))
                        for branch in dimension
                    )
                )
            )
        result = await context.execute(statement.group_by(*group_columns))
        return tuple(
            ApplicationGroupCount(values=tuple(row[:-1]), count=int(row[-1]))
            for row in result.all()
        )

    async def get(
        self,
        context: Any,
        *,
        entity: str,
        record_id: str,
        includes: tuple[str, ...] = (),
    ) -> ApplicationRecord | None:
        rows = await self.list(
            context,
            ApplicationQuery(
                entity=entity,
                filters=(ApplicationFilter("id", "eq", record_id),),
                includes=includes,
                limit=1,
            ),
        )
        return rows[0] if rows else None

    async def fence(
        self,
        context: Any,
        *,
        entity: str,
        record_id: str,
        expected_values: Mapping[str, object],
    ) -> bool:
        model = _model(entity)
        predicates = [model.id == record_id]
        for field_name, expected in expected_values.items():
            if field_name not in model.__table__.columns:
                raise ValueError(f"unsupported_application_fence_field:{field_name}")
            predicates.append(getattr(model, field_name) == expected)
        fence_values = {
            column.key: getattr(model, column.key)
            for column in model.__table__.columns
            if column.primary_key or column.onupdate is not None
        }
        try:
            result = await context.execute(
                update(model)
                .where(*predicates)
                .values(**fence_values)
                .execution_options(synchronize_session=False)
            )
        except OperationalError as exc:
            raw = getattr(exc, "orig", None)
            if getattr(raw, "sqlite_errorcode", None) == 517:
                return False
            raise
        return int(result.rowcount or 0) == 1

    async def add(
        self,
        context: Any,
        record: ApplicationRecord,
        *,
        conflict_error: Exception | None = None,
    ) -> ApplicationRecord:
        model = _model(record.entity)
        allowed = {column.key for column in model.__table__.columns}
        values = {
            key: copy.deepcopy(value)
            for key, value in record.values.items()
            if key in allowed
        }
        row = model(**values)
        context.add(row)
        try:
            await context.flush()
        except (IntegrityError, OperationalError) as exc:
            raw = getattr(exc, "orig", None)
            is_busy_snapshot = getattr(raw, "sqlite_errorcode", None) == 517
            message = str(exc).lower()
            table = "cards" if record.entity == "card" else "specs"
            is_target_collision = (
                f"unique constraint failed: {table}.id" in message
                or f"{table}_pkey" in message
            )
            if (
                isinstance(conflict_error, ApplicationRecordConflictError)
                and conflict_error.entity == record.entity
                and conflict_error.record_id == str(record.values.get("id") or "")
                and (is_busy_snapshot or is_target_collision)
            ):
                raise conflict_error from exc
            raise
        fresh = _record(record.entity, row)
        record.values.clear()
        record.values.update(fresh.values)
        record.dirty_fields.clear()
        return self._track(context, record, row)

    async def delete(self, context: Any, record: ApplicationRecord) -> None:
        row = await context.get(_model(record.entity), record.id)
        if row is not None:
            await context.delete(row)
            await context.flush()
        self._tracked[context] = [
            existing
            for existing in self._tracked.get(context, [])
            if existing is not record
        ]
        entries = context.sync_session.info.get(_DIRECT_COMMIT_RECORDS_KEY, [])
        entries[:] = [
            (existing, tracked_row)
            for existing, tracked_row in entries
            if not (
                existing.entity == record.entity
                and existing.values.get("id") == record.values.get("id")
            )
        ]

    async def flush(self, context: Any) -> None:
        for record in self._tracked.get(context, []):
            if not record.dirty_fields:
                continue
            row = await context.get(_model(record.entity), record.id)
            if row is None:
                continue
            for field_name in tuple(record.dirty_fields):
                setattr(row, field_name, copy.deepcopy(record.values[field_name]))
            record.dirty_fields.clear()
        await context.flush()

    async def refresh(
        self, context: Any, record: ApplicationRecord
    ) -> ApplicationRecord:
        await self.flush(context)
        row = await context.get(_model(record.entity), record.id)
        if row is None:
            raise ValueError(f"application record not found: {record.entity}:{record.id}")
        await context.refresh(row)
        fresh = _record(record.entity, row)
        record.values.clear()
        record.values.update(fresh.values)
        record.dirty_fields.clear()
        return record

    async def commit(self, context: Any) -> None:
        await self.flush(context)
        await context.commit()
        self._clear_tracking(context)

    async def rollback(self, context: Any) -> None:
        await context.rollback()
        self._clear_tracking(context)

    async def backfill_qa_answered_at(self, context: Any) -> dict[str, int]:
        from sqlalchemy import text

        tables = (
            ("ideation_qa_items", True),
            ("refinement_qa_items", True),
            ("spec_qa_items", True),
            ("sprint_qa_items", True),
            ("qa_items", False),
        )
        fixed: dict[str, int] = {}
        for table, has_selected in tables:
            answered = "(answer IS NOT NULL AND answer != '')"
            if has_selected:
                answered = (
                    f"({answered} OR (selected IS NOT NULL "
                    "AND CAST(selected AS TEXT) NOT IN ('', '[]', 'null')))"
                )
            result = await context.execute(
                text(
                    f"UPDATE {table} "
                    "SET answered_at = COALESCE(created_at, CURRENT_TIMESTAMP) "
                    f"WHERE answered_at IS NULL AND {answered}"
                )
            )
            count = result.rowcount if result.rowcount and result.rowcount > 0 else 0
            if count:
                fixed[table] = count
        await context.commit()
        return fixed


__all__ = ["TestSqlAlchemyApplicationPersistence"]

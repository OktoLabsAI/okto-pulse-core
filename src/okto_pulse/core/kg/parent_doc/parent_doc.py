"""Parent artifact resolver — parser + batch lookup.

Ideação fe55ff7c.
"""

from __future__ import annotations

import logging
from typing import Any

from okto_pulse.core.ports.parent_artifact import get_parent_artifact_read_port

logger = logging.getLogger("okto_pulse.kg.parent_doc")

#: Accepted artifact types. Tech entities (``tech_entities.yml``) and
#: other non-UUID refs that happen to exist in the corpus are NOT
#: parented — they return ``None`` from the parser.
_ACCEPTED_TYPES: tuple[str, ...] = ("spec", "sprint", "card")


def parse_artifact_ref(ref: str | None) -> tuple[str, str] | None:
    """Decompose a ``"type:uuid"`` ref. Returns ``None`` for:

    - Empty / None input.
    - Strings without ``":"`` (e.g. ``"tech_entities.yml"``).
    - Types outside the accepted set.
    """
    if not ref or ":" not in ref:
        return None
    artifact_type, _, uuid = ref.partition(":")
    if artifact_type not in _ACCEPTED_TYPES or not uuid:
        return None
    return artifact_type, uuid


async def resolve_parent_artifacts(
    db, refs: list[str]
) -> dict[str, dict[str, Any]]:
    """Batch-resolve a list of source_artifact_refs.

    Groups by artifact type and emits at most one SQL query per type.
    Returns a mapping ``{ref: parent_payload}`` where each payload has:
    ``{type, id, title, status}``. Refs that don't parse OR point to
    a UUID that doesn't exist in the DB are silently omitted — the
    caller interprets absence as "no parent" and keeps the row with
    ``parent_artifact=None``.

    The DB param is an async SQLAlchemy session. The function is
    async because the project's DB layer is async.
    """
    by_type: dict[str, set[str]] = {"spec": set(), "sprint": set(), "card": set()}
    ref_to_key: dict[str, tuple[str, str]] = {}
    for ref in refs:
        parsed = parse_artifact_ref(ref)
        if parsed is None:
            continue
        artifact_type, uuid = parsed
        by_type[artifact_type].add(uuid)
        ref_to_key[ref] = parsed

    out: dict[str, dict[str, Any]] = {}
    payload_by_key: dict[tuple[str, str], dict[str, Any]] = {}

    for artifact_type, ids in by_type.items():
        if not ids:
            continue
        try:
            records = await get_parent_artifact_read_port().read_many(
                db,
                artifact_type=artifact_type,
                ids=frozenset(ids),
            )
            for record in records:
                payload_by_key[(artifact_type, record.id)] = {
                    "type": artifact_type,
                    "id": record.id,
                    "title": record.title,
                    "status": record.status,
                }
        except Exception as e:  # noqa: BLE001 — any DB failure degrades silently
            logger.warning(
                "parent_doc.lookup_failed type=%s error=%s",
                artifact_type,
                type(e).__name__,
            )

    for ref, key in ref_to_key.items():
        payload = payload_by_key.get(key)
        if payload is not None:
            out[ref] = payload
    return out

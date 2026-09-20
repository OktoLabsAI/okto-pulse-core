"""Bounded discovery of explicit historical source references, without mutation.

These matches locate migration evidence; they neither authorize disclosure nor
prove that a reference remains operational. Prose mentioning Sprint is not a link.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EmbeddedSprintReference:
    path: tuple[str | int, ...]
    sprint_id: str
    board_hint: str | None
    form: str


_IDENTITIES = {"sprint_id", "origin_sprint_id", "source_sprint_id"}
_REF_FIELDS = {"source_ref", "source_refs", "evidence_ref", "evidence_refs", "membership_source_ref"}


def _identity(value):
    if not isinstance(value, str) or not value or value.strip() != value:
        raise ValueError("sprint_embedded_reference_identity_invalid")
    return value


def find_embedded_sprint_references(payload: object, *, field_name: str | None = None) -> tuple[EmbeddedSprintReference, ...]:
    """Inspect documented key shapes; preserve exact paths and opaque IDs.

    Plain strings are recognized only in reference fields, never arbitrary
    titles/comments. JSON pointer escaping belongs to the consuming presentation.
    Limits fail closed even when the matching reference would occur late.
    """
    found = []
    stack = [(payload, (), 0, field_name)]
    visited = 0
    while stack:
        value, path, depth, context = stack.pop()
        visited += 1
        if visited > 5000 or depth > 32:
            raise ValueError("sprint_embedded_reference_structure_limit")
        if isinstance(value, dict):
            if visited + len(stack) + len(value) > 5000:
                raise ValueError("sprint_embedded_reference_structure_limit")
            if any(not isinstance(key, str) for key in value):
                raise ValueError("sprint_embedded_reference_key_invalid")
            for key, item in value.items():
                if key in _IDENTITIES and item is not None:
                    hint = value.get("source_board_id", value.get("board_id"))
                    found.append(EmbeddedSprintReference(path + (key,), _identity(item),
                        None if hint is None else _identity(hint), "identity_field"))
            for prefix in ("artifact", "subject", "entity", "source"):
                if value.get(f"{prefix}_type") != "sprint":
                    continue
                key = f"{prefix}_id"
                # Policy receipt snapshots use entity_type with subject_id.
                if prefix == "entity" and key not in value and "subject_id" in value:
                    key = "subject_id"
                hint = value.get(f"{prefix}_board_id", value.get("board_id"))
                found.append(EmbeddedSprintReference(path + (key,), _identity(value.get(key)),
                    None if hint is None else _identity(hint), "typed_identity"))
            if isinstance(value.get("field"), str) and value["field"] in _IDENTITIES:
                for key in ("old", "new", "old_value", "new_value", "value"):
                    item = value.get(key)
                    if item is None:
                        continue
                    if isinstance(item, list) and len(item) > 5000:
                        raise ValueError("sprint_embedded_reference_structure_limit")
                    values = list(enumerate(item)) if isinstance(item, list) else [(None, item)]
                    for index, identity in values:
                        location = path + (key,) + (() if index is None else (index,))
                        found.append(EmbeddedSprintReference(location, _identity(identity), None, "field_value"))
            stack.extend((item, path + (key,), depth + 1, key) for key, item in reversed(tuple(value.items())))
        elif isinstance(value, list):
            if visited + len(stack) + len(value) > 5000:
                raise ValueError("sprint_embedded_reference_structure_limit")
            stack.extend((item, path + (index,), depth + 1, context) for index, item in reversed(tuple(enumerate(value))))
        elif isinstance(value, str) and context in _REF_FIELDS and value.startswith("sprint:"):
            found.append(EmbeddedSprintReference(path, _identity(value.split(":", 2)[1]), None, "source_ref"))
    return tuple(found)

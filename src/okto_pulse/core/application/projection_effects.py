"""Capture net property observations through the existing transaction Protocol."""

import json

from okto_pulse.core.ports.projection_effects import ProjectionPropertyEffect, ProjectionPropertyEffects


def capture_projection_property_effects(scope, records, *, board_id, session_id):
    created = {(record.entity_type, record.entity_id) for record in records if record.kind == 'node'}
    previous = {}
    for record in records:
        image = record.property_before_image
        if image is None or (image.node_type, image.node_id) in created:
            continue
        before = previous.setdefault((image.node_type, image.node_id), {})
        for name, value in image.attrs.items():
            before.setdefault(name, value)  # First before-image, even across repeated writes.
    effects = []
    for (kind, identity), before in sorted(previous.items()):
        after = scope.snapshot_node_properties(kind, identity, tuple(sorted(before)))
        if (after is None or (after.node_type, after.node_id) != (kind, identity)
                or set(after.attrs) != set(before)):
            raise ValueError('projection_effect_final_snapshot_missing')
        # JSON equality retains bool/int/float distinctions. No-op protection
        # records do not create an invented mutation in the observation.
        def encoded(value):
            return json.dumps(value, sort_keys=True, ensure_ascii=True, separators=(',', ':'), allow_nan=False)
        changed = [name for name in before if encoded(before[name]) != encoded(after.attrs[name])]
        if changed:
            effects.append(ProjectionPropertyEffect.from_values(kind, identity,
                {name: before[name] for name in changed}, {name: after.attrs[name] for name in changed}))
    return ProjectionPropertyEffects(board_id, session_id, tuple(effects)) if effects else None

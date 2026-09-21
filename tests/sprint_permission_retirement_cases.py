"""Deterministic input population for the pre-retirement permission comparison."""
from copy import deepcopy


def cases(fixture):
    full = fixture['registry']
    presets = fixture['presets']
    yield 'trusted', dict(agent_flags=None, board_overrides=None, preset_id=None, presets=())
    for name, flags in [('current', full), ('original', fixture['original'])]:
        yield name, dict(agent_flags=flags, board_overrides=None, preset_id=None, presets=())
    for preset in presets:
        yield 'preset:' + preset['name'], dict(agent_flags=None, board_overrides=None,
            preset_id='base', presets=(dict(id='base', flags=preset['flags']),))
    for path in fixture['sprint_flags']:
        for mutation in ('false', 'missing', 'integer'):
            flags = deepcopy(full)
            parent = flags
            parts = path.split('.')
            for part in parts[:-1]:
                parent = parent[part]
            if mutation == 'missing':
                del parent[parts[-1]]
            else:
                parent[parts[-1]] = False if mutation == 'false' else 1
            for layer in ('agent', 'preset', 'board'):
                yield f'{layer}:{path}:{mutation}', dict(
                    agent_flags=flags if layer == 'agent' else None,
                    board_overrides=flags if layer == 'board' else None,
                    preset_id='base' if layer == 'preset' else None,
                    presets=(dict(id='base', flags=flags),) if layer == 'preset' else ())
    for value in (None, False, {}, {'qa': False}, {'extension': False}):
        for layer in ('agent', 'preset', 'board'):
            flags = deepcopy(full)
            flags['sprint'] = value
            yield f'shape:{layer}:{value!r}', dict(
                agent_flags=flags if layer == 'agent' else None,
                board_overrides=flags if layer == 'board' else None,
                preset_id='base' if layer == 'preset' else None,
                presets=(dict(id='base', flags=flags),) if layer == 'preset' else ())
    for preset in presets:
        for denied in ('card.entity.assign', 'spec.qa.ask'):
            parent, branch, leaf = denied.split('.')
            yield f'child:{preset["name"]}:{denied}', dict(agent_flags=None, board_overrides=None,
                preset_id='child', presets=(dict(id='base', flags=preset['flags']),
                    dict(id='child', flags={parent: {branch: {leaf: False}}}, base_preset_id='base')))

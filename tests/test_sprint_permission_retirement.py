"""Compare every surviving access decision against the captured installed baseline."""
import json
from pathlib import Path

import pytest

from okto_pulse.core.domain.permissions import PERMISSION_INTRODUCTION_MANIFESTS
from okto_pulse.core.domain.sdlc_registry import SDLC_REGISTRY
from okto_pulse.core.ports.permission_policy import (
    PermissionPresetLineageNode, PermissionSet, builtin_permission_presets,
    flatten_permission_flags, registered_permission_flags, resolve_agent_permission_facts,
)
from sprint_permission_retirement_cases import cases
from okto_pulse.core.ports.permission_retirement import (
    capture_permission_retirement_authority,
    retired_feature_permission_flags,
    require_permission_retirement_parity,
)

FIXTURE = json.loads((Path(__file__).parent/'fixtures/sprint_permission_retirement_baseline.json').read_text())
RETIRED = frozenset(retired_feature_permission_flags())
SURVIVING = tuple(p for p in FIXTURE['surviving_flags'] if p not in RETIRED)


@pytest.mark.parametrize('name,inputs', list(cases(FIXTURE)), ids=[f'policy-{i:03d}' for i, _ in enumerate(cases(FIXTURE))])
def test_surviving_access_and_review_equal_pre_retirement(name, inputs):
    inputs = {**inputs, 'presets': tuple(PermissionPresetLineageNode(**node) for node in inputs['presets'])}
    result = resolve_agent_permission_facts(**inputs, legacy_permissions=None)
    if name == 'current':
        # This intermediate ea8b62a registry had already lost part of the
        # original KG generation. It is not a trusted v0.3.4 Full Control
        # fingerprint. Use the frozen original authority, never bless a
        # smaller all-True registry merely because more operations retired.
        source = capture_permission_retirement_authority(
            **inputs, legacy_permissions=None,
        )
        assert source.owner_review_required
        assert source.review_reason == 'unrecognized_direct_permissions'
        require_permission_retirement_parity(source, result, retired_flags=tuple(sorted(RETIRED)))
        return
    expected = FIXTURE['decisions'][name]
    vector = ''.join('1' if result.has(path) else '0' for path in FIXTURE['surviving_flags'])
    changed = [path for path, before, after in zip(FIXTURE['surviving_flags'], expected['vector'], vector, strict=True) if path not in RETIRED and before != after]
    assert not changed
    assert (result.owner_review_required, result.review_reason) == (expected['review'], expected['reason'])
    assert not any(result.has(path) for path in FIXTURE['sprint_flags'])


def test_sprint_has_no_live_lifecycle_permissions_or_new_builtin_preset():
    assert 'sprint' not in SDLC_REGISTRY
    assert set(flatten_permission_flags(registered_permission_flags())) == set(SURVIVING)
    assert all(not path.startswith('sprint.') for m in PERMISSION_INTRODUCTION_MANIFESTS for path in m.leaves)
    assert all(p['name'] != 'Sprint Manager' and 'sprint' not in p['flags'] for p in builtin_permission_presets())
    assert not PermissionSet({}).has('sprint.entity.read')


@pytest.mark.parametrize('preset', builtin_permission_presets(), ids=lambda p: p['name'])
def test_surviving_builtin_preset_grants_are_unchanged(preset):
    previous = next(p for p in FIXTURE['presets'] if p['name'] == preset['name'])
    old, new = PermissionSet(previous['flags']), PermissionSet(preset['flags'])
    assert [p for p in SURVIVING if old.has(p) != new.has(p)] == []

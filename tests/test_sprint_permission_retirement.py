"""Sprint is absent from the current permission and lifecycle contract."""
from okto_pulse.core.domain.permissions import PERMISSION_INTRODUCTION_MANIFESTS
from okto_pulse.core.domain.sdlc_registry import SDLC_REGISTRY
from okto_pulse.core.ports.permission_policy import (
    PermissionSet, builtin_permission_presets,
    flatten_permission_flags, registered_permission_flags,
)

def test_sprint_has_no_live_lifecycle_permissions_or_new_builtin_preset():
    assert 'sprint' not in SDLC_REGISTRY
    assert all(not path.startswith('sprint.') for path in flatten_permission_flags(registered_permission_flags()))
    assert all(not path.startswith('sprint.') for m in PERMISSION_INTRODUCTION_MANIFESTS for path in m.leaves)
    assert all(p['name'] != 'Sprint Manager' and 'sprint' not in p['flags'] for p in builtin_permission_presets())
    assert not PermissionSet({}).has('sprint.entity.read')

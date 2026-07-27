"""R11-D (CORE) — pure capability descriptor port + classification + scope gate.

Scenario mapping (spec ts_ ids):
  ts_a7a03ffb (TS01) — the pure DTOs import in isolation: core.ports.capability_
       descriptor pulls NO community / MCP server / discovery. + conformance.
  ts_ca2b389a (TS03) — classification is VIA the descriptors (changing the
       descriptors changes the classification), NOT a hard-coded table.
  ts_4cef79b5 (TS06, core slice) — the scope gate flags a SaaS/multi-tenant
       descriptor; a backend-only descriptor set passes.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

from okto_pulse.core.ports.capability_descriptor import (
    CapabilityDescriptor,
    CapabilityDescriptorSource,
    capability_scope_violations,
    classify_resources,
    descriptors_to_dicts,
)
from okto_pulse.core.ports.mcp_resources import McpResourceSpec


# ===========================================================================
# ts_a7a03ffb (TS01) — pure import (subprocess) + conformance.
# ===========================================================================
def test_ts_a7a03ffb_pure_import_no_community_server_discovery():
    code = (
        "import sys\n"
        "import okto_pulse.core.ports.capability_descriptor as cd\n"
        "from okto_pulse.core.ports import (CapabilityDescriptor, "
        "CapabilityDescriptorSource, classify_resources, capability_scope_violations)\n"
        "bad = [m for m in sys.modules if m.startswith('okto_pulse.community')]\n"
        "assert bad == [], bad\n"
        "assert 'okto_pulse.core.mcp.server' not in sys.modules\n"
        "assert 'fastmcp' not in sys.modules\n"
        "assert not any('discovery' in m for m in sys.modules if m.startswith('okto_pulse.core')), 'discovery leaked'\n"
        "print('PURE_OK')\n"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "PURE_OK" in proc.stdout


def test_ts_a7a03ffb_conformance_and_dto_determinism():
    # CapabilityDescriptorSource is a runtime_checkable Protocol -> isinstance.
    class _Src:
        def descriptors(self):
            return ()

    assert isinstance(_Src(), CapabilityDescriptorSource)
    assert not isinstance(object(), CapabilityDescriptorSource)

    # CapabilityDescriptor DTO: deterministic + serializable round-trip.
    d = CapabilityDescriptor(
        id="provider:kg_registry", kind="provider", provider="community-runtime",
        edition="community", capability="kg",
        metadata={"z": "1", "a": "2"},
    )
    # metadata serialized sorted (determinism); serialize twice -> identical.
    assert list(d.to_dict()["metadata"]) == ["a", "z"]
    assert d.to_dict() == d.to_dict()
    assert CapabilityDescriptor.from_dict(d.to_dict()) == d

    # metadata is IMMUTABLE (R05-A lesson): stored as a tuple; mutating the input
    # dict AFTER construction does NOT change the descriptor.
    src_md = {"k": "v"}
    d_imm = CapabilityDescriptor(id="p:x", kind="provider", provider="p", edition="e",
                                 capability="c", metadata=src_md)
    src_md["k"] = "MUTATED"
    src_md["new"] = "leak"
    assert d_imm.metadata_dict == {"k": "v"}  # unaffected by the input mutation
    assert isinstance(d_imm.metadata, tuple)  # stored immutably
    # validation has teeth.
    with pytest.raises(ValueError):
        CapabilityDescriptor(id="x", kind="bogus", provider="p", edition="e", capability="c")
    with pytest.raises(ValueError):
        CapabilityDescriptor(id="", kind="provider", provider="p", edition="e", capability="c")
    # serialize a set deterministically (sorted by id).
    other = CapabilityDescriptor(id="capability:kg", kind="capability", provider="p",
                                 edition="e", capability="kg")
    assert [x["id"] for x in descriptors_to_dicts((d, other))] == [
        "capability:kg", "provider:kg_registry",
    ]


# ===========================================================================
# ts_ca2b389a (TS03) — classification VIA descriptors (not hard-coded).
# ===========================================================================
def test_ts_ca2b389a_classification_follows_descriptors():
    spec_kg = McpResourceSpec(
        uri="okto-pulse://workflows/kg", description="d", category="workflows",
        edition="community", capability="kg", provider="community-embedded-kg",
        content="body",
    )
    spec_other = McpResourceSpec(
        uri="okto-pulse://workflows/specs", description="d", category="workflows",
        edition="core", capability="spec", content="body",
    )
    kg_desc = CapabilityDescriptor(
        id="capability:kg_backend", kind="capability", provider="community-embedded-kg",
        edition="community", capability="kg",
    )

    # With the kg descriptor present -> the kg spec is classified to it; the
    # spec with an unmatched capability is NOT classified.
    cls = {c["uri"]: c for c in classify_resources((spec_kg, spec_other), (kg_desc,))}
    assert cls["okto-pulse://workflows/kg"]["classified"] is True
    assert cls["okto-pulse://workflows/kg"]["descriptor_id"] == "capability:kg_backend"
    assert cls["okto-pulse://workflows/specs"]["classified"] is False

    # REMOVE the descriptor -> the SAME kg spec is now unclassified (proving the
    # classification follows the descriptors, not a hard-coded table).
    cls2 = {c["uri"]: c for c in classify_resources((spec_kg,), ())}
    assert cls2["okto-pulse://workflows/kg"]["classified"] is False

    # provider mismatch -> not classified (descriptor names a different provider).
    other_provider = CapabilityDescriptor(
        id="capability:kg_x", kind="capability", provider="some-other-kg",
        edition="community", capability="kg",
    )
    cls3 = {c["uri"]: c for c in classify_resources((spec_kg,), (other_provider,))}
    assert cls3["okto-pulse://workflows/kg"]["classified"] is False


# ===========================================================================
# ts_4cef79b5 (TS06, core slice) — scope gate: no SaaS provider.
# ===========================================================================
def test_ts_4cef79b5_scope_gate_flags_saas_provider():
    clean = (
        CapabilityDescriptor(id="provider:kg_registry", kind="provider",
                             provider="community-runtime", edition="community", capability="kg"),
        CapabilityDescriptor(id="capability:storage", kind="capability",
                             provider="community-embedded-kg", edition="community",
                             capability="storage", metadata={"backend": "embedded-graph-db"}),
    )
    assert capability_scope_violations(clean) == ()

    saas = (
        CapabilityDescriptor(id="provider:billing", kind="provider",
                             provider="saas-billing", edition="community", capability="billing"),
        CapabilityDescriptor(id="capability:tenancy", kind="capability",
                             provider="p", edition="e", capability="kg",
                             metadata={"mode": "multi_tenant"}),
    )
    violations = {v["id"] for v in capability_scope_violations(saas)}
    assert violations == {"provider:billing", "capability:tenancy"}

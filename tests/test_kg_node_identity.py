"""MKG-A C1 — NodeIdentityPolicy: determinism, natural key and generation.

Covers spec MKG-A-S1 scenario S1 (AC1): same inputs => same id across
repeated executions; any component change => different id; unicode
NFKC/casefold normalization of the content key; id format contract.
"""

from __future__ import annotations

import re

from okto_pulse.core.kg.node_identity import derive_natural_key, mint_node_id

BOARD = "2cd4d5ac-054c-4fa7-bc77-bdab3213322e"

ID_FORMAT = re.compile(r"^[a-z_]+_[0-9a-f]{24}$")


class TestMintDeterminism:
    def test_same_inputs_same_id_100_runs(self):
        ids = {
            mint_node_id(BOARD, "Learning", "spec:abc:learning:1", 0)
            for _ in range(100)
        }
        assert len(ids) == 1

    def test_id_format(self):
        node_id = mint_node_id(BOARD, "Learning", "spec:abc", 0)
        assert ID_FORMAT.match(node_id), node_id
        assert node_id.startswith("learning_")

    def test_each_component_changes_id(self):
        base = mint_node_id(BOARD, "Learning", "spec:abc", 0)
        assert mint_node_id("other-board", "Learning", "spec:abc", 0) != base
        assert mint_node_id(BOARD, "Assumption", "spec:abc", 0) != base
        assert mint_node_id(BOARD, "Learning", "spec:xyz", 0) != base
        assert mint_node_id(BOARD, "Learning", "spec:abc", 1) != base

    def test_generation_monotonic_distinct_and_stable(self):
        gen_ids = [mint_node_id(BOARD, "Decision", "spec:abc", g) for g in range(4)]
        assert len(set(gen_ids)) == 4
        # Re-execution of the same supersede mints the SAME successor id (AC2).
        assert mint_node_id(BOARD, "Decision", "spec:abc", 3) == gen_ids[3]

    def test_negative_generation_rejected(self):
        try:
            mint_node_id(BOARD, "Learning", "spec:abc", -1)
        except ValueError:
            pass
        else:  # pragma: no cover - defensive
            raise AssertionError("negative generation must raise ValueError")


class TestDeriveNaturalKey:
    def test_ref_takes_precedence(self):
        key = derive_natural_key("spec:abc:learning:1", "Learning", "Qualquer título")
        assert key == "spec:abc:learning:1"

    def test_ref_whitespace_trimmed(self):
        assert derive_natural_key("  spec:abc  ", "Learning", None) == "spec:abc"

    def test_empty_ref_falls_back_to_content_key(self):
        key = derive_natural_key("", "Learning", "Retry com backoff resolve lock")
        assert key.startswith("content:")
        assert len(key) == len("content:") + 16

    def test_content_key_nfkc_casefold(self):
        # Composed vs decomposed unicode + case variations derive the SAME key.
        composed = derive_natural_key("", "Learning", "Título É importante")
        decomposed = derive_natural_key("", "Learning", "Título É importante")
        upper = derive_natural_key("", "Learning", "TÍTULO É IMPORTANTE")
        assert composed == decomposed == upper

    def test_content_key_sensitive_to_type_and_title(self):
        a = derive_natural_key("", "Learning", "Título")
        assert derive_natural_key("", "Assumption", "Título") != a
        assert derive_natural_key("", "Learning", "Outro título") != a

    def test_none_inputs_are_safe(self):
        key = derive_natural_key(None, "Learning", None)
        assert key.startswith("content:")


class TestEndToEndRecipe:
    def test_ref_and_content_key_families_never_collide(self):
        via_ref = mint_node_id(
            BOARD, "Learning", derive_natural_key("content:abc", "Learning", None), 0
        )
        via_content = mint_node_id(
            BOARD, "Learning", derive_natural_key("", "Learning", "abc"), 0
        )
        assert via_ref != via_content

    def test_purity_no_io_imports(self):
        import okto_pulse.core.kg.node_identity as mod

        # stdlib-only, no I/O modules (TR1) — auditable by module surface.
        forbidden = {"os", "io", "sqlalchemy", "requests", "pathlib"}
        assert not (set(dir(mod)) & forbidden)

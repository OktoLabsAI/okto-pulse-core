from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import MappingProxyType

import pytest

from okto_pulse.core.domain.quality_canonicalization import taxonomy_digest_v1
from okto_pulse.core.domain.quality_taxonomy import (
    AMBIGUITY_TAXONOMY_CATALOG_V1,
    AMBIGUITY_TAXONOMY_CATEGORY_IDS,
    AMBIGUITY_TAXONOMY_CATEGORY_ID_SET,
    AMBIGUITY_TAXONOMY_DIGEST,
    AMBIGUITY_TAXONOMY_MANIFEST_V1,
    AMBIGUITY_TAXONOMY_VERSION,
    AmbiguityTaxonomyError,
    require_ambiguity_category,
)

EXPECTED_CATEGORY_IDS = (
    "functional_scope_behavior",
    "domain_data_model",
    "interaction_ux_flow",
    "nonfunctional_quality",
    "integration_dependencies",
    "edge_failure_handling",
    "constraints_tradeoffs",
    "terminology_consistency",
    "acceptance_measurability",
    "traceability_coverage",
)


def test_v1_identity_and_append_only_order_are_exactly_normative() -> None:
    assert AMBIGUITY_TAXONOMY_VERSION == "ambiguity_taxonomy/v1"
    assert isinstance(AMBIGUITY_TAXONOMY_CATALOG_V1, tuple)
    assert AMBIGUITY_TAXONOMY_CATEGORY_IDS == EXPECTED_CATEGORY_IDS
    assert (
        tuple(category.category_id for category in AMBIGUITY_TAXONOMY_CATALOG_V1)
        == EXPECTED_CATEGORY_IDS
    )
    assert len(AMBIGUITY_TAXONOMY_CATALOG_V1) == 10
    assert len(set(AMBIGUITY_TAXONOMY_CATEGORY_IDS)) == 10
    assert AMBIGUITY_TAXONOMY_CATEGORY_ID_SET == frozenset(EXPECTED_CATEGORY_IDS)


def test_every_category_has_stable_bilingual_normative_text() -> None:
    for category in AMBIGUITY_TAXONOMY_CATALOG_V1:
        assert category.title_en.strip() == category.title_en
        assert category.title_pt.strip() == category.title_pt
        assert category.description_en.strip() == category.description_en
        assert category.description_pt.strip() == category.description_pt
        assert all(
            (
                category.title_en,
                category.title_pt,
                category.description_en,
                category.description_pt,
            )
        )


def test_catalog_and_manifest_are_recursively_immutable() -> None:
    category = AMBIGUITY_TAXONOMY_CATALOG_V1[0]
    with pytest.raises(FrozenInstanceError):
        category.title_en = "mutated"  # type: ignore[misc]

    assert isinstance(AMBIGUITY_TAXONOMY_MANIFEST_V1, MappingProxyType)
    with pytest.raises(TypeError):
        AMBIGUITY_TAXONOMY_MANIFEST_V1["ordering"] = "mutated"  # type: ignore[index]

    manifest_categories = AMBIGUITY_TAXONOMY_MANIFEST_V1["categories"]
    assert isinstance(manifest_categories, tuple)
    assert all(isinstance(item, MappingProxyType) for item in manifest_categories)
    with pytest.raises(TypeError):
        manifest_categories[0]["title_en"] = "mutated"  # type: ignore[index]


def test_manifest_preserves_catalog_order_and_digest_contract() -> None:
    manifest_categories = AMBIGUITY_TAXONOMY_MANIFEST_V1["categories"]
    assert tuple(item["category_id"] for item in manifest_categories) == (
        EXPECTED_CATEGORY_IDS
    )
    assert AMBIGUITY_TAXONOMY_DIGEST == taxonomy_digest_v1(
        AMBIGUITY_TAXONOMY_VERSION,
        AMBIGUITY_TAXONOMY_MANIFEST_V1,
    )
    assert (
        AMBIGUITY_TAXONOMY_DIGEST
        == "936698f9d4d2a7b4433a42b0fae6b3a5ba6616d68e30111bd2b87aeedcb2f439"
    )
    assert len(AMBIGUITY_TAXONOMY_DIGEST) == 64


@pytest.mark.parametrize("category_id", EXPECTED_CATEGORY_IDS)
def test_lookup_returns_the_canonical_catalog_object(category_id: str) -> None:
    resolved = require_ambiguity_category(category_id)
    expected = next(
        category
        for category in AMBIGUITY_TAXONOMY_CATALOG_V1
        if category.category_id == category_id
    )
    assert resolved is expected


@pytest.mark.parametrize(
    ("category_id", "error_code"),
    [
        ("new_unversioned_category", "ambiguity_category_unknown"),
        (" acceptance_measurability", "ambiguity_category_id_invalid"),
        ("ACCEPTANCE_MEASURABILITY", "ambiguity_category_unknown"),
        ("", "ambiguity_category_id_invalid"),
        (None, "ambiguity_category_id_invalid"),
    ],
)
def test_lookup_is_closed_and_never_normalizes_or_invents_categories(
    category_id: object,
    error_code: str,
) -> None:
    with pytest.raises(AmbiguityTaxonomyError) as raised:
        require_ambiguity_category(category_id)  # type: ignore[arg-type]

    assert raised.value.code == error_code
    assert raised.value.category_id == category_id

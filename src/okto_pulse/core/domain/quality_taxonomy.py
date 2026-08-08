"""Shared, immutable ambiguity taxonomy for SK-A quality assessments.

``ambiguity_taxonomy/v1`` is a normative persisted identity. Its ten category
codes and their order are frozen. A successor taxonomy may preserve this tuple
as an append-only prefix, but must publish a new version and digest rather than
mutating v1.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Final

from okto_pulse.core.domain.quality_canonicalization import taxonomy_digest_v1

AMBIGUITY_TAXONOMY_VERSION: Final = "ambiguity_taxonomy/v1"


class AmbiguityTaxonomyError(ValueError):
    """A category lookup or catalog value violated the closed v1 contract."""

    def __init__(self, code: str, category_id: object | None = None) -> None:
        self.code = code
        self.category_id = category_id
        detail = f":{category_id}" if category_id is not None else ""
        super().__init__(f"{code}{detail}")


@dataclass(frozen=True, slots=True)
class AmbiguityCategory:
    """One stable bilingual category definition."""

    category_id: str
    title_en: str
    title_pt: str
    description_en: str
    description_pt: str

    def __post_init__(self) -> None:
        for field_name in (
            "category_id",
            "title_en",
            "title_pt",
            "description_en",
            "description_pt",
        ):
            value = getattr(self, field_name)
            if (
                not isinstance(value, str)
                or not value.strip()
                or value != value.strip()
            ):
                raise AmbiguityTaxonomyError(
                    "ambiguity_category_definition_invalid",
                    self.category_id,
                )


AMBIGUITY_TAXONOMY_CATALOG_V1: Final[tuple[AmbiguityCategory, ...]] = (
    AmbiguityCategory(
        category_id="functional_scope_behavior",
        title_en="Functional scope and behavior",
        title_pt="Escopo funcional e comportamento",
        description_en=(
            "Ambiguity in intended capabilities, actors, boundaries, triggers, "
            "or observable behavior."
        ),
        description_pt=(
            "Ambiguidade nas capacidades pretendidas, atores, limites, gatilhos "
            "ou comportamento observável."
        ),
    ),
    AmbiguityCategory(
        category_id="domain_data_model",
        title_en="Domain and data model",
        title_pt="Domínio e modelo de dados",
        description_en=(
            "Ambiguity in domain entities, data fields, relationships, ownership, "
            "lifecycle, or invariants."
        ),
        description_pt=(
            "Ambiguidade nas entidades de domínio, campos de dados, relações, "
            "responsabilidade, ciclo de vida ou invariantes."
        ),
    ),
    AmbiguityCategory(
        category_id="interaction_ux_flow",
        title_en="Interaction and UX flow",
        title_pt="Interação e fluxo de UX",
        description_en=(
            "Ambiguity in user journeys, interaction states, transitions, "
            "navigation, or user feedback."
        ),
        description_pt=(
            "Ambiguidade nas jornadas do usuário, estados de interação, "
            "transições, navegação ou feedback ao usuário."
        ),
    ),
    AmbiguityCategory(
        category_id="nonfunctional_quality",
        title_en="Non-functional quality",
        title_pt="Qualidade não funcional",
        description_en=(
            "Ambiguity in measurable performance, security, reliability, "
            "availability, privacy, scalability, or observability expectations."
        ),
        description_pt=(
            "Ambiguidade nas expectativas mensuráveis de desempenho, segurança, "
            "confiabilidade, disponibilidade, privacidade, escalabilidade ou "
            "observabilidade."
        ),
    ),
    AmbiguityCategory(
        category_id="integration_dependencies",
        title_en="Integration dependencies",
        title_pt="Dependências de integração",
        description_en=(
            "Ambiguity in external systems, APIs, protocols, exchanged data, "
            "versions, ownership, or dependency behavior."
        ),
        description_pt=(
            "Ambiguidade nos sistemas externos, APIs, protocolos, dados trocados, "
            "versões, responsabilidade ou comportamento das dependências."
        ),
    ),
    AmbiguityCategory(
        category_id="edge_failure_handling",
        title_en="Edge cases and failure handling",
        title_pt="Casos extremos e tratamento de falhas",
        description_en=(
            "Ambiguity in boundary conditions, invalid inputs, partial failures, "
            "timeouts, retries, recovery, or fallback behavior."
        ),
        description_pt=(
            "Ambiguidade nas condições de limite, entradas inválidas, falhas "
            "parciais, timeouts, retentativas, recuperação ou fallback."
        ),
    ),
    AmbiguityCategory(
        category_id="constraints_tradeoffs",
        title_en="Constraints and trade-offs",
        title_pt="Restrições e trade-offs",
        description_en=(
            "Ambiguity in technical, business, or regulatory constraints, "
            "considered alternatives, or accepted trade-offs."
        ),
        description_pt=(
            "Ambiguidade nas restrições técnicas, de negócio ou regulatórias, "
            "alternativas consideradas ou trade-offs aceitos."
        ),
    ),
    AmbiguityCategory(
        category_id="terminology_consistency",
        title_en="Terminology consistency",
        title_pt="Consistência terminológica",
        description_en=(
            "Ambiguity caused by undefined, conflicting, overloaded, or "
            "inconsistently used terms and aliases."
        ),
        description_pt=(
            "Ambiguidade causada por termos e aliases indefinidos, conflitantes, "
            "sobrecarregados ou usados de forma inconsistente."
        ),
    ),
    AmbiguityCategory(
        category_id="acceptance_measurability",
        title_en="Acceptance measurability",
        title_pt="Mensurabilidade do aceite",
        description_en=(
            "Ambiguity in the conditions, observable outcomes, signals, or "
            "thresholds that prove acceptance."
        ),
        description_pt=(
            "Ambiguidade nas condições, resultados observáveis, sinais ou "
            "limiares que comprovam o aceite."
        ),
    ),
    AmbiguityCategory(
        category_id="traceability_coverage",
        title_en="Traceability coverage",
        title_pt="Cobertura de rastreabilidade",
        description_en=(
            "Ambiguity in links among requirements, criteria, rules, scenarios, "
            "decisions, dependencies, and downstream delivery artifacts."
        ),
        description_pt=(
            "Ambiguidade nos vínculos entre requisitos, critérios, regras, "
            "cenários, decisões, dependências e artefatos de entrega derivados."
        ),
    ),
)

AMBIGUITY_TAXONOMY_CATEGORY_IDS: Final[tuple[str, ...]] = tuple(
    category.category_id for category in AMBIGUITY_TAXONOMY_CATALOG_V1
)
AMBIGUITY_TAXONOMY_CATEGORY_ID_SET: Final[frozenset[str]] = frozenset(
    AMBIGUITY_TAXONOMY_CATEGORY_IDS
)

_AMBIGUITY_CATEGORY_BY_ID: Final[Mapping[str, AmbiguityCategory]] = MappingProxyType(
    {category.category_id: category for category in AMBIGUITY_TAXONOMY_CATALOG_V1}
)


def _category_manifest(category: AmbiguityCategory) -> Mapping[str, str]:
    return MappingProxyType(
        {
            "category_id": category.category_id,
            "title_en": category.title_en,
            "title_pt": category.title_pt,
            "description_en": category.description_en,
            "description_pt": category.description_pt,
        }
    )


AMBIGUITY_TAXONOMY_MANIFEST_V1: Final[Mapping[str, object]] = MappingProxyType(
    {
        "ordering": "normative_append_only",
        "categories": tuple(
            _category_manifest(category) for category in AMBIGUITY_TAXONOMY_CATALOG_V1
        ),
    }
)
AMBIGUITY_TAXONOMY_DIGEST: Final = taxonomy_digest_v1(
    AMBIGUITY_TAXONOMY_VERSION,
    AMBIGUITY_TAXONOMY_MANIFEST_V1,
)


def require_ambiguity_category(category_id: str) -> AmbiguityCategory:
    """Resolve one exact v1 ID or fail closed; aliases and normalization are forbidden."""

    if (
        not isinstance(category_id, str)
        or not category_id
        or category_id != category_id.strip()
    ):
        raise AmbiguityTaxonomyError(
            "ambiguity_category_id_invalid",
            category_id,
        )
    try:
        return _AMBIGUITY_CATEGORY_BY_ID[category_id]
    except KeyError as exc:
        raise AmbiguityTaxonomyError(
            "ambiguity_category_unknown",
            category_id,
        ) from exc


__all__ = [
    "AMBIGUITY_TAXONOMY_CATALOG_V1",
    "AMBIGUITY_TAXONOMY_CATEGORY_IDS",
    "AMBIGUITY_TAXONOMY_CATEGORY_ID_SET",
    "AMBIGUITY_TAXONOMY_DIGEST",
    "AMBIGUITY_TAXONOMY_MANIFEST_V1",
    "AMBIGUITY_TAXONOMY_VERSION",
    "AmbiguityCategory",
    "AmbiguityTaxonomyError",
    "require_ambiguity_category",
]

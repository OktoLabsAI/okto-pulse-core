"""Deterministic A1a requirement lint ruleset.

The analyzer in this module is a pure domain leaf.  It has no persistence,
transport, service, or locale-detection dependency.  Callers must provide an
explicit locale and the assessment context that binds every finding anchor.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType
from typing import Any, Final

from okto_pulse.core.domain.quality_assessment import (
    MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1,
    AssessmentScale,
    AssessmentScaleKind,
    AssessmentSubjectType,
    FindingAnchor,
    FindingAnchorType,
    FindingSeverity,
    ProposedQuestionDraft,
    QualityFindingDraft,
    ScoreDirection,
)
from okto_pulse.core.domain.quality_canonicalization import (
    canonical_sha256,
    ruleset_digest_v1,
)

REQUIREMENT_LINT_RULESET_VERSION = "requirement_lint_ruleset_v1"
REQUIREMENT_LINT_ANALYZER_VERSION = "requirement-lint-analyzer/v1"
REQUIREMENT_LINT_TECHNICAL_LEXICON_VERSION = (
    "requirement-lint-technical-lexicon/v1"
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STABLE_CHILD_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_MUTABLE_CHILD_ID_RE = re.compile(r"(?:^|[./])\d+(?:$|[./])|\[\d+\]")

_NUMBER_WITH_UNIT_RE = re.compile(
    r"(?<!\w)\d+(?:[.,]\d+)?\s*"
    r"(?:%|"
    r"(?:ms|milliseconds?|milissegundos?|"
    r"seconds?|segundos?|secs?|segs?|"
    r"minutes?|minutos?|mins?|"
    r"hours?|horas?|hrs?|"
    r"bytes?|kib|mib|gib|kb|mb|gb|tb|"
    r"requests?/s|req/s|rps|items?|itens?|records?|registros?))"
    r"(?!\w)"
)
_SYMBOLIC_COMPARATOR_RE = re.compile(r"(?:<=|>=|==|=|<|>)\s*\d")
_GWT_EN_RE = re.compile(r"\bgiven\b[\s\S]*\bwhen\b[\s\S]*\bthen\b")
_DQT_PT_RE = re.compile(r"\bdado\b[\s\S]*\bquando\b[\s\S]*\bent(?:ão|ao)\b")

_NEUTRAL_PLACEHOLDERS: Final[tuple[str, ...]] = (
    "???",
    "tbc",
    "tbd",
    "xxx",
)
# ``TODO`` is an intentional authoring marker, but its Portuguese lowercase
# homograph (for example, "todo pedido") is ordinary prose.  Keep the marker
# case-sensitive instead of casefolding it with the language-neutral tokens.
_CASE_SENSITIVE_NEUTRAL_PLACEHOLDERS: Final[tuple[str, ...]] = ("TODO",)
_PT_PLACEHOLDERS: Final[tuple[str, ...]] = (
    "a definir",
)
_EN_PLACEHOLDERS: Final[tuple[str, ...]] = (
    "to be defined",
)
_PT_COMPARATORS: Final[tuple[str, ...]] = (
    "em até",
    "não mais que",
    "no máximo",
    "pelo menos",
)
_EN_COMPARATORS: Final[tuple[str, ...]] = (
    "at least",
    "at most",
    "no less than",
    "no more than",
    "within",
)
_PT_CONDITIONS: Final[tuple[str, ...]] = (
    "caso",
    "desde que",
    "quando",
    "se",
    "sempre que",
)
_EN_CONDITIONS: Final[tuple[str, ...]] = (
    "if",
    "provided that",
    "when",
    "whenever",
)
_PT_OBSERVABLE_RESULTS: Final[tuple[str, ...]] = (
    "apresenta",
    "exibe",
    "exibir",
    "emite",
    "emitir",
    "persiste",
    "persistir",
    "rejeita",
    "rejeitar",
    "resposta",
    "resultado",
    "retorna",
    "retornar",
    "salva",
    "salvar",
)
_EN_OBSERVABLE_RESULTS: Final[tuple[str, ...]] = (
    "display",
    "displays",
    "emit",
    "emits",
    "output",
    "persist",
    "persists",
    "reject",
    "rejects",
    "response",
    "result",
    "return",
    "returns",
    "save",
    "saves",
)
_PT_STATES: Final[tuple[str, ...]] = (
    "ativo",
    "concluído",
    "desabilitado",
    "estado",
    "habilitado",
    "inativo",
    "pendente",
    "status",
)
_EN_STATES: Final[tuple[str, ...]] = (
    "active",
    "completed",
    "disabled",
    "enabled",
    "inactive",
    "pending",
    "state",
    "status",
)
_PT_ERRORS: Final[tuple[str, ...]] = (
    "erro",
    "exceção",
    "falha",
    "inválido",
    "rejeita",
    "timeout",
)
_EN_ERRORS: Final[tuple[str, ...]] = (
    "error",
    "exception",
    "failure",
    "invalid",
    "reject",
    "timeout",
)
_PT_VAGUE_TERMS: Final[tuple[str, ...]] = (
    "adequado",
    "fácil",
    "intuitivo",
    "melhor",
    "rápido",
    "simples",
)
_EN_VAGUE_TERMS: Final[tuple[str, ...]] = (
    "adequate",
    "better",
    "easy",
    "fast",
    "intuitive",
    "simple",
)

# Named technologies, protocols, formats, and platforms are lexical evidence
# only for TR.  Their presence in FR or AC never creates a coupling finding.
REQUIREMENT_LINT_TECHNICAL_LEXICON_V1: Final[tuple[str, ...]] = tuple(
    sorted(
        (
            "aws",
            "azure",
            "docker",
            "gcp",
            "graphql",
            "grpc",
            "http",
            "https",
            "java",
            "javascript",
            "json",
            "jwt",
            "kafka",
            "kubernetes",
            "mysql",
            "oauth",
            "openapi",
            "postgresql",
            "python",
            "react",
            "redis",
            "rest",
            "soap",
            "sql",
            "sqlite",
            "tls",
            "typescript",
            "xml",
        )
    )
)
REQUIREMENT_LINT_TECHNICAL_LEXICON_DIGEST = canonical_sha256(
    {
        "version": REQUIREMENT_LINT_TECHNICAL_LEXICON_VERSION,
        "terms": REQUIREMENT_LINT_TECHNICAL_LEXICON_V1,
    }
)


class RequirementLintContractError(ValueError):
    """An input violates the frozen requirement-lint contract."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class RequirementLocale(str, Enum):
    PT = "pt"
    EN = "en"
    UNKNOWN = "unknown"


class RequirementEntityType(str, Enum):
    FR = "fr"
    AC = "ac"
    TR = "tr"


REQUIREMENT_LINT_SPEC_FIELDS: Final[
    tuple[tuple[str, RequirementEntityType], ...]
] = (
    ("functional_requirements", RequirementEntityType.FR),
    ("acceptance_criteria", RequirementEntityType.AC),
    ("technical_requirements", RequirementEntityType.TR),
)


@dataclass(frozen=True, slots=True)
class RequirementLintContext:
    board_id: str
    spec_id: str
    spec_version: int
    input_digest: str
    locale: RequirementLocale

    def __post_init__(self) -> None:
        for field_name in ("board_id", "spec_id"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise RequirementLintContractError(
                    f"requirement_lint_{field_name}_required"
                )
            object.__setattr__(self, field_name, value.strip())
        if (
            not isinstance(self.spec_version, int)
            or isinstance(self.spec_version, bool)
            or self.spec_version < 1
        ):
            raise RequirementLintContractError(
                "requirement_lint_spec_version_invalid"
            )
        if not isinstance(self.input_digest, str):
            raise RequirementLintContractError(
                "requirement_lint_input_digest_invalid"
            )
        digest = self.input_digest.strip().lower()
        if not _SHA256_RE.fullmatch(digest):
            raise RequirementLintContractError(
                "requirement_lint_input_digest_invalid"
            )
        object.__setattr__(self, "input_digest", digest)
        if not isinstance(self.locale, RequirementLocale):
            raise RequirementLintContractError("requirement_lint_locale_invalid")


@dataclass(frozen=True, slots=True)
class RequirementLintChild:
    entity_type: RequirementEntityType
    child_id: str
    text: str
    status: str = "active"
    locale: RequirementLocale | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.entity_type, RequirementEntityType):
            raise RequirementLintContractError(
                "requirement_lint_entity_type_invalid"
            )
        if not isinstance(self.child_id, str):
            raise RequirementLintContractError(
                "requirement_lint_child_id_invalid"
            )
        child_id = self.child_id.strip()
        if (
            not _STABLE_CHILD_ID_RE.fullmatch(child_id)
            or _MUTABLE_CHILD_ID_RE.search(child_id)
        ):
            raise RequirementLintContractError(
                "requirement_lint_child_id_invalid"
            )
        object.__setattr__(self, "child_id", child_id)
        if not isinstance(self.text, str):
            raise RequirementLintContractError("requirement_lint_text_invalid")
        object.__setattr__(self, "text", _normalize_text(self.text))
        if not isinstance(self.status, str):
            raise RequirementLintContractError("requirement_lint_status_invalid")
        object.__setattr__(self, "status", self.status.strip().casefold())
        if self.locale is not None and not isinstance(
            self.locale,
            RequirementLocale,
        ):
            raise RequirementLintContractError("requirement_lint_locale_invalid")


@dataclass(frozen=True, slots=True)
class RequirementLintSignals:
    number_with_unit: bool
    comparator: bool
    gwt_en: bool
    dado_quando_entao_pt: bool
    condition: bool
    observable_result: bool
    state: bool
    error: bool
    technical_term: bool
    neutral_placeholder: bool
    localized_placeholder: bool
    vague_term: bool

    @property
    def verifiable_signal(self) -> bool:
        return self.number_with_unit or self.comparator or self.state or self.error

    @property
    def technical_restriction(self) -> bool:
        return (
            self.technical_term
            or self.number_with_unit
            or self.comparator
        )


def requirement_lint_children_from_spec_payload(
    payload: Mapping[str, Any],
) -> tuple[RequirementLintChild, ...]:
    """Build the one canonical analyzer input shared by every Spec writer.

    The writer boundary already guarantees stable ids and canonical child
    shape. This mapper deliberately performs no locale inference: an omitted
    locale inherits the explicit run context, while an unsupported explicit
    locale fails closed.
    """

    if not isinstance(payload, Mapping):
        raise RequirementLintContractError(
            "requirement_lint_spec_payload_invalid"
        )
    children: list[RequirementLintChild] = []
    for field_name, entity_type in REQUIREMENT_LINT_SPEC_FIELDS:
        raw_items = payload.get(field_name) or ()
        if not isinstance(raw_items, Sequence) or isinstance(
            raw_items,
            str | bytes | bytearray,
        ):
            raise RequirementLintContractError(
                "requirement_lint_requirement_collection_invalid"
            )
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                raise RequirementLintContractError(
                    "requirement_lint_requirement_not_canonical"
                )
            raw_locale = raw_item.get("locale")
            locale: RequirementLocale | None
            if raw_locale is None:
                locale = None
            else:
                try:
                    locale = RequirementLocale(raw_locale)
                except (TypeError, ValueError) as exc:
                    raise RequirementLintContractError(
                        "requirement_lint_locale_invalid"
                    ) from exc
            children.append(
                RequirementLintChild(
                    entity_type=entity_type,
                    child_id=raw_item.get("id"),
                    text=raw_item.get("text"),
                    status=raw_item.get("status"),
                    locale=locale,
                )
            )
    return tuple(children)


@dataclass(frozen=True, slots=True)
class RequirementLintRule:
    code: str
    detector: str
    entity_types: tuple[RequirementEntityType, ...]
    locales: tuple[RequirementLocale, ...]
    category_code: str
    severity: FindingSeverity
    confidence: float
    title_en: str
    title_pt: str
    detail_en: str
    detail_pt: str
    remediation_en: str
    remediation_pt: str
    question_en: str
    question_pt: str


_ALL_ENTITY_TYPES = (
    RequirementEntityType.FR,
    RequirementEntityType.AC,
    RequirementEntityType.TR,
)
_ALL_LOCALES = (
    RequirementLocale.PT,
    RequirementLocale.EN,
    RequirementLocale.UNKNOWN,
)
_KNOWN_LOCALES = (RequirementLocale.PT, RequirementLocale.EN)

REQUIREMENT_LINT_RULE_CATALOG_V1: Final[tuple[RequirementLintRule, ...]] = (
    RequirementLintRule(
        code="ac_condition_missing",
        detector="missing_condition",
        entity_types=(RequirementEntityType.AC,),
        locales=_KNOWN_LOCALES,
        category_code="acceptance_measurability",
        severity=FindingSeverity.HIGH,
        confidence=0.95,
        title_en="Acceptance condition is missing",
        title_pt="A condição de aceite está ausente",
        detail_en="AC {child_id} has no explicit triggering condition.",
        detail_pt="O AC {child_id} não possui condição de disparo explícita.",
        remediation_en="State the precondition with Given/When, if, or when.",
        remediation_pt="Declare a pré-condição com Dado/Quando, se ou quando.",
        question_en="Under which condition must AC {child_id} apply?",
        question_pt="Sob qual condição o AC {child_id} deve se aplicar?",
    ),
    RequirementLintRule(
        code="ac_observable_result_missing",
        detector="missing_observable_result",
        entity_types=(RequirementEntityType.AC,),
        locales=_KNOWN_LOCALES,
        category_code="acceptance_measurability",
        severity=FindingSeverity.HIGH,
        confidence=0.95,
        title_en="Observable acceptance result is missing",
        title_pt="O resultado observável de aceite está ausente",
        detail_en="AC {child_id} does not state an observable result.",
        detail_pt="O AC {child_id} não declara um resultado observável.",
        remediation_en="State what is returned, emitted, displayed, or persisted.",
        remediation_pt="Declare o que é retornado, emitido, exibido ou persistido.",
        question_en="What observable result proves AC {child_id} was satisfied?",
        question_pt="Qual resultado observável comprova o AC {child_id}?",
    ),
    RequirementLintRule(
        code="ac_verifiable_signal_missing",
        detector="missing_verifiable_signal",
        entity_types=(RequirementEntityType.AC,),
        locales=_ALL_LOCALES,
        category_code="acceptance_measurability",
        severity=FindingSeverity.MEDIUM,
        confidence=0.9,
        title_en="Verifiable acceptance signal is missing",
        title_pt="O sinal verificável de aceite está ausente",
        detail_en=(
            "AC {child_id} has no measurement, comparator, state, or error signal."
        ),
        detail_pt=(
            "O AC {child_id} não possui medida, comparador, estado ou sinal de erro."
        ),
        remediation_en="Add a measurable bound, state, status, or error outcome.",
        remediation_pt="Adicione um limite mensurável, estado, status ou erro.",
        question_en="Which measurable bound or state verifies AC {child_id}?",
        question_pt="Qual limite mensurável ou estado verifica o AC {child_id}?",
    ),
    RequirementLintRule(
        code="fr_vague_without_observable_outcome",
        detector="vague_without_observable_outcome",
        entity_types=(RequirementEntityType.FR,),
        locales=_KNOWN_LOCALES,
        category_code="acceptance_measurability",
        severity=FindingSeverity.MEDIUM,
        confidence=0.85,
        title_en="Vague functional outcome",
        title_pt="Resultado funcional vago",
        detail_en=(
            "FR {child_id} uses a vague term without an observable condition "
            "or outcome."
        ),
        detail_pt=(
            "O FR {child_id} usa termo vago sem condição ou resultado observável."
        ),
        remediation_en="Replace the vague term with observable behavior.",
        remediation_pt="Substitua o termo vago por comportamento observável.",
        question_en="What observable behavior defines FR {child_id} precisely?",
        question_pt="Qual comportamento observável define o FR {child_id}?",
    ),
    RequirementLintRule(
        code="requirement_localized_placeholder",
        detector="localized_placeholder",
        entity_types=_ALL_ENTITY_TYPES,
        locales=_KNOWN_LOCALES,
        category_code="terminology_consistency",
        severity=FindingSeverity.HIGH,
        confidence=0.99,
        title_en="Localized placeholder remains",
        title_pt="Ainda existe um placeholder localizado",
        detail_en="Requirement {child_id} still contains placeholder text.",
        detail_pt="O requisito {child_id} ainda contém texto provisório.",
        remediation_en="Replace the placeholder with a concrete requirement.",
        remediation_pt="Substitua o texto provisório por um requisito concreto.",
        question_en="What final requirement replaces the placeholder in {child_id}?",
        question_pt="Qual requisito final substitui o placeholder em {child_id}?",
    ),
    RequirementLintRule(
        code="requirement_neutral_placeholder",
        detector="neutral_placeholder",
        entity_types=_ALL_ENTITY_TYPES,
        locales=_ALL_LOCALES,
        category_code="terminology_consistency",
        severity=FindingSeverity.HIGH,
        confidence=0.99,
        title_en="Placeholder marker remains",
        title_pt="Ainda existe um marcador provisório",
        detail_en="Requirement {child_id} still contains a placeholder marker.",
        detail_pt="O requisito {child_id} ainda contém um marcador provisório.",
        remediation_en="Replace the marker with a concrete requirement.",
        remediation_pt="Substitua o marcador por um requisito concreto.",
        question_en="What final requirement replaces the marker in {child_id}?",
        question_pt="Qual requisito final substitui o marcador em {child_id}?",
    ),
    RequirementLintRule(
        code="requirement_text_empty",
        detector="text_empty",
        entity_types=_ALL_ENTITY_TYPES,
        locales=_ALL_LOCALES,
        category_code="terminology_consistency",
        severity=FindingSeverity.HIGH,
        confidence=1.0,
        title_en="Requirement text is empty",
        title_pt="O texto do requisito está vazio",
        detail_en="Requirement {child_id} has no authored text.",
        detail_pt="O requisito {child_id} não possui texto.",
        remediation_en="Provide a concrete requirement statement.",
        remediation_pt="Forneça uma declaração de requisito concreta.",
        question_en="What must requirement {child_id} state?",
        question_pt="O que o requisito {child_id} deve declarar?",
    ),
    RequirementLintRule(
        code="tr_technical_restriction_missing",
        detector="missing_technical_restriction",
        entity_types=(RequirementEntityType.TR,),
        locales=_ALL_LOCALES,
        category_code="constraints_tradeoffs",
        severity=FindingSeverity.MEDIUM,
        confidence=0.9,
        title_en="Verifiable technical restriction is missing",
        title_pt="A restrição técnica verificável está ausente",
        detail_en=(
            "TR {child_id} names no versioned technology, protocol, format, "
            "platform, or measurable bound."
        ),
        detail_pt=(
            "O TR {child_id} não nomeia tecnologia, protocolo, formato, "
            "plataforma ou limite mensurável."
        ),
        remediation_en="Name a technical constraint or measurable bound.",
        remediation_pt="Declare uma restrição técnica ou limite mensurável.",
        question_en="Which technical constraint makes TR {child_id} verifiable?",
        question_pt="Qual restrição técnica torna o TR {child_id} verificável?",
    ),
)


def _rule_manifest(rule: RequirementLintRule) -> dict[str, object]:
    return {
        "code": rule.code,
        "detector": rule.detector,
        "entity_types": tuple(item.value for item in rule.entity_types),
        "locales": tuple(item.value for item in rule.locales),
        "category_code": rule.category_code,
        "severity": rule.severity.value,
        "confidence": rule.confidence,
        "title_en": rule.title_en,
        "title_pt": rule.title_pt,
        "detail_en": rule.detail_en,
        "detail_pt": rule.detail_pt,
        "remediation_en": rule.remediation_en,
        "remediation_pt": rule.remediation_pt,
        "question_en": rule.question_en,
        "question_pt": rule.question_pt,
    }


REQUIREMENT_LINT_RULESET_MANIFEST_V1 = MappingProxyType(
    {
        "analyzer_version": REQUIREMENT_LINT_ANALYZER_VERSION,
        "normalization": "unicode-nfc+lf;detector-scoped-casefold",
        "locale_contract": tuple(item.value for item in RequirementLocale),
        "unknown_locale_profile": "neutral_only",
        "question_budget": MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1,
        "ordering": "severity_desc,confidence_desc,finding_key_asc",
        "number_with_unit_pattern": _NUMBER_WITH_UNIT_RE.pattern,
        "symbolic_comparator_pattern": _SYMBOLIC_COMPARATOR_RE.pattern,
        "gwt_en_pattern": _GWT_EN_RE.pattern,
        "dado_quando_entao_pt_pattern": _DQT_PT_RE.pattern,
        "neutral_placeholders": _NEUTRAL_PLACEHOLDERS,
        "case_sensitive_neutral_placeholders": (
            _CASE_SENSITIVE_NEUTRAL_PLACEHOLDERS
        ),
        "pt_placeholders": _PT_PLACEHOLDERS,
        "en_placeholders": _EN_PLACEHOLDERS,
        "pt_comparators": _PT_COMPARATORS,
        "en_comparators": _EN_COMPARATORS,
        "pt_conditions": _PT_CONDITIONS,
        "en_conditions": _EN_CONDITIONS,
        "pt_observable_results": _PT_OBSERVABLE_RESULTS,
        "en_observable_results": _EN_OBSERVABLE_RESULTS,
        "pt_states": _PT_STATES,
        "en_states": _EN_STATES,
        "pt_errors": _PT_ERRORS,
        "en_errors": _EN_ERRORS,
        "pt_vague_terms": _PT_VAGUE_TERMS,
        "en_vague_terms": _EN_VAGUE_TERMS,
        "technical_lexicon": {
            "version": REQUIREMENT_LINT_TECHNICAL_LEXICON_VERSION,
            "digest": REQUIREMENT_LINT_TECHNICAL_LEXICON_DIGEST,
            "terms": REQUIREMENT_LINT_TECHNICAL_LEXICON_V1,
            "applies_as_restriction_to": (RequirementEntityType.TR.value,),
        },
        "rules": tuple(
            _rule_manifest(rule) for rule in REQUIREMENT_LINT_RULE_CATALOG_V1
        ),
    }
)
REQUIREMENT_LINT_RULESET_DIGEST = ruleset_digest_v1(
    REQUIREMENT_LINT_RULESET_VERSION,
    REQUIREMENT_LINT_RULESET_MANIFEST_V1,
)


@dataclass(frozen=True, slots=True)
class RequirementLintResult:
    context: RequirementLintContext
    ruleset_version: str
    ruleset_digest: str
    technical_lexicon_version: str
    technical_lexicon_digest: str
    active_child_count: int
    evaluated_rule_count: int
    score: float
    scale: AssessmentScale
    findings: tuple[QualityFindingDraft, ...]
    proposed_questions: tuple[ProposedQuestionDraft, ...]


@dataclass(frozen=True, slots=True)
class _FindingCandidate:
    rule: RequirementLintRule
    child: RequirementLintChild
    signals: RequirementLintSignals
    locale: RequirementLocale

    @property
    def finding_key(self) -> str:
        return (
            f"{REQUIREMENT_LINT_RULESET_VERSION}:"
            f"{self.child.entity_type.value}:{self.child.child_id}:"
            f"{self.rule.code}"
        )


_SEVERITY_RANK: Final[dict[FindingSeverity, int]] = {
    FindingSeverity.INFO: 0,
    FindingSeverity.LOW: 1,
    FindingSeverity.MEDIUM: 2,
    FindingSeverity.HIGH: 3,
    FindingSeverity.CRITICAL: 4,
}


def _normalize_text(value: str) -> str:
    return unicodedata.normalize(
        "NFC",
        value.replace("\r\n", "\n").replace("\r", "\n"),
    )


def _contains_term(text: str, terms: tuple[str, ...]) -> bool:
    return any(
        re.search(rf"(?<!\w){re.escape(term)}(?!\w)", text) is not None
        for term in terms
    )


def _contains_bounded_comparator(
    text: str,
    terms: tuple[str, ...],
) -> bool:
    return any(
        re.search(rf"(?<!\w){re.escape(term)}\s+\d", text) is not None
        for term in terms
    )


def detect_requirement_signals(
    text: str,
    *,
    locale: RequirementLocale,
) -> RequirementLintSignals:
    """Detect positive evidence without guessing the artifact language."""

    if not isinstance(text, str):
        raise RequirementLintContractError("requirement_lint_text_invalid")
    if not isinstance(locale, RequirementLocale):
        raise RequirementLintContractError("requirement_lint_locale_invalid")

    normalized = _normalize_text(text)
    normalized_casefold = normalized.casefold()
    number_with_unit = (
        _NUMBER_WITH_UNIT_RE.search(normalized_casefold) is not None
    )
    symbolic_comparator = (
        _SYMBOLIC_COMPARATOR_RE.search(normalized_casefold) is not None
    )
    technical_term = _contains_term(
        normalized_casefold,
        REQUIREMENT_LINT_TECHNICAL_LEXICON_V1,
    )
    neutral_placeholder = _contains_term(
        normalized_casefold,
        _NEUTRAL_PLACEHOLDERS,
    ) or _contains_term(
        normalized,
        _CASE_SENSITIVE_NEUTRAL_PLACEHOLDERS,
    )

    if locale is RequirementLocale.UNKNOWN:
        return RequirementLintSignals(
            number_with_unit=number_with_unit,
            comparator=symbolic_comparator,
            gwt_en=False,
            dado_quando_entao_pt=False,
            condition=False,
            observable_result=False,
            state=False,
            error=False,
            technical_term=technical_term,
            neutral_placeholder=neutral_placeholder,
            localized_placeholder=False,
            vague_term=False,
        )

    if locale is RequirementLocale.PT:
        gwt_en = False
        dado_quando_entao_pt = (
            _DQT_PT_RE.search(normalized_casefold) is not None
        )
        localized_comparator = _contains_bounded_comparator(
            normalized_casefold,
            _PT_COMPARATORS,
        )
        condition = dado_quando_entao_pt or _contains_term(
            normalized_casefold,
            _PT_CONDITIONS,
        )
        observable_result = dado_quando_entao_pt or _contains_term(
            normalized_casefold,
            _PT_OBSERVABLE_RESULTS,
        )
        state = _contains_term(normalized_casefold, _PT_STATES)
        error = _contains_term(normalized_casefold, _PT_ERRORS)
        localized_placeholder = _contains_term(
            normalized_casefold,
            _PT_PLACEHOLDERS,
        )
        vague_term = _contains_term(normalized_casefold, _PT_VAGUE_TERMS)
    else:
        gwt_en = _GWT_EN_RE.search(normalized_casefold) is not None
        dado_quando_entao_pt = False
        localized_comparator = _contains_bounded_comparator(
            normalized_casefold,
            _EN_COMPARATORS,
        )
        condition = gwt_en or _contains_term(
            normalized_casefold,
            _EN_CONDITIONS,
        )
        observable_result = gwt_en or _contains_term(
            normalized_casefold,
            _EN_OBSERVABLE_RESULTS,
        )
        state = _contains_term(normalized_casefold, _EN_STATES)
        error = _contains_term(normalized_casefold, _EN_ERRORS)
        localized_placeholder = _contains_term(
            normalized_casefold,
            _EN_PLACEHOLDERS,
        )
        vague_term = _contains_term(normalized_casefold, _EN_VAGUE_TERMS)

    return RequirementLintSignals(
        number_with_unit=number_with_unit,
        comparator=symbolic_comparator or localized_comparator,
        gwt_en=gwt_en,
        dado_quando_entao_pt=dado_quando_entao_pt,
        condition=condition,
        observable_result=observable_result,
        state=state,
        error=error,
        technical_term=technical_term,
        neutral_placeholder=neutral_placeholder,
        localized_placeholder=localized_placeholder,
        vague_term=vague_term,
    )


def _rule_fires(
    detector: str,
    *,
    text: str,
    signals: RequirementLintSignals,
) -> bool:
    if detector == "text_empty":
        return not text.strip()
    if detector == "neutral_placeholder":
        return signals.neutral_placeholder
    if detector == "localized_placeholder":
        return signals.localized_placeholder
    if detector == "missing_condition":
        return not signals.condition
    if detector == "missing_observable_result":
        return not signals.observable_result
    if detector == "missing_verifiable_signal":
        return not signals.verifiable_signal
    if detector == "missing_technical_restriction":
        return not signals.technical_restriction
    if detector == "vague_without_observable_outcome":
        return signals.vague_term and not (
            signals.condition
            or signals.observable_result
            or signals.verifiable_signal
        )
    raise RuntimeError(f"unknown_requirement_lint_detector:{detector}")


def _candidate_order(candidate: _FindingCandidate) -> tuple[int, float, str]:
    return (
        -_SEVERITY_RANK[candidate.rule.severity],
        -candidate.rule.confidence,
        candidate.finding_key,
    )


def _finding_from_candidate(
    candidate: _FindingCandidate,
    *,
    context: RequirementLintContext,
) -> QualityFindingDraft:
    is_pt = candidate.locale is RequirementLocale.PT
    child_id = candidate.child.child_id
    excerpt_hash = hashlib.sha256(
        candidate.child.text.encode("utf-8")
    ).hexdigest()
    return QualityFindingDraft(
        finding_key=candidate.finding_key,
        category_code=candidate.rule.category_code,
        severity=candidate.rule.severity,
        confidence=candidate.rule.confidence,
        deterministic=True,
        blocking_eligible=False,
        title=candidate.rule.title_pt if is_pt else candidate.rule.title_en,
        detail=(
            candidate.rule.detail_pt if is_pt else candidate.rule.detail_en
        ).format(child_id=child_id),
        remediation=(
            candidate.rule.remediation_pt
            if is_pt
            else candidate.rule.remediation_en
        ).format(child_id=child_id),
        rule_code=candidate.rule.code,
        anchor=FindingAnchor(
            board_id=context.board_id,
            subject_type=AssessmentSubjectType.SPEC,
            subject_id=context.spec_id,
            subject_version=context.spec_version,
            input_digest=context.input_digest,
            anchor_type=FindingAnchorType.STRUCTURED_CHILD,
            anchor_ref=child_id,
            excerpt_hash=excerpt_hash,
        ),
    )


def _question_from_candidate(
    candidate: _FindingCandidate,
    *,
    context: RequirementLintContext,
) -> ProposedQuestionDraft:
    is_pt = candidate.locale is RequirementLocale.PT
    question = (
        candidate.rule.question_pt if is_pt else candidate.rule.question_en
    ).format(child_id=candidate.child.child_id)
    return ProposedQuestionDraft(
        client_key=f"question:{candidate.finding_key}",
        question=question,
        question_type="free_text",
        allow_free_text=True,
        category_code=candidate.rule.category_code,
        finding_keys=(candidate.finding_key,),
    )


def lint_requirements(
    children: tuple[RequirementLintChild, ...] | list[RequirementLintChild],
    *,
    context: RequirementLintContext,
) -> RequirementLintResult:
    """Evaluate canonical FR/AC/TR children and return quality-domain drafts."""

    if not isinstance(context, RequirementLintContext):
        raise RequirementLintContractError("requirement_lint_context_invalid")
    if not isinstance(children, tuple | list):
        raise RequirementLintContractError("requirement_lint_children_invalid")
    resolved_children = tuple(children)
    if any(not isinstance(child, RequirementLintChild) for child in resolved_children):
        raise RequirementLintContractError("requirement_lint_children_invalid")

    child_ids = tuple(child.child_id for child in resolved_children)
    if len(set(child_ids)) != len(child_ids):
        raise RequirementLintContractError(
            "requirement_lint_duplicate_child_id"
        )

    active_children = tuple(
        child for child in resolved_children if child.status == "active"
    )
    candidates: list[_FindingCandidate] = []
    evaluated_rule_count = 0

    for child in active_children:
        effective_locale = child.locale or context.locale
        signals = detect_requirement_signals(
            child.text,
            locale=effective_locale,
        )
        for rule in REQUIREMENT_LINT_RULE_CATALOG_V1:
            if (
                child.entity_type not in rule.entity_types
                or effective_locale not in rule.locales
            ):
                continue
            evaluated_rule_count += 1
            if _rule_fires(rule.detector, text=child.text, signals=signals):
                candidates.append(
                    _FindingCandidate(
                        rule=rule,
                        child=child,
                        signals=signals,
                        locale=effective_locale,
                    )
                )

    ordered_candidates = tuple(sorted(candidates, key=_candidate_order))
    findings = tuple(
        _finding_from_candidate(candidate, context=context)
        for candidate in ordered_candidates
    )
    proposed_questions = tuple(
        _question_from_candidate(candidate, context=context)
        for candidate in ordered_candidates[
            :MAX_PROPOSED_QUESTIONS_PER_ASSESSMENT_V1
        ]
    )
    bounded_evaluated_rule_count = max(1, evaluated_rule_count)
    scale = AssessmentScale(
        kind=AssessmentScaleKind.FINDING_COUNT,
        minimum=0,
        maximum=bounded_evaluated_rule_count,
        direction=ScoreDirection.LOWER_BETTER,
    )

    return RequirementLintResult(
        context=context,
        ruleset_version=REQUIREMENT_LINT_RULESET_VERSION,
        ruleset_digest=REQUIREMENT_LINT_RULESET_DIGEST,
        technical_lexicon_version=REQUIREMENT_LINT_TECHNICAL_LEXICON_VERSION,
        technical_lexicon_digest=REQUIREMENT_LINT_TECHNICAL_LEXICON_DIGEST,
        active_child_count=len(active_children),
        evaluated_rule_count=bounded_evaluated_rule_count,
        score=scale.validate_score(len(findings)),
        scale=scale,
        findings=findings,
        proposed_questions=proposed_questions,
    )


__all__ = [
    "REQUIREMENT_LINT_ANALYZER_VERSION",
    "REQUIREMENT_LINT_SPEC_FIELDS",
    "REQUIREMENT_LINT_RULESET_DIGEST",
    "REQUIREMENT_LINT_RULESET_MANIFEST_V1",
    "REQUIREMENT_LINT_RULESET_VERSION",
    "REQUIREMENT_LINT_RULE_CATALOG_V1",
    "REQUIREMENT_LINT_TECHNICAL_LEXICON_DIGEST",
    "REQUIREMENT_LINT_TECHNICAL_LEXICON_V1",
    "REQUIREMENT_LINT_TECHNICAL_LEXICON_VERSION",
    "RequirementEntityType",
    "RequirementLintChild",
    "RequirementLintContext",
    "RequirementLintContractError",
    "RequirementLintResult",
    "RequirementLintRule",
    "RequirementLintSignals",
    "RequirementLocale",
    "detect_requirement_signals",
    "lint_requirements",
    "requirement_lint_children_from_spec_payload",
]

"""R13-A — Contrato LLMProvider, DTOs e gates de conformance.

Cobre os 4 test scenarios da spec R13-A (b7aa3c8d) 1:1:

  ts_9e55c4a1 (unit)        -> test_ts_9e55c4a1_contract_imports_in_isolation
  ts_cee91e6f (negative)    -> test_ts_cee91e6f_failure_reasons_preserve_semantics
  ts_06896ae7 (integration) -> test_ts_06896ae7_import_gate_no_concrete_provider
  ts_b161c1d3 (integration) -> test_ts_b161c1d3_metadata_preserves_provider_description

Princípio R13-A: o core expõe a porta LLMProvider e a descrição de provider por
metadata; NÃO recria EmbeddingProvider/Reranker; o caminho comum não importa
vendor SDK/sentence_transformers/provider concreto.
"""

from __future__ import annotations

import ast
import importlib
import inspect

import pytest

from okto_pulse.core.kg.interfaces.embedding import describe_embedding_provider
from okto_pulse.core.kg.interfaces.llm import (
    LLM_FAILURE_REASONS,
    LLM_INVALID_RESPONSE,
    LLM_OK,
    LLM_PROVIDER_ABSENT,
    LLM_PROVIDER_ERROR,
    LLM_TIMEOUT,
    LLMMessage,
    LLMProvider,
    LLMProviderError,
    LLMRequest,
    LLMResponse,
    LLMUsage,
)


# ---------------------------------------------------------------------------
# ts_9e55c4a1 — Contrato LLMProvider importa em isolamento sem provider concreto
# ---------------------------------------------------------------------------
def test_ts_9e55c4a1_contract_imports_in_isolation():
    """Os DTOs validam os campos esperados e nenhum provider concreto / vendor
    SDK / sentence_transformers é necessário para construí-los."""
    req = LLMRequest(
        purpose="hyde",
        prompt_id="p1",
        input="a query",
        messages=(LLMMessage(role="user", content="hi"),),
        board_id="board-1",
        actor_id="agent-1",
        timeout_seconds=2.5,
        max_tokens=256,
        budget_hint={"max_usd": 0.01},
        rate_limit_hint={"rpm": 60},
        telemetry_labels={"flow": "hyde"},
    )
    assert req.purpose == "hyde"
    assert req.prompt_id == "p1"
    assert req.input == "a query"
    assert req.messages[0].role == "user"
    assert req.board_id == "board-1" and req.actor_id == "agent-1"
    assert req.timeout_seconds == 2.5 and req.max_tokens == 256
    assert req.budget_hint == {"max_usd": 0.01}
    assert req.rate_limit_hint == {"rpm": 60}
    assert req.telemetry_labels == {"flow": "hyde"}

    # Field names MUST match the approved API contract api_r13a_llm_provider_protocol:
    # request optional includes `timeout_seconds` (not `timeout_s`); response
    # carries `failure_reason` (not `reason`).
    from dataclasses import fields as _fields

    req_fields = {f.name for f in _fields(LLMRequest)}
    assert req_fields == {
        "purpose", "prompt_id", "input", "messages", "board_id", "actor_id",
        "timeout_seconds", "max_tokens", "budget_hint", "rate_limit_hint",
        "telemetry_labels",
    }
    assert "timeout_s" not in req_fields
    resp_fields = {f.name for f in _fields(LLMResponse)}
    assert resp_fields == {
        "status", "text", "json", "usage", "finish_reason", "failure_reason",
    }
    assert "reason" not in resp_fields

    ok = LLMResponse(status=LLM_OK, text="hello", finish_reason="stop",
                     usage=LLMUsage(prompt_tokens=3, completion_tokens=1, total_tokens=4))
    assert ok.ok is True and ok.is_failure is False
    assert ok.text == "hello" and ok.usage.total_tokens == 4

    # Defaults: an empty response is OK by default and has no usage.
    empty = LLMResponse()
    assert empty.ok is True and empty.text is None and empty.usage is None

    # LLMProvider is a runtime_checkable Protocol satisfied by duck typing.
    class _DummyProvider:
        name = "dummy"

        def complete(self, request: LLMRequest) -> LLMResponse:
            return LLMResponse(text="x")

        def capabilities(self) -> dict:
            return {"is_stub": True}

    assert isinstance(_DummyProvider(), LLMProvider)


# ---------------------------------------------------------------------------
# ts_cee91e6f — Failure reasons normalizados preservam erro ou fallback
# ---------------------------------------------------------------------------
class _FakeFailingProvider:
    """Fake LLMProvider que normaliza falhas em LLMResponse.failure(reason)."""

    name = "fake-failing"

    def __init__(self, reason: str) -> None:
        self._reason = reason

    def complete(self, request: LLMRequest) -> LLMResponse:
        return LLMResponse.failure(self._reason, failure_reason=f"detail:{self._reason}")

    def capabilities(self) -> dict:
        return {"is_stub": True}


def _bridge_required(provider: LLMProvider, req: LLMRequest) -> LLMResponse:
    """Bridge de um fluxo que HOJE exige callable: falha explicitamente."""
    resp = provider.complete(req)
    if resp.is_failure:
        raise LLMProviderError(resp.status, message=resp.failure_reason or "")
    return resp


def _bridge_fallback(provider: LLMProvider, req: LLMRequest, *, fallback):
    """Bridge de um fluxo que HOJE degrada: preserva fallback/no-op."""
    resp = provider.complete(req)
    if resp.is_failure:
        return fallback
    return resp


@pytest.mark.parametrize(
    "reason",
    [LLM_PROVIDER_ABSENT, LLM_TIMEOUT, LLM_PROVIDER_ERROR, LLM_INVALID_RESPONSE],
)
def test_ts_cee91e6f_failure_reasons_preserve_semantics(reason):
    """Cada falha é exposta com reason normalizado e a bridge consegue preservar
    a semântica atual: erro obrigatório NÃO vira fallback silencioso, e fallback
    existente NÃO vira exceção nova."""
    provider = _FakeFailingProvider(reason)
    req = LLMRequest(purpose="rerank")

    # Reason é normalizado e pertence ao conjunto fechado.
    resp = provider.complete(req)
    assert resp.status == reason
    assert reason in LLM_FAILURE_REASONS
    assert resp.ok is False and resp.is_failure is True

    # Fluxo "required" preserva o erro explícito (não mascara).
    with pytest.raises(LLMProviderError) as exc:
        _bridge_required(provider, req)
    assert exc.value.reason == reason

    # Fluxo "fallback" preserva o no-op/fallback (não introduz exceção nova).
    sentinel = object()
    assert _bridge_fallback(provider, req, fallback=sentinel) is sentinel


def test_ts_cee91e6f_unknown_reason_is_rejected():
    """A fábrica de falha recusa um reason fora do contrato (fail-closed)."""
    with pytest.raises(ValueError):
        LLMResponse.failure("totally_unknown")
    with pytest.raises(ValueError):
        LLMProviderError("totally_unknown")


# ---------------------------------------------------------------------------
# ts_06896ae7 — Gate de import bloqueia provider concreto no contrato comum
# ---------------------------------------------------------------------------
_FORBIDDEN_IMPORT_TOKENS = (
    "sentence_transformers",
    "sentence-transformers",
    "torch",
    "openai",
    "anthropic",
    "cohere",
    "requests",
)
_FORBIDDEN_NAME_TOKENS = ("CrossEncoder", "SentenceTransformer")


def _imported_module_names(module) -> set[str]:
    """Colhe os nomes de módulos importados (import / from-import) via AST."""
    source = inspect.getsource(module)
    tree = ast.parse(source)
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.add(node.module)
    return names


def test_ts_06896ae7_import_gate_no_concrete_provider():
    """O módulo comum LLMProvider não importa vendor SDK, sentence_transformers,
    CrossEncoder/SentenceTransformer concreto, requests de provider ou settings
    Community-local."""
    import okto_pulse.core.kg.interfaces.llm as llm_module

    imported = _imported_module_names(llm_module)
    for mod in imported:
        low = mod.lower()
        for token in _FORBIDDEN_IMPORT_TOKENS:
            assert token not in low, (
                f"contrato comum LLMProvider importa modulo proibido: {mod!r} "
                f"(token {token!r})"
            )

    source = inspect.getsource(llm_module)
    for token in _FORBIDDEN_NAME_TOKENS:
        assert token not in source, (
            f"contrato comum LLMProvider referencia classe concreta proibida: {token!r}"
        )

    # Importar o contrato não deve, por si só, carregar sentence_transformers.
    import sys
    sys.modules.pop("okto_pulse.core.kg.interfaces.llm", None)
    importlib.import_module("okto_pulse.core.kg.interfaces.llm")
    # (sentence_transformers pode estar carregado por outra suite; o gate real
    #  é o AST acima — este passo só garante import limpo sem efeito colateral.)


# ---------------------------------------------------------------------------
# ts_b161c1d3 — Metadata/capabilities preserva descrição de providers atuais
# ---------------------------------------------------------------------------
class _FakeMetaProvider:
    """Provider duck-typed (NÃO Stub/ST) com embedding_metadata declarada —
    prova que a descrição é dirigida por metadata, não por isinstance."""

    dim = 7

    def embedding_metadata(self) -> dict:
        return {
            "model_name": "fake/model",
            "embedding_dimension": self.dim,
            "is_loaded": True,
            "is_stub": False,
        }


def test_ts_b161c1d3_metadata_preserves_provider_description():
    """A descrição observável dos providers atuais permanece equivalente e é
    obtida por metadata/capabilities, não por isinstance contra classe concreta."""
    from okto_pulse.core.kg.embedding import (
        SentenceTransformerProvider,
        StubEmbeddingProvider,
    )

    stub = StubEmbeddingProvider(dim=384)
    assert describe_embedding_provider(stub) == {
        "embedding_provider_name": "StubEmbeddingProvider",
        "model_name": None,
        "embedding_dimension": 384,
        "is_loaded": True,
        "is_stub": True,
    }

    st = SentenceTransformerProvider(model_name="all-MiniLM-L6-v2", dim=384)
    # Não carregado ainda: is_loaded=False, sem disparar _get_model().
    assert describe_embedding_provider(st) == {
        "embedding_provider_name": "SentenceTransformerProvider",
        "model_name": "all-MiniLM-L6-v2",
        "embedding_dimension": 384,
        "is_loaded": False,
        "is_stub": False,
    }
    assert st._model is None  # descrição não disparou load

    assert describe_embedding_provider(None) == {
        "embedding_provider_name": "NoneProvider",
        "model_name": None,
        "embedding_dimension": 0,
        "is_loaded": False,
        "is_stub": True,
    }

    # Provider duck-typed desconhecido com metadata: descrito por metadata.
    fake = _FakeMetaProvider()
    assert describe_embedding_provider(fake) == {
        "embedding_provider_name": "_FakeMetaProvider",
        "model_name": "fake/model",
        "embedding_dimension": 7,
        "is_loaded": True,
        "is_stub": False,
    }


def test_ts_b161c1d3_common_surface_has_no_concrete_isinstance():
    """A superfície comum (api/kg_routes) descreve provider por metadata: não
    importa as classes concretas nem usa isinstance no CORPO EXECUTÁVEL da função
    de descrição (a docstring pode mencioná-las ao explicar a mudança)."""
    import textwrap

    from okto_pulse.core.api import kg_routes

    src = textwrap.dedent(inspect.getsource(kg_routes._describe_embedding_provider))
    fn = ast.parse(src).body[0]
    body = fn.body
    # Remove a docstring (primeiro stmt se for uma constante string).
    if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
        body = body[1:]
    code_text = "\n".join(ast.unparse(node) for node in body)

    assert "isinstance" not in code_text
    assert "SentenceTransformerProvider" not in code_text
    assert "StubEmbeddingProvider" not in code_text
    assert "describe_embedding_provider" in code_text

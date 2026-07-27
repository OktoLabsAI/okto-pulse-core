"""Regressão (bug 07bdf670 / test card 42508970): canonical-only é FAIL-CLOSED.

Um node cujo ``graph_layer`` é NULL/ausente NÃO pode ser tratado como canonical.
Antes do fix, o predicado de leitura era ``coalesce(graph_layer, 'canonical') =
$graph_layer`` (fail-OPEN): um node sem layer caía em 'canonical' e VAZAVA. Agora
todo surface de leitura usa o helper centralizado
``cypher_templates.layer_filter_clause`` (``graph_layer = $graph_layer`` estrito)
e rotula a ausência como ``legacy_unknown`` (nunca canonical implícito).

  * TS1 (ts_772ec35d) — canonical-only EXCLUI o node sem classificação, em TODAS
    as superfícies reais de leitura: get_all_nodes, count_all_nodes,
    get_related_context (GET_RELATED_CONTEXT) e o /subgraph?center
    (kuzu_graph_store).
  * TS6 (ts_323672ab) — include_working/all ROTULA o node sem classificação como
    ``legacy_unknown`` (não 'canonical', não raw null).

Dentes: o node sem layer é semeado com ``graph_layer`` AUSENTE (NULL). No código
antigo ele apareceria em canonical-only e seria rotulado 'canonical'; aqui é
excluído e rotulado legacy_unknown. Sem source-inspection nas asserções
comportamentais: o veredito vem do grafo real lido pelos caminhos de produção.
"""

from __future__ import annotations

import os
import sys
import tempfile
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
os.environ.setdefault("KG_BASE_DIR", tempfile.mkdtemp(prefix="okto_kg_failclosed_"))

from okto_pulse.community.api.kg_routes import get_subgraph
from okto_pulse.core.kg import cypher_templates as tpl
from okto_pulse.core.kg.cypher_templates import (
    layer_filter_clause,
    layer_label_projection,
)
from okto_pulse.core.kg.kg_service import get_kg_service
from kg_schema_testing import bootstrap_board_graph, open_board_connection
from kg_registry_testing import configure_real_graph_test_kg_registry


CENTER_REF = "spec:center"
# (id, source_artifact_ref, title, graph_layer | None=NULL/ausente)
_NODES = [
    ("center", CENTER_REF, "CENTER", "canonical"),
    ("hop_canon", "x:canon", "CANON", "canonical"),
    ("hop_work", "x:work", "WORK", "working"),
    ("hop_unclass", "x:unclass", "UNCLASS", None),  # <-- graph_layer AUSENTE (o bug)
]


def _seed_layered_board() -> str:
    """center (canonical) + vizinhos canonical/working/UNCLASSIFIED(NULL)."""
    configure_real_graph_test_kg_registry()
    board_id = f"failclosed-{uuid.uuid4().hex[:10]}"
    bootstrap_board_graph(board_id)
    ts = "2026-06-15T00:00:00"

    with open_board_connection(board_id) as (_db, conn):
        for nid, ref, title, layer in _NODES:
            if layer is None:
                # Sem a propriedade graph_layer → NULL/ausente no Kuzu.
                conn.execute(
                    "CREATE (n:Decision {id:$id, title:$t, source_artifact_ref:$r, "
                    "source_confidence:1.0, relevance_score:0.9, "
                    "created_at: timestamp($ts)})",
                    {"id": nid, "t": title, "r": ref, "ts": ts},
                )
            else:
                conn.execute(
                    "CREATE (n:Decision {id:$id, title:$t, source_artifact_ref:$r, "
                    "source_confidence:1.0, relevance_score:0.9, "
                    "created_at: timestamp($ts), graph_layer:$l})",
                    {"id": nid, "t": title, "r": ref, "ts": ts, "l": layer},
                )
        for neighbor in ("hop_canon", "hop_work", "hop_unclass"):
            conn.execute(
                "MATCH (a:Decision {id:'center'}),(b:Decision {id:$b}) "
                "CREATE (a)-[:supersedes {confidence:1.0}]->(b)",
                {"b": neighbor},
            )
    return board_id


def _seeded(nodes: list[dict]) -> dict[str, dict]:
    seeded_ids = {nid for nid, *_ in _NODES}
    return {n["id"]: n for n in nodes if n["id"] in seeded_ids}


def _neighbor_titles(rows: list[dict]) -> set[str]:
    titles: set[str] = set()
    for r in rows:
        for key in ("hop1_title", "hop2_title"):
            if r.get(key):
                titles.add(r[key])
    return titles


async def _subgraph(board_id: str, **kw):
    params = {"depth": 2, "limit": 100, "cursor": "", "min_relevance": 0.0, "type": ""}
    params.update(kw)
    return await get_subgraph(board_id, **params)


# ---------------------------------------------------------------------------
# Unit + drift guard — the centralized helper is the single source of truth
# ---------------------------------------------------------------------------


def test_layer_filter_clause_is_fail_closed():
    clause = layer_filter_clause("n")
    # Comparação direta NULL-safe; sem coalesce-para-canonical.
    assert clause == "($graph_layer = 'all' OR n.graph_layer = $graph_layer)"
    assert "coalesce" not in clause
    assert "'canonical'" not in clause


def test_layer_label_projection_never_defaults_canonical():
    proj = layer_label_projection("n")
    assert proj == "coalesce(n.graph_layer, 'legacy_unknown') AS graph_layer"
    assert "'canonical'" not in proj


def test_no_read_template_carries_fail_open_canonical_filter():
    # Nenhum template de leitura pode reintroduzir o default inseguro
    # coalesce(...,'canonical') no FILTRO de layer.
    for name in (
        "GET_ALL_NODES",
        "GET_ALL_NODES_BY_TYPE",
        "GET_ALL_NODES_AFTER_CURSOR",
        "GET_ALL_NODES_BY_TYPE_AFTER_CURSOR",
        "COUNT_ALL_NODES",
        "COUNT_ALL_NODES_BY_TYPE",
        "GET_RELATED_CONTEXT",
    ):
        template = getattr(tpl, name)
        assert "coalesce(n.graph_layer, 'canonical')" not in template, name
        assert "coalesce(hop1.graph_layer, 'canonical')" not in template, name
        assert "coalesce(hop2.graph_layer, 'canonical')" not in template, name
        # E o filtro fail-closed estrito está presente.
        assert "graph_layer = $graph_layer" in template, name


# ---------------------------------------------------------------------------
# TS1 (ts_772ec35d) — canonical-only fail-closed exclui o node sem classificação
# ---------------------------------------------------------------------------


def test_ts1_get_all_nodes_canonical_excludes_unclassified():
    board_id = _seed_layered_board()
    svc = get_kg_service()

    canon = _seeded(svc.get_all_nodes(board_id, graph_layer="canonical"))
    assert "center" in canon
    assert "hop_canon" in canon
    # Dentes: NULL/ausente NÃO entra como canonical (no código antigo entrava).
    assert "hop_unclass" not in canon
    assert "hop_work" not in canon

    # count_all_nodes aplica o mesmo filtro.
    assert svc.count_all_nodes(board_id, graph_layer="canonical") == 2


def test_ts1_get_related_context_canonical_excludes_unclassified():
    board_id = _seed_layered_board()
    svc = get_kg_service()

    rows = svc.get_related_context(board_id, CENTER_REF)  # default = canonical
    titles = _neighbor_titles(rows)
    assert "CANON" in titles
    assert "UNCLASS" not in titles, "node sem graph_layer vazou no canonical-only"
    assert "WORK" not in titles


@pytest.mark.asyncio
async def test_ts1_subgraph_canonical_excludes_unclassified():
    board_id = _seed_layered_board()
    resp = await _subgraph(board_id, center=CENTER_REF, graph_layer="canonical")
    # get_subgraph?center retorna linhas de vizinhança centrada com
    # hop1_title/hop2_title (mesmo formato consumido por test_kg_layer_propagation).
    titles = _neighbor_titles(resp["nodes"])
    # Guard anti-vacuidade: a expansão tem que ter trazido ALGO (o vizinho
    # canonical), senão a asserção de exclusão abaixo seria trivialmente verde.
    assert titles, "subgraph não retornou nenhum vizinho (asserção seria vacuosa)"
    assert "CANON" in titles
    assert "UNCLASS" not in titles, "node sem graph_layer vazou na expansão canonical"
    assert "WORK" not in titles
    assert resp["metadata"]["graph_layer"] == "canonical"


# ---------------------------------------------------------------------------
# TS6 (ts_323672ab) — include_working/all rotula ausência como legacy_unknown
# ---------------------------------------------------------------------------


def test_ts6_all_labels_unclassified_as_legacy_unknown():
    board_id = _seed_layered_board()
    svc = get_kg_service()

    all_nodes = _seeded(svc.get_all_nodes(board_id, graph_layer="all"))
    # Os quatro aparecem em 'all'.
    assert set(all_nodes) == {"center", "hop_canon", "hop_work", "hop_unclass"}

    # O node sem classificação é rotulado legacy_unknown — NUNCA canonical implícito.
    assert all_nodes["hop_unclass"]["graph_layer"] == "legacy_unknown"
    assert all_nodes["hop_unclass"]["graph_layer"] != "canonical"
    # Os carimbados mantêm o próprio layer.
    assert all_nodes["center"]["graph_layer"] == "canonical"
    assert all_nodes["hop_canon"]["graph_layer"] == "canonical"
    assert all_nodes["hop_work"]["graph_layer"] == "working"


def test_ts6_working_scope_excludes_unclassified_and_canonical():
    # Fail-closed também no working-only: o node sem layer NÃO é working implícito.
    board_id = _seed_layered_board()
    svc = get_kg_service()

    work = _seeded(svc.get_all_nodes(board_id, graph_layer="working"))
    assert set(work) == {"hop_work"}
    assert "hop_unclass" not in work

# Plano de Desacoplamento do KG da Tecnologia Kuzu/Ladybug (Eixo #3)

> **Objetivo:** fazer com que **toda** a interação com o grafo passe pela porta
> `SemanticGraphStore` (e portas irmãs), de modo que `KuzuGraphStore` seja
> substituível por `Neo4jGraphStore` (ou outro) sem reescrever a camada KG. Hoje
> a porta cobre a *leitura*; a *escrita, o schema e o ciclo de vida* contornam a
> porta e falam Kuzu diretamente.
>
> **Status:** proposta de arquitetura. Nenhuma mudança de produção foi feita por
> este documento. Branch de análise: `feature/0.2.5` (v0.2.5).
>
> **Relação com outros eixos:** é pré-requisito do Eixo #2 Onda C (mover Kuzu
> para fora do core). Independente do Eixo #1.

---

## 1. O diagnóstico: porta boa, mas contornada

O KG já tem o esqueleto hexagonal mais maduro do projeto:
`KGProviderRegistry` + 16 interfaces em `kg/interfaces/`, com adaptadores em
`kg/providers/embedded/` (Kuzu) e `kg/providers/testing/` (fakes). A porta
`SemanticGraphStore` é **agnóstica de tecnologia** — métodos de domínio
(`find_by_topic`, `find_contradictions`, `vector_search`, `create_node`,
`create_edge`) e o próprio docstring já prevê `Neo4jGraphStore`.

**Mas a porta é contornada.** Medições na `feature/0.2.5`:

| Métrica | Valor |
|---|---|
| Arquivos do core que usam a porta (`get_kg_registry` / `graph_store`) | **11** |
| Arquivos que importam `kg.schema` / `session_manager` **direto** | **44** |
| `schema.py` | **2930 linhas, 143 refs Kuzu/Ladybug** |
| Arquivos que mencionam kuzu/cypher/`.lbug` (conceito) | **69** |

Ou seja: ~4× mais código fala com Kuzu por baixo da porta do que através dela. A
`SemanticGraphStore` cobre o **tier de leitura** (as 9 query tools), mas escrita,
manutenção, rebuild, lock e schema vão direto a `schema.py`.

### Onde a tecnologia vaza (lifecycle/concurrency fora da porta)

| Arquivo | Linhas | Refs Kuzu | Papel |
|---|---|---|---|
| `kg/schema.py` | 2930 | 143 | DDL/DML, criação de nós/arestas, índices HNSW |
| `kg/transaction.py` | 529 | 20 | orquestração de transação no grafo |
| `kg/commit_coordinator.py` | 192 | 18 | coordenação de commit + retries de lock |
| `kg/safe_write_lifecycle.py` | 527 | 7 | ciclo de vida de escrita segura |
| `kg/single_writer_lock.py` | 853 | 2 | **modelo single-writer** do Kuzu embarcado |
| `kg/write_barrier.py` | 311 | 3 | barreira de escrita |
| `kg/reconciliation.py` | 146 | 7 | reconciliação pós-falha |
| `kg/graph_availability.py` | 131 | 5 | disponibilidade do grafo |
| `kg/connection_pool.py` | 229 | 0 | pool de conexões Kuzu |
| `kg/session_manager.py` | 137 | 1 | ciclo de vida de sessão/`BoardConnection` |

Duas premissas Kuzu-específicas estão **soldadas como invariantes do core**, não
como propriedades de um adaptador:

1. **Single-writer.** `single_writer_lock.py` (853 linhas) existe porque o Kuzu
   embarcado só aceita um escritor. Um `Neo4jGraphStore` multi-writer não precisa
   disso — mas hoje a camada KG inteira assume o lock.
2. **Cypher como linguagem.** Há um `CypherExecutor` e `cypher_templates.py`; a
   porta `SemanticGraphStore` é agnóstica, mas muito código gera Cypher fora dela.

---

## 2. Arquitetura-alvo

```
 Consumidores KG (kg_service, tier_power, workers, mcp/kg_tools)
        │  dependem SOMENTE de:
        ▼
   SemanticGraphStore        (read + write de domínio — já existe)
   GraphTransaction          (NOVO — abstrai unidade de trabalho do grafo)
   GraphSchemaManager        (NOVO — bootstrap/migração de schema do grafo)
   GraphLifecycle            (NOVO — open/close/availability, esconde lock)
        │  implementado por:
        ▼
   Kuzu*  (adaptador embarcado: schema.py, transaction, lock, pool ficam AQUI)
   Neo4j* (futuro adaptador SaaS: sem single-writer lock, Cypher nativo)
```

Princípio: **toda assunção sobre o motor de grafo (single-writer, Cypher,
`.lbug`, HNSW) vira detalhe interno de um adaptador.** O core KG conhece só
operações de domínio + transação + schema abstratos.

---

## 3. Portas a definir/consolidar

| Porta | Status | Conteúdo |
|---|---|---|
| `SemanticGraphStore` | ✅ existe | read+write de nós/arestas de domínio |
| `CypherExecutor` | ✅ existe | já é uma porta — confinar geração de Cypher aos adaptadores |
| `GraphTransaction` | ❌ novo | `begin/commit/rollback` + retry de lock como detalhe do adaptador (absorve `transaction.py` + `commit_coordinator.py`) |
| `GraphSchemaManager` | ❌ novo | `bootstrap(board_id)`, `migrate(board_id, version)` (absorve o DDL de `schema.py`) |
| `GraphLifecycle` | ❌ novo | `open/close/health`, esconde `connection_pool`/`session_manager`/`single_writer_lock`/`graph_availability` |

O ponto crítico é o **single-writer**: ele deixa de ser invariante global e vira
uma propriedade declarada pelo adaptador (ex.: `GraphLifecycle.supports_concurrent_writers: bool`).
O `KuzuGraphLifecycle` declara `False` e mantém o lock; o `Neo4jGraphLifecycle`
declara `True` e o lock vira no-op.

---

## 4. Fases (strangler — sem big bang no schema.py)

### Fase 0 — Inventário e fronteira
- Mapear os 44 importadores diretos de `kg.schema`/`session_manager` e classificar:
  leitura, escrita, schema, lifecycle.
- **Critério:** lista fechada de call-sites a migrar, por categoria.

### Fase 1 — Rotear escritas pela `SemanticGraphStore`
- Cada call-site que hoje chama `schema.py` para criar/atualizar nós/arestas passa
  a usar `graph_store.create_node/create_edge/...`.
- **Critério:** nenhum consumidor fora de `kg/providers/embedded/` chama `schema.py`
  para escrita; refs Kuzu em consumidores → 0.

### Fase 2 — `GraphTransaction`
- Introduzir a porta; mover `transaction.py` + `commit_coordinator.py` para dentro
  do adaptador Kuzu. Consumidores usam `async with graph_tx:`.
- **Critério:** `commit_coordinator`/`transaction` não importados fora do adaptador.

### Fase 3 — `GraphSchemaManager`
- Extrair o DDL de `schema.py` para `KuzuGraphSchemaManager`. O `schema.py`
  vira implementação interna do adaptador, não API pública do KG.
- **Critério:** `bootstrap`/migração de grafo só acessíveis via porta.

### Fase 4 — `GraphLifecycle` + single-writer como propriedade
- Mover `connection_pool`/`session_manager`/`single_writer_lock`/`graph_availability`
  para trás de `GraphLifecycle`. Declarar `supports_concurrent_writers`.
- **Critério:** `single_writer_lock` referenciado só pelo adaptador Kuzu; o core
  KG não assume writer único.

### Fase 5 — Validação de substituibilidade
- Implementar um adaptador alternativo mínimo (mesmo que um `MemoryGraphStore`
  estendido ou um spike `Neo4jGraphStore`) e rodar o replay gate contra ele.
- **Critério:** a suíte KG passa trocando o adaptador via `configure_kg_registry`,
  sem tocar em consumidores. (Esta é a prova de que o Eixo #3 terminou.)

---

## 5. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| `schema.py` é enorme (2930 linhas) e central | Migrar por categoria de call-site, não reescrever o arquivo de uma vez; ele permanece como interno do adaptador até a Fase 3 |
| Single-writer está entrelaçado com correção (evita corrupção do Kuzu) | Não remover o lock — **encapsular**. O Kuzu adapter continua com lock; só o core para de assumir |
| Bug conhecido: `BoardConnection.close()` vaza handles no Linux | A Fase 4 (`GraphLifecycle`) é a oportunidade natural de corrigir o `close()` e reabilitar o pytest gate do release |
| Replay gate (862 eventos) é a principal rede de segurança | Rodar a cada fase; é o critério de não-regressão do caminho de escrita |
| Workers (consolidation/outbox/deterministic) escrevem no grafo | Migrá-los junto com a Fase 1/2 (eles são consumidores de escrita) |

---

## 6. Decisões em aberto

- **D1 — Granularidade das portas novas.** 3 portas (`GraphTransaction`,
  `GraphSchemaManager`, `GraphLifecycle`) ou consolidar lifecycle+transaction numa
  só? *Recomendação:* 3 portas — lifecycle e transação têm tempos de vida distintos.
- **D2 — Cypher na interface.** Manter `CypherExecutor` como porta pública (permite
  queries ad-hoc) ou torná-lo interno ao adaptador (mais agnóstico, menos flexível)?
- **D3 — Vector search / HNSW.** O índice vetorial é Kuzu-específico hoje. A porta
  `vector_search` já existe em `SemanticGraphStore`; confirmar que nenhum consumidor
  assume HNSW/Kuzu diretamente.
- **D4 — Ordem vs Eixo #2.** Concluir Fase 5 (substituibilidade provada) antes de
  mover o adaptador Kuzu para fora do core (Eixo #2 Onda C). *Recomendação:* sim.

---

## 7. Resultado esperado

A camada KG passa a depender **exclusivamente** de portas: operações de domínio,
transação, schema e lifecycle abstratos. As premissas Kuzu (single-writer,
Cypher, `.lbug`, HNSW) viram detalhes internos de `Kuzu*Adapter`. Trocar o motor
de grafo por Neo4j no SaaS deixa de exigir reescrita da camada KG — basta injetar
outro conjunto de adaptadores via `configure_kg_registry`, exatamente como o
registry já foi desenhado para permitir.

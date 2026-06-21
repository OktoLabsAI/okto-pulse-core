# Plano de Extração dos Adaptadores Técnicos do Core (Eixo #2)

> **Objetivo:** remover do `okto-pulse-core` as **implementações técnicas
> concretas** (Kuzu/Ladybug, filesystem, sentence-transformers, in-memory),
> deixando o core apenas com **interfaces (portas)**. Os adaptadores passam a
> viver num pacote de edição, instalado pela Community e substituível pelo SaaS.
>
> **Status:** proposta de arquitetura. Nenhuma mudança de produção foi feita por
> este documento. Branch de análise: `feature/0.2.5` (v0.2.5).
>
> **Relação com outros eixos:** depende parcialmente do Eixo #1 (porta de
> repositório relacional) e do Eixo #3 (fechar vazamentos de Kuzu) para que a
> extração seja completa. Pode começar pelos adaptadores já isolados.

---

## 1. O problema: o core *é* a edição Community disfarçada

O core já define portas excelentes (`StorageProvider`, `EmbeddingProvider`,
`SemanticGraphStore`, `CacheBackend`, `EventBus`, …), mas **as implementações
concretas dessas portas moram dentro do próprio core**, e o `pyproject.toml` do
core depende das tecnologias pesadas. Resultado: um SaaS que use Neo4j + Redis +
S3 ainda assim instala e carrega Kuzu/Ladybug, sentence-transformers e o stack
in-memory local.

### Adaptadores concretos hoje embarcados no core

| Adaptador | Arquivo atual | Porta que implementa | Natureza |
|---|---|---|---|
| `FileSystemStorageProvider` | `core/infra/storage.py` | `StorageProvider` | escolha local (filesystem) |
| `SentenceTransformerProvider` | `core/kg/embedding.py` | `EmbeddingProvider` | embeddings in-process |
| `StubEmbeddingProvider` | `core/kg/embedding.py` | `EmbeddingProvider` | fallback/local |
| `KuzuGraphStore` | `core/kg/providers/embedded/kuzu_graph_store.py` | `SemanticGraphStore` | grafo embarcado |
| `KuzuCypherExecutor` | `core/kg/providers/embedded/kuzu_cypher_executor.py` | `CypherExecutor` | grafo embarcado |
| `InMemoryCacheBackend` | `.../embedded/memory_cache.py` | `CacheBackend` | local single-process |
| `InMemoryTokenBucket` | `.../embedded/memory_rate_limiter.py` | `RateLimiter` | local single-process |
| `InMemorySessionStore` | `.../embedded/memory_session_store.py` | `SessionStore` | local single-process |
| `SqliteOutboxEventBus` | `.../embedded/sqlite_outbox_event_bus.py` | `EventBus` | acoplado a SQLite |
| `SqlAlchemyAuditRepository` | `.../embedded/sqlalchemy_audit_repo.py` | `AuditRepository` | acoplado a SQLAlchemy |
| `SettingsKGConfig` | `.../embedded/settings_config.py` | `KGConfig` | lê `CoreSettings` |
| `McpAuthContext` | `.../embedded/mcp_auth_context.py` | `auth_context` | local |

### Dependências do core que são escolhas técnicas (não regra de negócio)

`core/pyproject.toml` hoje exige:

```
ladybug>=0.16.0,<0.17.0          # grafo embarcado — inútil num SaaS Neo4j
aiosqlite>=0.19.0                # driver SQLite — local
asyncpg>=0.29                    # driver Postgres — SaaS, não o core
fastmcp>=2.0.0                   # transporte MCP
numpy>=1.26.0                    # suporte a embedding/vetor
apscheduler>=3.10.0             # scheduler local (decay tick)
sentence-transformers (extra)    # embeddings in-process
```

Nenhuma dessas deveria ser **obrigatória** para quem consome só as interfaces do
core. Hoje são.

### Sintoma no wiring

`community/main.py::create_community_app()` chama
`configure_kg_registry(session_factory=...)` **sem sobrescrever nenhum provider**
— ou seja, depende 100% do `_build_defaults()` do core, que instancia
`KuzuGraphStore()`, `InMemoryCacheBackend()`, etc. E importa
`FileSystemStorageProvider` e `SentenceTransformerProvider` **do core**. A
inversão de dependência existe na assinatura, mas **nunca é exercida** — não há
um segundo conjunto de adaptadores que prove que a troca funciona.

---

## 2. Arquitetura-alvo

```
 okto-pulse-core            (somente PORTAS + regra de negócio)
   infra/storage.py            → StorageProvider (ABC)            [sem FileSystem*]
   kg/interfaces/*             → CacheBackend, EventBus, ...      [sem providers/embedded]
   kg/interfaces/embedding.py  → EmbeddingProvider (Protocol)     [sem SentenceTransformer]
   pyproject: fastapi, pydantic, sqlalchemy(core ORM)*            [sem ladybug/st/asyncpg]

 okto-pulse-adapters-local  (NOVO — adaptadores da edição local/Community)
   storage_fs.py               → FileSystemStorageProvider
   embedding_st.py             → SentenceTransformerProvider / Stub
   kg_kuzu/*                   → KuzuGraphStore, KuzuCypherExecutor (+ schema.py — Eixo #3)
   cache_memory.py, ratelimit_memory.py, session_memory.py
   eventbus_sqlite.py, audit_sqlalchemy.py
   pyproject: depends ladybug, sentence-transformers, aiosqlite, ...

 okto-pulse-adapters-saas   (futuro — adaptadores do SaaS)
   storage_s3.py, kg_neo4j/*, cache_redis.py, eventbus_kafka.py, ...

 okto-pulse (community)     compõe core + adapters-local no create_community_app()
 okto-pulse-saas           compõe core + adapters-saas
```

O core fica com **zero** import de tecnologia concreta. O composition root de
cada edição é o único lugar que conhece os adaptadores.

> **Decisão de empacotamento (D1):** pacote separado `okto-pulse-adapters-local`
> vs. mover os adaptadores **para dentro do repo `okto-pulse` (community)**.
> *Recomendação:* mover para `okto-pulse` (community) primeiro — menos
> infraestrutura de release, e a Community já é o "consumidor local" natural.
> Extrair para pacote independente só quando o SaaS precisar compartilhar um
> subconjunto.

---

## 3. Classificação dos adaptadores por facilidade de extração

| Onda | Adaptadores | Bloqueio | Esforço |
|---|---|---|---|
| **A** | `FileSystemStorageProvider`, `SentenceTransformerProvider`/`Stub`, `InMemoryCacheBackend`, `InMemoryTokenBucket`, `InMemorySessionStore`, `McpAuthContext` | nenhum — já isolados atrás de porta limpa | **baixo** |
| **B** | `SqliteOutboxEventBus`, `SqlAlchemyAuditRepository`, `SettingsKGConfig` | acoplados a SQLAlchemy/CoreSettings; dependem do Eixo #1 para ficarem 100% limpos | médio |
| **C** | `KuzuGraphStore`, `KuzuCypherExecutor` + `schema.py` | dependem do Eixo #3 (44 arquivos contornam a porta indo direto a `kg.schema`) | **alto** |

A Onda A pode ser feita **isoladamente e já** — destrava o argumento "a inversão
é exercida por dois conjuntos de adaptadores".

---

## 4. Fases

### Fase 0 — Decidir empacotamento (D1) e criar o esqueleto
- Criar o destino (`okto-pulse/src/okto_pulse/community/adapters/` ou pacote novo).
- Mover `_build_defaults()` do registry **para o composition root da Community**:
  o core deixa de ter defaults técnicos; quem não configurar recebe erro explícito
  (mesmo padrão que `create_app` já usa para `auth_provider`/`storage_provider`).

### Fase 1 — Onda A (adaptadores já isolados)
- Mover os 6 adaptadores da Onda A para a edição.
- Community passa a instanciá-los e injetá-los via `configure_kg_registry(...)`
  e `configure_storage(...)` explicitamente.
- Remover `sentence-transformers` do core (vira dep da Community, onde já é
  mandatória).
- **Critério:** core não importa mais filesystem/sentence-transformers; smoke +
  replay verdes; `configure_kg_registry` recebe providers explícitos.

### Fase 2 — Onda B (event bus / audit / config)
- Depende do Eixo #1 para o audit/outbox falarem com a porta de repositório em
  vez de `AsyncSession` cru. Mover após a Fase 2 do Eixo #1.
- **Critério:** `SqliteOutboxEventBus`/`SqlAlchemyAuditRepository` fora do core;
  core expõe só `EventBus`/`AuditRepository`.

### Fase 3 — Onda C (Kuzu)
- Bloqueada pelo Eixo #3. Quando todas as escritas passarem pela
  `SemanticGraphStore`, mover `KuzuGraphStore`/`KuzuCypherExecutor`/`schema.py`
  para a edição.
- Remover `ladybug` (e `numpy`, se só usado pelo grafo) do core.
- **Critério:** `grep -r ladybug src/okto_pulse/core` retorna vazio.

### Fase 4 — Higiene de dependências e config
- Mover `aiosqlite`/`asyncpg`/`apscheduler` para as edições conforme uso.
- Extrair de `CoreSettings` os campos kuzu-específicos (ver Eixo #3 / config).
- **Critério:** `core/pyproject.toml` lista só deps agnósticas (fastapi, pydantic,
  e — até o Eixo #1 concluir — sqlalchemy do ORM).

---

## 5. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Remover defaults do registry quebra a Community no boot | Community passa a configurar tudo explicitamente **antes** de remover os defaults do core (ordem: adicionar wiring → remover default) |
| `schema.py` (2930 linhas, 143 refs Kuzu) é contornado por 44 arquivos | Onda C fica atrás do Eixo #3; não tentar mover Kuzu antes de fechar a porta |
| Release pipeline constrói wheels de ambos os repos | Atualizar `okto-pulse/Dockerfile` (wheel-builder) e `uv.lock` ao mover deps |
| Testes do core importam adaptadores embedded | Manter `kg/providers/testing/*` (fakes) no core — eles são utilitários de teste, não tecnologia de produção |

---

## 6. Decisões em aberto

- **D1 — Empacotamento:** pacote `okto-pulse-adapters-local` independente vs.
  `okto-pulse/community/adapters/`. *Recomendação:* dentro da Community primeiro.
- **D2 — Fakes de teste:** ficam no core (`kg/providers/testing`) ou descem junto?
  *Recomendação:* ficam no core — são contrato de teste das portas.
- **D3 — `numpy`:** é usado só pelo grafo/embedding ou também por scoring de
  domínio? Auditar antes de remover do core.
- **D4 — SQLAlchemy no core:** enquanto o Eixo #1 não concluir, o ORM (e portanto
  sqlalchemy) permanece dep do core. Esta é a última dep técnica a sair.

---

## 7. Resultado esperado

O `okto-pulse-core` vira um pacote de **regra de negócio + portas**, instalável
sem nenhuma tecnologia de banco/embedding concreta. A Community compõe core +
adaptadores locais (Kuzu/SQLite/filesystem/sentence-transformers); o SaaS compõe
core + adaptadores de nuvem (Neo4j/Postgres/Redis/S3). A inversão de dependência
deixa de ser teórica e passa a ser exercida por **dois** conjuntos reais de
adaptadores — a prova viva de que o core é agnóstico.

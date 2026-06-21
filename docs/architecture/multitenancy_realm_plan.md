# Plano de Multi-tenancy / Isolamento por Realm (Eixo #4)

> **Objetivo:** tornar `realm_id` (tenant/organização) um cidadão de primeira
> classe que flui da autenticação até a camada de dados, isolando os dados de
> cada tenant. Hoje o conceito existe na borda (auth) mas **não desce até a
> persistência** — o que é aceitável na Community single-user, mas é bloqueante
> para o SaaS.
>
> **Status:** proposta de arquitetura. Nenhuma mudança de produção foi feita por
> este documento. Branch de análise: `feature/0.2.5` (v0.2.5).
>
> **Relação com outros eixos:** depende fortemente do Eixo #1 — o isolamento por
> tenant deve ser imposto na **porta de repositório**, não espalhado em cada
> `select()`. Sem o Eixo #1, multi-tenancy vira filtro manual em 268 call-sites.

---

## 1. O diagnóstico: realm é vestigial

A infraestrutura de tenant existe na borda mas está **quase totalmente
desconectada** da camada de dados.

O que existe:
- `AuthProvider.get_realm_id(request, user)` — porta já prevê realm/org
  (`core/infra/auth.py`). ✅
- A dependency FastAPI `get_realm_id` resolve o realm por request. ✅

O que **falta** (medições na `feature/0.2.5`):

| Métrica | Valor | Implicação |
|---|---|---|
| Modelos ORM (de ~67) com coluna `realm_id` | **2** — `Board`, `ActivityLog` | cards, specs, sprints, ideations, refinements, stories, agents… **não têm realm** |
| `Board.realm_id` | `nullable=True` | tenant é opcional; nada força preenchimento |
| Rotas que leem `get_realm_id` | **2 arquivos** (só `boards.py` de fato) | criação/listagem de board; o resto ignora realm |
| Serviços que propagam `realm_id` | **1** | regra de negócio não conhece tenant |
| `LocalAuthProvider.get_realm_id` | retorna `None` | Community é single-tenant por design |

Tradução: hoje um board pode ter `realm_id`, mas suas **cards, specs e sprints
não** — então não há fronteira de isolamento real. Qualquer query de card cruza
tenants. No SaaS isso é vazamento de dados entre organizações.

---

## 2. Arquitetura-alvo

`realm_id` resolvido na auth → carregado num **contexto de request** →
**imposto na porta de repositório** (todo `get/list/add` recebe e filtra por
`realm_id`) → persistido em **toda tabela de domínio** com índice composto.

```
 Request → AuthProvider.get_realm_id()  →  RealmContext (ContextVar/Depends)
                                              │
                                              ▼
   Service(uow)  →  uow.cards.list(..., realm_id=ctx.realm_id)   # Eixo #1
                                              │
                                              ▼
   SqlAlchemyCardRepository:  select(Card).where(Card.realm_id == realm_id, ...)
                                              │
                                              ▼
   tabela cards (realm_id NOT NULL, índice (realm_id, board_id))
```

Três estratégias de isolamento possíveis no adaptador SaaS (escolha em D1):

1. **Discriminator column** (`realm_id` em cada linha + filtro na porta) — mais
   simples, um banco compartilhado. *Recomendado para começar.*
2. **Schema-per-tenant** (Postgres schema por realm) — isolamento forte, custo
   operacional médio.
3. **Database-per-tenant** — isolamento máximo, custo alto. Só para enterprise.

A escolha vive **no adaptador**, não no core. O core só garante que `realm_id`
atravessa todas as portas.

---

## 3. Onde o realm precisa entrar

### 3.1 Modelos (camada mais cara)
Adicionar `realm_id` a todas as tabelas de domínio raiz e propagá-lo por herança
nas filhas. Agregados que precisam (alinhado ao mapa de repositórios do Eixo #1):

`Board` (já tem), `Card`, `Spec`, `Sprint`, `Ideation`, `Refinement`, `Story`,
`Agent`, `Guideline`, `ArchitectureDesign`, `DiscoveryIntent`,
`DefaultBoardConfiguration`, `DesignSystem`, `AmendmentHotfixRevision`,
`CanonicalDebt`, e as tabelas de fila KG (`ConsolidationQueue`, `DeadLetter`,
`Outbox`, `DomainEventRow`).

> **Tabelas-filhas** (snapshots/history/QA/KB) podem derivar o realm da raiz via
> join, **ou** carregar a coluna desnormalizada para filtro barato. Decisão D2.

### 3.2 Portas de repositório (Eixo #1)
`realm_id` vira parâmetro **keyword-only obrigatório** em `get/list/add/update/delete`
desde a Fase 1 do Eixo #1 — mesmo que a Community passe `None`. Isso evita
reescrever assinaturas depois.

### 3.3 Contexto de request
Um `RealmContext` (ContextVar, análogo ao `_active_api_key` já existente) carrega
o realm resolvido para que serviços e workers não precisem receber `realm_id`
manualmente em cada chamada.

### 3.4 Caminho MCP
O MCP autentica por `api_key` (`_active_api_key` ContextVar). O realm precisa ser
derivado da API key do agente também — não só do JWT HTTP. Hoje o agente/board
não carrega realm consistentemente.

### 3.5 KG / grafo
O grafo é **por board** (`/data/boards/<board-id>/graph.lbug`). Como board já
tem realm, o isolamento de grafo no SaaS pode ser por namespace de realm+board.
Depende do Eixo #3 para o `Neo4jGraphStore` aceitar partição por tenant.

---

## 4. Fases

### Fase 0 — Contexto e contrato (sem migração de dados)
- Introduzir `RealmContext` (ContextVar) + dependency que o popula de `get_realm_id`.
- Tornar `realm_id` keyword-only nas portas de repositório do Eixo #1 (coordenar
  com a Fase 0/1 daquele eixo). Community passa `None`.
- **Critério:** assinaturas de porta já carregam realm; comportamento inalterado.

### Fase 1 — Coluna em todas as raízes
- Migração: adicionar `realm_id` (nullable inicialmente) às tabelas raiz que faltam.
- Backfill: linhas existentes recebem o realm do board ancestral (ou um sentinel
  `"local"` na Community).
- **Critério:** toda tabela raiz tem `realm_id`; índice `(realm_id, ...)` criado.

### Fase 2 — Imposição no adaptador SQLAlchemy
- O `SqlAlch[...]Repository` filtra por `realm_id` em toda leitura e carimba em
  toda escrita. Centralizado no adaptador (graças ao Eixo #1), não em 268 rotas.
- **Critério:** nenhuma query de domínio sem cláusula de realm no adaptador SaaS;
  teste de contrato prova que tenant A não enxerga dados de tenant B.

### Fase 3 — NOT NULL + endurecimento
- Após backfill provado, virar `realm_id` para `NOT NULL` nas tabelas de domínio.
- Imposição opcional via Postgres RLS (Row-Level Security) como defesa em
  profundidade no adaptador SaaS.
- **Critério:** `realm_id NOT NULL`; (SaaS) RLS ativo.

### Fase 4 — MCP e workers
- Derivar realm da API key do agente; propagar `RealmContext` para
  consolidation/outbox/deterministic workers (que hoje varrem global).
- **Critério:** workers processam por realm; nenhum cruzamento de tenant em
  background.

### Fase 5 — KG por tenant (depende do Eixo #3)
- Partição de grafo por realm no `Neo4jGraphStore`/adaptador SaaS.
- **Critério:** grafo isolado por tenant no SaaS; Community inalterada (single-tenant).

---

## 5. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Migração de `realm_id` em ~15 agregados é ampla | Nullable + backfill + NOT NULL em fases separadas; nunca NOT NULL direto |
| Esquecer o filtro em alguma query = vazamento entre tenants | Impor **na porta** (Eixo #1), não por rota; cobrir com teste de contrato A≠B; (SaaS) RLS como rede final |
| Community não precisa de realm e não pode pagar custo | `realm_id` keyword-only com default `None`; `LocalAuthProvider` segue retornando `None`; coluna aceita sentinel local |
| MCP/api_key não carrega realm hoje | Fase 4 dedicada; mapear agente→realm via `Agent`/`AgentBoard` |
| Tabelas-filhas multiplicam a migração | D2: derivar por join vs desnormalizar — escolher por custo de query |
| Depende do Eixo #1 ainda não pronto | Fase 0 deste eixo é **coordenada** com a Fase 0/1 do Eixo #1; não começar a imposição (Fase 2) antes das portas existirem |

---

## 6. Decisões em aberto

- **D1 — Estratégia de isolamento SaaS:** discriminator column vs schema-per-tenant
  vs database-per-tenant. *Recomendação:* discriminator column + RLS no início;
  reavaliar para enterprise.
- **D2 — Tabelas-filhas:** derivar realm por join da raiz ou desnormalizar a coluna?
  *Recomendação:* desnormalizar nas filhas mais consultadas (cards/specs), join nas
  raras.
- **D3 — Sentinel da Community:** `realm_id = NULL` permitido vs sentinel `"local"`.
  *Recomendação:* sentinel `"local"` para manter `NOT NULL` uniforme e simplificar o
  adaptador SaaS.
- **D4 — RLS:** usar Postgres Row-Level Security como imposição primária ou só como
  defesa em profundidade sobre o filtro de aplicação?
- **D5 — Realm no MCP:** mapear via `Agent.realm_id` direto ou via board do agente?

---

## 7. Resultado esperado

`realm_id` flui da autenticação ao banco e é imposto num único ponto (a porta de
repositório), tornando o isolamento entre tenants uma garantia estrutural e não
uma disciplina manual. A Community continua single-tenant (realm `local`, custo
zero); o SaaS ganha isolamento multi-tenant configurável (discriminator/schema/db)
escolhido no adaptador — sem que a regra de negócio do core saiba qual estratégia
está em uso.

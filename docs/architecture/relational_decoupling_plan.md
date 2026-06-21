# Plano de Desacoplamento da Camada Relacional (Eixo #1)

> **Objetivo:** tornar o `okto-pulse-core` agnóstico quanto à tecnologia de
> persistência relacional, introduzindo **portas de repositório** entre os
> serviços/rotas e o SQLAlchemy, para que edições downstream (Community local,
> SaaS multi-tenant) escolham o adaptador concreto (SQLite, Postgres, ou outro)
> sem reescrever regra de negócio.
>
> **Status:** proposta de arquitetura. Nenhuma mudança de produção foi feita por
> este documento. Branch de origem da análise: `feature/0.2.5` (v0.2.5).

---

## 1. Por que este é o eixo prioritário

O core já aplica arquitetura hexagonal em vários subsistemas (Auth via
`AuthProvider`, Storage via `StorageProvider`, KG via `KGProviderRegistry` +
`kg/interfaces/`). A **exceção é a espinha dorsal** — todo o domínio de gestão
de projeto (boards, cards, specs, sprints, ideations, refinements, stories,
agents, …) é escrito **diretamente contra SQLAlchemy**, sem porta.

Números medidos na `feature/0.2.5`:

| Métrica | Valor |
|---|---|
| Arquivos do core que importam `AsyncSession` / usam `select()` | **93** |
| Rotas de API que recebem `db: AsyncSession = Depends(get_db)` | **268 ocorrências** |
| Modelos ORM `Base` (domínio = tabela) em `models/db.py` | ~67 classes |
| Classes de serviço construídas como `Service(db: AsyncSession)` | **21** (em `services/main.py`, ~10k linhas) |
| `db.commit()` na camada **API** | **140** |
| `db.commit()` na camada **de serviço** | **13** |
| Migrações imperativas (`_migrate_*`) em `infra/database.py` | ~44 |

Dois problemas estruturais saltam:

1. **Sem porta de repositório.** Serviços recebem o `AsyncSession` e fazem
   `select(...)` inline. Não há um contrato que o SaaS possa reimplementar.
2. **Transação vaza para a API.** 140 `commit()` nas rotas vs. 13 nos serviços
   significa que a fronteira transacional vive no controller, não no domínio —
   o que impede introduzir um *Unit of Work* limpo e dificulta multi-tenancy.

E o gap **cresce a cada release**: a própria 0.2.5 adicionou 7 tabelas novas e
+22 arquivos acoplados a `AsyncSession`. Cada feature nova sem porta é dívida
composta.

---

## 2. Estado atual (fluxo real)

```
API route (api/boards.py)
  db: AsyncSession = Depends(get_db)        # core/infra/database.py — singleton global
  service = BoardService(db)                # core/services/main.py
  board = await service.create_board(...)   # faz select()/add() com SQLAlchemy
  await db.commit()                         # ⚠️ transação controlada na ROTA
```

Pontos de acoplamento, do mais externo ao mais interno:

- `core/infra/database.py` — `create_database(url)`, `get_db`, `get_session_factory`
  são singletons de módulo SQLAlchemy. Suportam `sqlite` e `postgresql` por URL
  (já há tuning de pool por dialeto e `asyncpg` nas deps), mas tudo é SQLAlchemy.
- `core/models/db.py` — entidades de domínio **são** `Base`-derived. Não há
  separação entre modelo de domínio e linha de tabela.
- `core/services/main.py` — 21 serviços, cada um `__init__(self, db: AsyncSession)`,
  com `select()`/`session.add()` no corpo.
- `core/api/*.py` — controllers chamam o serviço e dão `commit()`.

**Boa notícia:** o seam já existe. Como as rotas já delegam para `Service(db)`,
a porta de repositório pode ser introduzida **trocando a dependência do serviço**
de `AsyncSession` para `Repositories` + `UnitOfWork`, sem reescrever as rotas em
massa.

---

## 3. Arquitetura-alvo

```
            ┌─────────────────────────── core (agnóstico) ───────────────────────────┐
 API route ─┤  Service(uow: UnitOfWork)                                               │
            │     uow.boards: BoardRepository   (Protocol)                            │
            │     uow.cards:  CardRepository     (Protocol)                           │
            │     async with uow:  ...  await uow.commit()   # transação no domínio   │
            └────────────────────────────────────────────────────────────────────────┘
                                   ▲ implementa
            ┌──────────────────────┴───────────────── adaptador (edição) ─────────────┐
            │  SqlAlchemyUnitOfWork / SqlAlchemyBoardRepository  (default no core,     │
            │  candidato a migrar para pacote de adapters — ver Eixo #2)              │
            │  SaaS: PostgresUnitOfWork, multi-tenant por realm_id                     │
            └─────────────────────────────────────────────────────────────────────────┘
```

Três peças novas:

1. **Repository Protocols** (`core/repositories/interfaces/`) — um por agregado.
   Métodos de domínio (`get`, `list_for_user`, `add`, `update`, `delete`,
   queries específicas), **sem vazar SQLAlchemy** na assinatura. Tipos de entrada
   e saída são as entidades (ver decisão D2 sobre domínio vs ORM).
2. **UnitOfWork Protocol** (`core/repositories/interfaces/unit_of_work.py`) —
   agrupa os repositórios numa transação e expõe `commit()`/`rollback()`. É o
   ponto único onde a transação é controlada (tira os 140 `commit()` das rotas).
3. **Adaptador SQLAlchemy** (`core/repositories/sqlalchemy/`) — implementação
   default que envolve `AsyncSession`. Começa no core para não quebrar a Community;
   pode descer para o pacote de adapters depois (Eixo #2).

`realm_id` (já existente em `AuthProvider.get_realm_id` e em colunas como
`Board.realm_id`) passa a ser **parâmetro de primeira classe** das queries de
repositório, habilitando isolamento multi-tenant no SaaS sem mudar serviço.

---

## 4. Mapa de agregados → repositórios

Derivado dos 21 serviços e dos grupos de modelos em `models/db.py`. Ordem
sugerida de extração (menor blast radius primeiro):

| # | Repositório | Agregado / tabelas | Serviço atual | Acoplamento |
|---|---|---|---|---|
| 1 | `BoardRepository` | Board, BoardShare, BoardGuideline | BoardService, ShareService | médio (piloto) |
| 2 | `AgentRepository` | Agent, AgentBoard, AgentSeenItem, PermissionPreset | AgentService | baixo |
| 3 | `CardRepository` | Card, CardDependency, Comment, QAItem, Attachment | CardService, CommentService, QAService | **alto** |
| 4 | `IdeationRepository` | Ideation + snapshots/history/QA/KB/links | IdeationService (+QA/Knowledge) | alto |
| 5 | `RefinementRepository` | Refinement + snapshots/history/QA/KB | RefinementService (+QA/Knowledge) | alto |
| 6 | `StoryRepository` | Story, StoryIdeationLink, Topic | StoryService | médio |
| 7 | `SpecRepository` | Spec + history/QA/KB | SpecService (+QA/Knowledge) | **alto** |
| 8 | `SprintRepository` | Sprint + history/QA | SprintService | médio |
| 9 | `GuidelineRepository` | Guideline | GuidelineService | baixo |
| 10 | `ArchitectureRepository` | ArchitectureDesign + versions/findings/acks | services/architecture.py | médio |
| 11 | `KgQueueRepository` | ConsolidationQueue, ConsolidationDeadLetter, ConsolidationAudit, GlobalUpdateOutbox, DomainEventRow | workers + kg_health_service | **alto / sensível** |
| 12 | `ActivityRepository` | ActivityLog | activity_log.py | baixo |
| 13 | `DiscoveryRepository` | DiscoveryIntent, DiscoverySavedSearch, DiscoverySearchHistory | discovery_executor | médio |
| 14 | `ConfigRepository` | DefaultBoardConfiguration(+Audit), DesignSystem(+Board/Audit), AppSetting | default_board_configuration, design_system, settings_service | médio (0.2.5 novo) |
| 15 | `AmendmentRepository` | AmendmentHotfixRevision, CanonicalDebt | amendment_revision, canonical_debt_service | médio (0.2.5 novo) |

> **Atenção #11 (KgQueueRepository):** os workers (`consolidation`, `outbox`,
> `deterministic`) usam padrões de lock/advisory e `SELECT ... FOR UPDATE`
> dialeto-específicos. Esse repositório carrega semântica de concorrência que
> precisa ser parte explícita do contrato (ver decisão D4). Extrair por último.

---

## 5. Padrão de refatoração (antes → depois)

**Antes** — `api/boards.py`:

```python
@router.post("", ...)
async def create_board(data: BoardCreate, user_id: str = Depends(require_user),
                       realm_id: str | None = Depends(get_realm_id),
                       db: AsyncSession = Depends(get_db)):
    service = BoardService(db)
    board = await service.create_board(user_id, data, realm_id=realm_id)
    await db.commit()                      # ⚠️ transação na rota
    board = await service.get_board(board.id)
    ...
```

**Depois** — rota depende de `UnitOfWork`, serviço depende de repositórios:

```python
@router.post("", ...)
async def create_board(data: BoardCreate, user_id: str = Depends(require_user),
                       realm_id: str | None = Depends(get_realm_id),
                       uow: UnitOfWork = Depends(get_uow)):
    async with uow:                        # transação no domínio
        board = await BoardService(uow).create_board(user_id, data, realm_id=realm_id)
        await uow.commit()
    return await BoardService(uow).get_board(board.id)
```

```python
# core/services/main.py
class BoardService:
    def __init__(self, uow: UnitOfWork):   # antes: (self, db: AsyncSession)
        self._boards = uow.boards          # BoardRepository (Protocol)

    async def create_board(self, user_id, data, *, realm_id=None) -> Board:
        return await self._boards.add(Board(... ), realm_id=realm_id)
```

```python
# core/repositories/interfaces/board.py
class BoardRepository(Protocol):
    async def get(self, board_id: str, *, realm_id: str | None = None) -> Board | None: ...
    async def list_for_user(self, user_id: str, *, realm_id: str | None,
                            offset: int, limit: int, view: str) -> tuple[list[Board], int]: ...
    async def add(self, board: Board, *, realm_id: str | None = None) -> Board: ...
    async def update(self, board: Board) -> Board: ...
    async def delete(self, board_id: str) -> bool: ...
```

O `SqlAlchemyBoardRepository` contém o `select(Board).where(...)` que hoje vive
no serviço. **A regra de negócio fica no serviço; o SQL fica no adaptador.**

---

## 6. Fases de implementação

Estratégia: **strangler fig** — introduzir as portas e migrar agregado a agregado,
mantendo `get_db`/`AsyncSession` funcionando em paralelo até o fim. Nada de big bang.

### Fase 0 — Andaime (sem mudar comportamento)
- Criar `core/repositories/interfaces/` (Protocols) e `core/repositories/sqlalchemy/`.
- Definir `UnitOfWork` Protocol + `SqlAlchemyUnitOfWork` (envolve `get_session_factory()`).
- `get_uow` dependency em `infra/` que devolve uma UoW por request.
- **Critério:** `pyright`/`ruff` verdes; nenhuma rota alterada ainda.

### Fase 1 — Piloto: Board + Agent (repos 1–2, baixo risco)
- Extrair `BoardRepository`/`AgentRepository` e seus adaptadores SQLAlchemy.
- Migrar `BoardService`/`AgentService` para `UnitOfWork`; mover `commit()` das
  rotas de board/agent para `async with uow`.
- Escrever testes de contrato do repositório (rodam contra o adaptador SQLite).
- **Critério:** suíte existente de boards/agents verde; transação 100% na UoW
  nessas rotas; zero `AsyncSession` em `BoardService`/`AgentService`.

### Fase 2 — Núcleo de domínio (repos 3–8)
- Card, Ideation, Refinement, Story, Spec, Sprint. Maior volume e relacionamentos
  (snapshots/history/QA/KB). Um agregado por PR.
- **Critério:** cada PR remove `AsyncSession` do serviço correspondente; nenhuma
  rota dá `commit()` direto.

### Fase 3 — Periferia (repos 9–10, 12–15)
- Guideline, Architecture, Activity, Discovery, Config (0.2.5), Amendment (0.2.5).
- **Critério:** `grep -rl AsyncSession src/.../services` só sobra em main residual.

### Fase 4 — KG queue/workers (repo 11, sensível)
- Modelar lock/claim/advisory no contrato do `KgQueueRepository` (ver D4).
- Migrar `consolidation`/`outbox`/`deterministic_worker`.
- **Critério:** replay gate (862 eventos) verde; nenhum `SELECT FOR UPDATE`
  fora do adaptador.

### Fase 5 — Migrações e bootstrap
- Extrair as ~44 `_migrate_*` de `infra/database.py` para trás de um
  `SchemaMigrator` Protocol (o SaaS pode usar Alembic; o local mantém o
  imperativo). Ver D3.
- **Critério:** `init_db()` delega ao migrator injetado; core não assume SQLite.

### Fase 6 — Limpeza
- Marcar `get_db`/acesso direto a `AsyncSession` como deprecado no core.
- Atualizar `CLAUDE.md` (premissa "SQLAlchemy direto" → "porta de repositório").

---

## 7. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| Megafile `services/main.py` (~10k linhas, 374 refs) dificulta diffs | Migrar **por agregado**, não por arquivo; um serviço por PR |
| Transação vazada (140 commits na API) | Fase 1 estabelece o padrão UoW; revisar que nenhuma rota nova adicione `commit()` |
| Lazy-loading de relacionamentos do ORM atravessa a porta | Decisão D2: repositórios retornam agregados com carga explícita (`selectinload`) |
| Workers KG com concorrência dialeto-específica | Isolar na Fase 4, contrato explícito de lock (D4) |
| Sem pytest gate no release (caveat conhecido) | Apoiar-se no smoke + replay gate; adicionar testes de contrato de repositório que rodam em SQLite |
| Regressão silenciosa durante strangler | Manter os dois caminhos vivos; só remover `get_db` na Fase 6 |

---

## 8. Decisões em aberto (precisam de definição antes de codar)

- **D1 — Onde mora o adaptador SQLAlchemy?** Começa no core (default, não quebra
  Community) ou já nasce no pacote de adapters do Eixo #2? *Recomendação:* nasce
  no core, migra no Eixo #2.
- **D2 — Domínio vs ORM.** Repositórios devolvem as entidades `Base` atuais
  (pragmático, menos código) ou dataclasses de domínio puras (desacoplamento
  total, custo alto de mapeamento)? *Recomendação:* manter `Base` no curto prazo;
  reavaliar quando um backend não-relacional for real.
- **D3 — Migrações.** Adotar Alembic atrás de `SchemaMigrator`, ou manter o
  imperativo `_migrate_*` como um adaptador entre outros?
- **D4 — Contrato de concorrência do KgQueueRepository.** Como expor
  claim/lock/advisory de forma agnóstica (o SaaS em Postgres usa `FOR UPDATE
  SKIP LOCKED`; o local em SQLite usa busy_timeout)?
- **D5 — `realm_id` em todas as portas.** Tornar obrigatório desde a Fase 1
  (mesmo que a Community passe `None`) para não reescrever assinaturas no SaaS?
  *Recomendação:* sim — parâmetro keyword-only `realm_id` em toda query desde já.

---

## 9. Resultado esperado

Ao fim das fases, o core expõe **portas de repositório + Unit of Work** como
único caminho de persistência relacional. A Community injeta o adaptador
SQLAlchemy/SQLite (provavelmente do pacote de adapters); o SaaS injeta um
adaptador Postgres multi-tenant com `realm_id`. **A regra de negócio do core
deixa de saber qual banco está embaixo** — fechando, no eixo relacional, o mesmo
padrão que Auth, Storage e KG já alcançaram.

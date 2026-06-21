# Estudo: Desacoplamento da Autenticação de Agentes via MCP (Eixo #5)

> **Objetivo:** transformar o **mecanismo de autenticação** dos agentes no
> servidor MCP numa **porta** plugável, para que a Community mantenha o que tem
> hoje (chave opaca por agente, validada localmente) e o SaaS injete um mecanismo
> mais robusto (OIDC/JWT, chaves rotacionáveis com expiração e escopo, revogação),
> **preservando a api_key como identificador por agente**.
>
> **Status:** estudo de arquitetura. Nenhuma mudança de produção foi feita por
> este documento. Branch de análise: `feature/0.2.5` (v0.2.5).
>
> **Relação com outros eixos:** complementa o `AuthProvider` HTTP (eixo já
> resolvido) e o Eixo #4 (realm/multi-tenancy — a identidade do agente deve
> carregar o realm no SaaS). Independe do Eixo #1, mas se beneficia dele para o
> lookup de agente sair do `AsyncSession` cru.

---

## 1. Como funciona hoje

Fluxo de uma chamada MCP autenticada (`core/mcp/server.py`):

```
Request MCP (HTTP, porta 8101)
  └─ ApiKeySessionMiddleware (ASGI)            # extrai a credencial
        api_key = ?api_key=  |  X-API-Key:  |  Authorization: Bearer ...
        _active_api_key.set(api_key)            # ContextVar por request
  └─ tool handler
        _get_authenticated_agent() / _get_agent_ctx(board_id)
           api_key = _active_api_key.get()
           AgentService.get_agent_by_key(api_key)        # VALIDAÇÃO
              key_hash = hash_api_key(api_key)
              select(Agent).where(api_key_hash == key_hash, is_active)   # SQLAlchemy
        → AgentContext(agent_id, name, board_id, permissions)
```

Características atuais:

| Aspecto | Situação hoje |
|---|---|
| Formato da credencial | `dash_{secrets.token_hex(24)}` — bearer opaco |
| Onde validar | `AgentService.get_agent_by_key` — hash + lookup na tabela `Agent` |
| Armazenamento | `Agent.api_key_hash` (hash, ok) **e** `Agent.api_key` plaintext (coluna única indexada — *smell*) |
| Expiração / rotação | **nenhuma** — chave é eterna até `is_active=False` |
| Escopo | board ACL (`AgentBoard`) + `permission_flags`; sem scopes no token |
| Revogação | flag `is_active` + cache de permissão TTL 60s |
| Transporte | query param `?api_key=` na URL (logável/cacheável), header, ou Bearer |
| Mecanismo plugável? | **Não.** A validação chama `get_agent_by_key` diretamente |

### O que JÁ é porta e o que NÃO é

- ✅ **Autorização** já é uma porta: `AuthContext` Protocol
  (`kg/interfaces/auth_context.py`) — `get_agent_id()`, `get_accessible_boards()`,
  `has_admin_role()`, com `MCPAuthContext` como adaptador.
- ❌ **Autenticação** (credencial → identidade do agente) **não é porta**. Está
  embutida: `_get_authenticated_agent` → `AgentService.get_agent_by_key` →
  SQLAlchemy. Não há onde o SaaS injetar OIDC/JWT/rotação sem editar
  `mcp/server.py`.
- ⚠️ Há um caminho legado/alternativo em `MCPSettings`
  (`require_agent_key`, `agent_keys_env` — lista estática de chaves por env), que
  convive com o lookup por banco. Precisa ser unificado sob a mesma porta.

---

## 2. O problema para o SaaS

O modelo atual é correto para a Community (single-user, local, offline): uma
chave opaca por agente, validada contra o SQLite local. Mas o SaaS precisa de
garantias que **não cabem** num bearer eterno validado por lookup direto:

1. **Expiração e rotação** — chaves de vida longa vazadas são um risco; o SaaS
   quer tokens curtos (ex.: trocar a api_key longa por um JWT de minutos) e
   rotação sem downtime.
2. **Escopo no token** — limitar o que um agente pode fazer por *claims*, não só
   por flags no banco (defense-in-depth).
3. **Revogação imediata** — listas de revogação / introspecção, além do
   `is_active` com cache de 60s.
4. **Realm/tenant na identidade** — no multi-tenant (Eixo #4) a identidade do
   agente precisa carregar o `realm_id`; hoje não carrega.
5. **Federação** — validar contra um IdP (Clerk/Auth0/Cognito) ou um serviço de
   identidade próprio, não contra a tabela `Agent` local.
6. **Higiene de transporte** — desencorajar `?api_key=` na URL (vai para logs de
   proxy); preferir header/Bearer.

Nada disso é configurável hoje sem mexer no core.

---

## 3. Arquitetura-alvo

Separar três responsabilidades que hoje estão grudadas — **extração**,
**validação** e **autorização** — e tornar a validação uma porta.

### Princípio reitor: auth é invisível e externa ao agente

O **agente de IA (o LLM que faz tool-calls) nunca vê, gera, nem gerencia
credencial.** Quem porta a credencial é o **cliente MCP** (a config da
ferramenta — ex.: o `mcp.json`); quem a valida é o **servidor** (middleware +
`McpAuthenticator`), **antes** de qualquer tool handler rodar. Isso já é verdade
hoje (o LLM só conhece a URL configurada uma vez) e o desacoplamento **preserva**
essa transparência — no SaaS, com troca de credencial por token curto, quem faz o
handshake é o transporte/cliente, não o agente.

Decorrência: **nenhuma informação de autenticação pertence ao escopo do agente.**
A entidade de domínio `Agent` carrega só identidade + permissões de board; o
metadado de auth (escopos, expiração, claims do token) vive num objeto de infra
efêmero e some no fim da request. O único dado que cruza da auth para o domínio é
"esta request foi autenticada como o agente X".

```
 LLM (agente)  ──tool call──►  Cliente MCP  ──credencial no transporte──►  servidor
   ▲ nunca toca auth             ▲ guarda o segredo

 ── no servidor ─────────────────────────────────────────────────────────────────
 ApiKeySessionMiddleware (extração)        # já existe, vira agnóstico de esquema
        │  McpCredential = {scheme, value, headers}
        ▼
 McpAuthenticator (PORTA — validação)      # NOVO
        authenticate(credential) -> AuthSession | None
        │
        ├─ Community: LocalApiKeyAuthenticator
        │     hash + lookup Agent.api_key_hash  (== comportamento atual)
        │
        └─ SaaS: OidcJwtAuthenticator / RotatingKeyAuthenticator
              valida JWT/introspecção, escopos, expiração, realm
        ▼
 AuthSession (INFRA, efêmero, server-side)   # NOVO — metadado de auth, NÃO é do agente
   { scopes, expires_at, claims, realm_id }   #   descartado no fim da request
        │  exporta para o domínio APENAS:
        ▼
 agent_id (+ realm_id)                        # único elo que cruza para o domínio
        ▼
 AuthContext (PORTA — autorização)         # já existe (boards/admin/permissions)
```

Contratos novos:

```python
# core/mcp/interfaces/authenticator.py
@dataclass(frozen=True)
class McpCredential:
    """O que o transporte apresentou. Vive só durante a request."""
    scheme: str                 # "api_key" | "bearer" | "x-api-key"
    value: str
    headers: Mapping[str, str]  # para mecanismos que precisam de mais contexto

@dataclass(frozen=True)
class AuthSession:
    """Resultado da autenticação — objeto de INFRA, efêmero, server-side.
    NÃO é exposto ao agente nem persistido no Agent. Some no fim da request.
    """
    agent_id: str                              # único campo que cruza p/ o domínio
    realm_id: str | None = None                # derivado p/ o RealmContext (Eixo #4)
    scopes: frozenset[str] = frozenset()       # metadado de auth — não é do agente
    expires_at: datetime | None = None         # idem
    claims: Mapping[str, Any] | None = None    # idem (JWT/IdP)

class McpAuthenticator(Protocol):
    async def authenticate(self, cred: McpCredential) -> AuthSession | None: ...
```

Os tool handlers recebem do contexto apenas o `agent_id` já autenticado (e o
`realm_id` via `RealmContext`). Nunca o `AuthSession` inteiro, nunca a
`McpCredential`.

Registro via o mesmo padrão dos outros providers
(`configure_mcp_authenticator(...)` no composition root, default explícito na
edição — não no core). A Community registra `LocalApiKeyAuthenticator`; o SaaS
registra o seu.

> **Preserva a api_key por agente — sem colocá-la no escopo do agente:** existe
> uma credencial por agente (o identificador), mas ela é guardada pelo **cliente
> MCP** e validada pelo **autenticador**, não pela entidade `Agent`. No SaaS a
> api_key longa é só o segredo inicial trocado por um token curto. O conceito "uma
> credencial por agente" permanece; a *gerência* da auth fica fora do agente.

---

## 4. Pontos de acoplamento a mover

| Item | Hoje | Alvo |
|---|---|---|
| `_get_authenticated_agent()` | chama `AgentService.get_agent_by_key` | chama `McpAuthenticator.authenticate` |
| `_get_agent_ctx(board_id)` | idem + checa `AgentBoard` | usa `AuthSession.agent_id` + `AuthContext` |
| `ApiKeySessionMiddleware` | seta `_active_api_key: str` | seta `_active_credential: McpCredential` |
| `MCPSettings.agent_keys_env` | lista estática paralela | um `StaticKeyAuthenticator` opcional sob a porta |
| `Agent.api_key` / `api_key_hash` | credencial **na entidade de domínio** | sai do `Agent`; segredo é responsabilidade do mecanismo de auth (ver D4) |
| Geração `dash_{token_hex(24)}` | em `AgentService` | fica no adaptador local; SaaS define o seu formato |
| Metadado de auth (escopos/expiração) | n/a | vive no `AuthSession` efêmero, **nunca** no `Agent` |

---

## 5. Fases

### Fase 0 — Definir as portas (sem mudar comportamento)
- Criar `McpCredential`, `AuthSession`, `McpAuthenticator` em
  `core/mcp/interfaces/`.
- `configure_mcp_authenticator()` + lazy default que embrulha o
  `get_agent_by_key` atual (compat total).
- **Critério:** pyright/ruff verdes; fluxo idêntico ao atual.

### Fase 1 — Rotear a validação pela porta
- `_get_authenticated_agent`/`_get_agent_ctx` passam a chamar
  `McpAuthenticator.authenticate`; o lookup por banco vira o
  `LocalApiKeyAuthenticator` (adaptador).
- Unificar o caminho `agent_keys_env` sob um `StaticKeyAuthenticator` (composável).
- **Critério:** replay gate verde; `mcp/server.py` não chama `get_agent_by_key`
  diretamente; comportamento Community inalterado.

### Fase 2 — Credencial estruturada + higiene de transporte
- Middleware passa a propagar `McpCredential` (esquema + valor), não uma string.
- Emitir *warning* quando a credencial vier por `?api_key=` na URL (recomendar
  header/Bearer); manter aceitação para compat.
- **Critério:** `_active_credential` carrega o esquema; logs alertam uso de query
  param.

### Fase 3 — Sessão carrega realm e escopo (liga ao Eixo #4)
- `AuthSession.realm_id` populado pelo autenticador → propagado ao `RealmContext`.
  Scopes do `AuthSession` consultados na autorização. Nada disso encosta no `Agent`.
- **Critério:** no SaaS, a sessão de auth carrega tenant; Community usa realm
  `local`; a entidade `Agent` permanece sem campos de auth.

### Fase 3.5 — Tirar a credencial do `Agent` (ver D4)
- Migração: mover `api_key`/`api_key_hash` para fora da entidade `Agent` (tabela
  de credenciais própria do adaptador de auth, ou serviço externo no SaaS). O
  `Agent` fica só com identidade + permissões.
- **Critério:** `Agent` sem campos de credencial; chave exibida só na criação
  (one-time reveal); auth deixa de pertencer ao escopo do agente.

### Fase 4 — Adaptador SaaS de referência (prova de plugabilidade)
- Implementar um `OidcJwtAuthenticator` (ou `RotatingKeyAuthenticator`) na edição
  SaaS: valida JWT/introspecção, expiração, revogação, troca api_key↔token curto
  (sempre no transporte/cliente — o agente nunca participa).
- **Critério:** trocar o autenticador via `configure_mcp_authenticator` sem tocar
  no core nem nos tool handlers. (Prova de que o eixo terminou.)

---

## 6. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| `_active_api_key` (ContextVar) é usado em vários pontos | Manter um shim `_active_api_key` derivado de `_active_credential` durante a transição |
| Cache de permissão (TTL 60s) pode mascarar revogação | No SaaS, autenticador valida expiração no token a cada request; cache só cobre autorização, não autenticação |
| Caminho legado `agent_keys_env` diverge do lookup por banco | Fase 1 unifica ambos sob a porta como autenticadores composáveis |
| Credencial (`api_key`/`hash`) hoje vive na entidade `Agent` | D4: mover o segredo para o mecanismo de auth (fora do `Agent`); exibir a chave só na criação (one-time reveal) |
| Replay gate é a rede de segurança do MCP | Rodar a cada fase; é o critério de não-regressão |
| MCP roda como sub-app no mesmo processo (single-process) | A porta é registrada no mesmo composition root que já registra session factory; sem mudança no modelo de processo |

---

## 7. Decisões em aberto

- **D1 — Composição de autenticadores:** permitir *cadeia* (ex.: tenta JWT, cai
  para api_key) via um `ChainAuthenticator`, ou um único por edição?
  *Recomendação:* cadeia — facilita migração e o caminho `agent_keys_env`.
- **D2 — Troca api_key→token curto no SaaS:** o **cliente MCP** apresenta a api_key
  longa e o servidor troca por um token curto (introspecção por request) **ou** o
  cliente faz um handshake OAuth2 antes? Em ambos os casos o **agente (LLM) não
  participa** — é transporte/cliente. *Recomendação:* começar por introspecção
  server-side (menos mudança no cliente MCP).
- **D3 — Realm na sessão:** derivar `AuthSession.realm_id` do `Agent` (via Eixo #4)
  ou dos claims do IdP no SaaS? *Recomendação:* dos claims no SaaS; do
  `Agent.realm_id` na Community. (Vai para o `AuthSession`, não para o agente.)
- **D4 — Credencial fora do `Agent`:** remover as colunas `Agent.api_key` /
  `api_key_hash` da entidade de domínio e mover o armazenamento do segredo para o
  mecanismo de auth (tabela/serviço próprio do adaptador). A entidade `Agent` fica
  só com identidade + permissões. Impacta telas que relistam a chave (passar a
  one-time reveal na criação). *Recomendação:* sim — é o cerne de "auth não pertence
  ao escopo do agente".
- **D5 — Onde mora o adaptador:** `LocalApiKeyAuthenticator` no core (default) ou
  no pacote de adapters (Eixo #2)? *Recomendação:* nasce no core, migra no #2.

---

## 8. Resultado esperado

A autenticação de agentes MCP deixa de ser um lookup de chave embutido e passa a
ser uma **porta** (`McpAuthenticator`) cujo resultado é um `AuthSession` de
infra, efêmero e server-side. **Auth fica invisível e externa ao agente:** o LLM
nunca toca em credencial, a entidade `Agent` não carrega segredo nem metadado de
auth, e o único dado que cruza para o domínio é o `agent_id` autenticado. A
Community mantém exatamente o que tem hoje — uma api_key opaca por agente,
validada localmente — como um adaptador entre outros. O SaaS injeta um mecanismo
robusto (OIDC/JWT, rotação, expiração, escopo, revogação, realm) **sem tocar no
core nem nos handlers de ferramenta**, fechando para a autenticação MCP o mesmo
padrão hexagonal que o `AuthProvider` HTTP, o `StorageProvider` e o
`KGProviderRegistry` já alcançaram. A api_key por agente é preservada como
identificador — guardada pelo cliente MCP e validada pelo autenticador, **fora do
escopo do agente**, exatamente como pedido.

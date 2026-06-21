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
**validação** e **autorização** — e tornar a validação uma porta:

```
 ApiKeySessionMiddleware (extração)        # já existe, vira agnóstico de esquema
        │  credential = {scheme, value, headers}
        ▼
 McpAuthenticator (PORTA — validação)      # NOVO
        authenticate(credential) -> AgentIdentity | None
        │
        ├─ Community: LocalApiKeyAuthenticator
        │     hash + lookup Agent.api_key_hash  (== comportamento atual)
        │
        └─ SaaS: OidcJwtAuthenticator / RotatingKeyAuthenticator
              valida JWT/introspecção, escopos, expiração, realm
        ▼
 AgentIdentity { agent_id, realm_id, scopes, ... }   # NOVO — DTO agnóstico
        │
        ▼
 AuthContext (PORTA — autorização)         # já existe (boards/admin/permissions)
```

Contratos novos:

```python
# core/mcp/interfaces/authenticator.py
@dataclass(frozen=True)
class McpCredential:
    scheme: str                 # "api_key" | "bearer" | "x-api-key"
    value: str
    headers: Mapping[str, str]  # para mecanismos que precisam de mais contexto

@dataclass(frozen=True)
class AgentIdentity:
    agent_id: str
    realm_id: str | None = None
    scopes: frozenset[str] = frozenset()
    expires_at: datetime | None = None

class McpAuthenticator(Protocol):
    async def authenticate(self, cred: McpCredential) -> AgentIdentity | None: ...
```

Registro via o mesmo padrão dos outros providers
(`configure_mcp_authenticator(...)` no composition root, default explícito na
edição — não no core). A Community registra `LocalApiKeyAuthenticator`; o SaaS
registra o seu.

> **Preserva a api_key por agente:** o `AgentIdentity.agent_id` continua sendo
> resolvido a partir de uma credencial por agente. No SaaS, a api_key longa pode
> ser apenas o *identificador*/segredo inicial que é trocado por um token curto —
> mas o conceito "uma credencial por agente" permanece, como você pediu.

---

## 4. Pontos de acoplamento a mover

| Item | Hoje | Alvo |
|---|---|---|
| `_get_authenticated_agent()` | chama `AgentService.get_agent_by_key` | chama `McpAuthenticator.authenticate` |
| `_get_agent_ctx(board_id)` | idem + checa `AgentBoard` | usa `AgentIdentity` + `AuthContext` |
| `ApiKeySessionMiddleware` | seta `_active_api_key: str` | seta `_active_credential: McpCredential` |
| `MCPSettings.agent_keys_env` | lista estática paralela | um `StaticKeyAuthenticator` opcional sob a porta |
| `Agent.api_key` (plaintext) | coluna única indexada | manter só `api_key_hash`; o plaintext sai (ver D4) |
| Geração `dash_{token_hex(24)}` | em `AgentService` | fica no adaptador local; SaaS define o seu formato |

---

## 5. Fases

### Fase 0 — Definir as portas (sem mudar comportamento)
- Criar `McpCredential`, `AgentIdentity`, `McpAuthenticator` em
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

### Fase 3 — Identidade carrega realm e escopo (liga ao Eixo #4)
- `AgentIdentity.realm_id` populado pelo autenticador; propagar para o
  `RealmContext`. Scopes opcionais consultados na autorização.
- **Critério:** no SaaS, identidade do agente carrega tenant; Community usa realm
  `local`.

### Fase 4 — Adaptador SaaS de referência (prova de plugabilidade)
- Implementar um `OidcJwtAuthenticator` (ou `RotatingKeyAuthenticator`) na edição
  SaaS: valida JWT/introspecção, expiração, revogação, troca api_key↔token curto.
- **Critério:** trocar o autenticador via `configure_mcp_authenticator` sem tocar
  no core nem nos tool handlers. (Prova de que o eixo terminou.)

---

## 6. Riscos e mitigações

| Risco | Mitigação |
|---|---|
| `_active_api_key` (ContextVar) é usado em vários pontos | Manter um shim `_active_api_key` derivado de `_active_credential` durante a transição |
| Cache de permissão (TTL 60s) pode mascarar revogação | No SaaS, autenticador valida expiração no token a cada request; cache só cobre autorização, não autenticação |
| Caminho legado `agent_keys_env` diverge do lookup por banco | Fase 1 unifica ambos sob a porta como autenticadores composáveis |
| Plaintext `Agent.api_key` no banco | D4: parar de persistir o plaintext; exibir a chave só na criação (one-time reveal) |
| Replay gate é a rede de segurança do MCP | Rodar a cada fase; é o critério de não-regressão |
| MCP roda como sub-app no mesmo processo (single-process) | A porta é registrada no mesmo composition root que já registra session factory; sem mudança no modelo de processo |

---

## 7. Decisões em aberto

- **D1 — Composição de autenticadores:** permitir *cadeia* (ex.: tenta JWT, cai
  para api_key) via um `ChainAuthenticator`, ou um único por edição?
  *Recomendação:* cadeia — facilita migração e o caminho `agent_keys_env`.
- **D2 — Troca api_key→token curto no SaaS:** o agente manda a api_key longa e o
  servidor troca por um token curto (introspecção por request) **ou** o cliente MCP
  faz um handshake OAuth2 antes? *Recomendação:* começar por introspecção
  server-side (menos mudança no cliente MCP).
- **D3 — Realm na identidade:** derivar `realm_id` do `Agent` (via Eixo #4) ou do
  IdP/claims no SaaS? *Recomendação:* dos claims no SaaS; do `Agent.realm_id` na
  Community.
- **D4 — Plaintext da api_key:** remover a coluna `Agent.api_key` e expor a chave
  só na criação (one-time)? Impacta telas que relistam a chave.
- **D5 — Onde mora o adaptador:** `LocalApiKeyAuthenticator` no core (default) ou
  no pacote de adapters (Eixo #2)? *Recomendação:* nasce no core, migra no #2.

---

## 8. Resultado esperado

A autenticação de agentes MCP deixa de ser um lookup de chave embutido e passa a
ser uma **porta** (`McpAuthenticator`) com uma identidade agnóstica
(`AgentIdentity`). A Community mantém exatamente o que tem hoje — uma api_key
opaca por agente, validada localmente — como um adaptador entre outros. O SaaS
injeta um mecanismo robusto (OIDC/JWT, rotação, expiração, escopo, revogação,
realm) **sem tocar no core nem nos handlers de ferramenta**, fechando para a
autenticação MCP o mesmo padrão hexagonal que o `AuthProvider` HTTP, o
`StorageProvider` e o `KGProviderRegistry` já alcançaram. A api_key por agente é
preservada como identificador, exatamente como pedido.

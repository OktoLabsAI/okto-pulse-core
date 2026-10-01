# Reavaliação da 0.4.0 — contrato único, sem migração de legado

Data: 2026-10-01. Assessment inicial sobre Core `e8009733` e Community `746a57c6`;
revisão corrente sobre Core `e7f6a8e6` e Community `d8e4fddf`, ambos em
`feature/v0.4.0`, com alterações locais de C1/C2 ainda não publicadas.
**Execução retomada por instrução explícita do usuário: concluir a implementação,
com commits e pushes nos milestones e ledger atualizado. A parada para revisão
foi encerrada; seguir C1–C4 sem reintroduzir suporte ao legado.**
O estado factual, inclusive o trabalho incompleto, está registrado abaixo e no ledger.

## Direção

Adotar a 0.4.0 como instalação nova, com um único contrato de dados, execução e API.
Retirar o suporte a bases, payloads e fluxos anteriores, inclusive os caminhos de
compatibilidade introduzidos durante esta iniciativa. Não criar um pacote de legado,
um conversor separado ou uma segunda modalidade de execução.

Esta direção decorre da instrução do usuário para uma breaking change completa.
Ela substitui as obrigações de migração e compatibilidade retroativa do pacote v1.3.
Os requisitos funcionais e de governança continuam sendo os dos quatro documentos
consolidados, com as decisões já autorizadas. A avaliação não reabre a ideação original.

Uma instalação usa armazenamento novo. Armazenamento incompatível deve ser recusado
antes de qualquer alteração, com diagnóstico explícito; não será convertido, apagado
ou reinicializado automaticamente. Isso inclui bases de desenvolvimento desta branch
que tenham um formato anterior ao schema final. Reiniciar uma base já criada no formato
final da 0.4.0 continua sendo uma operação normal e deve preservar os dados.

## O que já foi feito e continua aproveitável

O trabalho existente implementou remoção de Sprint das superfícies operacionais,
governança sem Sprint, contratos arquiteturais e verificabilidade, resolução de
obrigações diretas/herdadas, evidência incremental por Card, lote atômico, retomada,
admissão de provas e consultas de bugs, cobertura, impacto e linhagem. Há implementação
em Core, adapters Community, REST/MCP e frontend, com evidências no ledger.

Essas funcionalidades permanecem. A simplificação elimina a convivência com o modelo
anterior; não exige reescrevê-las nem substituí-las por funcionalidades novas.

O inventário de aceite anterior registra 154 critérios verificados, 29 parciais e
63 não auditados. Esses números descrevem a auditoria v1.3, não uma entrega completa
nem uma porcentagem de conclusão do novo escopo. Testes que dependem de contratos
retirados precisarão ser removidos ou reescritos; resultados anteriores não certificam
automaticamente o produto após a retirada.

## Onde está a complexidade removível

### Estado confirmado nesta revisão

| Parte | Estado real | Consequência para a execução futura |
|---|---|---|
| Contrato de execução obrigatório, retirada de adoção de versão e fallback de Delivery no adapter/UI | Publicados em Core `cb11a2f2` / Community `d8e4fddf` | Preservar e incluir na regressão final; não implementar novamente. |
| Veredito de Delivery exige contexto efetivo | Publicado em Core `e7f6a8e6` | Preservar os predicados internos de recibos, lifecycle e waivers; eles atendem ao contrato atual. |
| Remoção da policy migrada por Card | WIP nos dois repos: domínio, schemas, MCP, coluna SQL, etapa DDL, porta/adapter e frontend | Concluir como um incremento coordenado com os consumidores offline de C2. Não publicar o WIP isoladamente. |
| Conversões de startup | Retiradas no WIP dos dois lifespans; implementações exclusivas de Q&A/achados/sweep também retiradas | Preservar inicialização, seeds e recuperação atuais; qualificar novamente no par final. |
| Inicialização relacional sem upgrade | WIP com admissão de formato antes de WAL, criação transacional e seeds atuais | Schema ainda contém campos/tabelas/guards mistos; não é o schema final limpo. |
| Cadeia offline de retirement e arquivo importado | Ainda presentes, inclusive consumidores do adapter já excluído | Retirar em conjunto; substituir proteção de raízes por admissão atual também no grafo. |
| Waivers, normalizadores antigos e demais superfícies | Retirada/separação ainda pendente | Seguir C1–C3, preservando as responsabilidades atuais explicitadas neste documento. |
| Qualificação integral do produto simplificado | Não realizada | C4 permanece aberto; resultados parciais não certificam o par final. |

O WIP excluiu `adapters/card_validation_retirement.py`, mas
`retirement_bootstrap.py`, `retirement_data_journal.py`, `retirement_offline_run.py`
e `sprint_work_retirement.py` ainda o importam. Essa dependência já existe no estado
local; não é uma nova frente. A execução deverá retirar a cadeia obsoleta e resolver
seus consumidores atuais, sem restaurar a compatibilidade ou deixar módulos vazios.

Recibos locais consultados nesta revisão, sem reexecução:

- `clean-break-policy4-core.xml`: 18 aprovados, incluindo os seis casos MCP com
  adapter atual; a projeção residual de `sprint_id` foi retirada. Não refazer a correção.
- `clean-break-startup2-core.xml`: 82 aprovados;
  `clean-break-startup1-community.xml`: 60 aprovados.
- `clean-break-schema3.xml`: 18 aprovados para criação/admissão/seeds atuais.
- `clean-break-schema-locks1.xml`: 8 aprovados e 3 falhas. Os três testes acessam
  `_migrator`, removido do orchestrator. Adaptar sua instrumentação ao inicializador
  atual e ao seed, mantendo a prova de mutex nos caminhos concrete/Core/Community.
  Não considerar essa propriedade qualificada enquanto os testes não passarem.

O frontend e o par instalado deverão ser reconstruídos após fechar o incremento.
Esses resultados parciais não certificam a retirada integral do legado.

O [inventário estático](clean-break-040-static-inventory.json) registra arquivos,
hashes, contagens e consumidores de imports da base inicial; não representa uma nova
contagem do WIP corrente. Os caminhos abaixo são relativos a
`src/okto_pulse/core/` ou `src/okto_pulse/community/`, conforme a coluna.

| Frente | Evidência no código | Alteração planejada |
|---|---|---|
| Upgrade de Sprint | Community `adapters/*retirement*.py`; portas correspondentes no Core | Retirar inventário antigo, preflight de conversão, journals, snapshots de corte, checkpoints e transformação de policies/fontes. |
| Startup com reparos antigos | Community `main.py`: `migrate_all_boards`, backfills de Q&A/achados arquiteturais e adoção antes do schema sweep | Retirar chamadas e implementações de conversão; manter inicialização e seeds necessários à instalação nova. |
| Evolução relacional | Community `adapters/relational_schema_lifecycle.py`, `relational_schema_migrator.py`, `relational_schema_steps.py`, `data_bootstrap_steps.py` | Criar/verificar somente o schema final; eliminar sequência de upgrades, import de avaliações antigas e backfill de propagação. |
| Overrides migrados por Card | Core `domain/task_validation_policy.py`; coluna `migrated_validation_policy` Community | Retirar tipos, proveniência e resolver migrados. Cards novos usam a configuração vigente de Spec/Board/defaults, sem herança de Sprint. |
| Permissões de migração e arquivos de Sprint | Core `ports/historical_archive*`, `domain/permission_migration_review.py`; Community `api/historical_archives.py` e adapters associados | Retirar a superfície e os grants destinados à origem arquivada; atualizar composition, UoW e contexto. Manter autorização de recursos atuais. |
| Dois contratos de execução | Core `domain/execution_contract.py`; Community `adapters/sqlalchemy_delivery_evidence.py`; frontend `RequirementVerificationPanel.tsx` | Aplicar o contrato atual a toda Spec. Eliminar ausência de contrato como caminho antigo e a ação de adoção/conversão de Spec legada. |
| Dois caminhos de evidência | Community `adapters/delivery_migration.py`, `delivery_progress_migration.py`, leitores antigos em `sqlalchemy_delivery_evidence.py` | Prova de implementação/teste passa somente pelo ledger canônico por Card. Eliminar import, mapeamento entre ledgers e inferências de completude antigas. Preservar waivers atuais conforme ressalva abaixo. |
| Formatos e classificação antigos | Core `models/schemas.py`, `services/legacy_code_evidence_classification.py`, módulos de import de avaliação; frontend `LegacyEvidenceClassificationDrawer` e normalização de requisitos em `SpecModal` | Retirar conversores, aliases e estados apenas legados. Usar tipos atuais fechados; rejeitar formato inválido sem inventar IDs, perfil ou aprovação. |
| KG e reconstrução de dados antigos | Community `adapters/legacy_rebuild_reconciliation.py`, adapters Grafx de retirement e consumidores em rebuild/health | Retirar reconciliação de fontes antigas e corte entre schemas. Preservar projeção, replay e recuperação necessários a dados criados na 0.4.0. |
| CLI e distribuição | Fallbacks antigos em Community `cli.py`, schemas públicos, resources e frontend | Retirar comandos/aliases e importadores exclusivos de formatos anteriores; atualizar contratos, geração e assets como um único par de release. |

Dimensionamento por nomes de módulos, sem testes, frontend ou documentação:

| Grupo de candidatos | Core: arquivos / linhas | Community: arquivos / linhas |
|---|---:|---:|
| Retirement | 8 / 874 | 49 / 9.487 |
| Nome contém legacy | 7 / 2.595 | 4 / 2.989 |
| Migration/migrator | 3 / 344 | 5 / 1.388 |
| Backfill | 0 / 0 | 1 / 361 |
| Historical archive | 4 / 512 | 5 / 627 |
| **Total** | **22 / 4.325** | **64 / 14.852** |

São **86 arquivos candidatos, com 19.177 linhas físicas**, não uma lista de exclusão
segura nem uma promessa de redução. Há funções atuais misturadas nesses arquivos e
branches de compatibilidade em outros módulos. O levantamento encontrou ainda
11 consumidores externos ao grupo no Core e 18 no Community, considerando imports
na própria edição; não é um grafo completo de chamadas entre os repositórios.

## Dependências que precisam ser resolvidas junto com a retirada

1. **Waivers continuam tendo função atual.** O adapter de Delivery ainda consulta a
   tabela antiga para exceções humanas e revogações, mesmo quando a prova vem do
   ledger por Card. Retirar os kinds antigos de implementation/test e reduzir a
   persistência remanescente à responsabilidade de exceção. Manter permissões,
   auditoria, revogação e efeito no rollup; não converter waiver em progresso nem
   conceder ao executor uma nova forma de dispensar prova.
2. **Adoção de arquitetura não é sinônimo de conversão de versão.** A seleção dos
   Architecture Designs efetivos, suas revisões e proveniência permanecem. Em
   `architecture_adoption.py`, retirar a interpretação legada da ausência do campo;
   manter a seleção explícita e a resolução de candidatos do contrato atual.
3. **Recuperação atual não é migração.** Outbox, replay, retomada de operação interrompida,
   active sets, integridade relacional e reconstrução das projeções atuais permanecem
   quando necessários ao funcionamento da 0.4.0. Cortar apenas a conversão entre
   modelos. Não acrescentar APIs/tools de manutenção.
4. **Compatibilidade do par de distribuição permanece.** `distribution_compatibility.py`
   verifica Core/Community/frontend e o contrato entre as edições. Isso evita executar
   componentes incompatíveis; não sustenta duas versões do produto. Manter também
   versão/fingerprint de armazenamento e validação de métodos de prova.
5. **Histórico da própria 0.4.0 permanece.** Avaliações de edições anteriores da mesma
   Spec, revisões, decisões, rejeições, reabertura, evidências assinadas e activity log
   são parte do domínio atual. Retirar arquivo importado de Sprint não autoriza apagar
   esses registros nem tornar uma aprovação antiga válida para conteúdo alterado.

Helpers ainda necessários que estejam em módulos de retirement devem passar para
seus donos atuais, sem wrappers com o nome antigo. A remoção precisa alcançar a cadeia
porta → composição → adapter → persistência/projeção → consumidor, incluindo imports
entre os dois repositórios. Renomear um arquivo sem retirar a dupla semântica não fecha
a frente.

## Como o plano consolidado muda

| Fonte do plano v1.3 | Tratamento para 0.4.0 |
|---|---|
| BASE F2A–F2D | Superadas as obrigações de inventariar, preservar, converter e cortar bases de Sprint. Preservadas ausência de Sprint no schema novo e integridade dos recursos atuais. |
| BASE F3 e demais frentes funcionais | Mantidas, incluindo execução sem Sprint, hotfix, permissões, Done e separação de autoridade. |
| KG §8.1 | Mantido schema final coerente, identidade, fingerprint e ausência de Sprint. Okto Grafx continua sendo o runtime; não reintroduzir Kùzu/Ladybug. |
| KG §8.2–8.4 | Retirados migração histórica, census de bases antigas e candidato/cutover de upgrade. Mantidos Q15 sem Sprint, raízes legítimas, proveniência e consistência das projeções atuais. |
| DEI §11 | Retirados leitura/import/conversão de prova antiga e paridade de veredito retroativo. Mantidos um único writer efetivo, waivers/revogações e prova atual sem fabricação de evidência. |
| ARQVER §11 | Toda Spec nova obedece ao contrato atual; retirados rollout por situação legada, adoção assistida e tratamento especial de metadados antigos. |
| Instalação/distribuição e instrução de entrada | Substituir upgrade pelo aceite de instalação nova + recusa de armazenamento incompatível. Manter par instalado, frontend empacotado e diagnóstico de incompatibilidade. |

As autorizações anteriores para overrides migrados por Card e ACLs de arquivos
históricos resolviam exigências de compatibilidade agora removidas. Sua motivação e
os comentários de depreciação ficam registrados no histórico Git/ledger; não é necessário
transportá-los como funcionalidade na 0.4.0. As decisões sobre bloquear trabalho normal
em Spec Done e delimitar avaliações por edição continuam válidas.

O pacote v1.3 original e o inventário de aceite ficam preservados como baseline.
Este documento e a entrada no topo do ledger registram a precedência da nova instrução.
Durante a implementação, a disposição abaixo orienta a revisão dos testes; critério
superado deve ser identificado como tal, nunca convertido artificialmente em aprovado.

| Critérios anteriores | Disposição |
|---|---|
| BASE T26, T29, T30, T45 | Retirar do aceite da 0.4.0: upgrade, migração repetida, falha de cutover e import/export do histórico anterior. |
| BASE T27, T28, T31 | Retirar fixtures de preservação/conversão de Sprint. Manter os testes de resolução atual de policy, negações/identidade e integridade relacional já cobertos pelos invariantes funcionais. |
| BASE T25, T33, T34 | Manter: schema/superfície sem Sprint e chamadas/campos removidos recusados antes de efeitos. Não construir um adaptador para o cliente antigo. |
| BASE T37 | Reescrever para navegação válida e estado atual da UI; URL/estado antigo usa descarte/fallback genérico, sem conversão de dados nem request de Sprint. |
| BASE T44 | Manter compatibilidade do par de pacotes/frontend e recusa antecipada; retirar a obrigação de upgrade parcial. |
| KG-02, KG-38 | Retirar conversão de AC textual e reconciliação de dívida histórica importada. Manter identidade inequívoca dos critérios atuais e comportamento de dívida/Learning da 0.4.0. |
| KG-52, KG-53 | Retirar migração de fontes/estado e ensaios de upgrade combinado. Manter Q15 e ausência de Sprint, já exigidos no contrato final. |
| DEI-T54, DEI-T55, DEI-T56 | Retirar migração entre ledgers, claims históricos importados e fallback de partial/complete para registros anteriores. |
| DEI-T59 | Reescrever para criação limpa, reinício da mesma versão, integridade e recusa de formato incompatível; sem migração idempotente. |
| DEI-T61 | Retirar migração da fonte de ImpactEvidenceTest; manter projeção canônica Card→cenário e convergência dos active sets atuais. |
| ARQVER AC-INT-09, ADV-20, ADV-22 | Retirar upgrade legado, rollback desse upgrade e interpretação de Done anterior sem metadados. |
| ARQVER AC-INT-10 | Manter método desconhecido recusado e paridade fonte/pacote; cliente com formato antigo recebe erro explícito, sem adaptação silenciosa. |
| Demais critérios funcionais | Permanecem. Revisões históricas criadas na 0.4.0, replay, concorrência, rollback transacional e provas não se tornam “legado” pelo nome. |

O benchmark também precisa seguir o contrato final: a fixture publicada de comparação
Delivery usa execução antiga `in_progress` e não servirá para validar o fluxo único.
Os recibos ficam preservados como evidência anterior. Manter a medição prevista do
fluxo atual e os limites declarados; não acrescentar uma nova campanha de migração.
Os resultados publicados (catálogo +4,96% e segmento Delivery +4,78% em tokens) não
demonstram economia. A retirada de migradores internos, por si só, não garante redução
de schemas públicos ou custo de uso e não muda esse resultado para “meta atendida”.
Comparações podem usar snapshots e recibos congelados de engenharia; não manter uma
segunda implementação para satisfazer o benchmark, nem adaptar o contrato 0.4.0 para
aceitar fixtures antigas. Se as populações não forem equivalentes, registrar o limite
da comparação em vez de inferir redução.

## Regra para todo o desenvolvimento restante

Cada requisito funcional ainda aberto do plano original será implementado somente
para objetos criados no contrato final da 0.4.0. Isso vale para domínio, persistência,
grafo, REST/MCP, CLI, frontend, documentação de uso e testes distribuídos:

- Não acrescentar migration, backfill, importador antigo, wrapper, alias ou flag de
  adoção para completar uma feature. O fluxo antigo não é um segundo caso de aceite.
- Não inferir dados normativos faltantes por serem antigos. Ausência inválida recebe
  erro; defaults previstos para autoria nova continuam permitidos, sem inventar prova.
- Não carregar permissões, avaliações, overrides ou proveniência de Sprint na criação
  de objetos novos. Usar somente as autoridades e relações atuais.
- Não criar branches `legacy` nos readers, resolvers, rollups, rebuilds ou componentes
  visuais. Resolver as dependências atuais descritas acima antes de excluir seu suporte
  antigo; o objetivo é retirar o caminho, não transferi-lo a outro módulo.
- Manter apenas testes negativos de rejeição de versões/entradas removidas. Retirar
  testes positivos de conversão; testes mistos passam a usar fixtures nativas 0.4.0.
- Conservar os documentos e recibos antigos como histórico de engenharia. Não confundir
  sua permanência no Git com suporte executável/distribuído ao legado.

Essas regras delimitam as etapas C1–C4 e todas as pendências funcionais que vierem do
inventário original. Nenhuma dessas pendências reintroduz implicitamente F2 ou os
rollouts antigos dos complementos.

## Sequência fechada de implementação

| Etapa | Alteração coordenada | Critério de encerramento |
|---|---|---|
| C1 — contrato único | Tornar o contrato atual universal; retirar adoção de versão, resolvers de policy migrada, prova antiga e fallback de requisitos. Separar a responsabilidade atual de waiver e preservar seleção de Designs. | Nenhuma Spec válida escolhe uma execução antiga pela ausência de metadados; testes dos gates e dos métodos de prova atuais preservados. |
| C2 — armazenamento novo | Simplificar lifecycle/schema/seeds; retirar migradores, retirement, arquivos Sprint, colunas/presets exclusivos e branches de upgrade/reconciliação. Resolver consumidores em composition, UoW, recovery, health e KG. | Instalação limpa e reinício funcionam; armazenamento incompatível é recusado antes de escrita; não há upgrade no startup ou conversão em leitura. |
| C3 — superfícies e consumidores | Retirar DTOs/aliases/rotas/resources/CLI/UI de legado e seus testes exclusivos; adaptar testes mistos. Atualizar instruções de uso, schemas gerados, catálogo MCP oficial e frontend embarcado. | REST/MCP/UI oferecem um único caminho; frontend testa os fluxos alterados; payload antigo não ativa fallback e não ganha autoridade. |
| C4 — qualificação e entrega | Completar os critérios funcionais remanescentes do escopo revisado; validar o par 0.4.0 instalado, build do frontend, cenários de uso e auditoria arquitetural. Registrar resultados e limites reais. | Gates aplicáveis verdes, oito budgets ZERO, evidência de bytes do build/testes e ledger atualizado; commits e pushes em ambos os repos. Sem tag/release/deploy automático. |

Cada etapa resolve também os consumidores que quebra; os dois repositórios avançam
como um único produto. Não manter feature flag, fallback temporário ou segunda versão
para deixar uma etapa “verde”. As etapas organizam a execução autorizada;
não exigem pausas entre milestones.

### Ordem concreta de retomada após a revisão

1. Preservar os commits publicados e partir do WIP registrado, sem reset/reaplicação.
   Primeiro adaptar os três testes de lifecycle ao inicializador e seeds atuais,
   mantendo sua asserção de mutex entre processos. Não restaurar o migrator para passar.
   Fechar o incremento de policy junto da retirada de sua cadeia offline: portas e
   adapters de retirement, seus registros no lifecycle/composition e fixtures antigas.
   A fronteira entre C1 e C2 não justifica manter imports quebrados em um commit.
2. Antes de retirar o guard antigo, substituir sua função de proteção por verificação
   do armazenamento final em Community. Auditar `sqlalchemy_database.py`, inclusive
   o listener `PRAGMA journal_mode=WAL`, para impedir mutação de uma base incompatível
   antes da recusa. Criar somente o schema final e seeds atuais; manter exclusão mútua
   de inicialização. Remover conversões de startup e seus jobs/teardown juntos.
   Parte disso já está implementada no WIP: revisar/completar, não reimplementar.
   Retirar os resíduos em `Base.metadata` e `current_relational_objects.json` junto
   dos respectivos writers. O artefato contém 39 índices e 311 triggers suplementares;
   preserva guards atuais, mas ainda aceita estados como `uncategorized_legacy`.
   Remover esses ramos também dos modelos/readers; manter os guards do contrato atual.
3. Concluir o restante de C1: persistência de waiver/revoke, normalizadores e seleção
   explícita dos Designs. Fechar C2 com schema relacional/Grafx, histórico atual,
   filas, health, replay e rebuild sem dependência de retirement.
4. Fechar C3 no mesmo contrato: rotas, DTOs, CLI, MCP, frontend, instruções e fixtures.
   Regerar catálogo e assets depois de estabilizar essas superfícies.
5. Fechar C4 usando [acceptance-inventory.json](acceptance-inventory.json) e a tabela
   de disposição deste assessment: auditar cada critério ainda aplicável, implementar
   somente lacunas reais e registrar prova ou pendência explícita por ID de origem.
   Não reiniciar auditorias de migração nem contar exclusões de escopo como testes verdes.

Essa ordem detalha C1–C4 e resolve a dependência encontrada; não acrescenta milestone,
framework, feature ou campanha autônoma. T23 e KG-10 continuam decisões isoladas,
descritas abaixo, sem impedir a especificação do restante do plano.

### C1 — tarefas e provas de encerramento

1. Em `execution_contract.py`, exigir o contrato vigente na criação de Spec e em
   suas entradas canônicas; eliminar `None` como escolha de comportamento e retirar
   a operação `adopt_execution_contract`. Atualizar seus callers nos dois repos.
2. Em `task_validation_policy.py`, retirar `MigratedTaskValidationPolicy`, leitores/
   planners/rejeições de escrita do campo migrado e a precedência histórica de Sprint.
   Manter os campos atuais de policy e sua autoridade; remover o campo de contratos
   públicos e modelos de criação/leitura.
3. Em `sqlalchemy_delivery_evidence.py`, eliminar leitura/escrita de implementation/test
   do ledger antigo, `_execution_plan`/rollup alternativos e tradução de registros
   incompletos antigos. Fechar a responsabilidade atual de waiver/revoke numa persistência
   de exceções, sem cópia/migração e sem um segundo ledger de prova.
4. Remover normalizadores de formatos antigos de critérios/requisitos/evidências e
   importadores/classificadores de legado. Manter herança explícita, identidade, métodos
   suportados e os defaults legítimos de criação nova.
5. Nas rotas de adoção arquitetural, manter a seleção real de Designs e revisões; retirar
   somente o caminho que interpreta ausência como contrato anterior.

Provas: fixtures nativas para criação, planejamento, início, registro incremental,
rollup e conclusão; negação por falta de grant/independência; waiver/revoke autorizado;
policies de Spec/Board; ausência de contrato inválido sem bypass. Não testar migração
de uma Spec antiga para tornar esses cenários verdes.

### C2 — tarefas e provas de encerramento

1. No lifecycle Community, substituir a chamada ao migrator por criação/verificação
   do formato final. Fazer a identificação de armazenamento incompatível antes de
   DDL, seeds, writers ou reparos de grafo. Não remover a verificação de versão/fingerprint.
2. Retirar de `main.py` as chamadas de migração/backfill e de schema sweep voltadas à
   conversão antiga. Manter apenas os seeds e a composição necessários ao runtime atual.
3. Retirar os módulos de retirement, migração e arquivo Sprint com suas portas, colunas,
   journals e presets exclusivos. Remover `migrated_validation_policy` e os campos
   `permission_migration_review` do schema final. Não escrever uma migration de DROP.
4. Resolver imports e responsabilidades restantes em composition, UoW, histórico atual,
   health, filas, rebuild e recuperação. Helpers reutilizados passam ao dono atual;
   módulos vazios de compatibilidade não permanecem.
5. Fixar o schema final do Okto Grafx e seus emissores/consumidores da 0.4.0. Retirar
   conversão/reconciliação de fontes anteriores, preservando reconstrução determinística
   e recuperação de operações atuais onde o runtime depende delas.

Dependências concretas já identificadas, a fechar dentro desses cinco itens:

| Ponto de intervenção Community | Fechamento necessário |
|---|---|
| `composition.py` → `require_retirement_activation_roots` | Substituir a proteção de cutover pela admissão de armazenamento atual; não deixar adoção automática de grafo antigo no resolver de rotas. |
| `retirement_bootstrap`, `retirement_data_journal`, `retirement_offline_run`, `sprint_work_retirement` | Excluir com a cadeia offline e suas exports/portas; resolver os imports do adapter Card já removido, sem stub. |
| `relational_schema_migrator`, `relational_schema_steps`, `data_bootstrapper`, `data_bootstrap_steps` | Excluir depois de separar todos os guards/seeds atuais; revisar exports de `adapters/__init__.py` e manifests. Não manter o ledger de upgrades nem corrigir a tupla órfã para reativá-lo. |
| `historical_archive_grant_installation`, `historical_archive_reader`, `historical_context_reader` | Retirar arquivo/contexto originado de Sprint com portas, UoW, ACLs e REST associados; preservar histórico nativo e suas negações. |
| `kg_operational`, `relational_effects`, `sqlalchemy_consolidation`, `sqlalchemy_domain_event_delivery` | Retirar status/filtros exclusivos de work retirement; preservar entrega, retry e replay de eventos atuais. |
| `materialization_health`, `sqlalchemy_kg_health`, `sqlalchemy_queue_health` | Retirar filtros `retired_work_origin_exists` com a porta/helper correspondente; testar contagem e visibilidade da fila atual. |
| `joint_recovery_snapshot`, `legacy_rebuild_reconciliation` e consumidores | Separar helpers necessários à recuperação atual antes da exclusão; não remover recuperação apenas pelo nome do módulo. |

Isso é uma lista de dependências verificadas, não autorização para exclusão por glob.
Toda referência remanescente deve ser classificada pelo uso: conversão removida,
responsabilidade atual preservada ou histórico de engenharia fora do runtime.

Provas em armazenamento descartável: criação vazia, reinício com dados 0.4.0 preservados,
recusa de armazenamento incompatível sem alteração do conteúdo, interrupção/recuperação
de operação atual, integridade referencial e paridade incremental/rebuild nos cenários
autorizados. Nenhuma base de usuário será usada ou apagada para demonstrar instalação limpa.

### C3 — tarefas e provas de encerramento

1. Retirar API de arquivos históricos importados, grants exclusivos, imports no router,
   capabilities e registros públicos correspondentes. Manter acesso ao histórico atual
   por seus controles próprios, sem ampliá-lo para `board.read`.
2. Remover ação de adoção de versão no `RequirementVerificationPanel`, drawer de
   classificação legada, fallback de requisitos antigos no `SpecModal` e tipos/clients
   associados. Ajustar testes frontend de autoria, prova, policy e estados de erro.
3. Retirar aliases e formatos antigos da CLI e transportes, incluindo os que apenas
   redirecionam para o writer novo. Chamadas removidas devem falhar sem efeitos.
4. Retirar fixtures/suites cujo único propósito é upgrade. Reescrever as partes atuais
   dos testes mistos conforme a tabela de disposição, sem apagar testes de autoridade,
   prova, integridade ou recuperação da própria 0.4.0.
5. Regenerar catálogo MCP com o gerador oficial, manifests e frontend distribuído;
   atualizar instruções atuais para instalação nova e um único fluxo de execução.

Provas: chamadas REST/MCP e UI dos fluxos alterados; frontend sem requests para entidades
removidas; campos antigos/unknown method recusados; catálogo byte a byte igual ao gerador;
build e assets incorporados coerentes. A busca por referências operacionais deve alcançar
os dois repos; referências do ledger e testes negativos não são falha de limpeza.

### C4 — tarefas e provas de encerramento

1. Aplicar a disposição de critérios ao inventário de trabalho, preservando referência
   ao original e distinguindo `superado`, `mantido` e `reescrito` de resultado de teste.
   Completar somente as lacunas funcionais ainda aplicáveis; não reabrir migrações.
2. Qualificar os fluxos principais e os cenários adversariais afetados: autoria/classificação,
   obrigações efetivas, provas, retomada, conclusão, grants, concorrência e replay, incluindo
   frontend. Reutilizar suites existentes adaptadas à instalação nova.
3. Construir/instalar o par 0.4.0, conferir origem e bytes de fontes/wheels/assets,
   executar os gates aplicáveis e `okto-pulse-saas-closure` com oito budgets ZERO.
   Atualizar matrizes geradas quando necessário, sem abrir exceções arquiteturais.
4. Registrar medição do fluxo final e seus limites, disposição das pendências e provas
   de aceite. Não declarar cobertura total enquanto houver falha material aplicável
   ou decisão necessária sem resposta.
5. Registrar commits/SHAs, estado dos dois repos e instruções de instalação nova no
   ledger; fazer commits/pushes. Release, tag, deploy ou alteração de dados reais não
   fazem parte desta autorização de engenharia.

Antes de testes comportamentais, provar correspondência byte a byte entre as árvores
`src/` e o par instalado. Em testes fora do install, usar `PYTHONPATH` com as duas
árvores. Validar também assets empacotados e idade do processo quando houver runtime
em teste. Elevar as versões coordenadas do produto de 0.3.4 para 0.4.0 faz parte da
implementação, sem confundir versão de pacote com versão/fingerprint dos contratos.

Não há motivo para acrescentar outro framework, ferramenta pública de manutenção,
entidade de domínio ou arquitetura de plugins para realizar essa retirada. O ganho
deve vir de menos caminhos de execução, persistências e contratos sustentados.

## Invariantes e pendências que o corte não resolve

- Core continua sem implementações concretas; Community consome apenas portas públicas.
  `okto-pulse-saas-closure` mantém ZERO em `import_boundary_baseline`,
  `singleton_baseline`, `dependency_temporary_exceptions`, `graph_runtime_compatibility`,
  `rebuild_artifact_compatibility`, `community_private_reach_ins`,
  `community_adapter_bridges` e `af35_relational_residue`.
- Conteúdo de Spec, policy, avaliações, assinatura de prova, locks e reabertura continuam
  sujeitos à autoridade atual. Breaking change não transforma claims em prova nem
  autoriza o executor a avaliar ou dispensar controles.
- Duas lacunas já reproduzidas permanecem delimitadas no ledger: **BASE T23**, confirmação
  Path B ainda válida após alteração do cenário, e **KG-10**, divergência incremental/
  rebuild para Decision de Spec sem vínculo semântico. Ambas ocorrem com operações
  atuais e não desaparecem com uma base nova. As decisões pendentes não foram respondidas
  pela autorização de eliminar legado; não fechá-las como parte da remoção nem alterar
  seus gates silenciosamente. O trabalho independente C1–C3 pode prosseguir.
- Nenhum dado real, processo ativo, conta ou permissão real foi alterado nesta avaliação.

## Estado desta entrega

Avaliação estática e plano atualizados; execução retomada após a revisão do usuário.
Na revisão documental foram alterados somente este documento
e o ledger; nenhum código de produto ou teste foi alterado ou executado, e o WIP
preexistente foi preservado. Não houve build, commit ou push nesta etapa.
Na execução, seguir a ordem concreta acima, verificando os
consumidores reais antes de cada exclusão. Não retomar a auditoria de migração do
plano anterior. Artefatos antigos permanecem como histórico de engenharia, sem
sustentar mecanismos executáveis de compatibilidade no produto.

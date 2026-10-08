# Custo observado da simplificação 0.4.0

Data: 2026-10-08. Relatório consolidado das capturas existentes; nenhuma campanha
nova e nenhuma alteração de produto. Fonte normativa: plano-base, Fase 7.
Este relatório não fecha BASE:T43/KG-64: falta comparação integral com população
equivalente antes/depois. Não transformar recortes aprovados em ganho global.

## Resultados reproduzíveis

| População medida | Antes | Depois | Redução observada |
|---|---:|---:|---:|
| Catálogo congelado, tokens cl100k_base | 52.627 | 51.391 | 2,35% |
| Classificação/autoria, agente amplo, calls individuais versus lote | 79 | 54 | 31,65% |
| Mesmo cenário amplo, tokens completos | 270.784 | 261.834 | 3,31% |
| Classificação/autoria, segundo agente com recusa de escrita, calls | 80 | 55 | 31,25% |
| Mesmo cenário com recusa, tokens completos | 273.207 | 261.603 | 4,25% |

Catálogo: 340→282 tools e 229.702→224.701 bytes. É uma comparação de metadata
congelada, não execução de runtime antigo. Os cenários individuais/lote são
duas estratégias do contrato nativo atual, não a versão antiga versus nova.
Ambas inspecionam os mesmos 27 contratos completos em duas Specs da mesma
Ideation, conservam as mesmas decisões e autoria, qualificam FR/BR com um
critério compartilhado e incluem a retomada por outro agente. A variante
restrita inclui a recusa real; não elimina catálogo para fabricar economia.

O custo completo de cada amostra inclui initialize, tools/list, resources/read
e tools/call. São duas sessões por variante de classificação. UUIDs/timestamps
variam: 25 chamadas evitadas é diferença estrutural; diferenças de token são
amostras, não billing nem estimativa de rede.

## Execução, governança e reuso

Uma captura separada percorre Draft→Done com validações, avaliação, prova de
implementação, teste autenticado e tentativa de fechamento prematuro recusada.

| Execução sem mudança tardia do requisito | Sessões | Calls | Tokens totais | Metadata/instruções/resources |
|---|---:|---:|---:|---:|
| Agente amplo com revisão permitida | 1 | 25 | 125.208 | 101.048 (80,70%) |
| Revisor separado | 4 | 25 | 375.177 | 350.910 (93,53%) |

Essas linhas têm políticas de revisão distintas. Não comparar a diferença como
economia: remover o revisor seria mudar a governança. Cada nova sessão inclui
seu custo integral. As variantes com associação tardia de requisito fazem a
reexecução exigida; a prova antiga não recebe escopo novo por inferência.

A campanha registrou seis execuções HTTP reais, seis passos e doze assertions.
Quatro replays exatos de associação não executaram HTTP nem criaram novos
registros. O rollup compartilha um test_id entre FR/BR/AC quando a mesma prova
realmente verifica os três. Isso prova reuso, não uma economia contrafactual de
execuções que nunca aconteceram. Tempo do executor está dentro da chamada MCP;
somá-lo novamente duplicaria a medição.

## Tradeoff e ajuste delimitado

A meta de redução de 50% de schemas+bootstrap não está demonstrada. A redução
de tools não equivale à redução de tokens: record_delivery_evidence cresceu de
578 para 4.480 tokens de schema ao incorporar o contrato tipado de prova,
progresso e lote. Não remover campos, guards ou representações exigidas para
alcançar a meta.

O resultado de 30% ocorre no segmento de classificação; não foi demonstrado
para o fluxo administrativo inteiro. Somar autoria e execução de fixtures
distintas não produz uma iniciativa contínua antes/depois.

A proposta localizada, caso seja necessário trabalho adicional de custo, é
examinar redundância de bootstrap/resources já identificados por estes números,
preservando catálogo completo, contratos, descoberta por permissão e instruções
obrigatórias em cada sessão. Isso é uma proposta, não mudança implementada ou
novo requisito de entrega. Não abrir refatoração geral de MCP nem novo subsistema.

Para encerrar BASE:T43/KG-64, falta um comparador integral de fatos, atores,
policies e gates equivalentes, com todas as sessões e payloads. Ausência dessa
evidência deve permanecer explícita. Não repetir medições dos recortes acima
nem alterar a população para aparentar uma comparação integral.

## Evidências e validade

- [Contabilidade e catálogo](clean-break-native-benchmark-accounting-20261008.json):
  snapshot congelado e atual recontados com cl100k_base 0.14.0; testes recusam
  resposta ausente e ignoram contagens embarcadas desatualizadas.
- [Autoria, execução e reuso](clean-break-native-execution-cost-20261008.json):
  oito casos aprovados em 59,14s; resumo por etapa e hashes.
- Capturas integrais preservadas nos arquivos
  benchmark-native-payloads-main90-20261008.json.gz e
  benchmark-native-execution-main90-20261008.json.gz; hashes nos recibos.
- A campanha é main90. Main91 regenerou manifesto de recursos; main92 corrigiu
  somente apresentação frontend. Não atribuir retroativamente a main92 uma
  nova medição de custo. A validação instalada main92 tem recibo próprio.
- Universos e decisões de autoridade continuam no índice de aceite e ledger.

## Adendo — comparação contínua nativa (main92)

A nova campanha fecha a lacuna de continuidade entre classificação e execução:
a mesma Spec vai de Draft até Done nas quatro variantes. São 26 contratos
inspecionados integralmente, com arquitetura copiada para a tarefa, avaliações,
teste autenticado, recusa de fechamento prematuro, revisão e replay. O schema
nativo permanece ativo, incluindo triggers de proveniência.

| Política | Estratégia | Sessões | Calls | Tokens totais | Bytes |
|---|---|---:|---:|---:|---:|
| Agente amplo | Individual | 1 | 84 | 216.155 | 857.725 |
| Agente amplo | Lote | 1 | 59 | 206.413 | 820.575 |
| Revisor separado | Individual | 4 | 84 | 466.152 | 1.938.514 |
| Revisor separado | Lote | 4 | 59 | 456.596 | 1.901.364 |

Redução de calls: **29,76%**, abaixo da meta de 30%. Não ajustar quantidade
de contratos para alcançar o percentual. Os resultados persistidos, decisões,
autores, revisores e provas compartilhadas são semanticamente iguais dentro
de cada política. Mudam IDs técnicos/versionamento próprios da atomicidade.

Oito testes passaram em90,35s: quatro variantes comparáveis e quatro regressões
originais. Captura e resultados completos em
[recibo da comparação contínua](clean-break-native-continuous-cost-main92-20261008.json).

Limite remanescente preciso: KG§13.1 pede três estados históricos equivalentes
(baseline v0.3.3, simplificação sem complemento KG e com complemento KG).
Individual/lote do contrato nativo não substitui esses estados. Este ensaio
também parte de fatos iniciais controlados; não mede autoria pública integral
da Ideation e todos os fatos iniciais. BASE:T43/KG64 continuam pendentes dessa
comparação histórica, não de repetir o fluxo nativo agora demonstrado.

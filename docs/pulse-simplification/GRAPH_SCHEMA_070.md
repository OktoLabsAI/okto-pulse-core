# Contrato de evolução do grafo 0.7.0

Estado: contrato e evolução interna qualificados no escopo registrado em
`acceptance-schema-v070.json`. A entrega integral do pacote v1.3 permanece
aberta; resultados terminais e limitações estão no `IMPLEMENTATION_LEDGER.md`.

## Delta e autoridade

`SpecService.update_spec` admite e persiste referências IR/OR→TR. O schema
0.6.0 não possuía os pares físicos correspondentes. O complemento KG §8.1 exige
uma nova versão para esse delta, sem reutilizar um fingerprint sob o mesmo nome.

O contrato 0.7.0 acrescenta somente `derives_from` Requirement→Constraint e
Constraint→Constraint. Passa de 80 para 82 layouts; mantém os 11 tipos de nó,
BoardMeta, propriedades anteriores e espaços vetoriais. O census é
12 tipos de nó incluindo BoardMeta, 82 layouts, 11 espaços, 544 definições de
propriedade de nó e 574 de relação.

- 0.6.0: `3ab6faf0fd8a7fe3694ed7ddd336faa97a6b4af4a1626aafe20c75b0922b2bbe`
- 0.7.0: `099a8da29e07ccd0002a2000a5c5135d439e83cb15d4a84c8e2a71c001340a32`

O Core declara o contrato lógico; DDL, snapshots e transformação física vivem
nos adapters Community. Capacidade de armazenamento não autoriza um writer,
não prova implementação de requisito e não satisfaz um gate de domínio.
As famílias de projeção continuam delimitadas por owner, seção de origem,
seção de destino e proveniência do writer.

## Evolução interna

| Origem | Destino | Propriedades acrescentadas por nó de domínio | Layouts adicionais | Recibo |
|---|---|---:|---:|---|
| 0.5.0 | 0.6.0 histórico | 5 | 11 | `retirement-schema-evolution/v2` preservado |
| 0.5.0 | 0.7.0 | 5 | 13 | `retirement-schema-evolution/v3` |
| 0.6.0 | 0.7.0 | 0 | 2 | `retirement-schema-evolution/v3` |

Os contratos históricos têm fingerprints fixos. O reconhecimento de backup
confere o catálogo e o schema completo; um rótulo em BoardMeta não basta.
O bootstrap corrente recusa um banco 0.6.0 versionado antes de mutação.

A transformação lê um snapshot autenticado e constrói outra geração, com
identidade física nova. Preserva valores anteriores, identidades semânticas,
vetores e cada ocorrência de relação, inclusive paralelas. As cinco propriedades
introduzidas ao sair de 0.5.0 recebem NULL; não são datas históricas inventadas.
Somente o BoardMeta único, do Board esperado e da versão de origem comprovada,
recebe a versão de destino no candidato.

O recibo v3 registra versões de origem/destino e delta exato. A verificação do
baseline rederiva a transformação e compara o recibo integralmente. O recibo v2
mantém a transformação 0.5.0→0.6.0; não é reinterpretado como 0.7.0.
O estado após transformação é `evolved_not_reconciled`, com
`runtime_admission=not_authorized`.

Na cadeia integrada, o candidato ainda precisa passar por projeção das fontes,
reconciliação, preservação de histórico, checkpoint, publicação e admissão. A
existência das tabelas ou de um recibo de evolução não dispensa essas provas.
Não há nova tool, rota, botão ou CLI pública de manutenção.

## Retomada e rollback

Preservar snapshot autenticado, recibos, identificação do par de builds,
bindings e backup nativo do predecessor. A cópia nativa anterior fica sem binding
no candidato; uma importação lógica não reivindica os commits, cursores ou UUID
nativos da origem. Retomada exige os mesmos recibos e fences da cadeia existente.

Rollback de schema exige restauração do backup e par de binários compatível.
Trocar somente uma wheel não converte uma geração 0.7.0 em 0.6.0. Os ensaios
usam dados descartáveis; nenhuma migração ou restauração de dados reais foi
autorizada ou executada por esta implementação.

## Evidências e limites

As regressões de schema, bootstrap e transferência incluem os dois predecessores
e a preservação do formato v2. Os testes de admissão física verificam a recusa
sem mutação de um banco 0.6.0 e a gravação dos dois pares em um banco novo.
Consultar o ledger para falhas de oráculo, reexecuções e campanhas ainda ativas.

Emissão de IR/OR→TR, OR→FR, active sets, consumidores e paridade incremental/rebuild
precisam de provas próprias. A versão exibida no frontend e a SPA distribuída
também precisam acompanhar o contrato; testes de canvas não provam esse texto.
Este documento não é evidência de aceite integral, benchmark ou release 0.4.0.

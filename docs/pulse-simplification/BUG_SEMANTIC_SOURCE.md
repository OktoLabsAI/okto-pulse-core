# Fonte semântica do Bug para F6

Este contrato implementa a separação de leitura exigida por KG §7.2–7.4.
Não implementa ainda submissão de Learning, alteração de policy ou gate de conclusão.

## Porta e divisão de responsabilidade

`BugCognitiveContextAssembler.assemble_semantic(context, board_id, bug_id)`
retorna `BugCognitiveContext` com `contract_version=bug-semantic-context/v1`.
O adapter Community lê a fonte relacional no contexto fornecido. Não consulta o
grafo, não abre uma transação de escrita e não exige Bug Done ou nó canônico.

O Core qualifica tipo Bug, versão positiva e ausência de fatos de projeção no
resultado. O adapter informa falta de Spec ou Test Card vinculado no Board.
Bug ausente ou de outro Board retorna `card_exists=False`, sem seus conteúdos.
`canonical_bug_present=None` significa projeção não consultada; não prova ausência
nem sucesso. A proveniência dessa leitura contém apenas referências relacionais.

`source_policy_version` é o valor persistido de `Card.policy_version`. Uma nova
leitura em outra sessão observa sua alteração. Esse token não substitui hashes e
versões das evidências e fontes vinculadas: uma mudança no Test Card ou na Spec
não é necessariamente uma mudança desse contador no Bug.

O qualificador calcula `source_digest` sobre o contexto completo, incluindo a
versão do Bug, conclusões, validações, cenários, Test Cards e proveniência.
O formato canônico ordena chaves de objetos, conserva ordem de listas e normaliza
timestamps relacionais para UTC. Recusa valores não representáveis e snapshots
acima de 8 MiB; não trunca evidência para produzir um hash aparentemente válido.
Ao receber um contexto já qualificado, uma divergência do digest é erro, sem
recalcular e substituir silenciosamente o valor anterior.

Esse digest detecta alterações; não é assinatura, autorização nem autenticação
de evidência. O writer deve recompor a fonte no contexto relacional atual e
aplicar as permissões e admission paths próprios. Não aceitar um digest enviado
pelo cliente como prova de que essa revalidação ocorreu.

`verified` informa completude da leitura e qualificação estrutural. Não autentica
uma evidência, não aprova a narrativa, não concede permissão de captura e não
autoriza uma transição. O consumidor deve aplicar esses controles separadamente.

## Compatibilidade

`assemble` conserva `bug-cognitive-context/v1`, a consulta ao grafo e a falha
explícita do probe. Os consumidores existentes continuam nesse caminho.
Nenhuma política persistida graph-backed foi convertida por esta mudança.
Não há novo endpoint, tool MCP, coluna SQL ou lifecycle de Card.

## Dependências ainda abertas

A captura deve reutilizar o store cognitivo durável, sob autorização e CAS,
antes de depender da materialização. `CognitiveSourceRecord.metadata` não serve
como selo: fica fora do fingerprint e não é persistida pelo adapter atual.
O payload atualmente representa propriedades gráficas literais; introduzir um
envelope requer contrato fechado e consumidores de replay/paridade coerentes,
preservando os registros históricos existentes.

Admissão precisa vincular conteúdo, autor, Bug, versão e evidências atuais; uma
fila vazia ou uma marca do cliente não demonstra captura válida. Reuso e
supersedência requerem intenção explícita, escopo e concorrência controlada.
Na conclusão, revalidar a captura no contexto da transição e conservar a história
quando o Bug reabrir. Não declarar atomicidade entre stores independentes.

Faltam também integração da policy humana advisory/blocking, resposta de contexto,
MCP/REST/UI, materialização determinística, reconciliação R7 delimitada e provas
de evento, replay, upgrade e rollback. O ledger registra a evidência executada;
este documento não constitui aceite integral de F6.

## Collector

## Control

- Vai configurar o rabbitMQ, para criar um exchange, as filas de ar e luz.
- Vai ficar com o compose para a infraestrutura do rabbit, e do banco de dados mongo, para salvar os registro que foram para re-try, definir um numero de tentativas para então marcar como falho no processamento.
- Background service, que vai rodar de x em x segundos, para garantir que o que não foi processado correntamente seja reprocessado, após um numero X de tentativas, marcado como falha (????).
- printar na tela o corpo que recebeu das filas, de luz e ar.
- Conteinerizar a aplicação.

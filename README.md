# EventBus em C#

Barramento de eventos assíncrono em memória para C# (.NET 8), construído sobre `System.Threading.Channels`. Cada assinante tem sua própria fila limitada e seu próprio worker, o que garante ordem por assinante, isolamento de falhas, *back-pressure* e encerramento gracioso. Os handlers são tipados e também recebem eventos de tipos derivados.

## Por que este projeto

Publicar/assinar é a base de arquiteturas orientadas a eventos (Kafka, SNS/SQS, MediatR, barramentos de domínio). Em entrevistas e em code reviews de empresas grandes, o que diferencia uma boa implementação é o tratamento dos casos difíceis: o que acontece quando um handler lança exceção, quando um assinante é lento, quando a fila enche, quando o processo precisa desligar. Este projeto trata cada um desses pontos de maneira explícita e testada, usando as primitivas modernas de concorrência do .NET.

## Recursos

- `Subscribe<TEvent>` com handler assíncrono (`Func<TEvent, CancellationToken, ValueTask>`) ou síncrono (`Action<TEvent>`); o retorno é um `IDisposable` para cancelar a assinatura.
- Despacho polimórfico: handlers de `object`, classe base ou interface recebem eventos derivados.
- **Isolamento de erros:** exceções em um handler são capturadas e entregues ao callback `OnError` com o evento, a exceção e o id da assinatura; nada propaga para o publicador nem para outros assinantes.
- Fila limitada por assinante com três políticas: `Wait` (back-pressure), `DropOldest` e `DropNewest`.
- Ordem de entrega preservada por assinante.
- `ShutdownAsync`/`DisposeAsync`: para de aceitar eventos, drena as filas e aguarda os workers; com um `CancellationToken` o desligamento pode ser forçado, cancelando os handlers em execução.

## Como executar

Pré-requisito: .NET 8 SDK.

```bash
dotnet build
```

```csharp
await using var bus = new EventBus.EventBus(new EventBusOptions
{
    QueueCapacity = 256,
    OnError = err => Console.Error.WriteLine($"handler falhou: {err.Exception.Message}")
});

bus.Subscribe<OrderPlaced>(async (e, ct) => await SendEmailAsync(e.Id, ct));
bus.Subscribe<object>(e => Console.WriteLine($"auditoria: {e}"));

await bus.PublishAsync(new OrderPlaced(42));
```

## Como testar

```bash
dotnet test
```

Os testes cobrem: roteamento por tipo, ordem, handler que lança exceção (e callback de erro que também lança), assinante lento que não bloqueia o rápido, despacho por interface/base, cancelamento de assinatura, back-pressure real (o publicador fica pendente até haver espaço), política `DropNewest`, uso após o desligamento, desligamento forçado e 10 publicadores concorrentes (10 mil eventos).

## Decisões de projeto

- **Um channel + um worker por assinatura** (em vez de um loop central). Um assinante lento só atrasa a si mesmo, e a ordem por assinante vem de graça com `SingleReader`. O custo é uma tarefa por assinante.
- **Lista de assinaturas copy-on-write.** Subscribe/unsubscribe são O(n) e raros; `PublishAsync` lê um array imutável sem lock, em O(s) para s assinaturas.
- **`PublishAsync` completa ao enfileirar**, não ao tratar o evento: é semântica de *fire-and-forget com back-pressure*. Quem precisa de confirmação deve modelar request/response por cima.
- **Entrega *at-most-once* e sem persistência.** Se o processo cair, eventos enfileirados são perdidos; não há retentativa automática (o `OnError` permite ao usuário decidir).
- **Desligamento em duas fases:** drenar normalmente e, se o token expirar, cancelar os handlers. Evita tanto perda silenciosa quanto *hang* eterno.

## Estrutura de pastas

```
EventBus/
├── EventBus.sln
├── src/EventBus/           # EventBus.cs (bus, opções, HandlerError)
└── tests/EventBus.Tests/   # testes xUnit
```

## Limitações conhecidas

- Sem garantia de ordem global entre assinantes diferentes.
- Sem retentativas, *dead-letter queue* ou persistência.
- O tipo é resolvido pelo tipo de tempo de execução do evento; `IsAssignableFrom` é avaliado a cada publicação (sem cache).
- Handlers que ignoram o `CancellationToken` podem impedir um desligamento forçado de terminar.

## Licença

MIT. Veja o arquivo [LICENSE](LICENSE).

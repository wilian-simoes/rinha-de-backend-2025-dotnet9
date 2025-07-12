using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentStreamWorker : BackgroundService
    {
        private readonly RedisStreamService _streamService;
        private readonly ILogger<PaymentStreamWorker> _logger;
        private readonly SummaryService _summaryService;
        private readonly IServiceScopeFactory _scopeFactory;

        public PaymentStreamWorker(
            RedisStreamService streamService,
            ILogger<PaymentStreamWorker> logger,
            SummaryService summaryService,
            IServiceScopeFactory scopeFactory)
        {
            _streamService = streamService;
            _logger = logger;
            _summaryService = summaryService;
            _scopeFactory = scopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker de pagamentos (Redis Stream) iniciado.");

            var lastRetryCheck = DateTime.UtcNow;

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var (messageId, payment) = await _streamService.ReadNextAsync();

                    if (payment != null)
                    {
                        _logger.LogInformation($"[Worker] Processando pagamento {payment.correlationId} - R$ {payment.amount}");

                        // TODO: lógica de processamento

                        using var scope = _scopeFactory.CreateScope();
                        var paymentProcessorService = scope.ServiceProvider.GetRequiredService<PaymentProcessorService>();
                        var health = await paymentProcessorService.GetServiceHealthAsync();

                        var request = new Models.PaymentProcessor.PaymentRequest()
                        {
                            correlationId = payment.correlationId,
                            amount = payment.amount,
                            requestedAt = DateTime.UtcNow,
                        };

                        // TODO: Criar método para decidir se usa default ou fallback
                        var response = await paymentProcessorService.PostPaymentsAsync(request);

                        await _summaryService.IncrementSummaryAsync("default", request.amount, request.requestedAt);
                        _logger.LogInformation($"[Worker] {response} {request.correlationId} - R$ {request.amount}");

                        await _streamService.AcknowledgeAsync(messageId);
                    }
                    else
                    {
                        await Task.Delay(500, stoppingToken);
                    }

                    // RETRY A CADA 5 segundos
                    if ((DateTime.UtcNow - lastRetryCheck).TotalSeconds >= 5)
                    {
                        await ReprocessarPendentesAsync(stoppingToken);
                        lastRetryCheck = DateTime.UtcNow;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro ao processar mensagem do Redis Stream.");
                    await Task.Delay(1000, stoppingToken);
                }
            }

            _logger.LogInformation("Worker de pagamentos encerrado.");
        }

        private async Task ReprocessarPendentesAsync(CancellationToken stoppingToken)
        {
            const int minIdleTimeMs = 60000;
            const int maxAttempts = 5;

            var allPending = await _streamService.StreamPendingMessagesAsync(500);
            var pendings = allPending.Where(x => x.IdleTimeInMilliseconds >= minIdleTimeMs).ToArray();

            if (!pendings.Any())
            {
                await Task.Delay(5000, stoppingToken);
                return;
            }

            foreach (var msg in pendings)
            {
                if (msg.IdleTimeInMilliseconds < minIdleTimeMs)
                    continue;

                if (msg.DeliveryCount > maxAttempts)
                {
                    _logger.LogWarning("[Retry] Mensagem {MessageId} falhou {DeliveryCount} vezes. A mensagem foi descartada.",
                                    msg.MessageId, msg.DeliveryCount);

                    // TODO: se necessário, depois posso mover para uma DQL (fila morta, para análisar)

                    await _streamService.AcknowledgeAsync(msg.MessageId);

                    continue;
                }

                var claimed = await _streamService.StreamClaimAsync(minIdleTimeMs, msg.MessageId);

                foreach (var entry in claimed)
                {
                    try
                    {
                        var json = entry.Values.First(x => x.Name == "data").Value;
                        var payment = JsonSerializer.Deserialize<Payment>(json);

                        if (payment is null)
                            throw new Exception("Dados inválidos para retry");

                        _logger.LogInformation("[Retry] Reprocessando pagamento pendente {CorrelationId}", payment.correlationId);

                        using var scope = _scopeFactory.CreateScope();
                        var processor = scope.ServiceProvider.GetRequiredService<PaymentProcessorService>();

                        var request = new Models.PaymentProcessor.PaymentRequest
                        {
                            correlationId = payment.correlationId,
                            amount = payment.amount,
                            requestedAt = DateTime.UtcNow
                        };

                        var response = await processor.PostPaymentsAsync(request);
                        await _summaryService.IncrementSummaryAsync("default", request.amount, request.requestedAt);

                        await _streamService.AcknowledgeAsync(msg.MessageId);

                        _logger.LogInformation("[Retry] Sucesso {CorrelationId} - R$ {Amount}", request.correlationId, request.amount);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "[Retry] Falha ao reprocessar {MessageId}. Permanecerá pendente.", msg.MessageId);
                    }
                }
            }
        }
    }
}
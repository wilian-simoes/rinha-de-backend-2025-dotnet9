using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentStreamWorkerRetry : BackgroundService
    {
        private readonly IConnectionMultiplexer _redis;
        private readonly ILogger<PaymentStreamWorkerRetry> _logger;
        private readonly IServiceScopeFactory _scopeFactory;
        private readonly SummaryService _summaryService;

        private const string StreamKey = "payments-stream";
        private const string GroupName = "payments-group";
        private const string RetryConsumerName = "worker";//"retry-consumer";
        private const int MinIdleTimeMs = 60000; // 60 segundos
        private const int MaxDeliveryCount = 5;

        public PaymentStreamWorkerRetry(
            IConnectionMultiplexer redis,
            ILogger<PaymentStreamWorkerRetry> logger,
            IServiceScopeFactory scopeFactory,
            SummaryService summaryService)
        {
            _redis = redis;
            _logger = logger;
            _scopeFactory = scopeFactory;
            _summaryService = summaryService;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var db = _redis.GetDatabase();
            _logger.LogInformation("Worker de retry (Redis Stream) iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    // Busca até 10 mensagens pendentes
                    var allPending = await db.StreamPendingMessagesAsync(StreamKey, GroupName, 10, RetryConsumerName);
                    var teste = allPending.Count();
                    // Filtra por tempo mínimo de idle
                    var pending = allPending.Where(x => x.IdleTimeInMilliseconds >= MinIdleTimeMs).ToArray();

                    if (!pending.Any())
                    {
                        await Task.Delay(5000, stoppingToken);
                        continue;
                    }

                    foreach (var pendingMsg in pending)
                    {
                        if (pendingMsg.DeliveryCount > MaxDeliveryCount)
                        {
                            _logger.LogWarning("[Retry] Mensagem {MessageId} falhou {DeliveryCount} vezes. A mensagem será descartada.",
                                pendingMsg.MessageId, pendingMsg.DeliveryCount);

                            // TODO: se necessário, depois posso mover para uma DQL (fila morta, para análisar)
                            await db.StreamAcknowledgeAsync(StreamKey, GroupName, pendingMsg.MessageId);
                            continue;
                        }

                        var claimed = await db.StreamClaimAsync(
                            StreamKey,
                            GroupName,
                            RetryConsumerName,
                            MinIdleTimeMs,
                            new[] { pendingMsg.MessageId });

                        foreach (var entry in claimed)
                        {
                            try
                            {
                                var json = entry.Values.FirstOrDefault(x => x.Name == "data").Value;
                                var payment = JsonSerializer.Deserialize<Payment>(json);

                                if (payment is null)
                                {
                                    _logger.LogWarning("[Retry] Mensagem {MessageId} inválida ou corrompida.", entry.Id);
                                    continue;
                                }

                                _logger.LogInformation($"[Retry] Reprocessando pagamento {payment.correlationId} - R$ {payment.amount}");

                                // TODO: Lógica de reprocessamento
                                using var scope = _scopeFactory.CreateScope();
                                var paymentProcessorService = scope.ServiceProvider.GetRequiredService<PaymentProcessorService>();

                                var request = new Models.PaymentProcessor.PaymentRequest
                                {
                                    correlationId = payment.correlationId,
                                    amount = payment.amount,
                                    requestedAt = DateTime.UtcNow
                                };

                                var response = await paymentProcessorService.PostPaymentsAsync(request);
                                await _summaryService.IncrementSummaryAsync("default", request.amount, request.requestedAt);

                                await db.StreamAcknowledgeAsync(StreamKey, GroupName, entry.Id);
                                _logger.LogInformation($"[Retry] Processado com sucesso {request.correlationId}");
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, $"[Retry] Falha ao processar mensagem {entry.Id}. Permanecerá pendente.");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro geral no worker de retry.");
                }

                await Task.Delay(3000, stoppingToken);
            }

            _logger.LogInformation("Worker de retry encerrado.");
        }
    }
}
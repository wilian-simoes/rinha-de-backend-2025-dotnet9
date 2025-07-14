using rinha_de_backend_2025_dotnet9.Models;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentStreamWorker : BackgroundService
    {
        private readonly RedisStreamService _streamService;
        private readonly ILogger<PaymentStreamWorker> _logger;
        private readonly SummaryService _summaryService;
        private readonly IServiceProvider _provider;
        private static DateTime _lastValidateHealth;
        private bool _validateHealth;

        public PaymentStreamWorker(
            RedisStreamService streamService,
            ILogger<PaymentStreamWorker> logger,
            SummaryService summaryService,
            IServiceProvider provider)
        {
            _streamService = streamService;
            _logger = logger;
            _summaryService = summaryService;
            _provider = provider;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker de pagamentos (Redis Stream) iniciado.");

            const int batchSize = 2;//2;
            const int maxConcurrency = 5;//5;

            var concurrencyLimiter = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var now = DateTime.UtcNow.AddHours(-3);
                    _validateHealth = (now - _lastValidateHealth).TotalSeconds >= 5;
                    if (_validateHealth) _lastValidateHealth = now;

                    var entries = await _streamService.ReadNextAsync(batchSize);

                    if (entries.Length == 0)
                    {
                        await Task.Delay(50, stoppingToken);
                        continue;
                    }

                    foreach (var entry in entries)
                    {
                        await concurrencyLimiter.WaitAsync(stoppingToken);

                        var task = Task.Run(async () =>
                        {
                            try
                            {
                                var payment = JsonSerializer.Deserialize<Payment>(entry["data"]);
                                if (payment == null)
                                    return;

                                _logger.LogInformation($"[Worker] Processando {payment.correlationId} - R$ {payment.amount}");

                                var response = await ProcessPayment(payment, entry.Id, validateHealth: _validateHealth);

                                _logger.LogInformation($"[Worker] {response} {payment.correlationId} - R$ {payment.amount}");
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, $"Erro ao processar mensagem {entry.Id}");
                            }
                            finally
                            {
                                concurrencyLimiter.Release();
                            }
                        }, stoppingToken);

                        tasks.Add(task);
                    }

                    tasks.RemoveAll(t => t.IsCompleted);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro no loop principal do worker.");
                }
            }

            _logger.LogInformation("Worker de pagamentos encerrado.");

            await Task.WhenAll(tasks);
        }

        public bool ChooseService(
            (bool failing, int minResponseTime) healthDefault,
            (bool failing, int minResponseTime) healthFallback,
            int toleranciaMs = 100)
        {
            if (!healthDefault.failing)
            {
                //if (!healthFallback.failing &&
                //    (healthDefault.minResponseTime - healthFallback.minResponseTime) > toleranciaMs)
                //{
                //    return true; // fallback
                //}

                return false; // default
            }

            if (!healthFallback.failing)
                return true; // fallback

            return healthDefault.minResponseTime <= healthFallback.minResponseTime ? false : true;
        }


        private async Task<string> ProcessPayment(Payment payment, string messageId, bool validateHealth = false)
        {
            var processor = ActivatorUtilities.CreateInstance<PaymentProcessorService>(_provider);

            var now = DateTime.UtcNow;
            var request = new Models.PaymentProcessor.PaymentRequest
            {
                correlationId = payment.correlationId,
                amount = payment.amount,
                requestedAt = now,
            };

            bool useFallback = false;

            if (validateHealth)
            {
                try
                {
                    var healthPaymentsDefault = await processor.GetServiceHealthAsync(useFallback: false);
                    var healthPaymentsFallback = await processor.GetServiceHealthAsync(useFallback: true);

                    useFallback = ChooseService((healthPaymentsDefault.failing, healthPaymentsDefault.minResponseTime),
                                                (healthPaymentsFallback.failing, healthPaymentsFallback.minResponseTime));
                }
                catch (Exception)
                {
                    useFallback = false;
                }
            }

            var response = await processor.PostPaymentsAsync(request, useFallback);

            await _summaryService.IncrementSummaryAsync(useFallback == false ? "default" : "fallback", request.amount, now);
            await _streamService.AcknowledgeAsync(messageId);

            return response;
        }

        //private async Task RetryLoopAsync(CancellationToken stoppingToken)
        //{
        //    while (!stoppingToken.IsCancellationRequested)
        //    {
        //        await ReprocessarPendentesAsync(stoppingToken);
        //        await Task.Delay(5000, stoppingToken);
        //    }
        //}

        //private async Task ReprocessarPendentesAsync(CancellationToken stoppingToken)
        //{
        //    const int minIdleTimeMs = 5000;
        //    const int maxAttempts = 3;

        //    var allPending = await _streamService.StreamPendingMessagesAsync(500);

        //    var pendings = allPending.Where(x => x.IdleTimeInMilliseconds >= minIdleTimeMs).ToArray();

        //    if (pendings.Length == 0)
        //        return;

        //    await Parallel.ForEachAsync(pendings, new ParallelOptions
        //    {
        //        MaxDegreeOfParallelism = Environment.ProcessorCount,
        //        CancellationToken = stoppingToken
        //    },
        //    async (msg, ct) =>
        //    {
        //        try
        //        {
        //            var claimed = await _streamService.StreamClaimAsync(minIdleTimeMs, msg.MessageId);
        //            foreach (var entry in claimed)
        //            {
        //                var json = entry.Values.FirstOrDefault(x => x.Name == "data").Value;
        //                if (!json.HasValue)
        //                {
        //                    _logger.LogWarning("[Retry] Mensagem {MessageId} sem campo 'data'", msg.MessageId);
        //                    return;
        //                }

        //                var payment = JsonSerializer.Deserialize<Payment>(json);
        //                if (payment is null)
        //                    throw new Exception("Dados inválidos para retry");

        //                _logger.LogInformation("[Retry] Reprocessando pagamento pendente {CorrelationId}", payment.correlationId);

        //                var response = await ProcessPayment(payment, msg.MessageId, validateHealth: true);

        //                _logger.LogInformation("[Retry] Sucesso {CorrelationId} - R$ {Amount}", payment.correlationId, payment.amount);
        //            }
        //        }
        //        catch (Exception ex)
        //        {
        //            if (msg.DeliveryCount >= maxAttempts)
        //            {
        //                _logger.LogWarning("[Retry] Descartando mensagem {MessageId} após falhar {Count} tentativas", msg.MessageId, msg.DeliveryCount);
        //                await _streamService.AcknowledgeAsync(msg.MessageId);
        //            }
        //            else
        //            {
        //                _logger.LogError(ex, "[Retry] Falha ao reprocessar {MessageId}. Permanecerá pendente. Tentativas [{DeliveryCount}]", msg.MessageId, msg.DeliveryCount);
        //            }
        //        }
        //    });
        //}
    }
}
using rinha_de_backend_2025_dotnet9.Models;
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

        private async Task<bool> ValidateHealthAsync()
        {
            var key = "health:last-check";
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var lastCheck = await _streamService.StringGetAsync(key);
            if (lastCheck.HasValue && long.TryParse(lastCheck, out var ts))
            {
                if (now - ts < 5)
                    return false;
            }

            await _streamService.StringSetAsync(key, now);
            return true;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker de pagamentos (Redis Stream) iniciado.");

            const int batchSize = 20;
            const int maxConcurrency = 40;

            var concurrencyLimiter = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
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

                                var validateHealth = await ValidateHealthAsync();
                                var response = await ProcessPayment(payment, entry.Id, validateHealth: validateHealth);

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
            int toleranciaMs = 1)
        {
            if (!healthDefault.failing)
            {
                if (!healthFallback.failing &&
                    (healthDefault.minResponseTime - healthFallback.minResponseTime) > toleranciaMs)
                {
                    return true; // fallback
                }

                return false; // default
            }

            if (!healthFallback.failing)
                return true; // fallback

            return healthDefault.minResponseTime <= healthFallback.minResponseTime ? false : true;
        }


        private async Task<string> ProcessPayment(Payment payment, string messageId, bool validateHealth = false)
        {
            using var scope = _scopeFactory.CreateScope();
            var processor = scope.ServiceProvider.GetRequiredService<PaymentProcessorService>();

            var now = DateTime.UtcNow;
            var request = new Models.PaymentProcessor.PaymentRequest
            {
                correlationId = payment.correlationId,
                amount = payment.amount,
                requestedAt = now,
            };

            bool useFallback = false;

            var useFallbackCache = await _streamService.StringGetAsync("health:useFallback");
            if (useFallbackCache.HasValue && bool.TryParse(useFallbackCache, out var fb))
            {
                useFallback = fb;
            }

            if (validateHealth)
            {
                try
                {
                    var healthPaymentsDefault = await processor.GetServiceHealthAsync(useFallback: false);
                    var healthPaymentsFallback = await processor.GetServiceHealthAsync(useFallback: true);

                    useFallback = ChooseService((healthPaymentsDefault.failing, healthPaymentsDefault.minResponseTime),
                                                (healthPaymentsFallback.failing, healthPaymentsFallback.minResponseTime));

                    await _streamService.StringSetAsync("health:useFallback", useFallback);
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
    }
}
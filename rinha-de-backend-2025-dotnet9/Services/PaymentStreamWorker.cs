using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentStreamWorker : BackgroundService
    {
        private readonly IDatabase _redis;
        private readonly ILogger<PaymentStreamWorker> _logger;
        private readonly SummaryService _summaryService;
        private readonly IServiceScopeFactory _scopeFactory;

        public PaymentStreamWorker(
            IConnectionMultiplexer connectionMultiplexer,
            ILogger<PaymentStreamWorker> logger,
            SummaryService summaryService,
            IServiceScopeFactory scopeFactory)
        {
            _redis = connectionMultiplexer.GetDatabase();
            _logger = logger;
            _summaryService = summaryService;
            _scopeFactory = scopeFactory;
        }

        private async Task<bool> ValidateHealthAsync()
        {
            var key = "health:last-check";
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();

            var lastCheck = await _redis.StringGetAsync(key);
            if (lastCheck.HasValue && long.TryParse(lastCheck, out var ts))
            {
                if (now - ts < 5)
                    return false;
            }

            await _redis.StringSetAsync(key, now);
            return true;
        }

        public async Task<bool> UseFallback(bool validateHealth)
        {
            bool useFallback = false;

            var useFallbackCache = await _redis.StringGetAsync("health:useFallback");
            if (useFallbackCache.HasValue && bool.TryParse(useFallbackCache, out var fb))
            {
                useFallback = fb;
            }

            if (validateHealth)
            {
                try
                {
                    using var scope = _scopeFactory.CreateScope();
                    var processor = scope.ServiceProvider.GetRequiredService<PaymentProcessorService>();

                    var healthPaymentsDefault = await processor.GetServiceHealthAsync(useFallback: false);
                    var healthPaymentsFallback = await processor.GetServiceHealthAsync(useFallback: true);

                    useFallback = ChooseService((healthPaymentsDefault.failing, healthPaymentsDefault.minResponseTime),
                                                (healthPaymentsFallback.failing, healthPaymentsFallback.minResponseTime));

                    await _redis.StringSetAsync("health:useFallback", useFallback);
                }
                catch (Exception)
                {
                    useFallback = false;
                }
            }

            return useFallback;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker de pagamentos (Redis Stream) iniciado.");

            const int batchSize = 10;
            const int maxConcurrency = 20;

            var concurrencyLimiter = new SemaphoreSlim(maxConcurrency);
            var tasks = new List<Task>();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var entries = await _redis.ListLeftPopAsync("payments:queue", batchSize);

                    if (entries == null || entries.Length == 0)
                    {
                        await Task.Delay(50, stoppingToken);
                        continue;
                    }

                    var validEntries = Array.FindAll(entries, p => !p.IsNullOrEmpty);
                    if (validEntries.Length > 0)
                    {
                        var failedPaymentsToRequeue = new List<RedisValue>();

                        foreach (var payload in validEntries)
                        {
                            await concurrencyLimiter.WaitAsync(stoppingToken);

                            var task = Task.Run(async () =>
                            {
                                try
                                {
                                    var payment = JsonSerializer.Deserialize<Payment>(payload);
                                    if (payment == null)
                                        return;

                                    var validateHealth = await ValidateHealthAsync();
                                    var useFallback = await UseFallback(validateHealth);
                                    var response = await ProcessPayment(payment, useFallback: useFallback);
                                }
                                catch (Exception ex)
                                {
                                    _logger.LogError(ex, $"Falha. adicionando na fila para reprocessar.");
                                    failedPaymentsToRequeue.Add(payload);
                                }
                                finally
                                {
                                    concurrencyLimiter.Release();
                                }
                            }, stoppingToken);

                            tasks.Add(task);

                            if (failedPaymentsToRequeue.Count > 0)
                            {
                                await _redis.ListRightPushAsync("payments:queue", failedPaymentsToRequeue.ToArray());
                            }
                        }

                        tasks.RemoveAll(t => t.IsCompleted);
                    }
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
            int toleranciaMs = 10)
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


        private async Task<string> ProcessPayment(Payment payment, bool useFallback)
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

            await _summaryService.IncrementSummaryAsync(useFallback == false ? "default" : "fallback", request.amount, now);

            var response = await processor.PostPaymentsAsync(request, useFallback);

            return response;
        }
    }
}
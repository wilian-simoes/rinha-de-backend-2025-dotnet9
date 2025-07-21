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

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            const int batchSize = 14;
            const int maxConcurrency = 6;

            var concurrencyLimiter = new SemaphoreSlim(maxConcurrency);

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

                    var tasks = validEntries.Select(async payload =>
                    {
                        await concurrencyLimiter.WaitAsync(stoppingToken);

                        try
                        {
                            var payment = JsonSerializer.Deserialize<Payment>(payload);
                            if (payment == null)
                                return;

                            bool useFallback = false;
                            var useFallbackCache = await _redis.StringGetAsync("health:useFallback");
                            if (useFallbackCache.HasValue)
                                useFallback = (bool)useFallbackCache;

                            await ProcessPayment(payment, useFallback);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Erro ao processar pagamento. Reenfileirando.");
                            await _redis.ListRightPushAsync("payments:queue", payload);
                        }
                        finally
                        {
                            concurrencyLimiter.Release();
                        }
                    }).ToList();

                    await Task.WhenAll(tasks);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro no loop principal do worker.");
                    await Task.Delay(100, stoppingToken);
                }
            }

            _logger.LogInformation("Worker de pagamentos encerrado.");
        }

        private async Task ProcessPayment(Payment payment, bool useFallback)
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

            await processor.PostPaymentsAsync(request, useFallback);

            await _summaryService.IncrementSummaryAsync(useFallback == false ? "default" : "fallback", request);
        }
    }
}
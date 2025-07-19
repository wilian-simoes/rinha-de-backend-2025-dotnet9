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
            _logger.LogInformation("Worker de pagamentos (Redis Stream) iniciado.");

            const int batchSize = 20;
            const int maxConcurrency = 40;

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

                                    bool useFallback = false;
                                    
                                    var useFallbackCache = await _redis.StringGetAsync("health:useFallback");
                                    if(useFallbackCache.HasValue)
                                        useFallback = (bool)useFallbackCache;

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

            var response = await processor.PostPaymentsAsync(request, useFallback);

            await _summaryService.IncrementSummaryAsync(useFallback == false ? "default" : "fallback", request.amount, request.requestedAt, request.correlationId);

            return response;
        }
    }
}
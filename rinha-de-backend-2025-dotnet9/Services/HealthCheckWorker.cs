using StackExchange.Redis;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class HealthCheckWorker : BackgroundService
    {
        private readonly ILogger<HealthCheckWorker> _logger;
        private readonly IDatabase _redis;
        private readonly IServiceScopeFactory _scopeFactory;
        public HealthCheckWorker(
            ILogger<HealthCheckWorker> logger,
            IConnectionMultiplexer connectionMultiplexer,
            IServiceScopeFactory scopeFactory)
        {
            _logger = logger;
            _redis = connectionMultiplexer.GetDatabase();
            _scopeFactory = scopeFactory;
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var validateHealth = await ValidateHealthAsync();
                    var useFallback = await UseFallback(validateHealth);
                    await _redis.StringSetAsync("health:useFallback", useFallback);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Redis health check failed.");
                }
                await Task.Delay(TimeSpan.FromMilliseconds(50), stoppingToken);
            }
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

        private async Task<bool> UseFallback(bool validateHealth)
        {
            bool useFallback = false;

            var useFallbackCache = await _redis.StringGetAsync("health:useFallback");
            if (useFallbackCache.HasValue)
            {
                useFallback = (bool)useFallbackCache;
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
                }
                catch
                {    
                }
            }

            return useFallback;
        }

        private bool ChooseService(
            (bool failing, int minResponseTime) healthDefault,
            (bool failing, int minResponseTime) healthFallback,
            int toleranciaMs = 1000)
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
    }
}

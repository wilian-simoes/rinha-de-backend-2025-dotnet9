using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class SummaryService
    {
        private readonly IDatabase _db;

        public SummaryService(IConnectionMultiplexer redis)
        {
            _db = redis.GetDatabase();
        }

        public async Task IncrementSummaryAsync(string summaryType, decimal amount)
        {
            string redisKey = $"summary:{summaryType}";
            long amountInCents = (long)(amount * 100);

            await _db.HashIncrementAsync(redisKey, "totalRequests", 1);
            await _db.HashIncrementAsync(redisKey, "totalAmount", amountInCents);
        }

        public async Task<PaymentSummary> GetFullSummaryAsync()
        {
            var defaultValues = await _db.HashGetAllAsync("summary:default");
            var fallbackValues = await _db.HashGetAllAsync("summary:fallback");

            var defaultSummary = defaultValues.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());
            var fallbackSummary = fallbackValues.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

            return new PaymentSummary
            {
                _default = new Models.Shared.Summary
                {
                    totalRequests = long.Parse(defaultSummary.GetValueOrDefault("totalRequests") ?? "0"),
                    totalAmount = decimal.Parse(defaultSummary.GetValueOrDefault("totalAmount") ?? "0") / 100
                },
                fallback = new Models.Shared.Summary
                {
                    totalRequests = long.Parse(fallbackSummary.GetValueOrDefault("totalRequests") ?? "0"),
                    totalAmount = decimal.Parse(fallbackSummary.GetValueOrDefault("totalAmount") ?? "0") / 100
                }
            };
        }
    }
}
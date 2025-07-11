using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;
using System.Globalization;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class SummaryService
    {
        private readonly IDatabase _db;

        public SummaryService(IConnectionMultiplexer redis)
        {
            _db = redis.GetDatabase();
        }

        public async Task IncrementSummaryAsync(string summaryType, decimal amount, DateTime processingDate)
        {
            var timeKey = processingDate.ToString("yyyy-MM-ddTHH:mm");
            var redisKey = $"summary:{summaryType}:{timeKey}";

            long amountInCents = (long)(amount * 100);

            await _db.HashIncrementAsync(redisKey, "totalRequests", 1);
            await _db.HashIncrementAsync(redisKey, "totalAmount", amountInCents);
        }

        public async Task<PaymentSummary> GetFullSummaryAsync(DateTime? from, DateTime? to)
        {
            long defaultRequests = 0, defaultAmount = 0;
            long fallbackRequests = 0, fallbackAmount = 0;

            if (from.HasValue && to.HasValue)
            {
                var current = from.Value;
                while (current <= to.Value)
                {
                    var keyTime = current.ToString("yyyy-MM-ddTHH:mm");

                    var defaultData = (await _db.HashGetAllAsync($"summary:default:{keyTime}"))
                        .ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

                    var fallbackData = (await _db.HashGetAllAsync($"summary:fallback:{keyTime}"))
                        .ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

                    defaultRequests += long.Parse(defaultData.GetValueOrDefault("totalRequests") ?? "0");
                    defaultAmount += long.Parse(defaultData.GetValueOrDefault("totalAmount") ?? "0");

                    fallbackRequests += long.Parse(fallbackData.GetValueOrDefault("totalRequests") ?? "0");
                    fallbackAmount += long.Parse(fallbackData.GetValueOrDefault("totalAmount") ?? "0");

                    current = current.AddMinutes(1);
                }
            }
            else
            {
                var server = _db.Multiplexer.GetServer(_db.Multiplexer.GetEndPoints().First());

                await foreach (var key in server.KeysAsync(pattern: "summary:default:*"))
                {
                    var data = await _db.HashGetAllAsync(key);
                    var dict = data.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

                    defaultRequests += long.Parse(dict.GetValueOrDefault("totalRequests") ?? "0");
                    defaultAmount += long.Parse(dict.GetValueOrDefault("totalAmount") ?? "0");
                }

                await foreach (var key in server.KeysAsync(pattern: "summary:fallback:*"))
                {
                    var data = await _db.HashGetAllAsync(key);
                    var dict = data.ToDictionary(x => x.Name.ToString(), x => x.Value.ToString());

                    fallbackRequests += long.Parse(dict.GetValueOrDefault("totalRequests") ?? "0");
                    fallbackAmount += long.Parse(dict.GetValueOrDefault("totalAmount") ?? "0");
                }
            }

            return new PaymentSummary
            {
                _default = new Models.Shared.Summary
                {
                    totalRequests = defaultRequests,
                    totalAmount = defaultAmount / 100m
                },
                fallback = new Models.Shared.Summary
                {
                    totalRequests = fallbackRequests,
                    totalAmount = fallbackAmount / 100m
                }
            };
        }
    }
}
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

        public async Task IncrementSummaryAsync(string summaryType, decimal amount, DateTime processingDate)
        {
            var timeKey = processingDate.ToString("yyyy-MM-ddTHH:mm");
            var redisKey = $"summary:{summaryType}:{timeKey}";

            long amountInCents = (long)(amount * 100);

            await _db.HashIncrementAsync(redisKey, "totalRequests", 1);
            await _db.HashIncrementAsync(redisKey, "totalAmount", amountInCents);

            // Adiciona ao índice
            await _db.SetAddAsync($"summary:{summaryType}:index", timeKey);
        }

        public async Task<PaymentSummary> GetFullSummaryAsync(DateTime? from, DateTime? to)
        {
            long defaultRequests = 0, defaultAmount = 0;
            long fallbackRequests = 0, fallbackAmount = 0;

            // Evita null
            if (!from.HasValue || !to.HasValue)
                return new PaymentSummary();

            // Pega os índices salvos no Redis
            var defaultIndex = await _db.SetMembersAsync("summary:default:index");
            var fallbackIndex = await _db.SetMembersAsync("summary:fallback:index");

            // Filtra apenas os timestamps dentro do intervalo
            var filteredDefault = defaultIndex
                .Select(x => x.ToString())
                .Where(x =>
                    DateTime.TryParseExact(x, "yyyy-MM-ddTHH:mm", null, System.Globalization.DateTimeStyles.AssumeUniversal, out var dt) &&
                    dt >= from && dt <= to)
                .ToList();

            var filteredFallback = fallbackIndex
                .Select(x => x.ToString())
                .Where(x =>
                    DateTime.TryParseExact(x, "yyyy-MM-ddTHH:mm", null, System.Globalization.DateTimeStyles.AssumeUniversal, out var dt) &&
                    dt >= from && dt <= to)
                .ToList();

            // Faz leitura paralela
            var defaultTasks = filteredDefault.Select(key => _db.HashGetAllAsync($"summary:default:{key}")).ToList();
            var fallbackTasks = filteredFallback.Select(key => _db.HashGetAllAsync($"summary:fallback:{key}")).ToList();

            var defaultResults = await Task.WhenAll(defaultTasks);
            var fallbackResults = await Task.WhenAll(fallbackTasks);

            foreach (var entries in defaultResults)
            {
                foreach (var entry in entries)
                {
                    if (entry.Name == "totalRequests") defaultRequests += (long)entry.Value;
                    else if (entry.Name == "totalAmount") defaultAmount += (long)entry.Value;
                }
            }

            foreach (var entries in fallbackResults)
            {
                foreach (var entry in entries)
                {
                    if (entry.Name == "totalRequests") fallbackRequests += (long)entry.Value;
                    else if (entry.Name == "totalAmount") fallbackAmount += (long)entry.Value;
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
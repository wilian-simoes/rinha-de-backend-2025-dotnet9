using rinha_de_backend_2025_dotnet9.Models;
using rinha_de_backend_2025_dotnet9.Models.PaymentProcessor;
using StackExchange.Redis;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class SummaryService
    {
        private readonly IDatabase _redis;
        private const string paymentsHashKey = "summary:payments";

        public SummaryService(IConnectionMultiplexer redis)
        {
            _redis = redis.GetDatabase();
        }

        public async Task IncrementSummaryAsync(string processorType, PaymentRequest paymentRequest)
        {
            var payment = new PaymentRedisEntry
            {
                CorrelationId = paymentRequest.correlationId,
                Amount = paymentRequest.amount,
                ProcessorType = processorType,
                RequestedAt = paymentRequest.requestedAt.ToUniversalTime()
            };

            var json = JsonSerializer.Serialize(payment);
            await _redis.HashSetAsync(paymentsHashKey, payment.CorrelationId, json);
        }

        public async Task<PaymentSummary> GetFullSummaryAsync(DateTime? from, DateTime? to)
        {
            var entries = await _redis.HashGetAllAsync(paymentsHashKey);
            var payments = new List<PaymentRedisEntry>();

            foreach (var entry in entries)
            {
                try
                {
                    var paymentData = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(entry.Value);

                    var payment = new PaymentRedisEntry
                    {
                        CorrelationId = paymentData["CorrelationId"].GetString(),
                        Amount = paymentData["Amount"].GetDecimal(),
                        ProcessorType = paymentData["ProcessorType"].GetString(),
                        RequestedAt = paymentData["RequestedAt"].GetDateTime().ToUniversalTime()
                    };

                    payments.Add(payment);
                }
                catch
                {
                }
            }

            var paymentsFiltreds = payments.Where(p => p.RequestedAt >= (from ?? DateTime.MinValue).ToUniversalTime() &&
                                                        p.RequestedAt <= (to ?? DateTime.MaxValue).ToUniversalTime()).ToList();

            return new PaymentSummary
            {
                _default = new Models.Shared.Summary
                {
                    totalRequests = paymentsFiltreds.Count(p => p.ProcessorType == "default"),
                    totalAmount = paymentsFiltreds.Where(p => p.ProcessorType == "default").Sum(p => p.Amount)
                },

                fallback = new Models.Shared.Summary
                {
                    totalRequests = paymentsFiltreds.Count(p => p.ProcessorType == "fallback"),
                    totalAmount = paymentsFiltreds.Where(p => p.ProcessorType == "fallback").Sum(p => p.Amount)
                }
            };
        }
    }

    public class PaymentRedisEntry
    {
        public string CorrelationId { get; set; }
        public decimal Amount { get; set; }
        public string ProcessorType { get; set; }
        public DateTime RequestedAt { get; set; }
    }
}
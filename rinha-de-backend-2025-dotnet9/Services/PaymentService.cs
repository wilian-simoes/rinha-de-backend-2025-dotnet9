using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentService
    {
        private readonly IDatabase _redis;
        private readonly SummaryService _summaryService;
        private readonly ILogger<PaymentService> _logger;

        public PaymentService(IConnectionMultiplexer connectionMultiplexer, SummaryService summaryService, ILogger<PaymentService> logger)
        {
            _redis = connectionMultiplexer.GetDatabase();
            _summaryService = summaryService;
            _logger = logger;
        }

        public async Task<PaymentSummary> GetPaymentsSummary(DateTime? from, DateTime? to)
        {
            var summary = await _summaryService.GetFullSummaryAsync(from, to);
            return summary;
        }

        public async Task RegisterPayment(Payment payment)
        {
            try
            {
                var json = JsonSerializer.Serialize(payment);
                await _redis.ListRightPushAsync("payments:queue", json, flags: CommandFlags.FireAndForget);
            }
            catch (Exception ex)
            {
                var messageError = $"Erro ao adicionar pagamento {payment.correlationId} na fila: {ex.Message}";
                _logger.LogError(messageError);
            }
        }
    }
}
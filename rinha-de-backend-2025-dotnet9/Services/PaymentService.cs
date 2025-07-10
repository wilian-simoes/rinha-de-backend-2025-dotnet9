using rinha_de_backend_2025_dotnet9.Models;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentService
    {
        private readonly RedisStreamService _streamService;
        private readonly SummaryService _summaryService;
        private readonly ILogger<PaymentService> _logger;

        public PaymentService(RedisStreamService streamService, SummaryService summaryService, ILogger<PaymentService> logger)
        {
            _streamService = streamService;
            _summaryService = summaryService;
            _logger = logger;
        }

        public async Task<PaymentSummary> GetPaymentsSummary(DateTime from, DateTime to)
        {
            // TODO: Lembrar de implementar lógica de filtragem por data
            var summary = await _summaryService.GetFullSummaryAsync();
            return summary;
        }

        public async Task<string> RegisterPayment(Payment payment)
        {
            try
            {
                await _streamService.EnqueueAsync(payment);
                var message = $"Pagamento {payment.correlationId} adicionado na fila.";
                _logger.LogInformation(message);
                return message;
            }
            catch (Exception ex)
            {
                var messageError = $"Erro ao adicionar pagamento {payment.correlationId} na fila: {ex.Message}";
                _logger.LogError(messageError);
                return messageError;
            }
        }
    }
}
using rinha_de_backend_2025_dotnet9.Models.PaymentProcessor;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentProcessorService
    {
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ILogger<PaymentProcessorService> _logger;

        public PaymentProcessorService(IHttpClientFactory httpClientFactory, ILogger<PaymentProcessorService> logger)
        {
            _httpClientFactory = httpClientFactory;
            _logger = logger;
        }

        private HttpClient GetClient(bool useFallback)
        {
            if (useFallback)
            {
                return _httpClientFactory.CreateClient("payment-processor-fallback");
            }

            return _httpClientFactory.CreateClient("payment-processor-default");
        }

        public async Task<string> Payments(PaymentRequest paymentRequest, bool useFallback = false)
        {
            var client = GetClient(useFallback);
            var response = await client.PostAsJsonAsync("/payments", paymentRequest);

            if(!response.IsSuccessStatusCode)
            {
                var messageError = $"Erro ao processar pagamento {paymentRequest.correlationId}: {response.StatusCode}";
                _logger.LogError(messageError);
                throw new Exception(messageError);
            }

            _logger.LogInformation($"Pagamento processado com sucesso: {paymentRequest.correlationId}");
            return await response.Content.ReadAsStringAsync();
        }
    }
}
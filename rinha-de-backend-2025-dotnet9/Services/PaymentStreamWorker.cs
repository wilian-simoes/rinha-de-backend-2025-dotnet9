namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentStreamWorker : BackgroundService
    {
        private readonly RedisStreamService _streamService;
        private readonly ILogger<PaymentStreamWorker> _logger;
        private readonly SummaryService _summaryService;
        private readonly IServiceScopeFactory _scopeFactory;

        public PaymentStreamWorker(
            RedisStreamService streamService,
            ILogger<PaymentStreamWorker> logger,
            SummaryService summaryService,
            IServiceScopeFactory scopeFactory)
        {
            _streamService = streamService;
            _logger = logger;
            _summaryService = summaryService;
            _scopeFactory = scopeFactory;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Worker de pagamentos (Redis Stream) iniciado.");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var (messageId, payment) = await _streamService.ReadNextAsync();

                    if (payment != null)
                    {
                        _logger.LogInformation($"[Worker] Processando pagamento {payment.correlationId} - R$ {payment.amount}");

                        // TODO: lógica de processamento

                        using var scope = _scopeFactory.CreateScope();
                        var paymentProcessorService = scope.ServiceProvider.GetRequiredService<PaymentProcessorService>();
                        var health = await paymentProcessorService.GetServiceHealthAsync();

                        var request = new Models.PaymentProcessor.PaymentRequest()
                        {
                            correlationId = payment.correlationId,
                            amount = payment.amount,
                            requestedAt = DateTime.UtcNow,
                        };

                        // TODO: Criar método para decidir se usa default ou fallback
                        var response = await paymentProcessorService.PostPaymentsAsync(request);

                        await _summaryService.IncrementSummaryAsync("default", request.amount, request.requestedAt);
                        _logger.LogInformation($"[Worker] {response} {request.correlationId} - R$ {request.amount}");

                        await _streamService.AcknowledgeAsync(messageId);
                    }
                    else
                    {
                        await Task.Delay(500, stoppingToken);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Erro ao processar mensagem do Redis Stream.");
                    await Task.Delay(1000, stoppingToken);
                }
            }

            _logger.LogInformation("Worker de pagamentos encerrado.");
        }
    }
}
namespace rinha_de_backend_2025_dotnet9.Services
{
    public class PaymentStreamWorker : BackgroundService
    {
        private readonly RedisStreamService _streamService;
        private readonly ILogger<PaymentStreamWorker> _logger;
        private readonly SummaryService _summaryService;

        public PaymentStreamWorker(RedisStreamService streamService, ILogger<PaymentStreamWorker> logger, SummaryService summaryService)
        {
            _streamService = streamService;
            _logger = logger;
            _summaryService = summaryService;
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

                        await _summaryService.IncrementSummaryAsync("default", payment.amount, DateTime.UtcNow);
                        _logger.LogInformation($"[Worker] Processado! pagamento {payment.correlationId} - R$ {payment.amount}");

                        //

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
namespace rinha_de_backend_2025_dotnet9.Models.PaymentProcessor
{
    public class PaymentRequest
    {
        public string correlationId { get; set; }
        public decimal amount { get; set; }
        public DateTime requestedAt { get; set; }
    }
}
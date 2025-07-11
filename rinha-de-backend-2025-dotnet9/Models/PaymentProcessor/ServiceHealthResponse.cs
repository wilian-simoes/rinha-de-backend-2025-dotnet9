namespace rinha_de_backend_2025_dotnet9.Models.PaymentProcessor
{
    public class ServiceHealthResponse
    {
        public bool failing { get; set; }
        public int minResponseTime { get; set; }
    }
}
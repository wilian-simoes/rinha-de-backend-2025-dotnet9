using rinha_de_backend_2025_dotnet9.Models.Shared;
using System.Text.Json.Serialization;

namespace rinha_de_backend_2025_dotnet9.Models
{
    public class PaymentSummary
    {
        [JsonPropertyName("default")]
        public Summary _default { get; set; }
        public Summary fallback { get; set; }
    }
}
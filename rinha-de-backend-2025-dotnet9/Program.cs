using rinha_de_backend_2025_dotnet9.Models;
using rinha_de_backend_2025_dotnet9.Services;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

// Configurar Redis Stream
builder.Services.AddSingleton<RedisStreamService>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var conn = config["Redis:ConnectionString"] ?? "redis:6379";

    return new RedisStreamService(conn,
        streamKey: "payments-stream",
        consumerGroup: "payments-group",
        consumerName: $"worker-{Environment.MachineName}");
});

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var conn = config["Redis:ConnectionString"] ?? "redis:6379";

    return ConnectionMultiplexer.Connect(conn);
});

builder.Services.AddHttpClient("payment-processor-default", client =>
{
    client.BaseAddress = new Uri("http://localhost:8001");
    client.DefaultRequestHeaders.Add("Accept", "application/json");
});

builder.Services.AddHttpClient("payment-processor-fallback", client =>
{
    client.BaseAddress = new Uri("http://localhost:8002");
    client.DefaultRequestHeaders.Add("Accept", "application/json");
});

builder.Services.AddLogging();
builder.Services.AddHostedService<PaymentStreamWorker>();
builder.Services.AddScoped<PaymentService>();
builder.Services.AddSingleton<SummaryService>();
builder.Services.AddScoped<PaymentProcessorService>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapPost("/payments", async (Payment payment, PaymentService paymentService) =>
{
    var result = await paymentService.RegisterPayment(payment);
    return Results.Ok(result);
})
.WithName("payments");

app.MapGet("/payments-summary", async (DateTime? from, DateTime? to, PaymentService paymentService) =>
{
    var summary = await paymentService.GetPaymentsSummary(from, to);
    return Results.Ok(summary);
})
.WithName("payments-summary");
    
app.Run();
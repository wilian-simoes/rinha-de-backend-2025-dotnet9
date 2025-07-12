using rinha_de_backend_2025_dotnet9.Models;
using StackExchange.Redis;
using System.Text.Json;

namespace rinha_de_backend_2025_dotnet9.Services
{
    public class RedisStreamService
    {
        private readonly IDatabase _db;
        private readonly string _streamKey;
        private readonly string _consumerGroup;
        private readonly string _consumerName;

        public RedisStreamService(string connectionString, string streamKey = "payments-stream", string consumerGroup = "payments-group", string consumerName = "api-worker-1")
        {
            var redis = ConnectionMultiplexer.Connect(connectionString);
            _db = redis.GetDatabase();
            _streamKey = streamKey;
            _consumerGroup = consumerGroup;
            _consumerName = consumerName;

            // Tenta criar o grupo (ignora se já existe)
            try
            {
                _db.StreamCreateConsumerGroup(_streamKey, _consumerGroup, "$", createStream: true);
            }
            catch (RedisServerException ex) when (ex.Message.Contains("BUSYGROUP"))
            {
                // grupo já existe, ignora
            }
        }

        public async Task EnqueueAsync(Payment payment)
        {
            var json = JsonSerializer.Serialize(payment);

            var entry = new NameValueEntry[]
            {
            new("data", json)
            };

            await _db.StreamAddAsync(_streamKey, entry);
        }

        public async Task<(string messageId, Payment?)> ReadNextAsync(int timeoutMs = 5000)
        {
            var entries = await _db.StreamReadGroupAsync(
                _streamKey,
                _consumerGroup,
                _consumerName,
                count: 1,
                noAck: false,
                position: ">"
            );

            if (entries.Length == 0)
                return (string.Empty, null);

            var message = entries[0];
            var json = message["data"];
            var payment = JsonSerializer.Deserialize<Payment>(json);

            return (message.Id, payment);
        }

        public async Task AcknowledgeAsync(string messageId)
        {
            await _db.StreamAcknowledgeAsync(_streamKey, _consumerGroup, messageId);
        }

        public async Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(int count)
        {
            return await _db.StreamPendingMessagesAsync(_streamKey, _consumerGroup, count, _consumerName);
        }

        public async Task<StreamEntry[]> StreamRangeAsync(RedisValue messageId)
        {
            return await _db.StreamRangeAsync(_streamKey, messageId, messageId);
        }

        // teste
        public async Task<StreamEntry[]> StreamClaimAsync(int minIdleTimeMs, RedisValue messageId)
        {
            return await _db.StreamClaimAsync(_streamKey, _consumerGroup, _consumerName, minIdleTimeMs, [messageId]);
        }
    }
}
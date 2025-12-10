using System;
using System.Text;
using System.Threading.Tasks;
using Confluent.Kafka;
using UnityEngine;

public class KafkaProducer : IDisposable
{
    private IProducer<string, byte[]> _producer;

    public KafkaProducer(string bootstrapServers = "localhost:9094", int lingerMs = 5, int retries = 3)
    {
        var cfg = new ProducerConfig
        {
            BootstrapServers = bootstrapServers,
            LingerMs = lingerMs,
            MessageSendMaxRetries = retries
        };
        _producer = new ProducerBuilder<string, byte[]>(cfg).Build();
    }

    public async Task<string> SendAsync(string topic, string key, byte[] value)
    {
        return await SendAsync(topic, key, value, null, null);
    }

    public async Task<string> SendAsync(
        string topic,
        string key,
        byte[] value,
        string userId,
        string gameId)
    {
        try
        {
            var msg = new Message<string, byte[]>
            {
                Key = key,
                Value = value
            };

            if (!string.IsNullOrEmpty(userId) || !string.IsNullOrEmpty(gameId))
            {
                msg.Headers = new Headers();

                if (!string.IsNullOrEmpty(userId))
                    msg.Headers.Add("x-user-id", Encoding.UTF8.GetBytes(userId));

                if (!string.IsNullOrEmpty(gameId))
                    msg.Headers.Add("x-game-id", Encoding.UTF8.GetBytes(gameId));
            }

            var res = await _producer.ProduceAsync(topic, msg).ConfigureAwait(false);

            var tpoff = res.TopicPartitionOffset.ToString();
            Debug.Log($"[KafkaProducer] Delivered to: {tpoff}");
            return tpoff;
        }
        catch (Exception ex)
        {
            Debug.LogError($"[KafkaProducer] SendAsync exception: {ex}");
            return null;
        }
    }

    public void Flush() => _producer?.Flush(TimeSpan.FromSeconds(5));

    public void Dispose()
    {
        _producer?.Dispose();
        GC.SuppressFinalize(this);
    }
}

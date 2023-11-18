using StackExchange.Redis;

namespace Redis.Api;

public class RedisStreamReaderBackgroundWorker:BackgroundService
{
    private readonly IDatabase _redisDatabase;
    private readonly ILogger<RedisStreamReaderBackgroundWorker> _logger;
    private readonly string _streamKey= "Streaming_Actions";
    public RedisStreamReaderBackgroundWorker(IDatabase redisDatabase, ILogger<RedisStreamReaderBackgroundWorker> logger)
    {
        _redisDatabase = redisDatabase;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var streamLength = await _redisDatabase.StreamLengthAsync(_streamKey);

            if (streamLength == 0)
            {
                await Task.Delay(1000,stoppingToken);
                continue;
            }

            var streamValues = await _redisDatabase.StreamReadAsync(_streamKey, "0", (int)streamLength);

            foreach (var streamValue in streamValues)
            {
                _logger.LogWarning("Stream entry received {streamId} , {streamValue}",streamValue.Id, string.Join("\n", streamValue.Values.Select(v => $"{v.Name}={v.Value}")));

                await _redisDatabase.KeyDeleteAsync(_streamKey,CommandFlags.FireAndForget);
            }

            await Task.Delay(2000,stoppingToken);
        }
    }
}
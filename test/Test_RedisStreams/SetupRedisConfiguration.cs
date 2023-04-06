using Microsoft.Extensions.DependencyInjection;
using SlugEnt;
using SlugEnt.SLRStreamProcessing;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;

namespace Test_RedisStreams;

public class SetupRedisConfiguration
{
    protected SLRStreamEngine    _slrStreamEngine;
    protected IServiceCollection _services;
    protected ServiceProvider    _serviceProvider;
    protected RedisConfiguration _redisConfiguration;
    protected UniqueKeys         _uniqueKeys;


    public void Initialize()
    {
        _services = new ServiceCollection().AddLogging();
        _services.AddTransient<SLRStreamEngine>();
        _services.AddTransient<SLRStream>();
        _serviceProvider = _services.BuildServiceProvider();


        // NOTE:  PoolSize must be set to 1.  If not it appears sometimes that Redis is still doing some background work to finish up, and the next call will not return the expected results.
        // For example.  Write a message to the stream.  Then immediately read it back.  With Pool sizes > 4 it seems I can consistently get an error about 40% of the time saying there are no messages.
        // If you set a thread.sleep(1) then it all works fine.  Or set PoolSize to 1 and it will work.  This just proves the ConnectionPooling is actually doing something.
        _redisConfiguration = new RedisConfiguration
        {
            Password = "redispw", Hosts = new[] { new RedisHost { Host = "localhost", Port = 6379 } }, ConnectTimeout = 700, PoolSize = 1,
        };


        // This is purely to validate we have a local Redis DB and that it is available.  If its not all tests will fail.
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        engine.RedisConfiguration = _redisConfiguration;
        Assert.IsTrue(engine.Initialize(), "A10:  Engine is not connected to Redis DB.  For Testing purposes ensure you have a local Redis DB running.");

        // Store engine so other test methods can use
        _slrStreamEngine = engine;

        _uniqueKeys = new();
    }


    /// <summary>
    /// Setups the producer for a given test.  It also removes any initial messages created as part of the setup process.  For instance
    /// the accessing of a stream that does not exist automatically creates it by sending a create message.  This removes that initial message
    /// so it does not mess with the tests.
    /// </summary>
    /// <param name="config"></param>
    /// <returns></returns>
    public async Task<SLRStream> SetupTestProducer(SLRStreamConfig config)
    {
        // A.  Produce
        SLRStream stream = await _slrStreamEngine.GetSLRStreamAsync(config);
        Assert.IsNotNull(stream, "A10:");

        stream.ResetStatistics();


        // Ensure no messages - there probably is at least 1 - the creation message
        StreamEntry[] emptyMessages;
        if (stream.IsConsumerGroup)
        {
            emptyMessages = await stream.ReadStreamGroupAsync(1000);
            foreach (StreamEntry emptyMessage in emptyMessages)
            {
                await stream.AddPendingAcknowledgementAsync(emptyMessage);
            }

            // Ensure it is flushed.
            await stream.FlushPendingAcknowledgementsAsync();
        }
        else
        {
            emptyMessages = await stream.ReadStreamAsync(1000);
        }


        RedisValue[] values = new RedisValue[emptyMessages.Length];
        int          i      = 0;
        foreach (StreamEntry emptyMessage in emptyMessages)
        {
            values[i++] = emptyMessage.Id;
        }

        await stream.DeleteMessages(values);

        // Reset any counters;
        stream.ResetStatistics();
        return stream;
    }
}
using Microsoft.Extensions.DependencyInjection;
using SlugEnt.SLRStreamProcessing;
using StackExchange.Redis.Extensions.Core.Configuration;

namespace Test_RedisStreams;

[TestFixture]
public class Tests
{
    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _services = new ServiceCollection().AddLogging();
        _services.AddTransient<SLRStreamEngine>();
        _services.AddTransient<SLRStream>();
        _serviceProvider = _services.BuildServiceProvider();


        _configuration = new RedisConfiguration
        {
            Password = "redispw", Hosts = new[] { new RedisHost { Host = "localhost", Port = 6379 } }, ConnectTimeout = 700
        };

        // This is purely to validate we have a local Redis DB and that it is available.  If its not all tests will fail.
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        engine.RedisConfiguration = _configuration;
        Assert.IsTrue(engine.Initialize(), "A10:  Engine is not connected to Redis DB.  For Testing purposes ensure you have a local Redis DB running.");

        // Store engine so other test methods can use
        _slrStreamEngine = engine;
    }


    [SetUp]
    public void Setup() { }


    private SLRStreamEngine    _slrStreamEngine;
    private IServiceCollection _services;
    private ServiceProvider    _serviceProvider;
    private RedisConfiguration _configuration;


    [Test]
    public void InitializeRedisEngineWithoutConfig_Throws()
    {
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        Assert.Throws<ApplicationException>(() => engine.Initialize(), "A10:");
    }


    [Test]
    public void InitializeEngineWithConfigPropertySet_Success()
    {
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        engine.RedisConfiguration = _configuration;
        Assert.IsTrue(engine.Initialize(), "A10:");
    }



    [Test]
    public void InitializeEngineWithConfigParameterSet_Success()
    {
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        Assert.IsTrue(engine.Initialize(_configuration), "A10:");
    }


    [Test]
    public void InitializeEngine_SetsIsConnected()
    {
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        engine.Initialize(_configuration);
        Assert.IsTrue(engine.IsConnected, "A10:");
    }



    [Test]
    public void InitializeEngine_SetsIsInitialized()
    {
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        engine.Initialize(_configuration);
        Assert.IsTrue(engine.IsInitialized, "A10:");
    }


    [Test]
    public async Task GetStream_ProducerOnly()
    {
        // Setup
        string             streamName  = "testA";
        string             appName     = "testerApp";
        int                pendingAcks = 10;
        EnumSLRStreamTypes streamType  = EnumSLRStreamTypes.ProducerOnly;

        // We use the standard testing engine.
        SLRStreamConfig config = new() { StreamName = streamName, ApplicationName = appName, StreamType = streamType };

        SLRStream producer = await _slrStreamEngine.GetSLRStreamAsync(config);
        Assert.IsNotNull(producer, "A10:");
        Assert.AreEqual(streamName, producer.StreamName, "A20:");
        Assert.AreEqual(appName, producer.ApplicationName, "A20:");
        Assert.IsTrue(producer.CanProduceMessages, "A30:");
        Assert.IsFalse(producer.CanConsumeMessages, "A40:");
    }



    [Test]
    public async Task GetStream_SimpleConsumerOnly()
    {
        // Setup
        string             streamName  = "testA";
        string             appName     = "testerApp";
        int                pendingAcks = 10;
        EnumSLRStreamTypes streamType  = EnumSLRStreamTypes.SimpleConsumerOnly;

        // We use the standard testing engine.
        SLRStreamConfig config = new() { StreamName = streamName, ApplicationName = appName, StreamType = streamType };

        SLRStream consumer = await _slrStreamEngine.GetSLRStreamAsync(config);
        Assert.IsNotNull(consumer, "A10:");
        Assert.AreEqual(streamName, consumer.StreamName, "A20:");
        Assert.AreEqual(appName, consumer.ApplicationName, "A20:");
        Assert.IsFalse(consumer.CanProduceMessages, "A30:");
        Assert.IsTrue(consumer.CanConsumeMessages, "A40:");
    }



    [Test]
    public async Task GetStream_ProducerSimpleConsumer()
    {
        // Setup
        string             streamName  = "testA";
        string             appName     = "testerApp";
        int                pendingAcks = 10;
        EnumSLRStreamTypes streamType  = EnumSLRStreamTypes.ProducerAndSimpleConsumer;

        // We use the standard testing engine.
        SLRStreamConfig config = new() { StreamName = streamName, ApplicationName = appName, StreamType = streamType };

        SLRStream combo = await _slrStreamEngine.GetSLRStreamAsync(config);
        Assert.IsNotNull(combo, "A10:");
        Assert.IsTrue(combo.CanProduceMessages, "A20:");
        Assert.IsTrue(combo.CanConsumeMessages, "A30:");
    }



    [Test]
    public async Task GetStream_ProducerAndConsumerGroup()
    {
        // Setup
        string             streamName  = "testA";
        string             appName     = "testerApp";
        int                pendingAcks = 10;
        EnumSLRStreamTypes streamType  = EnumSLRStreamTypes.ProducerAndConsumerGroup;

        // We use the standard testing engine.
        SLRStreamConfig config = new() { StreamName = streamName, ApplicationName = appName, StreamType = streamType };

        SLRStream combo = await _slrStreamEngine.GetSLRStreamAsync(config);
        Assert.IsNotNull(combo, "A10:");
        Assert.IsTrue(combo.CanProduceMessages, "A20:");
        Assert.IsTrue(combo.CanConsumeMessages, "A30:");
        Assert.IsTrue(combo.IsConsumerGroup, "A40:");
    }



    [Test]
    public async Task GetStream_ConsumerGroupOnly()
    {
        // Setup
        string             streamName  = "testA";
        string             appName     = "testerApp";
        int                pendingAcks = 10;
        EnumSLRStreamTypes streamType  = EnumSLRStreamTypes.ConsumerGroupOnly;

        // We use the standard testing engine.
        SLRStreamConfig config = new() { StreamName = streamName, ApplicationName = appName, StreamType = streamType };

        SLRStream combo = await _slrStreamEngine.GetSLRStreamAsync(config);
        Assert.IsNotNull(combo, "A10:");
        Assert.IsFalse(combo.CanProduceMessages, "A20:");
        Assert.IsTrue(combo.CanConsumeMessages, "A30:");
        Assert.IsTrue(combo.IsConsumerGroup, "A40:");
    }
}
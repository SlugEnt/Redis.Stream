using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;

namespace Redis.Stream;

public class RedisStreamEngine
{
    private readonly ILogger                         _logger;
    private          IServiceProvider                _serviceProvider;
    private          ILoggerFactory                  _loggerFactory;
    private          Dictionary<string, RedisStream> _streams;
    private          ConnectionMultiplexer           _multiplexer;



    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="serviceProvider"></param>
    /// <param name="loggerFactory"></param>
    public RedisStreamEngine(ILogger<RedisStreamEngine> logger, IServiceProvider serviceProvider, ILoggerFactory loggerFactory)
    {
        _logger          = logger;
        _loggerFactory   = loggerFactory;
        _serviceProvider = serviceProvider;
    }



    /// <summary>
    /// The Configuration options for connecting to Redis Servers
    /// </summary>
    public ConfigurationOptions RedisConfigurationOptions { get; set; }



    /// <summary>
    /// Initializes the engine, builds the connection Multiplexer
    /// </summary>
    /// <param name="redisConfigurationOptions"></param>
    public void Initialize(ConfigurationOptions redisConfigurationOptions = null)
    {
        if (redisConfigurationOptions != null)
            RedisConfigurationOptions = redisConfigurationOptions;
        _multiplexer  = ConnectionMultiplexer.Connect(RedisConfigurationOptions);
        IsInitialized = true;
    }


    /// <summary>
    /// Returns true if the engine has been initialized and is ready
    /// </summary>
    public bool IsInitialized { get; set; }


    /// <summary>
    /// Returns a Dictionary of all the streams
    /// </summary>
    public IReadOnlyDictionary<string, RedisStream> Streams
    {
        get { return _streams; }
    }



    /// <summary>
    /// Returns a new Stream of the type specified.  If the type is a Consumer Group, the consumer group name is the application name.
    /// </summary>
    /// <param name="streamName">Name of the stream to work with</param>
    /// <param name="applicationName">Name of this application</param>
    /// <param name="redisStreamType">The type of stream.  Defaults to a simple producer/consumer</param>
    /// <returns></returns>
    public async Task<RedisStream> GetRedisStream(string streamName, string applicationName,
                                                  EnumRedisStreamTypes redisStreamType = EnumRedisStreamTypes.ProducerAndSimpleConsumer)
    {
        if (RedisConfigurationOptions == null)
            throw new
                ApplicationException("The RedisStreamEngine does not have a valid ConfigurationOptions value.  You must set configuration options before starting a stream.");

        ILogger<RedisStream> logStream = _loggerFactory.CreateLogger<RedisStream>();
        RedisStream          stream    = _serviceProvider.GetService<RedisStream>();

        StreamConfig config = new StreamConfig()
        {
            StreamName                 = streamName,
            ApplicationName            = applicationName,
            Multiplexer                = _multiplexer,
            StreamType                 = redisStreamType,
            MaxPendingAcknowledgements = 25,
        };

        await stream.SetStreamConfig(config);

        //await stream.SetStreamValues(streamName, applicationName, redisStreamType, _multiplexer);
        return stream;
    }


    public async Task RemoveStreamFromEngine(string streamName) { throw new NotImplementedException(); }


    public async Task StopStreamAsync(string streamName) { throw new NotImplementedException(); }


    public async Task StopAllStreamsAsync(string streamName) { throw new NotSupportedException(); }
}
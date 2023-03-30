using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Implementations;

namespace SlugEnt.SLRStreamProcessing;

public class SLRStreamEngine
{
    private readonly ILogger                       _logger;
    private          IServiceProvider              _serviceProvider;
    private          ILoggerFactory                _loggerFactory;
    private          Dictionary<string, SLRStream> _streams;
    private          ConnectionMultiplexer         _multiplexer;



    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="serviceProvider"></param>
    /// <param name="loggerFactory"></param>
    public SLRStreamEngine(ILogger<SLRStreamEngine> logger, IServiceProvider serviceProvider, ILoggerFactory loggerFactory)
    {
        _logger          = logger;
        _loggerFactory   = loggerFactory;
        _serviceProvider = serviceProvider;
    }



    //public ConfigurationOptions RedisConfigurationOptions { get; set; }

    /// <summary>
    /// The Configuration options for connecting to Redis Servers
    /// </summary>
    public RedisConfiguration RedisConfiguration { get; set; }

    protected RedisConnectionPoolManager RedisConnectionPoolManager { get; set; }


    /// <summary>
    /// Initializes the engine, builds the connection Multiplexer.  Returns the connection status.
    /// </summary>
    /// <param name="redisConfigurationOptions"></param>
    /// <returns>RedisConnectionException if it cannot connect.</returns>
    public bool Initialize(RedisConfiguration redisConfiguration = null)
    {
        if (redisConfiguration == null)
        {
            if (RedisConfiguration == null)
                throw new ApplicationException("You must set RedisConfiguration on initialization of the engine.");
        }
        else
            RedisConfiguration = redisConfiguration;

        IsInitialized = true;

        ILogger<RedisConnectionPoolManager> redisConnLogger = _loggerFactory.CreateLogger<RedisConnectionPoolManager>();
        RedisConnectionPoolManager = new(RedisConfiguration, redisConnLogger);
        IsConnected                = RedisConnectionPoolManager.GetConnection().IsConnected;

//        _multiplexer = ConnectionMultiplexer.Connect(RedisConfigurationOptions);
        //IsConnected  = _multiplexer.IsConnected;
        return IsConnected;
    }


    /// <summary>
    /// Returns true if the engine has been initialized and is ready
    /// </summary>
    public bool IsInitialized { get; set; }


    /// <summary>
    /// True if the Engine is currently connected to a Redis Server
    /// </summary>
    public bool IsConnected { get; set; }


    /// <summary>
    /// Returns a Dictionary of all the streams
    /// </summary>
    public IReadOnlyDictionary<string, SLRStream> Streams
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
    /// 
    public async Task<SLRStream> GetSLRStreamAsync(SLRStreamConfig slrStreamConfig)
    {
        if (!IsInitialized)
            throw new ApplicationException("The RedisStreamEngine has not been initialized yet.");
        if (RedisConfiguration == null)
            throw new
                ApplicationException("The RedisStreamEngine does not have a valid RedisConfiguration value.  You must set configuration before starting a stream.");

        ILogger<SLRStream> logStream = _loggerFactory.CreateLogger<SLRStream>();
        SLRStream          stream    = _serviceProvider.GetService<SLRStream>();
        if (stream == null)
            throw new ApplicationException("Unable to create a SLRStream object from the ServiceProvider.");

        await stream.SetStreamConfig(slrStreamConfig, RedisConnectionPoolManager, RedisConfiguration);

        //await stream.SetStreamValues(streamName, applicationName, redisStreamType, _multiplexer);
        return stream;
    }


    public async Task RemoveStreamFromEngine(string streamName) { throw new NotImplementedException(); }


    public async Task StopStreamAsync(string streamName) { throw new NotImplementedException(); }


    public async Task StopAllStreamsAsync(string streamName) { throw new NotSupportedException(); }
}
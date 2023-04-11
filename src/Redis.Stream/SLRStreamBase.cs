using ByteSizeLib;
using Microsoft.Extensions.Logging;
using SlugEnt.SLRStreamProcessing;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Implementations;
using StackExchange.Redis.Extensions.System.Text.Json;

namespace SLRStreamProcessing;

public abstract class SLRStreamBase
{
    public const string STREAM_POSITION_BEGINNING     = "0-0";
    public const string STREAM_POSITION_MAX           = "+";
    public const string STREAM_POSITION_MIN           = "-";
    public const string STREAM_POSITION_SINCE_MESSAGE = "(";

    protected ILogger<SLRStream>         _logger;
    protected RedisClient                _redisClient;
    protected RedisConnectionPoolManager _redisConnectionPoolManager;


    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="serviceProvider"></param>
    public SLRStreamBase(ILogger<SLRStream> logger, IServiceProvider serviceProvider) { _logger = logger; }


    public async Task CloseStream() { IsInitialized = false; }



    /// <summary>
    ///     Creates a stream on the Redis server by sending an initial message.  Note, this bypasses the CanProduceMessages
    ///     check, so both producers and consumers can create the streaam
    ///     This is necessary, because we cannot create the Stream groups without the stream.
    /// </summary>
    /// <returns></returns>
    public async Task CreateStreamAsync()
    {
        SLRMessage       message = SLRMessage.CreateMessage("Create Stream");
        NameValueEntry[] values  = message.GetNameValueEntries();
        await _redisClient.Db0.Database.StreamAddAsync(StreamName, values);
    }



    /// <summary>
    ///     Deletes the given message ID's.
    ///     <para>This should be used with caution with ConsumerGroups</para>
    /// </summary>
    /// <param name="messageId"></param>
    /// <returns></returns>
    public async Task<long> DeleteMessages(RedisValue[] messageId)
    {
        if (messageId.Length == 0)
            return 0;

        List<string> xGroupArgs = new();
        xGroupArgs.Add(StreamName);
        string cmd = "XDEL";
        foreach (RedisValue messageID in messageId)
            xGroupArgs.Add(messageID);

        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        return (long)result;
    }


    /// <summary>
    ///     Permanently removes the stream and all message. Use Caution!
    /// </summary>
    public void DeleteStream() { _redisClient.Db0.Database.KeyDelete(StreamName); }


    /// <summary>
    ///     Returns information about all the consumers in the application group this stream is a part of.
    /// </summary>
    /// <returns></returns>
    internal async Task<StreamConsumerInfo[]> GetConsumers()
    {
        try
        {
            return await _redisClient.Db0.Database.StreamConsumerInfoAsync(StreamName, ApplicationName);
        }
        catch (RedisServerException rse)
        {
            if (rse.Message.Contains("NOGROUP"))
            {
                StreamConsumerInfo[] empty = new StreamConsumerInfo[0];
                return empty;
            }

            throw;
        }
    }


    /// <summary>
    ///     Returns the size of the current stream.
    /// </summary>
    /// <returns>ByteSize value</returns>
    public async Task<ByteSize> GetSize()
    {
        List<string> xGroupArgs = new();
        xGroupArgs.Add("USAGE");
        xGroupArgs.Add(StreamName);
        string      cmd    = "MEMORY";
        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        ByteSize x = ByteSize.FromBytes((double)result);
        return x;
    }



    /// <summary>
    ///     Gets information about all the applications that process this stream.
    /// </summary>
    /// <remarks>This is here vs the SLRStreamAppGroup, since it is needed to read the Vitals</remarks>
    /// <returns></returns>
    internal async Task<StreamGroupInfo[]> GetStreamApplications() => await _redisClient.Db0.Database.StreamGroupInfoAsync(StreamName);


    /// <summary>
    ///     Retries info about the stream
    /// </summary>
    /// <returns></returns>
    public async Task<StreamInfo> GetStreamInfo()
    {
        StreamInfo info = await _redisClient.Db0.Database.StreamInfoAsync(StreamName);
        return info;
    }


    /// <summary>
    ///     Returns vital statistics and information about this stream
    /// </summary>
    /// <returns></returns>
    public async Task<SLRStreamVitals> GetStreamVitals()
    {
        SLRStreamVitals streamVitals = await SLRStreamVitals.GetStreamVitals(this);
        return streamVitals;
    }



    /// <summary>
    ///     Reads up to max messages from the stream.  Note, it will starting with messages after the last message consumed
    ///     DURING THIS SESSION.  Other consumers know nothing about
    ///     what messages you have read or consumed and will start by default at the very first message in the queue.
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve"></param>
    /// <returns></returns>
    protected async Task<StreamEntry[]> ReadStreamAsync(int numberOfMessagesToRetrieve = 6)
    {
        if (!IsInitialized)
            throw new ApplicationException($"You must Initialize a SLRStream object before you can use it.  Stream Name: {StreamName}");

        if (!CanConsumeMessages)
            throw new ApplicationException("Attempted to read messages on a stream that you have NOT specified as a consumable stream for this application");

        RedisValue    lastMessageId = STREAM_POSITION_SINCE_MESSAGE + LastMessageId;
        StreamEntry[] messages = await _redisClient.Db0.Database.StreamRangeAsync(StreamName, lastMessageId, STREAM_POSITION_MAX, numberOfMessagesToRetrieve);


        // Store the Last Read Message ID
        if (messages.Length > 0)
            LastMessageId = messages[messages.Length - 1].Id;


        StatisticMessagesReceived += (ulong)messages.Length;
        return messages;
    }


    /// <summary>
    ///     Removes approximately or exactly all messages with an id before the passed in value.   For performance reasons it
    ///     is best to let Redis determine the exact Id it deletes messages upto.
    ///     <para>Thus, there may still exist some messages older than the messageIDToRemoveBefore value still present in DB</para>
    ///     <para>
    ///         Note: This does no checking to see if the message has been processed by all applications!  You could delete
    ///         messages that still need to be processed
    ///     </para>
    /// </summary>
    /// <param name="messageIDToRemoveBefore"></param>
    /// <param name="optimized">
    ///     If false, it will delete upto exactly the message Id provided.  If true, it will get close to
    ///     that Id, whatever it determines is optimal and fastest.
    /// </param>
    /// <returns></returns>
    public async Task<int> RemoveMessages(RedisValue messageIDToRemoveBefore, bool optimized = true)
    {
        string       optimal    = optimized ? "~" : "=";
        List<string> xGroupArgs = new();
        xGroupArgs.Add(StreamName);
        xGroupArgs.Add("MINID");
        xGroupArgs.Add(optimal);
        xGroupArgs.Add(messageIDToRemoveBefore);
        string      cmd    = "XTRIM";
        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        return (int)result;
    }


    /// <summary>
    ///     This should really never be called in a production app.  Resets all statistic counters to 0.
    /// </summary>
    public void ResetStatistics()
    {
        StatisticMessagesReceived = 0;
        StatisticMessagesConsumed = 0;
        StatisticMessagesSent     = 0;
        StatisticMessagesReceived = 0;
        LastMessageId             = STREAM_POSITION_BEGINNING;
    }



    /// <summary>
    ///     Sends the given message to the stream
    /// </summary>
    /// <param name="message"></param>
    public async Task SendMessageAsync(SLRMessage message)
    {
        if (!IsInitialized)
            throw new ApplicationException($"You must Initialize a SLRStream object before you can use it.  Stream Name: {StreamName}");

        if (!CanProduceMessages)
            throw new
                ApplicationException("Attempted to send a message to a stream that you have NOT specified as a stream you can produce messages for with this application");

        NameValueEntry[] values = message.GetNameValueEntries();
        await _redisClient.Db0.Database.StreamAddAsync(StreamName, values);
        StatisticMessagesSent++;
    }


    /// <summary>
    ///     Since we create via DependencyInjection we need a way of setting the stream properties after creation
    /// </summary>
    /// <param name="streamConfig">The configuration object for the stream</param>
    /// <param name="multiplexer">If not null, it overrides any multiplexer set in the config</param>
    internal async Task SetStreamConfig(SLRStreamConfig streamConfig, RedisConnectionPoolManager redisConnectionPoolManager,
                                        RedisConfiguration redisConfiguration)
    {
        try
        {
            //  TODO Add a StreamConfig object so set some of these variables, especially starting offset??
            StreamName      = streamConfig.StreamName;
            ApplicationName = streamConfig.ApplicationName;
            StreamType      = streamConfig.StreamType;


            // Configure Redis Connection
            // TODO RedisconnectionPoolManager can take an ILogger.  Probably need to create one...
            _redisConnectionPoolManager = redisConnectionPoolManager;
            SystemTextJsonSerializer serializer = new();
            _redisClient = new RedisClient(_redisConnectionPoolManager, serializer, redisConfiguration);


            // Get Stream Vitals
            StreamVitals = await GetStreamVitals();


            switch (StreamType)
            {
                case EnumSLRStreamTypes.ProducerOnly:
                    CanProduceMessages = true;
                    break;

                case EnumSLRStreamTypes.SimpleConsumerOnly:
                    CanConsumeMessages = true;
                    break;

                case EnumSLRStreamTypes.ConsumerGroupOnly:
                    CanConsumeMessages = true;
                    IsConsumerGroup    = true;
                    break;

                case EnumSLRStreamTypes.ProducerAndConsumerGroup:
                    CanProduceMessages = true;
                    CanConsumeMessages = true;
                    IsConsumerGroup    = true;
                    break;

                case EnumSLRStreamTypes.ProducerAndSimpleConsumer:
                    CanProduceMessages = true;
                    CanConsumeMessages = true;
                    break;
            }


            // If stream does not exist - create it if a producing stream
            if (!StreamVitals.StreamExists)
            {
                await CreateStreamAsync();
                StreamVitals = await GetStreamVitals();
            }


            // Set Stream Read Starting Position
            if (CanConsumeMessages)
                switch (streamConfig.StartingMessage)
                {
                    case EnumSLRStreamStartingPoints.Beginning:
                        LastMessageId = STREAM_POSITION_BEGINNING;
                        break;
                    case EnumSLRStreamStartingPoints.Now:
                        if (_redisClient.Db0.Database.KeyExists(StreamName))
                        {
                            StreamInfo streamInfo = _redisClient.Db0.Database.StreamInfo(StreamName);
                            LastMessageId = streamInfo.LastGeneratedId;
                        }
                        else
                        {
                            LastMessageId = STREAM_POSITION_BEGINNING;
                        }

                        break;
                    case EnumSLRStreamStartingPoints.LastConsumedForConsumerGroup: break;
                    case EnumSLRStreamStartingPoints.SpecifiedValue:
                        if (streamConfig.StartingMessageId == RedisValue.EmptyString)
                            throw new
                                ApplicationException($"You said you wanted to specify the starting message for Stream: {StreamName}, but the StartingMessageId in the config was never set.");

                        break;
                }


            IsInitialized = true;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error In StreamConfig.  {ex.Message}", ex);
            throw;
        }
    }



#region "Properties"

    /// <summary>
    ///     The type of stream
    /// </summary>
    public EnumSLRStreamTypes StreamType { get; protected set; }

    /// <summary>
    ///     If true, this stream can consume messages
    /// </summary>
    public bool CanConsumeMessages { get; protected set; }

    /// <summary>
    ///     If true, this stream can produce messages
    /// </summary>
    public bool CanProduceMessages { get; protected set; }

    /// <summary>
    ///     True, if this application is part of a consumer group
    /// </summary>
    public bool IsConsumerGroup { get; protected set; }

    /// <summary>
    ///     True if the stream has been initialized
    /// </summary>
    public bool IsInitialized { get; protected set; }

    /// <summary>
    ///     The name of the stream
    /// </summary>
    public string StreamName { get; protected set; }

    /// <summary>
    ///     Name of the Application that is processing.
    /// </summary>
    public string ApplicationName { get; protected set; }

    /// <summary>
    ///     Uniquely identifies a specific instance of the application.
    /// </summary>
    public int ApplicationId { get; protected set; }


    /// <summary>
    ///     Application Name + Application Id
    /// </summary>
    public string ApplicationFullName => ApplicationName + "." + ApplicationId;

    /// <summary>
    ///     The Id of the last message read by this Stream
    /// </summary>
    public RedisValue LastMessageId { get; protected set; }


    /// <summary>
    ///     Returns the latest stream vitals.  Note, it does not read them, so they could be stale.
    /// </summary>
    protected SLRStreamVitals StreamVitals { get; set; }


    /// <summary>
    ///     Number of Messages we have received.
    /// </summary>
    public ulong StatisticMessagesReceived { get; protected set; }


    /// <summary>
    ///     Number of messages that have been sent.
    /// </summary>
    public ulong StatisticMessagesSent { get; protected set; }


    public long StatisticMessagesConsumed { get; protected set; }

#endregion
}
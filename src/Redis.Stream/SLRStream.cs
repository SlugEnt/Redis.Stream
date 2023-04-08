using ByteSizeLib;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core;
using StackExchange.Redis.Extensions.Core.Abstractions;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Implementations;
using StackExchange.Redis.Extensions.System.Text.Json;

namespace SlugEnt.SLRStreamProcessing;

public class SLRStream
{
    public const string STREAM_POSITION_BEGINNING        = "0-0";
    public const string STREAM_POSITION_MAX              = "+";
    public const string STREAM_POSITION_MIN              = "-";
    public const string STREAM_POSITION_SINCE_MESSAGE    = "(";
    public const string STREAM_POSITION_CG_HISTORY_START = "0";
    public const string STREAM_POSITION_CG_NEW_MESSAGES  = ">";


    private   ILogger<SLRStream>         _logger;
    protected RedisConnectionPoolManager _redisConnectionPoolManager;
    protected RedisClient                _redisClient;
    private   List<RedisValue>           _pendingAcknowledgements;


    /// <summary>
    /// Constructor
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="serviceProvider"></param>
    public SLRStream(ILogger<SLRStream> logger, IServiceProvider serviceProvider) { _logger = logger; }



    /// <summary>
    /// Since we create via DependencyInjection we need a way of setting the stream properties after creation
    /// </summary>
    /// <param name="streamConfig">The configuration object for the stream</param>
    /// <param name="multiplexer">If not null, it overrides any multiplexer set in the config</param>
    internal async Task SetStreamConfig(SLRStreamConfig streamConfig, RedisConnectionPoolManager redisConnectionPoolManager,
                                        RedisConfiguration redisConfiguration)
    {
        try
        {
            //  TODO Add a StreamConfig object so set some of these variables, especially starting offset??
            StreamName                        = streamConfig.StreamName;
            ApplicationName                   = streamConfig.ApplicationName;
            StreamType                        = streamConfig.StreamType;
            MaxPendingMessageAcknowledgements = streamConfig.MaxPendingAcknowledgements;
            AutoClaimPendingMessagesThreshold = streamConfig.ClaimMessagesOlderThan;
            AutoAcknowledgeMessagesOnDelivery = streamConfig.AcknowledgeOnDelivery;

            _pendingAcknowledgements = new();


            // Configure Redis Connection
            // TODO RedisconnectionPoolManager can take an ILogger.  Probably need to create one...
            _redisConnectionPoolManager = redisConnectionPoolManager;
            SystemTextJsonSerializer serializer = new();
            _redisClient = new RedisClient(_redisConnectionPoolManager, serializer, redisConfiguration);


            // Get Stream Vitals
            SLRStreamVitals vitals = await GetStreamVitals();


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
            if (!vitals.StreamExists)
            {
                await CreateStreamAsync();
                vitals = await GetStreamVitals();

                /*if (CanProduceMessages)
                {
                    SLRMessage msg = SLRMessage.CreateMessage("Create Stream");
                    await SendMessageAsync(msg);
                    vitals = await GetStreamVitals();
                }
                */
            }


            // Set Stream Read Starting Position
            if (CanConsumeMessages)
            {
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
                            LastMessageId = STREAM_POSITION_BEGINNING;

                        break;
                    case EnumSLRStreamStartingPoints.LastConsumedForConsumerGroup: break;
                    case EnumSLRStreamStartingPoints.SpecifiedValue:
                        if (streamConfig.StartingMessageId == RedisValue.EmptyString)
                            throw new
                                ApplicationException($"You said you wanted to specify the starting message for Stream: {StreamName}, but the StartingMessageId in the config was never set.");

                        break;
                }
            }


            try
            {
                // Create the Consumer Group if that type of Stream
                if (IsConsumerGroup)
                {
                    if (!vitals.ApplicationExistsOnStream())
                    {
                        if (!await TryCreateConsumerGroup())
                            throw new
                                ApplicationException($"Failed to create the Consumer Group:  StreamName: {StreamName}   ApplicationName: {ApplicationName}");
                        if (!await SetConsumerApplicationId())
                            throw new
                                ApplicationException($"Failed to create the Consumer Application:  StreamName: {StreamName}   ApplicationConsumerName: {ApplicationFullName}");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex.Message, ex);
                throw;
            }

            IsInitialized = true;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error In StreamConfig.  {ex.Message}", ex);
        }
    }



    /// <summary>
    /// Saves this applications consumer ID to Redis
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    protected async Task SaveConsumerId()
    {
        List<string> xGroupArgs = new();
        xGroupArgs.Add("CREATECONSUMER");
        xGroupArgs.Add(StreamName);
        xGroupArgs.Add(ApplicationName);
        xGroupArgs.Add(ApplicationId.ToString());
        string      cmd    = $"XGROUP";
        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());
        if ((int)result != 1)
            throw new
                ApplicationException($"Unable to create consumer group:  Stream: {StreamName}, ConsumerGroup: {ApplicationName},  ConsumerName: {ApplicationId} ");
    }


    /// <summary>
    /// Sets a unique consumer ID for this consumer.  It then creates the consumer ID in Redis and assigns it to the application consumer group.  If it fails at any point it returns False
    /// </summary>
    /// <returns></returns>
    protected async Task<bool> SetConsumerApplicationId()
    {
        bool                 success      = false;
        StreamConsumerInfo[] consumerInfo = await _redisClient.Db0.Database.StreamConsumerInfoAsync(StreamName, ApplicationName);
        if (consumerInfo.Length == 0)
        {
            ApplicationId = 1;
            success       = true;
        }
        else
        {
            Dictionary<string, string> existingIds = new();
            foreach (StreamConsumerInfo streamConsumerInfo in consumerInfo)
                existingIds.Add(streamConsumerInfo.Name, streamConsumerInfo.Name);

            for (int i = 0; i < 1000; i++)
            {
                if (!existingIds.ContainsKey(i.ToString()))
                {
                    ApplicationId = i;
                    success       = true;
                    break;
                }
            }
        }

        if (success)
        {
            await SaveConsumerId();
            return true;
        }

        return false;
    }


    /// <summary>
    /// Attempts to create the Consumer Group.  Returns True if the group was created / exists.
    /// </summary>
    /// <returns></returns>
    protected async Task<bool> TryCreateConsumerGroup()
    {
        try
        {
            await _redisClient.Db0.Database.StreamCreateConsumerGroupAsync(StreamName, ApplicationName, STREAM_POSITION_BEGINNING);
        }
        catch (RedisServerException ex)
        {
            if (ex.Message.Contains("Consumer Group name already exists"))
                return true;

            _logger.LogError($"RedisServerError in TryCreateConsumerGroup.  {ex.Message}", ex);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error in TryCreateConsumerGroup:  {ex.Message}", ex);
            return false;
        }

        return true;
    }



#region "Properties"

    /// <summary>
    /// The type of stream
    /// </summary>
    public EnumSLRStreamTypes StreamType { get; protected set; }

    /// <summary>
    /// If true, this stream can consume messages
    /// </summary>
    public bool CanConsumeMessages { get; protected set; }

    /// <summary>
    /// If true, this stream can produce messages
    /// </summary>
    public bool CanProduceMessages { get; protected set; }

    /// <summary>
    /// True, if this application is part of a consumer group
    /// </summary>
    public bool IsConsumerGroup { get; protected set; }

    /// <summary>
    /// True if the stream has been initialized
    /// </summary>
    public bool IsInitialized { get; protected set; }

    /// <summary>
    /// The name of the stream
    /// </summary>
    public string StreamName { get; protected set; }

    /// <summary>
    /// Name of the Application that is processing.  
    /// </summary>
    public string ApplicationName { get; protected set; }

    /// <summary>
    /// Uniquely identifies a specific instance of the application.
    /// </summary>
    public int ApplicationId { get; protected set; }


    /// <summary>
    /// If true, messages are acknowledged as soon as Redis delivers them to us.
    /// </summary>
    public bool AutoAcknowledgeMessagesOnDelivery { get; set; }


    /// <summary>
    /// Application Name + Application Id
    /// </summary>
    public string ApplicationFullName
    {
        get { return ApplicationName + "." + ApplicationId; }
    }


    /// <summary>
    /// The maximum number of pending message acknowledgements, before an acknowledgement is automatically sent to Redis.
    /// </summary>
    public int MaxPendingMessageAcknowledgements { get; set; }


    /// <summary>
    /// The Id of the last message read by this Stream
    /// </summary>
    public RedisValue LastMessageId { get; protected set; }


    /// <summary>
    /// The amount of time to allow pending messages from other consumers to be in an unacknowledged state, before attempting to claim and process them.
    /// </summary>
    public TimeSpan AutoClaimPendingMessagesThreshold { get; set; }


    /// <summary>
    /// Returns the number of acknowledgements that are waiting to be sent to the Redis Server
    /// </summary>
    public int StatisticPendingAcknowledgements
    {
        get { return _pendingAcknowledgements.Count; }
    }


    /// <summary>
    /// Number of Messages we have received.
    /// </summary>
    public ulong StatisticMessagesReceived { get; protected set; }


    /// <summary>
    /// Number of messages that have been sent.
    /// </summary>
    public ulong StatisticMessagesSent { get; protected set; }


    /// <summary>
    /// The number of times the FlushPendingMessages method was called.
    /// </summary>
    public ulong StatisticFlushedMessageCalls { get; protected set; }


    public long StatisticMessagesConsumed { get; protected set; }

#endregion


    /// <summary>
    /// Creates a stream on the Redis server by sending an initial message.  Note, this bypasses the CanProduceMessages check, so both producers and consumers can create the streaam
    /// This is necessary, because we cannot create the Stream groups without the stream.
    /// </summary>
    /// <returns></returns>
    public async Task CreateStreamAsync()
    {
        SLRMessage       message = SLRMessage.CreateMessage("Create Stream");
        NameValueEntry[] values  = message.GetNameValueEntries();
        await _redisClient.Db0.Database.StreamAddAsync(StreamName, values);
    }


    /// <summary>
    /// Permanently removes the stream and all message. Use Caution!
    /// </summary>
    public void DeleteStream() { _redisClient.Db0.Database.KeyDelete(StreamName); }


    /// <summary>
    /// Sends the given message to the stream
    /// </summary>
    /// <param name="message"></param>
    public async Task SendMessageAsync(SLRMessage message)
    {
        if (!CanProduceMessages)
            throw new
                ApplicationException("Attempted to send a message to a stream that you have NOT specified as a stream you can produce messages for with this application");

        NameValueEntry[] values = message.GetNameValueEntries();
        await _redisClient.Db0.Database.StreamAddAsync(StreamName, values);
        StatisticMessagesSent++;
    }



    /// <summary>
    /// Reads up to max messages from the stream.  Note, it will starting with messages after the last message consumed DURING THIS SESSION.  Other consumers know nothing about
    /// what messages you have read or consumed and will start by default at the very first message in the queue.
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve"></param>
    /// <returns></returns>
    public async Task<StreamEntry[]> ReadStreamAsync(int numberOfMessagesToRetrieve = 6)
    {
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
    /// Reads messages for a consumer group.  
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve">Number of messages to retrieve</param>
    /// <param name="readPendingMessages">If true, it should read from the pending "queue" instead of the new queue.  Pending are messages already read once by this consumer, but never acknowledged.</param>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    public async Task<StreamEntry[]> ReadStreamGroupAsync(int numberOfMessagesToRetrieve = 6) //(, bool readPendingMessages = false))
    {
        if (!CanConsumeMessages)
            throw new
                ApplicationException($"Attempted to read messages on a stream that you have NOT specified as a consumable stream for this application.  StreamName: {StreamName}.");
        if (!IsConsumerGroup)
            throw new
                ApplicationException($"Tried to read a stream as a group, but you have not specified Group Consumption on this stream. StreamName: {StreamName}.");

        //string streamPosition = readPendingMessages == false ? STREAM_POSITION_CG_NEW_MESSAGES : STREAM_POSITION_CG_HISTORY_START;
        StreamEntry[] messages =
            await _redisClient.Db0.Database.StreamReadGroupAsync(StreamName, ApplicationName, ApplicationId, STREAM_POSITION_CG_NEW_MESSAGES,
                                                                 numberOfMessagesToRetrieve,
                                                                 AutoAcknowledgeMessagesOnDelivery);
        StatisticMessagesReceived += (ulong)messages.Length;
        return messages;
    }



    /// <summary>
    /// If there are any messages pending by other instances of this application that are older than AutoClaimPendingMessagesThreshold then this method will
    /// claim up to numberOfMessagesToRetrieve messages and return them to the caller.
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve">Maximum number of messages to attempt to claim</param>
    /// <returns>StreamAutoClaimResult - Which will contain 3 values.  The NextStartId if it is anything other than 0-0 then there are more messages to claim.</returns>
    /// <exception cref="ApplicationException"></exception>
    public async Task<StreamAutoClaimResult> ReadStreamGroupPendingMessagesAsync(int numberOfMessagesToRetrieve = 6)
    {
        if (!CanConsumeMessages)
            throw new
                ApplicationException($"Attempted to read messages on a stream that you have NOT specified as a consumable stream for this application.  StreamName: {StreamName}.");
        if (!IsConsumerGroup)
            throw new
                ApplicationException($"Tried to read a stream as a group, but you have not specified Group Consumption on this stream. StreamName: {StreamName}.");

        // Read any pending messages
        StreamAutoClaimResult result = await _redisClient.Db0.Database.StreamAutoClaimAsync(StreamName, ApplicationName, ApplicationId,
                                                                                            (long)AutoClaimPendingMessagesThreshold.TotalMilliseconds,
                                                                                            STREAM_POSITION_BEGINNING, numberOfMessagesToRetrieve);
        return result;
    }



    /// <summary>
    /// Acknowledges the given message.  Immediately sends the acknowlegement to the redis server.  Bypasses the pending acknowledgement process at the expense of more roundtrip calls to the Redis Server
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task AcknowledgeMessage(SLRMessage message)
    {
        await _redisClient.Db0.Database.StreamAcknowledgeAsync(StreamName, ApplicationName, message.Id);
    }



    /// <summary>
    /// Sends an acknowledgement of the messages to the Redis server
    /// </summary>
    /// <returns></returns>
    protected async Task<long> AcknowledgePendingMessages(RedisValue[] acks)
    {
        long count = await _redisClient.Db0.Database.StreamAcknowledgeAsync(StreamName, ApplicationName, acks);
        return count;
    }



    /// <summary>
    /// Adds an acknowledgement to the pending list.  If maximum allowed pending messages is reached it performs an auto-flush.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task AddPendingAcknowledgementAsync(StreamEntry message)
    {
        _pendingAcknowledgements.Add(message.Id);

        if (_pendingAcknowledgements.Count >= MaxPendingMessageAcknowledgements)
        {
            await FlushPendingAcknowledgementsAsync();
        }
    }


    /// <summary>
    /// Sends acknowledgements to the Redis server and resets the pending array.
    /// </summary>
    /// <returns></returns>
    public async Task FlushPendingAcknowledgementsAsync()
    {
        // Acknowledge the messages and then clear the list.
        RedisValue[] acks  = _pendingAcknowledgements.ToArray();
        long         count = await AcknowledgePendingMessages(acks);
        _pendingAcknowledgements.Clear();
        StatisticFlushedMessageCalls++;
    }



    /// <summary>
    /// Claims and Retrieves messages currently claimed by other consumers in the group if they have remained unacknowledged past the AutoClaimPendingMessagesThreshold.
    /// <para>NOTE:  You must acknowledge these messages manually.  There is NO AUTO ACKNOWLEDGEMENT.</para>
    /// </summary>
    /// <param name="numberOfMessagesToClaim">Maximum number of messages to claim from the dead consumer</param>
    /// <param name="startingMessageId">Message ID to start with in the pending list.</param>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    public async Task<StreamEntry[]> ClaimPendingMessagesAsync(int numberOfMessagesToClaim, RedisValue startingMessageId)
    {
        if (!CanConsumeMessages)
            throw new
                ApplicationException($"Attempted to read messages on a stream that you have NOT specified as a consumable stream for this application.  StreamName: {StreamName}.");
        if (!IsConsumerGroup)
            throw new
                ApplicationException($"Tried to read a stream as a group, but you have not specified Group Consumption on this stream. StreamName: {StreamName}.");


        //string streamPosition = readPendingMessages == false ? STREAM_POSITION_CG_NEW_MESSAGES : STREAM_POSITION_CG_HISTORY_START;
        StreamAutoClaimResult claimedMessages = await _redisClient.Db0.Database.StreamAutoClaimAsync(StreamName, ApplicationName, ApplicationId,
                                                                                                     AutoClaimPendingMessagesThreshold.Milliseconds, "0-0",
                                                                                                     numberOfMessagesToClaim);
        return claimedMessages.ClaimedEntries;
    }



    /// <summary>
    /// Retrieves pending message information
    /// </summary>
    /// <returns></returns>
    public async Task<StreamPendingMessageInfo[]> GetPendingMessageInfo()
    {
        return await _redisClient.Db0.Database.StreamPendingMessagesAsync(StreamName, ApplicationName, 10, ApplicationId);
    }



    /// <summary>
    /// Returns the total number of messages pending for all the consumers of the Application group.
    /// </summary>
    /// <returns></returns>
    public async Task<long> GetApplicationPendingMessageCount()
    {
        StreamGroupInfo[] groups          = await GetApplicationInfo();
        long              pendingMessages = 0;
        foreach (StreamGroupInfo group in groups)
        {
            if (group.Name == ApplicationName)
                pendingMessages += group.PendingMessageCount;
        }

        return pendingMessages;
    }



    /// <summary>
    /// Retries info about the stream
    /// </summary>
    /// <returns></returns>
    public async Task<StreamInfo> GetStreamInfo()
    {
        StreamInfo info = await _redisClient.Db0.Database.StreamInfoAsync(StreamName);
        return info;
    }


    /// <summary>
    /// Gets information about the groups in the stream
    /// </summary>
    /// <returns></returns>
    public async Task<StreamGroupInfo[]> GetApplicationInfo() { return await _redisClient.Db0.Database.StreamGroupInfoAsync(StreamName); }



    /// <summary>
    /// Returns information about the consumers in group.
    /// </summary>
    /// <returns></returns>
    public async Task<StreamConsumerInfo[]> GetConsumerInfo()
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
            else
                throw;
        }
    }


    /// <summary>
    /// Returns vital statistics and information about the stream
    /// </summary>
    /// <returns></returns>
    public async Task<SLRStreamVitals> GetStreamVitals()
    {
        SLRStreamVitals streamVitals = await SLRStreamVitals.GetStreamVitals(this);
        return streamVitals;
    }



    /// <summary>
    /// Removes aproximately all messages with an id before the passed in value.  Redis will determine up to what ID less than requested ID will be deleted for optimization and speed reasons
    /// <para>Thus, there may still exist some messages older than the messageIDToRemoveBefore value still present in DB</para>
    /// </summary>
    /// <param name="messageIDToRemoveBefore"></param>
    /// <returns></returns>
    public async Task<int> RemoveFullyProcessedEntries(RedisValue messageIDToRemoveBefore)
    {
        List<string> xGroupArgs = new();
        xGroupArgs.Add(StreamName);
        xGroupArgs.Add("MINID");
        xGroupArgs.Add("~");
        xGroupArgs.Add(messageIDToRemoveBefore);
        string      cmd    = $"XTRIM";
        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        return (int)result;
    }



    /// <summary>
    /// Returns the size of the current stream.
    /// </summary>
    /// <returns>ByteSize value</returns>
    public async Task<ByteSize> GetSize()
    {
        List<string> xGroupArgs = new();
        xGroupArgs.Add("USAGE");
        xGroupArgs.Add(StreamName);
        string      cmd    = $"MEMORY";
        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        ByteSize x = ByteSize.FromBytes((double)result);
        return x;
    }



    /// <summary>
    /// Deletes the given message ID's.
    /// <para>This should be used with caution with ConsumerGroups</para>
    /// </summary>
    /// <param name="messageId"></param>
    /// <returns></returns>
    public async Task<long> DeleteMessages(RedisValue[] messageId)
    {
        if (messageId.Length == 0)
            return 0;

        List<string> xGroupArgs = new();
        xGroupArgs.Add(StreamName);
        string cmd = $"XDEL";
        foreach (RedisValue messageID in messageId)
        {
            xGroupArgs.Add(messageID);
        }

        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        //long deleted = await _redisClient.Db0.Database.StreamDeleteAsync(StreamName, messageId);
        return (long)result;
    }


    /// <summary>
    /// This should really never be called in a production app.  Resets all statistic counters to 0.
    /// </summary>
    public void ResetStatistics()
    {
        StatisticFlushedMessageCalls = 0;
        StatisticMessagesReceived    = 0;
        StatisticMessagesConsumed    = 0;
        StatisticMessagesSent        = 0;
        StatisticMessagesReceived    = 0;
        LastMessageId                = STREAM_POSITION_BEGINNING;
    }
}
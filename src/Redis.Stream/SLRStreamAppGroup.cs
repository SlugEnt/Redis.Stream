using Microsoft.Extensions.Logging;
using SlugEnt.SLRStreamProcessing;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.Core.Implementations;

namespace SLRStreamProcessing;

public class SLRStreamAppGroup : SLRStreamBase
{
    public const string STREAM_POSITION_CG_HISTORY_START = "0";
    public const string STREAM_POSITION_CG_NEW_MESSAGES  = ">";


    private List<RedisValue> _pendingAcknowledgements;


    public SLRStreamAppGroup(ILogger<SLRStream> logger, IServiceProvider serviceProvider) : base(logger, serviceProvider) { }



    /// <summary>
    ///     Acknowledges the given message.  Immediately sends the acknowlegement to the redis server.  Bypasses the pending
    ///     acknowledgement process at the expense of more roundtrip calls to the Redis Server
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task AcknowledgeMessage(SLRMessage message)
    {
        await _redisClient.Db0.Database.StreamAcknowledgeAsync(StreamName, ApplicationName, message.Id);
    }



    /// <summary>
    ///     Sends an acknowledgement of the messages to the Redis server
    /// </summary>
    /// <returns></returns>
    protected async Task<long> AcknowledgePendingMessages(RedisValue[] acks)
    {
        long count = await _redisClient.Db0.Database.StreamAcknowledgeAsync(StreamName, ApplicationName, acks);
        return count;
    }



    /// <summary>
    ///     Adds an acknowledgement to the pending list.  If maximum allowed pending messages is reached it performs an
    ///     auto-flush.
    /// </summary>
    /// <param name="message"></param>
    /// <returns></returns>
    public async Task AddPendingAcknowledgementAsync(StreamEntry message)
    {
        _pendingAcknowledgements.Add(message.Id);

        if (_pendingAcknowledgements.Count >= MaxPendingMessageAcknowledgements)
            await FlushPendingAcknowledgementsAsync();
    }



    /// <summary>
    ///     Claims and Retrieves messages currently claimed by other consumers in the group if they have remained
    ///     unacknowledged past the AutoClaimPendingMessagesThreshold.
    ///     <para>NOTE:  You must acknowledge these messages manually.  There is NO AUTO ACKNOWLEDGEMENT.</para>
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
    ///     Removes this instances consumer ID from the consumer ID List
    /// </summary>
    /// <returns></returns>
    public async Task CloseStream()
    {
        await DeleteConsumer(ApplicationId);
        await base.CloseStream();
    }



    /// <summary>
    ///     Deletes the given consumer ID.  VERY IMPORTANT:  This will fail if it has any pending messages.  Set Force to True
    ///     to force the consumer to be deleted.
    ///     Any Pending Messages will be lost forever.
    ///     <para>
    ///         It is probably best to let the deleteConsumer fail and then allow another instance or a restart to then
    ///         consume those messages before deleting the consumer.
    ///     </para>
    /// </summary>
    /// <param name="consumerID">The consumer ID to delete</param>
    /// <param name="forceDeletion">Use very cautiously.  If the consumer has pending messages they will be lost forever.</param>
    /// <returns></returns>
    public async Task<bool> DeleteConsumer(int consumerID, bool forceDeletion = false)
    {
        if (!IsConsumerGroup)
            return true;

        int pendingCount = 0;

        // Need to see if the consumer has any pending messages. 
        if (!forceDeletion)
        {
            StreamConsumerInfo[] consumers = await GetConsumers();
            string               appId     = consumerID.ToString();
            foreach (StreamConsumerInfo streamConsumerInfo in consumers)
            {
                if (streamConsumerInfo.Name == appId)
                {
                    pendingCount = streamConsumerInfo.PendingMessageCount;
                    break;
                }
            }

            if (pendingCount > 0)
                return false;
        }


        List<string> xGroupArgs = new();
        xGroupArgs.Add("DELCONSUMER");
        xGroupArgs.Add(StreamName);
        xGroupArgs.Add(ApplicationName);
        xGroupArgs.Add(consumerID.ToString());
        string cmd = "XGROUP";

        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());

        return true;
    }


    /// <summary>
    ///     Sends acknowledgements to the Redis server and resets the pending array.
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
    ///     Returns the total number of messages pending for all the consumers of the Application group.
    ///     <para>
    ///         Note:  This will return the total pending count.  It does not take into consideration message age or how long
    ///         it has been pending.
    ///         If the message was delivered to a consumer it is included in this count.
    ///     </para>
    /// </summary>
    /// <returns>
    ///     Total number of messages pending for this Application across all consumers that are a part of this
    ///     application.
    /// </returns>
    public async Task<long> GetApplicationPendingMessageCount()
    {
        StreamGroupInfo[] groups          = await GetStreamApplications();
        long              pendingMessages = 0;
        foreach (StreamGroupInfo group in groups)
        {
            if (group.Name == ApplicationName)
                pendingMessages += group.PendingMessageCount;
        }

        return pendingMessages;
    }


    public async Task<StreamConsumerInfo[]> GetConsumers() => await base.GetConsumers();



    /// <summary>
    ///     Retrieves pending message information
    /// </summary>
    /// <returns></returns>
    public async Task<StreamPendingMessageInfo[]> GetPendingMessageInfo() =>
        await _redisClient.Db0.Database.StreamPendingMessagesAsync(StreamName, ApplicationName, 10, ApplicationId);



    /// <summary>
    ///     Returns the applications that have or are consumers on the stream
    /// </summary>
    /// <returns></returns>
    public async Task<StreamGroupInfo[]> GetStreamApplications() => await base.GetStreamApplications();



    /// <summary>
    ///     Reads messages for a consumer group.
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve">Number of messages to retrieve</param>
    /// <param name="readPendingMessages">
    ///     If true, it should read from the pending "queue" instead of the new queue.  Pending
    ///     are messages already read once by this consumer, but never acknowledged.
    /// </param>
    /// <returns></returns>
    /// <exception cref="ApplicationException"></exception>
    public async Task<StreamEntry[]> ReadStreamGroupAsync(int numberOfMessagesToRetrieve = 6) //(, bool readPendingMessages = false))
    {
        if (!IsInitialized)
            throw new ApplicationException($"You must Initialize a SLRStream object before you can use it.  Stream Name: {StreamName}");
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
    ///     If there are any messages pending by other instances of this application that are older than
    ///     AutoClaimPendingMessagesThreshold then this method will
    ///     claim up to numberOfMessagesToRetrieve messages and return them to the caller.
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve">Maximum number of messages to attempt to claim</param>
    /// <returns>
    ///     StreamAutoClaimResult - Which will contain 3 values.  The NextStartId if it is anything other than 0-0 then
    ///     there are more messages to claim.
    /// </returns>
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
    ///     This will remove all messages that have been fully processed by all applications.   Note:  If the application is
    ///     removed from the stream for some reason, we would have no idea, and could
    ///     remove messages that it did not process if it becomes available again...  You must ensure your applications do not
    ///     remove the application from a stream for any reason if you are going to use this functionality.
    /// </summary>
    /// <returns></returns>
    public async Task<int> RemoveMessagesFullyProcessed(bool optimized = true)
    {
        SLRStreamVitals vitals = await GetStreamVitals();

        int removedQty = await RemoveMessages(vitals.FirstFullyUnprocessedMessageID, optimized);
        return removedQty;
    }


    public void ResetStatistics()
    {
        base.ResetStatistics();
        StatisticFlushedMessageCalls = 0;
    }



    /// <summary>
    ///     Saves this applications consumer ID to Redis
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
        string      cmd    = "XGROUP";
        RedisResult result = await _redisClient.Db0.Database.ExecuteAsync(cmd, xGroupArgs.ToArray());
        if ((int)result != 1)
            throw new
                ApplicationException($"Unable to create consumer group:  Stream: {StreamName}, ConsumerGroup: {ApplicationName},  ConsumerName: {ApplicationId} ");
    }


    /// <summary>
    ///     Sets a unique consumer ID for this consumer.  It then creates the consumer ID in Redis and assigns it to the
    ///     application consumer group.  If it fails at any point it returns False
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

            for (int i = 1; i < 1000; i++)
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
    ///     Since we create via DependencyInjection we need a way of setting the stream properties after creation
    /// </summary>
    /// <param name="streamConfig">The configuration object for the stream</param>
    /// <param name="multiplexer">If not null, it overrides any multiplexer set in the config</param>
    internal async Task SetStreamConfig(SLRStreamConfigAppGroup streamConfig, RedisConnectionPoolManager redisConnectionPoolManager,
                                        RedisConfiguration redisConfiguration)
    {
        try
        {
            await base.SetStreamConfig(streamConfig, redisConnectionPoolManager, redisConfiguration);
            MaxPendingMessageAcknowledgements = streamConfig.MaxPendingAcknowledgements;
            AutoClaimPendingMessagesThreshold = streamConfig.ClaimMessagesOlderThan;
            AutoAcknowledgeMessagesOnDelivery = streamConfig.AcknowledgeOnDelivery;

            _pendingAcknowledgements = new List<RedisValue>();

            // Create the Consumer Group if that type of Stream
            if (IsConsumerGroup)
            {
                if (!StreamVitals.ApplicationExistsOnStream())
                    if (!await TryCreateConsumerGroup())
                        throw new
                            ApplicationException($"Failed to create the Consumer Group:  StreamName: {StreamName}   ApplicationName: {ApplicationName}");

                // Create the actual Consumer ID
                if (!await SetConsumerApplicationId())
                    throw new
                        ApplicationException($"Failed to create the Consumer Application:  StreamName: {StreamName}   ApplicationConsumerName: {ApplicationFullName}");
            }
        }
        catch (Exception ex)
        {
            _logger.LogError($"Error In StreamConfig.  {ex.Message}", ex);
        }
    }


    /// <summary>
    ///     Attempts to create the Consumer Group.  Returns True if the group was created / exists.
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
    ///     If true, messages are acknowledged as soon as Redis delivers them to us.
    /// </summary>
    public bool AutoAcknowledgeMessagesOnDelivery { get; set; }



    /// <summary>
    ///     The maximum number of pending message acknowledgements, before an acknowledgement is automatically sent to Redis.
    /// </summary>
    public int MaxPendingMessageAcknowledgements { get; set; }



    /// <summary>
    ///     The amount of time to allow pending messages from other consumers to be in an unacknowledged state, before
    ///     attempting to claim and process them.
    /// </summary>
    public TimeSpan AutoClaimPendingMessagesThreshold { get; set; }


    /// <summary>
    ///     Returns the number of acknowledgements that are waiting to be sent to the Redis Server
    /// </summary>
    public int StatisticPendingAcknowledgements => _pendingAcknowledgements.Count;


    /// <summary>
    ///     The number of times the FlushPendingMessages method was called.
    /// </summary>
    public ulong StatisticFlushedMessageCalls { get; protected set; }

#endregion
}
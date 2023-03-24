using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Redis.Stream;

public class RedisStream
{
    private ILogger<RedisStream>  _logger;
    private ConnectionMultiplexer _redisMultiplexer;
    private IDatabase             _db;


    public RedisStream(ILogger<RedisStream> logger, IServiceProvider serviceProvider) { _logger = logger; }



    /// <summary>
    /// Since we create via DependencyInjection we need a way of setting the stream properties after creation
    /// </summary>
    /// <param name="streamName"></param>
    /// <param name="applicationName"></param>
    internal async Task SetStreamValues(string streamName, string applicationName, EnumRedisStreamTypes streamType, ConnectionMultiplexer multiplexer)
    {
        //  TODO Add a StreamConfig object so set some of these variables, especially starting offset??
        StreamName        = streamName;
        ApplicationName   = applicationName;
        _redisMultiplexer = multiplexer;
        _db               = _redisMultiplexer.GetDatabase();
        StreamType        = streamType;

        switch (streamType)
        {
            case EnumRedisStreamTypes.ProducerOnly:
                CanProduceMessages = true;
                break;

            case EnumRedisStreamTypes.SimpleConsumerOnly:
                CanConsumeMessages = true;
                break;

            case EnumRedisStreamTypes.ConsumerGroupOnly:
                CanConsumeMessages = true;
                IsConsumerGroup    = true;
                break;

            case EnumRedisStreamTypes.ProducerAndConsumerGroup:
                CanProduceMessages = true;
                CanConsumeMessages = true;
                IsConsumerGroup    = true;
                break;

            case EnumRedisStreamTypes.ProducerAndSimpleConsumer:
                CanProduceMessages = true;
                CanConsumeMessages = true;
                break;
        }

        try
        {
            // Create the Consumer Group if that type of Stream
            if (IsConsumerGroup)
            {
                bool success = await TryCreateConsumerGroup();
                await SetConsumerApplicationId();
                await SaveConsumerId();

                // Need to determine the ID of this instance of the consumer group - so query redis and get the list of current consumers.
                StreamConsumerInfo[] consumerInfo = await _db.StreamConsumerInfoAsync(streamName, applicationName);
                if (consumerInfo.Length == 0) { }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message, ex);
            throw;
        }

        IsInitialized = true;
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
        RedisResult result = await _db.ExecuteAsync(cmd, xGroupArgs.ToArray());
        if ((int)result != 1)
            throw new
                ApplicationException($"Unable to create consumer group:  Stream: {StreamName}, ConsumerGroup: {ApplicationName},  ConsumerName: {ApplicationId} ");
    }


    /// <summary>
    /// Sets a unique consumer ID for this consumer
    /// </summary>
    /// <returns></returns>
    protected async Task SetConsumerApplicationId()
    {
        StreamConsumerInfo[] consumerInfo = await _db.StreamConsumerInfoAsync(StreamName, ApplicationName);
        if (consumerInfo.Length == 0)
            ApplicationId = 1;
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
                    return;
                }
            }
        }

        return;
    }


    /// <summary>
    /// Attempts to create the Consumer Group.  Returns True if the group was created / exists.
    /// </summary>
    /// <returns></returns>
    protected async Task<bool> TryCreateConsumerGroup()
    {
        try
        {
            // TODO Change the Zero to be a parameter?
            await _db.StreamCreateConsumerGroupAsync(StreamName, ApplicationName, "0-0");
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
    public EnumRedisStreamTypes StreamType { get; protected set; }

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
    /// Application Name + Application Id
    /// </summary>
    public string ApplicationFullName
    {
        get { return ApplicationName + "." + ApplicationId; }
    }


    /// <summary>
    /// Number of Messages we have received.
    /// </summary>
    public long StatisticMessagesReceived { get; protected set; }

    public long StatisticMessagesConsumed { get; protected set; }

#endregion


    /// <summary>
    /// Permanently removes the stream and all message. Use Caution!
    /// </summary>
    public void DeleteStream() { _db.KeyDelete(StreamName); }


    /// <summary>
    /// Sends the given message to the stream
    /// </summary>
    /// <param name="message"></param>
    public void SendMessage(RedisMessage message)
    {
        if (!CanProduceMessages)
            throw new
                ApplicationException("Attempted to send a message to a stream that you have NOT specified as a stream you can produce messages for with this application");

        NameValueEntry[] values = message.GetNameValueEntries();
        _db.StreamAdd(StreamName, values);
    }



    /// <summary>
    /// Reads upto max messages from the stream
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve"></param>
    /// <returns></returns>
    public async Task<StreamEntry[]> ReadStreamAsync(int numberOfMessagesToRetrieve = 3)
    {
        if (!CanConsumeMessages)
            throw new ApplicationException("Attempted to read messages on a stream that you have NOT specified as a consumable stream for this application");

        StreamPosition                    streamPosition    = new(StreamName, "0-0");
        StreamPosition[]                  streamsToRetrieve = new StreamPosition[] { streamPosition };
        StackExchange.Redis.RedisStream[] streams           = await _db.StreamReadAsync(streamsToRetrieve, numberOfMessagesToRetrieve);

        //                _db.StreamRead(new StreamPosition[] { new StreamPosition(StreamName, "0-0"), new StreamPosition("B", "0-0"), new StreamPosition("C", "0-0") }, 3);

        StackExchange.Redis.RedisStream stream = streams[0];

        Console.WriteLine($"Stream: {stream.Key} | ");
        StatisticMessagesReceived += stream.Entries.Length;
        return stream.Entries;
    }


    public void CreateConsumerGroup()
    {
        if (!_db.StreamCreateConsumerGroup(StreamName, ApplicationName, 0)) { }
    }
}
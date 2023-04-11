using ByteSizeLib;
using SLRStreamProcessing;
using StackExchange.Redis;

namespace SlugEnt.SLRStreamProcessing;

/// <summary>
///     Returns important information about the stream.
/// </summary>
public class SLRStreamVitals
{
    protected SLRStreamBase _stream;


    protected SLRStreamVitals(SLRStreamBase stream) { _stream = stream; }


    /// <summary>
    ///     Returns the Application (Group) information for this application on this stream
    /// </summary>
    public StreamGroupInfo ApplicationInfo { get; internal set; }


    /// <summary>
    ///     Returns all known consumer groups for the stream
    /// </summary>
    public StreamGroupInfo[] ApplicationsOnStream { get; protected set; }


    /// <summary>
    ///     Returns information about this particular consumer for this application on this stream
    /// </summary>
    public StreamConsumerInfo[] ConsumerInfo { get; protected set; }



    /// <summary>
    ///     This is the date time of the first message in the stream that has not been processed by ALL consumer groups yet.
    ///     It may have been processed by some consumer groups, but at least 1 consumer group still has not processed messages
    ///     past this time.
    /// </summary>
    public DateTime FirstFullyUnprocessedMessageDateTime { get; protected set; }

    /// <summary>
    ///     This is the first message in the stream that has 1 or more consumers that still have not processed this message.
    ///     Note, this is not an actual Message ID.  It might or might
    ///     not exist in the Redis Stream.  What it is, is
    /// </summary>
    public RedisValue FirstFullyUnprocessedMessageID { get; protected set; }


    /// <summary>
    ///     When this object was last updated
    /// </summary>
    public DateTime LastUpdated { get; protected set; }


    /// <summary>
    ///     Total number of messages in stream.
    /// </summary>
    public long MessageCount { get; protected set; }

    public DateTime OldestMessageDateTime { get; protected set; }


    public RedisValue OldestMessageId { get; protected set; }

    /// <summary>
    ///     Returns the Size of the stream in Bytes.
    /// </summary>
    public ByteSize SizeInBytes { get; protected set; }


    /// <summary>
    ///     The number of Application Groups that are setup to read from the stream
    /// </summary>
    public int Statistic_NumberOfApplicationGroups => ApplicationsOnStream.Length;


    /// <summary>
    ///     True if the stream exists on the Redis server
    /// </summary>
    public bool StreamExists { get; protected set; }


    /// <summary>
    ///     The StreamInfo information about the stream
    /// </summary>
    public StreamInfo StreamInfo { get; protected set; }



    /// <summary>
    ///     Returns True if the application already exists as a Consumer group on the stream
    /// </summary>
    /// <param name="applicationName"></param>
    /// <returns></returns>
    public bool ApplicationExistsOnStream(string applicationName = "")
    {
        if (applicationName == string.Empty)
            applicationName = _stream.ApplicationName;

        foreach (StreamGroupInfo streamGroupInfo in ApplicationsOnStream)
        {
            if (applicationName == streamGroupInfo.Name)
                return true;
        }

        return false;
    }



    /// <summary>
    ///     Calculates statistics based upon the read in information
    ///     <para>Specifically, figure out what the oldest message that has not been processed by all streams yet is.</para>
    /// </summary>
    protected void CalculateStats()
    {
        RedisValue lastDeliveredId;
        long       lastDeliveredUnixTime = DateTimeOffset.MaxValue.ToUnixTimeMilliseconds();
        long       lastDeliveredSequence = long.MaxValue;

        // This is the greatest possible message id in the system that is possible.
        string lastDeliveredMessage = lastDeliveredUnixTime + "-" + lastDeliveredUnixTime;

        foreach (StreamGroupInfo gInfo in ApplicationsOnStream)
        {
            // Figure out the LastDelivered Message Time and sequence number
            (long unixTime, long sequence) = SLRMessage.GetMessageIdAndSequence(gInfo.LastDeliveredId);

            // Store if this message ID is older than the current one.
            if (unixTime < lastDeliveredUnixTime)
            {
                lastDeliveredUnixTime = unixTime;
                lastDeliveredSequence = sequence;
            }
            else if (unixTime == lastDeliveredUnixTime)
            {
                if (sequence < lastDeliveredSequence)
                    lastDeliveredSequence = sequence;
            }
        }


        // There are no messages in the stream
        if (StreamInfo.FirstEntry.IsNull)
        {
            DateTime current = DateTime.Now;
            OldestMessageId                      = SLRStreamBase.STREAM_POSITION_BEGINNING;
            OldestMessageDateTime                = current;
            FirstFullyUnprocessedMessageDateTime = current;
            FirstFullyUnprocessedMessageID       = SLRStreamBase.STREAM_POSITION_BEGINNING;
            LastUpdated                          = current;
            return;
        }


        //Get oldest message ID and time
        OldestMessageId   = StreamInfo.FirstEntry.Id;
        (long oldTime, _) = SLRMessage.GetMessageIdAndSequence(OldestMessageId);
        DateTimeOffset oldestDateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(oldTime);
        OldestMessageDateTime = new DateTime(oldestDateTimeOffset.Ticks, DateTimeKind.Utc);


        // Determine first Fully unprocessed message
        DateTimeOffset lastDateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(lastDeliveredUnixTime);
        FirstFullyUnprocessedMessageDateTime = new DateTime(lastDateTimeOffset.Ticks, DateTimeKind.Utc);
        FirstFullyUnprocessedMessageID       = lastDeliveredUnixTime + "-" + lastDeliveredSequence;
        LastUpdated                          = DateTime.Now;
    }



    /// <summary>
    ///     Creates a fully populated StreamVitals object with information about the stream, groups and consumer (if the stream
    ///     provided is able to consume messages)
    /// </summary>
    /// <param name="theStream"></param>
    /// <returns></returns>
    public static async Task<SLRStreamVitals> GetStreamVitals(SLRStreamBase theStream)
    {
        SLRStreamVitals vitals = new(theStream);

        try
        {
            // Retrieve the individual pieces of info from Redis
            vitals.StreamInfo           = await theStream.GetStreamInfo();
            vitals.StreamExists         = true;
            vitals.ApplicationsOnStream = await theStream.GetStreamApplications();
            foreach (StreamGroupInfo streamGroupInfo in vitals.ApplicationsOnStream)
            {
                if (streamGroupInfo.Name == theStream.ApplicationName)
                {
                    vitals.ApplicationInfo = streamGroupInfo;
                    break;
                }
            }


            if (theStream.CanConsumeMessages)
                vitals.ConsumerInfo = await theStream.GetConsumers();
            else
                vitals.ConsumerInfo = null;

            // Get the size in bytes
            vitals.SizeInBytes = await theStream.GetSize();


            vitals.MessageCount = vitals.StreamInfo.Length;

            vitals.CalculateStats();
        }
        catch (RedisServerException rse)
        {
            if (rse.Message.Contains("ERR no such key"))
                return vitals;

            throw rse;
        }
        catch (Exception ex)
        {
            throw ex;
        }

        return vitals;
    }
}
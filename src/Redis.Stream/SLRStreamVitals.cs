﻿using StackExchange.Redis;

namespace Redis.Stream;

public class SLRStreamVitals
{
    protected SLRStream _stream;


    protected SLRStreamVitals(SLRStream stream) { _stream = stream; }


    public StreamInfo StreamInfo { get; protected set; }

    public StreamGroupInfo[] ApplicationInfo { get; protected set; }

    public StreamConsumerInfo[] ConsumerInfo { get; protected set; }


    /// <summary>
    /// When this object was last updated
    /// </summary>
    public DateTime LastUpdated { get; protected set; }


    public RedisValue OldestMessageId { get; protected set; }

    public DateTime OldestMessageDateTime { get; protected set; }

    /// <summary>
    /// This is the first message in the stream that has 1 or more consumers that still have not processed this message.
    /// </summary>
    public RedisValue FirstFullyUnprocessedMessageID { get; protected set; }


    /// <summary>
    /// The number of Application Groups that are setup to read from the stream
    /// </summary>
    public int Statistic_NumberOfApplicationGroups
    {
        get { return ApplicationInfo.Length; }
    }



    /// <summary>
    /// This is the date time of the first message in the stream that has not been processed by ALL consumer groups yet.
    /// It may have been processed by some consumer groups, but at least 1 consumer group still has not processed messages past this time.
    /// </summary>
    public DateTime FirstFullyUnprocessedMessageDateTime { get; protected set; }



    /// <summary>
    /// Creates a fully populated StreamVitals object with information about the stream, groups and consumer (if the stream provided is able to consume messages)
    /// </summary>
    /// <param name="theStream"></param>
    /// <returns></returns>
    public static async Task<SLRStreamVitals> GetStreamVitals(SLRStream theStream)
    {
        SLRStreamVitals vitals = new SLRStreamVitals(theStream);

        // Retrieve the individuals pieces of info from Redis
        vitals.StreamInfo      = await theStream.GetStreamInfo();
        vitals.ApplicationInfo = await theStream.GetApplicationInfo();

        if (theStream.CanConsumeMessages)
            vitals.ConsumerInfo = await theStream.GetConsumerInfo();
        else
            vitals.ConsumerInfo = null;


        vitals.CalculateStats();

        return vitals;
    }



    /// <summary>
    /// Calculates statistics based upon the read in information
    /// <para>Specifically, figure out what the oldest message that has not been processed by all streams yet is.</para>
    /// </summary>
    protected void CalculateStats()
    {
        RedisValue lastDeliveredId;
        long       lastDeliveredUnixTime = long.MaxValue;
        long       lastDeliveredSequence = long.MaxValue;

        // This is the greatest possible message id in the system that is possible.
        string lastDeliveredMessage = lastDeliveredUnixTime + "-" + lastDeliveredUnixTime;

        foreach (StreamGroupInfo gInfo in ApplicationInfo)
        {
            // Figure out the LastDelivered Message Time and sequence number
            (long unixTime, long sequence) = GetMessageIdAndSequence(gInfo.LastDeliveredId);

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

        //Get oldest message ID and time
        OldestMessageId   = StreamInfo.FirstEntry.Id;
        (long oldTime, _) = GetMessageIdAndSequence(OldestMessageId);
        DateTimeOffset oldestDateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(oldTime);
        OldestMessageDateTime = new DateTime(oldestDateTimeOffset.Ticks, DateTimeKind.Utc);


        // Determine first Fully unprocessed message
        DateTimeOffset lastDateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(lastDeliveredUnixTime);
        FirstFullyUnprocessedMessageDateTime = new DateTime(lastDateTimeOffset.Ticks, DateTimeKind.Utc);
        FirstFullyUnprocessedMessageID       = lastDeliveredUnixTime + "-" + lastDeliveredSequence;
        LastUpdated                          = DateTime.Now;
    }


    public static (long id, long sequence) GetMessageIdAndSequence(string messageId)
    {
        int indexPtr = messageId.LastIndexOf("-");
        if (indexPtr == 0)
            throw new ArgumentException($"The parameter messageId is not a Redis Message ID. It needs to be in format #-#.  Value passed: {messageId}");

        ReadOnlySpan<char> idSpan  = messageId.AsSpan().Slice(start: 0, length: indexPtr);
        ReadOnlySpan<char> seqSpan = messageId.AsSpan().Slice(start: indexPtr + 1);
        if (!long.TryParse(seqSpan, out long seq))
            throw new
                ArgumentException($"The parameter messageId is not a Redis Message ID. It needs to be in format #-#.  Unable to parse the value after the dash to a number: {messageId}");
        if (!long.TryParse(idSpan, out long unixTime))
            throw new
                ArgumentException($"The parameter messageId is not a Redis Message ID. It needs to be in format #-#.  Unable to parse the value after the dash to a number: {messageId}");

        return (unixTime, seq);
    }
}
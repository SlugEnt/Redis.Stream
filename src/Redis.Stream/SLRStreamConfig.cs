using StackExchange.Redis;

namespace SlugEnt.SLRStreamProcessing;

/// <summary>
///     Provides the configuration for a normal producer / consumer stream
/// </summary>
public class SLRStreamConfig
{
    public string ApplicationName { get; set; }


    /// <summary>
    ///     Where should the stream start consuming messages from.
    /// </summary>
    public EnumSLRStreamStartingPoints StartingMessage { get; set; } = EnumSLRStreamStartingPoints.Now;


    /// <summary>
    ///     This only applies if the StartingMessage is set to SpecifiedValue
    /// </summary>
    public RedisValue StartingMessageId { get; set; }

    public string StreamName { get; set; }


    /// <summary>
    ///     The type of stream this is - what it can do:  Produce, Consume, Both, Consume via Group
    /// </summary>
    public EnumSLRStreamTypes StreamType { get; set; }
}
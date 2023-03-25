using StackExchange.Redis;

namespace Redis.Stream;

public class StreamConfig
{
    public StreamConfig() { }

    public string StreamName { get; set; }

    public string ApplicationName { get; set; }

    /// <summary>
    /// The maximum number of allowed Pending Acknowledgements.  Once this value is hit an automatic sending of acknowledgements to server occurs.
    /// </summary>
    public int MaxPendingAcknowledgements { get; set; } = 20;


    /// <summary>
    /// The type of stream this is - what it can do:  Produce, Consume, Both, Consume via Group
    /// </summary>
    public EnumRedisStreamTypes StreamType { get; set; }


    /// <summary>
    /// The Redis Connection Object
    /// </summary>
    public ConnectionMultiplexer Multiplexer { get; set; }
}
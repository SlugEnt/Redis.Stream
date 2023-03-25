﻿using StackExchange.Redis;

namespace Redis.Stream;

/// <summary>
/// Provides the configuration for a stream
/// </summary>
public class SLRStreamConfig
{
    public SLRStreamConfig() { }

    public string StreamName { get; set; }

    public string ApplicationName { get; set; }

    /// <summary>
    /// The maximum number of allowed Pending Acknowledgements.  Once this value is hit an automatic sending of acknowledgements to server occurs.
    /// </summary>
    public int MaxPendingAcknowledgements { get; set; } = 20;


    /// <summary>
    /// The type of stream this is - what it can do:  Produce, Consume, Both, Consume via Group
    /// </summary>
    public EnumSLRStreamTypes StreamType { get; set; }


    /// <summary>
    /// The Redis Connection Object.  This typically is set by the engine and does not need to be manually set.
    /// </summary>
    public ConnectionMultiplexer Multiplexer { get; set; }
}
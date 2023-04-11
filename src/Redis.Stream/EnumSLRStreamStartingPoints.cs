namespace SlugEnt.SLRStreamProcessing;

/// <summary>
///     Defines the possible starting points in the stream for an application when it 1st starts
/// </summary>
public enum EnumSLRStreamStartingPoints
{
    /// <summary>
    ///     Start with the very 1st message
    /// </summary>
    Beginning = 0,

    /// <summary>
    ///     Start at the specified point
    /// </summary>
    SpecifiedValue = 200,

    /// <summary>
    ///     Start at the next message after the last one consumed by the ConsumerGroup / ApplicationGroup
    /// </summary>
    LastConsumedForConsumerGroup = 210,

    /// <summary>
    ///     Start at the very last message in the stream at this very moment.
    /// </summary>
    Now = 254
}
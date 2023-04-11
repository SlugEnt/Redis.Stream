using SlugEnt.SLRStreamProcessing;

namespace SLRStreamProcessing;

/// <summary>
///     Provides the configuration for a stream that is part of an Application Consuming Group Stream
/// </summary>
public class SLRStreamConfigAppGroup : SLRStreamConfig
{
    /// <summary>
    ///     If true, the consumer group does not need to be acknowledged, so it is auto acknowledged as soon as read /
    ///     delivered to consumer.
    /// </summary>
    public bool AcknowledgeOnDelivery { get; set; }


    /// <summary>
    ///     The threshold for consuming a message from another consumer that has not acknowledged the message yet
    /// </summary>
    public TimeSpan ClaimMessagesOlderThan { get; set; } = TimeSpan.FromMinutes(1);

    /// <summary>
    ///     The maximum number of allowed Pending Acknowledgements.  Once this value is hit an automatic sending of
    ///     acknowledgements to server occurs.
    /// </summary>
    public int MaxPendingAcknowledgements { get; set; } = 20;
}
namespace SlugEnt.SLRStreamProcessing;

public enum EnumSLRStreamTypes
{
    /// <summary>
    ///     Can only produce messages
    /// </summary>
    ProducerOnly = 0,

    /// <summary>
    ///     Can produce messages and read messages, but does not belong to a consumer group
    /// </summary>
    ProducerAndSimpleConsumer = 20,

    /// <summary>
    ///     Can produce messages and read messages as part of a consumer group
    /// </summary>
    ProducerAndConsumerGroup = 40,

    /// <summary>
    ///     Can only read messages.  Does not belong to a consumer group
    /// </summary>
    SimpleConsumerOnly = 100,


    /// <summary>
    ///     Can only consume messages as part of a consumer group
    /// </summary>
    ConsumerGroupOnly = 110
}
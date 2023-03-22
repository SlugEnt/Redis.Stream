using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Redis.Stream.Sample;

public class Stats
{
    public Stats(string name)
    {
        Name = name;

//            ProducedMessages = new List<ConfirmationMessage>();
//            ReceivedMessages = new List<ConfirmationMessage>();
    }


    public string Name { get; set; }

    /// <summary>
    /// Number of messages we received as a consumer
    /// </summary>
    public ulong ConsumedMessages { get; set; }

    /// <summary>
    /// Number of messages we received producer confirms for
    /// </summary>
    public ulong SuccessMessages { get; set; }

    /// <summary>
    /// Number of messages we received producer failure confirms for
    /// </summary>
    public ulong FailureMessages { get; set; }

    /// <summary>
    /// Number of messages we created
    /// </summary>
    public ulong CreatedMessages { get; set; }

    /// <summary>
    /// If Circuit Breaker is currently tripped
    /// </summary>
    public bool CircuitBreakerTripped { get; set; }

//        public List<ConfirmationMessage> ProducedMessages { get; set; }

//        public List<ConfirmationMessage> ReceivedMessages { get; set; }


    // Consumption Stats
    public string ConsumeLastBatchReceived { get; set; }
    public ulong ConsumeLastCheckpoint { get; set; }
    public int ConsumeCurrentAwaitingCheckpoint { get; set; }
}
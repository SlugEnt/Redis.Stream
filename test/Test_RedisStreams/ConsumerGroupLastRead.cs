using System.Text;
using StackExchange.Redis;

namespace Test_RedisStreams;

/// <summary>
/// Used to test the LastMessageDelivered logic
/// </summary>
public class ConsumerGroupLastRead
{
    private bool[] _received = new bool[5];


    public ConsumerGroupLastRead(int index, RedisValue messageId, int totalConsumers)
    {
        Index         = index;
        MessageId     = messageId;
        ConsumerCount = totalConsumers;

        _received = new bool[ConsumerCount];
        for (int i = 0; i < ConsumerCount; i++)
            _received[i] = false;
    }


    public int GroupCount { get; set; }
    public int ConsumerCount { get; set; }

    public int Index { get; set; }

    public RedisValue MessageId { get; set; }


    public bool[] Received
    {
        get { return _received; }
    }


    public void SetTrue(int index) { _received[index] = true; }

    public bool GetStatus(int index) { return _received[index]; }


    public string ToString()
    {
        StringBuilder sb = new StringBuilder();
        sb.Append(MessageId);
        foreach (bool b in _received)
        {
            sb.Append(" |  " + b);
        }

        return sb.ToString();
    }
}
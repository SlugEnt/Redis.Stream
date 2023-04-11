using System.Text;
using System.Text.Json;
using StackExchange.Redis;

namespace SlugEnt.SLRStreamProcessing;

public class SLRMessage
{
    private static readonly string MSG_TYPE_OBJECT = ".DO";
    private static readonly string MSG_TYPE_TEXT   = ".DT";

    private readonly Dictionary<RedisValue, RedisValue> _properties = new();



    /// <summary>
    ///     Constructor when creating message to send to Redis Stream.  This will be considered a Generic message as it
    ///     contains no default text or object based content
    /// </summary>
    public SLRMessage() { }


    /// <summary>
    ///     Constructor when creating message read from Redis Stream
    /// </summary>
    /// <param name="id"></param>
    public SLRMessage(StreamEntry streamEntry) : this()
    {
        Id             = streamEntry.Id;
        RawStreamEntry = streamEntry;

        for (int i = 0; i < streamEntry.Values.Length; i++)
            _properties.Add(streamEntry.Values[i].Name, streamEntry.Values[i].Value);
    }


    internal string Data { get; set; } = "";


    /// <summary>
    ///     The unique ID that identifies this message
    /// </summary>
    public RedisValue Id { get; protected set; }



    /// <summary>
    ///     If the message is neither Text or Object Based, then it is a Generic message
    /// </summary>
    /// <returns></returns>
    public bool IsGenericMessage => !IsTextualMessage && !IsObjectEncodedMessage;



    /// <summary>
    ///     Returns true if the message contains an object encoded into it.  False if text based message
    /// </summary>
    /// <returns></returns>
    public bool IsObjectEncodedMessage => _properties.ContainsKey(MSG_TYPE_OBJECT);



    /// <summary>
    ///     Returns true if the message contains a text message. False if it is an object based message
    /// </summary>
    /// <returns></returns>
    public bool IsTextualMessage => _properties.ContainsKey(MSG_TYPE_TEXT);



    public IReadOnlyDictionary<RedisValue, RedisValue> Properties => _properties;

    internal StreamEntry RawStreamEntry { get; set; }



    /// <summary>
    ///     Adds the given RedisValue type (int, string, bool, etc) property to the Application Property Dictionary
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of the property</param>
    /// <param name="propertyValue">Value of the property (as string)</param>
    public void AddProperty(string propertyName, RedisValue propertyValue) { _properties.Add(propertyName, propertyValue); }



    /// <summary>
    ///     Adds the given object as a property to the Application Property Dictionary
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of the property</param>
    /// <param name="value">Value Object</param>
    public void AddPropertyObject<T>(string propertyName, T value) where T : class
    {
        string json = JsonSerializer.Serialize(value);
        _properties.Add(propertyName, json);
    }



    /// <summary>
    ///     Creates a message from the given text.  The text is stored in the Data property.
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    public static SLRMessage CreateMessage(string text)
    {
        SLRMessage message = new();
        message.AddProperty(MSG_TYPE_TEXT, text);
        return message;
    }


    /// <summary>
    ///     Creates a message with the given object.  The object data is serialized into the data property.
    /// </summary>
    /// <typeparam name="T">Any Nullable object type</typeparam>
    /// <param name="value">An instance of T</param>
    /// <returns></returns>
    public static SLRMessage CreateMessage<T>(T value)
    {
        SLRMessage message = new();
        string     json    = JsonSerializer.Serialize(value);
        message.AddProperty(MSG_TYPE_OBJECT, json);
        return message;
    }



#region "Static Methods"

    /// <summary>
    ///     Takes a message Id and decrypts it into its message-Id and Sequence number
    /// </summary>
    /// <param name="messageId"></param>
    /// <returns>
    ///     The Unix Time part of the message ID which is a long Unix time in milliseconds.  And the Sequence number which
    ///     is an incrementing number for each message produced during that millisecond time.
    /// </returns>
    /// <exception cref="ArgumentException"></exception>
    public static (long id, long sequence) GetMessageIdAndSequence(string messageId)
    {
        int indexPtr = messageId.LastIndexOf("-");
        if (indexPtr == 0)
            throw new ArgumentException($"The parameter messageId is not a Redis Message ID. It needs to be in format #-#.  Value passed: {messageId}");

        ReadOnlySpan<char> idSpan  = messageId.AsSpan().Slice(0, indexPtr);
        ReadOnlySpan<char> seqSpan = messageId.AsSpan().Slice(indexPtr + 1);
        if (!long.TryParse(seqSpan, out long seq))
            throw new
                ArgumentException($"The parameter messageId is not a Redis Message ID. It needs to be in format #-#.  Unable to parse the value after the dash to a number: {messageId}");
        if (!long.TryParse(idSpan, out long unixTime))
            throw new
                ArgumentException($"The parameter messageId is not a Redis Message ID. It needs to be in format #-#.  Unable to parse the value after the dash to a number: {messageId}");

        return (unixTime, seq);
    }

#endregion



    /// <summary>
    ///     Will return the primary Data object that was encoded into the message if there was one.  Otherwise returns default
    ///     for T
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public T GetMessageObject<T>()
    {
        if (!_properties.TryGetValue(MSG_TYPE_OBJECT, out RedisValue dataObject))
            return default;

        T value = JsonSerializer.Deserialize<T>(dataObject);
        return value;
    }



    /// <summary>
    ///     Retrieves the message text if it was a text encoded message.
    /// </summary>
    /// <returns></returns>
    public string GetMessageText()
    {
        if (!_properties.TryGetValue(MSG_TYPE_TEXT, out RedisValue dataObject))
            return string.Empty;

        return dataObject.ToString();
    }



    public NameValueEntry[] GetNameValueEntries()
    {
        NameValueEntry[] entries = new NameValueEntry[_properties.Count];
        int              i       = 0;
        foreach (KeyValuePair<RedisValue, RedisValue> keyValuePair in _properties)
            entries[i++] = new NameValueEntry(keyValuePair.Key, keyValuePair.Value);

        return entries;
    }



    public RedisValue GetProperty(string propertyName)
    {
        if (!_properties.TryGetValue(propertyName, out RedisValue redisValue))
            return default;

        return redisValue;
    }



    /// <summary>
    ///     Attempts to retrieve the Application Property with the given name from the dictionary.  Returns String.Empty if not
    ///     found
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of application property to retrieve</param>
    /// <returns></returns>
    public string GetPropertyAsString(string propertyName)
    {
        if (_properties.TryGetValue(propertyName, out RedisValue value))
            return value.ToString();

        return string.Empty;
    }


    /// <summary>
    ///     Attempts to retrieve the Application Property with the given name from the dictionary.  Returns default value for T
    ///     object.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of application property to retrieve</param>
    /// <returns>Object T or Null if not found</returns>
    public T? GetPropertyObject<T>(string propertyName)
    {
        if (_properties.TryGetValue(propertyName, out RedisValue value))
        {
            string json     = value.ToString();
            T      valueAsT = JsonSerializer.Deserialize<T>(json);
            return valueAsT;
        }

        return default;
    }


    /// <summary>
    ///     Prints information about the message.  So key properties and ApplicationProperties
    /// </summary>
    /// <returns></returns>
    public string PrintMessageInfo()
    {
        StringBuilder sb = new();

        foreach (KeyValuePair<RedisValue, RedisValue> appProperty in Properties)
            sb.Append($"\nAppProp:  {appProperty.Key} --> {appProperty.Value.ToString()}");

        return sb.ToString();
    }
}
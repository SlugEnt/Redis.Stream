using System.Text;
using StackExchange.Redis;

namespace Redis.Stream;

public class SLRMessage
{
    private Dictionary<RedisValue, RedisValue> _properties;



    /// <summary>
    /// Constructor when creating message to send to Redis Stream
    /// </summary>
    public SLRMessage() { _properties = new(); }


    /// <summary>
    /// Constructor when creating message read from Redis Stream
    /// </summary>
    /// <param name="id"></param>
    public SLRMessage(StreamEntry streamEntry) : this()
    {
        Id             = streamEntry.Id;
        RawStreamEntry = streamEntry;
    }


    /// <summary>
    /// The unique ID that identifies this message
    /// </summary>
    public RedisValue Id { get; protected set; }

    public string CorrelationId { get; set; }

    internal string Data { get; set; } = "";

    internal StreamEntry RawStreamEntry { get; set; }



    public IReadOnlyDictionary<RedisValue, RedisValue> Properties
    {
        get { return _properties; }
    }


    /// <summary>
    /// Creates a message from the given text, identifying the encoding type of the text
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    public static SLRMessage CreateMessage(string text, EnumEncodingType encodingType = EnumEncodingType.Text)
    {
        SLRMessage message = new SLRMessage();
        message.AddProperty("enc", encodingType);
        message.Data = text;
        return message;
    }


    /// <summary>
    /// Creates a message from the given text.
    /// </summary>
    /// <param name="text"></param>
    /// <returns></returns>
    public static SLRMessage CreateMessage(string text)
    {
        SLRMessage message = new SLRMessage();
        message.AddProperty("enc", EnumEncodingType.Text);
        message.Data = text;
        return message;
    }


    /// <summary>
    /// Creates a message with the given object
    /// </summary>
    /// <typeparam name="T">Any Nullable object type</typeparam>
    /// <param name="value">An instance of T</param>
    /// <returns></returns>
    public static SLRMessage CreateMessage<T>(T value)
    {
        string     json    = System.Text.Json.JsonSerializer.Serialize(value);
        SLRMessage message = CreateMessage(json, EnumEncodingType.ApplicationJson);
        return message;
    }


    public T GetObject<T>()
    {
        T value = System.Text.Json.JsonSerializer.Deserialize<T>(Data);
        return value;
    }



    /// <summary>
    /// Adds the given property to the Application Property Dictionary
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of the property</param>
    /// <param name="propertyValue">Value of the property (as string)</param>
    public void AddProperty(string propertyName, RedisValue propertyValue) { _properties.Add(propertyName, propertyValue); }


    /// <summary>
    /// Adds the given property to the Application Property Dictionary
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of the property</param>
    /// <param name="value">Value Object</param>
    public void AddProperty<T>(string propertyName, T value)
    {
        string json = System.Text.Json.JsonSerializer.Serialize(value);
        _properties.Add(propertyName, json);
    }



    public NameValueEntry[] GetNameValueEntries()
    {
        // We add 1 as we also need to add in the Data property.
        NameValueEntry[] entries = new NameValueEntry[_properties.Count + 1];
        int              i       = 0;
        foreach (KeyValuePair<RedisValue, RedisValue> keyValuePair in _properties)
        {
            entries[i++] = new NameValueEntry(keyValuePair.Key, keyValuePair.Value);
        }

        // Add data property if it exists.
        entries[i] = new NameValueEntry("data", Data);
        return entries;
    }



    /// <summary>
    /// Attempts to retrieve the Application Property with the given name from the dictionary.  Returns String.Empty if not found
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of application property to retrieve</param>
    /// <returns></returns>
    public string GetPropertyAsString(string propertyName)
    {
        if (_properties.TryGetValue(propertyName, out RedisValue value))
        {
            return value.ToString();
        }

        return string.Empty;
    }


    /// <summary>
    /// Attempts to retrieve the Application Property with the given name from the dictionary.  Returns default value for T object.
    /// </summary>
    /// <param name="message"></param>
    /// <param name="propertyName">Name of application property to retrieve</param>
    /// <returns>Object T or Null if not found</returns>
    public T GetProperty<T>(string propertyName)
    {
        if (_properties.TryGetValue(propertyName, out RedisValue value))
        {
            string json     = value.ToString();
            T      valueAsT = System.Text.Json.JsonSerializer.Deserialize<T>(json);
            return valueAsT;
        }

        return default(T);
    }



#region "Static Methods"

#endregion
}
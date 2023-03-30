using System.Text;
using StackExchange.Redis;

namespace SlugEnt.SLRStreamProcessing;

public static class ExtensionMethod_StackExchangeRedis
{
    /// <summary>
    /// Converts the specified Fields value into the requested T type.  If field does not exist or is null or empty then it returns Default for type T.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="streamEntry"></param>
    /// <param name="field">Name of the field to retrieve.  Defaults to the Data field</param>
    /// <returns></returns>
    public static T GetJsonObject<T>(this StackExchange.Redis.StreamEntry streamEntry, string field = "data")
    {
        RedisValue rvalue = streamEntry[field];
        if (rvalue.IsNullOrEmpty)
            return default(T);

        T value = System.Text.Json.JsonSerializer.Deserialize<T>(rvalue.ToString());
        return value;
    }
}
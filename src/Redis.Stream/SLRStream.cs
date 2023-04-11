using Microsoft.Extensions.Logging;
using SLRStreamProcessing;
using StackExchange.Redis;

namespace SlugEnt.SLRStreamProcessing;

/// <summary>
///     The Basic stream producing / consuming class.
/// </summary>
public class SLRStream : SLRStreamBase
{
    /// <summary>
    ///     Constructor
    /// </summary>
    /// <param name="logger"></param>
    /// <param name="serviceProvider"></param>
    public SLRStream(ILogger<SLRStream> logger, IServiceProvider serviceProvider) : base(logger, serviceProvider) { }



    /// <summary>
    ///     Reads up to max messages from the stream.  Note, it will starting with messages after the last message consumed
    ///     DURING THIS SESSION.  Other consumers know nothing about
    ///     what messages you have read or consumed and will start by default at the very first message in the queue.
    /// </summary>
    /// <param name="numberOfMessagesToRetrieve"></param>
    /// <returns></returns>
    public async Task<StreamEntry[]> ReadStreamAsync(int numberOfMessagesToRetrieve = 6) => await base.ReadStreamAsync(numberOfMessagesToRetrieve);
}
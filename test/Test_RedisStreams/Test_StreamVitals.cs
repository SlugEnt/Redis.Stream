using SLRStreamProcessing;
using SlugEnt.SLRStreamProcessing;
using StackExchange.Redis;

namespace Test_RedisStreams;

[TestFixture]
public class Test_StreamVitals : SetupRedisConfiguration
{
    [OneTimeSetUp]
    public void OneTimeSetup() { Initialize(); }


    [Test]
    public async Task ApplicationInfo()
    {
        SLRStreamAppGroup stream = null;
        SLRStreamConfigAppGroup config = new()
        {
            StreamName = _uniqueKeys.GetKey("Tst"), ApplicationName = _uniqueKeys.GetKey("App"), StreamType = EnumSLRStreamTypes.ProducerAndConsumerGroup
        };

        try
        {
            // Produce a few messages
            stream = await _slrStreamEngine.GetSLRStreamAppGroupAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messagesProduced = 0;
            for (int i = 0; i < 8; i++, messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={i}");
                await stream.SendMessageAsync(message);
            }


            // Consume half the messages
            int j;
            int received = 0;
            for (j = 0; j < messagesProduced / 2; j++)
            {
                StreamEntry[] messages = await stream.ReadStreamGroupAsync(1);
                if (messages.Length > 0)
                    Assert.AreEqual(1, messages.Length, "A20: Received more than the expected number of messages. Expected 1.");
                received += messages.Length;
            }


            // C. Test the Vitals
            SLRStreamVitals vitals = await stream.GetStreamVitals();
            Assert.AreEqual(1, vitals.ApplicationsOnStream.Length, "C300:");
            Assert.AreEqual(config.ApplicationName, vitals.ApplicationsOnStream[0].Name, "C310:");
            Assert.IsTrue(vitals.ApplicationExistsOnStream(), "C320:");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        finally
        {
            if (stream != null)
                stream.DeleteStream();
        }
    }
}
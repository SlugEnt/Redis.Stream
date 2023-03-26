using System.Net;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Redis.Stream;
using SlugEnt;
using StackExchange.Redis;

namespace Test_RedisStreams;

[TestFixture]
public class Test_SLRStream_ConsumerGroup
{
    private SLRStreamEngine      _slrStreamEngine;
    private IServiceCollection   _services;
    private ServiceProvider      _serviceProvider;
    private ConfigurationOptions _configuration;
    private UniqueKeys           _uniqueKeys;


    [OneTimeSetUp]
    public void OneTimeSetup()
    {
        _services = new ServiceCollection().AddLogging();
        _services.AddTransient<SLRStreamEngine>();
        _services.AddTransient<SLRStream>();
        _serviceProvider = _services.BuildServiceProvider();

        _configuration = new ConfigurationOptions { Password = "redispw", EndPoints = { new DnsEndPoint("localhost", 32768) }, ConnectTimeout = 700, };

        // This is purely to validate we have a local Redis DB and that it is available.  If its not all tests will fail.
        SLRStreamEngine engine = _serviceProvider.GetService<SLRStreamEngine>();
        engine.RedisConfigurationOptions = _configuration;
        Assert.IsTrue(engine.Initialize(), "A10:  Engine is not connected to Redis DB.  For Testing purposes ensure you have a local Redis DB running.");

        // Store engine so other test methods can use
        _slrStreamEngine = engine;

        _uniqueKeys = new();
    }



    // Reads the messages, but does not acknowledge, then issues another read, should not receive any messages.
    [Test]
    public async Task ReadMessagesNoAck()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName      = _uniqueKeys.GetKey("TstCG"),
            ApplicationName = _uniqueKeys.GetKey("AppCG"),
            StreamType      = EnumSLRStreamTypes.ProducerAndConsumerGroup,
        };

        try
        {
            // A.  Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messageLimit     = 5;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Consume
            int           received = 0;
            StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            received += messages.Length;

            Assert.AreEqual(messagesProduced, received, "A100:");


            // C. Consume again, since we did not acknowledge we should not see any new messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "A110:");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        finally
        {
            if (stream != null)
            {
                stream.DeleteStream();
            }
        }
    }



    // Reads the messages, Does not initially acknowledge, then re-reads but with pending indicator set.
    [Test]
    public async Task ReadMessagesNoAck_RereadWithPending_Success()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName      = _uniqueKeys.GetKey("TstCG"),
            ApplicationName = _uniqueKeys.GetKey("AppCG"),
            StreamType      = EnumSLRStreamTypes.ProducerAndConsumerGroup,
        };

        try
        {
            // A.  Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messageLimit     = 5;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Consume
            int           received = 0;
            StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            received += messages.Length;

            Assert.AreEqual(messagesProduced, received, "A100:");


            // C. Consume again, since we did not acknowledge we should not see any new messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "A110:");


            // D. Re-read the stream, but with the start with pending indicator set.
            StreamEntry[] historyMessages = await stream.ReadStreamGroupAsync(messageLimit * 2, true);
            Assert.AreEqual(messageLimit, historyMessages.Length, "A110:");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        finally
        {
            if (stream != null)
            {
                stream.DeleteStream();
            }
        }
    }


    // Reads the messages, and then does a manual acknowledgement.
    [Test]
    public async Task ReadMessages_WithManualAck()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName      = _uniqueKeys.GetKey("TstCG"),
            ApplicationName = _uniqueKeys.GetKey("AppCG"),
            StreamType      = EnumSLRStreamTypes.ProducerAndConsumerGroup,
        };

        try
        {
            // A.  Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messageLimit     = 5;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Consume
            int           received = 0;
            StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            received += messages.Length;
            Assert.AreEqual(messagesProduced, received, "a100:");


            // C. Acknowledge the Messages
            foreach (StreamEntry streamEntry in messages)
            {
                stream.AddPendingAcknowledgement(streamEntry);
            }

            Assert.AreEqual(messagesProduced, stream.StatisticPendingAcknowledgements, "A200:");
            await stream.FlushPendingAcknowledgements();


            Assert.AreEqual(0, stream.StatisticPendingAcknowledgements, "A210:");


            // D. Consume again  There should be no more pending messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "A300:");

            StreamEntry[] noPendingMessage = await stream.ReadStreamGroupAsync(messageLimit * 2, true);
            Assert.AreEqual(0, noPendingMessage.Length, "A310:");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        finally
        {
            if (stream != null)
            {
                stream.DeleteStream();
            }
        }
    }



    // Sets auto-acknowledgement or acknowledge on delivery.  
    [Test]
    public async Task ReadMessages_WithAutomaticAck()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName            = _uniqueKeys.GetKey("TstCG"),
            ApplicationName       = _uniqueKeys.GetKey("AppCG"),
            StreamType            = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            AcknowledgeOnDelivery = true,
        };

        try
        {
            // A.  Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messageLimit     = 5;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Consume
            int           received = 0;
            StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            received += messages.Length;
            Assert.AreEqual(messagesProduced, received, "a100:");


            // C. Acknowledge the Messages
/*            foreach (StreamEntry streamEntry in messages)
            {
                stream.AddPendingAcknowledgement(streamEntry);
            }

            Assert.AreEqual(messagesProduced, stream.StatisticPendingAcknowledgements, "A200:");
            await stream.FlushPendingAcknowledgements();


            Assert.AreEqual(0, stream.StatisticPendingAcknowledgements, "A210:");
*/

            // D. Consume again  There should be no more pending messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "A300:");

            StreamEntry[] noPendingMessage = await stream.ReadStreamGroupAsync(messageLimit * 2, true);
            Assert.AreEqual(0, noPendingMessage.Length, "A310:");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
        finally
        {
            if (stream != null)
            {
                stream.DeleteStream();
            }
        }
    }
}
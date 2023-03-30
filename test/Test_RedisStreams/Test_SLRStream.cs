﻿using System.Net;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using SlugEnt.SLRStreamProcessing;
using SlugEnt;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;

namespace Test_RedisStreams;

[TestFixture]
public class Test_SLRStream : SetupRedisConfiguration
{
    [OneTimeSetUp]
    public void OneTimeSetup() { Initialize(); }


    /// <summary>
    /// Confirm we can publish and consume messages individually
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task PublishConsumeMessages1by1()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName = _uniqueKeys.GetKey("Tst"), ApplicationName = _uniqueKeys.GetKey("App"), StreamType = EnumSLRStreamTypes.ProducerAndSimpleConsumer,
        };

        try
        {
            // Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messagesProduced = 0;
            for (int i = 0; i < 8; i++, messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={i}");
                await stream.SendMessageAsync(message);
            }

            // Consume - We set to bigger number to ensure we only have the same number of messages as the # produced
            int j;
            int received = 0;
            for (j = 0; j < messagesProduced * 2; j++)
            {
                StreamEntry[] messages = await stream.ReadStreamAsync(1);
                if (messages.Length > 0)
                    Assert.AreEqual(1, messages.Length, "A20: Received more than the expected number of messages. Expected 1.");
                received += messages.Length;
            }

            Assert.AreEqual(messagesProduced, received, "A30:");
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



    /// <summary>
    /// Confirm we can read in batches and that only the number of messages produced is actually read.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task PublishConsumeMessagesInBatches()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName = _uniqueKeys.GetKey("Tst"), ApplicationName = _uniqueKeys.GetKey("App"), StreamType = EnumSLRStreamTypes.ProducerAndSimpleConsumer,
        };

        try
        {
            // Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int batchLimit = 4;

            int i;
            for (i = 0; i < 8; i++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={i}");
                await stream.SendMessageAsync(message);
            }

            // Consume - We set to bigger number to ensure we only have the same number of messages as the # produced
            int j;
            int received = 0;
            for (j = 0; j < i * 2; j++)
            {
                StreamEntry[] messages = await stream.ReadStreamAsync(batchLimit);
                if (messages.Length > 0)
                    Assert.AreEqual(batchLimit, messages.Length, $"A20: Received more than the expected number of messages. Expected {batchLimit}.");
                received += messages.Length;
            }

            Assert.AreEqual(i, received, "A30:");
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


    /// <summary>
    /// Confirm that last message ID is set on read and = the last message in the messages Array
    /// </summary>
    /// <returns></returns>
    [Test]
    [Repeat(60)]
    public async Task ReadStreamSetsLastMessageId()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName = _uniqueKeys.GetKey("Tst"), ApplicationName = _uniqueKeys.GetKey("App"), StreamType = EnumSLRStreamTypes.ProducerAndSimpleConsumer,
        };

        try
        {
            // Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int batchLimit = 1;

            int i;
            for (i = 0; i < 2; i++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={i}");
                await stream.SendMessageAsync(message);
            }

            // Consume - We set to bigger number to ensure we only have the same number of messages as the # produced
            int        j;
            int        received  = 0;
            RedisValue lastMsgId = stream.LastMessageId;

            //Thread.Sleep(1);
            for (j = 0; j < i; j++)
            {
                StreamEntry[] messages = await stream.ReadStreamAsync(batchLimit);
                Assert.AreEqual(batchLimit, messages.Length, "A20:");
                Assert.AreNotEqual(lastMsgId, stream.LastMessageId, $"A30: J={j}");
                Assert.AreEqual(messages[0].Id, stream.LastMessageId, "A40:");
                received += messages.Length;
            }

            Assert.AreEqual(i, received, "A99:");
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


    /// <summary>
    /// Confirm we can publish and consume messages individually
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task ConsumeMessagesFromNow_Success()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName      = _uniqueKeys.GetKey("Tst"),
            ApplicationName = _uniqueKeys.GetKey("App"),
            StreamType      = EnumSLRStreamTypes.ProducerAndSimpleConsumer,
            StartingMessage = EnumSLRStreamStartingPoints.Now,
        };

        try
        {
            // Produce - And Read Verify
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messagesProduced = 0;
            for (int i = 0; i < 3; i++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={i}");
                await stream.SendMessageAsync(message);
            }

            // Now read the messages and store the last ID
            StreamEntry[] oldMessages  = await stream.ReadStreamAsync(100);
            RedisValue    lastOldMsgId = oldMessages[oldMessages.Length - 1].Id;


            // B Now We build a new stream object but accessing the stream we just sent messages to.. It should initially read no messages
            SLRStream     stream2     = await _slrStreamEngine.GetSLRStreamAsync(config);
            StreamEntry[] newMessages = await stream2.ReadStreamAsync(1);
            Assert.AreEqual(0, newMessages.Length, "B200");

            // Now write another message to the stream.
            SLRMessage message2 = SLRMessage.CreateMessage("part2");
            await stream.SendMessageAsync(message2);
            Thread.Sleep(10);

            // Now read the message
            StreamEntry[] newMessages2 = await stream2.ReadStreamAsync(100);
            Assert.AreEqual(1, newMessages2.Length, "B210");
            Assert.AreEqual(newMessages2[0].Id, stream2.LastMessageId, "B220:");
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



    // SAmple
    [Test]
    public async Task Sample()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName = _uniqueKeys.GetKey("Tst"), ApplicationName = _uniqueKeys.GetKey("App"), StreamType = EnumSLRStreamTypes.ProducerAndSimpleConsumer,
        };

        try
        {
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
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
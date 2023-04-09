using System.Net;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using SlugEnt.SLRStreamProcessing;
using SlugEnt;
using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;
using System.IO;
using NUnit.Framework.Constraints;
using NUnit.Framework.Interfaces;

namespace Test_RedisStreams;

[TestFixture]
public class Test_SLRStream_ConsumerGroup : SetupRedisConfiguration
{
    [OneTimeSetUp]
    public void OneTimeSetup() { Initialize(); }



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
            stream = await SetupTestProducer(config);


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
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.Zero,
        };

        try
        {
            stream = await SetupTestProducer(config);

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

            Assert.AreEqual(messagesProduced, received, "B10:");


            // C. Consume again, since we did not acknowledge we should not see any new messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "C10:");


            // D. Read the stream again to acquire the pending messages
            StreamAutoClaimResult claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit * 2);
            Assert.AreEqual(messageLimit, claimed.ClaimedEntries.Length, "D10:");
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
            stream = await SetupTestProducer(config);

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
            Assert.AreEqual(messagesProduced, received, "B10:");


            // C. Acknowledge the Messages
            foreach (StreamEntry streamEntry in messages)
            {
                stream.AddPendingAcknowledgementAsync(streamEntry);
            }

            Assert.AreEqual(messagesProduced, stream.StatisticPendingAcknowledgements, "C10:");
            await stream.FlushPendingAcknowledgementsAsync();


            Assert.AreEqual(0, stream.StatisticPendingAcknowledgements, "C20:");


            // D. Consume again  There should be no more pending messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "D10:");


            // E. Read the stream again to acquire the pending messages
            StreamAutoClaimResult claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit * 2);
            Assert.AreEqual(0, claimed.ClaimedEntries.Length, "E10:");
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



    // Reads the messages, and then does a manual acknowledgement.  But the acknowledged list is larger than a given threshold and thus is auto-flushed
    [Test]
    public async Task ReadMessages_WithManualAckExceedsMaxLimit()
    {
        SLRStream stream                = null;
        int       autoFlushMessageLimit = 5;
        SLRStreamConfig config = new()
        {
            StreamName                 = _uniqueKeys.GetKey("TstCG"),
            ApplicationName            = _uniqueKeys.GetKey("AppCG"),
            StreamType                 = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            MaxPendingAcknowledgements = autoFlushMessageLimit,
        };

        try
        {
            stream = await SetupTestProducer(config);


            int messageLimit     = 8;
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
                await stream.AddPendingAcknowledgementAsync(streamEntry);
            }

            Assert.AreEqual(1, stream.StatisticFlushedMessageCalls, "A200:");
            int remainingMessages = messageLimit - autoFlushMessageLimit;
            Assert.AreEqual(remainingMessages, stream.StatisticPendingAcknowledgements, "A205:");

            await stream.FlushPendingAcknowledgementsAsync();
            Assert.AreEqual(0, stream.StatisticPendingAcknowledgements, "A210:");


            // D. Consume again  There should be no more pending messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "A300:");

            // E. Read the stream again to acquire the pending messages
            StreamAutoClaimResult claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit * 2);
            Assert.AreEqual(0, claimed.ClaimedEntries.Length, "E10:");
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
            stream = await SetupTestProducer(config);


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


            // C. Consume again  There should be no Normal messages
            StreamEntry[] noMessages = await stream.ReadStreamGroupAsync(messageLimit * 2);
            Assert.AreEqual(0, noMessages.Length, "A300:");
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
    public async Task ClaimMessagesFromDeadConsumer()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(50),
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            int messageLimit     = 6;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Create 2 consumers.
            SLRStream consumerA = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(consumerA, "B10:");
            SLRStream consumerB = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(consumerB, "B20:");


            // C. Start consuming one at a time.  But Stream B never acknowledges.
            int loopMax = messageLimit / 2;
            for (int i = 0; i < loopMax; i++)
            {
                StreamEntry[] messagesA = await consumerA.ReadStreamGroupAsync(1);
                await consumerA.AddPendingAcknowledgementAsync(messagesA[0]);

                StreamEntry[] messagesB = await consumerB.ReadStreamGroupAsync(1);
            }

            await consumerA.FlushPendingAcknowledgementsAsync();


            // D. Now we sleep for the ClaimMessagesOlderThan timespan to ensure we meet the criteria for claiming messages
            Thread.Sleep(config.ClaimMessagesOlderThan);


            // E. Confirm we can claim 3 messages from consumerB.
            StreamPendingMessageInfo[] pendingMessageInfoA = await consumerB.GetPendingMessageInfo();
            StreamEntry[]              claimedMessages     = await consumerB.ClaimPendingMessagesAsync(3, SLRStream.STREAM_POSITION_BEGINNING);
            Assert.AreEqual(3, claimedMessages.Length, "E10:");

            foreach (StreamEntry message in claimedMessages)
            {
                await consumerB.AddPendingAcknowledgementAsync(message);
            }

            Assert.AreEqual(loopMax, consumerB.StatisticPendingAcknowledgements, "E20:");
            await consumerB.FlushPendingAcknowledgementsAsync();
            Assert.AreEqual(0, consumerB.StatisticPendingAcknowledgements, "E20:");

            // Now Get 
            StreamPendingMessageInfo[] pendingMessageInfo = await consumerB.GetPendingMessageInfo();
            Assert.AreEqual(0, pendingMessageInfo.Length, "E99:");
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


    [Test]
    public async Task GetConsumerInfo()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(50),
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            int messageLimit     = 3;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Create 4 consumers in 2 groups - Odd / Even
            string          group         = "odd";
            int             consumerCount = 4;
            List<SLRStream> consumers     = new();
            for (int i = 0; i < consumerCount; i++)
            {
                group                  = i % 2 == 0 ? "Even" : "Odd";
                config.ApplicationName = group;
                SLRStream consumer = await _slrStreamEngine.GetSLRStreamAsync(config);
                consumers.Add(consumer);
                Assert.IsNotNull(consumer, $"B10: Consumer: {consumer.ApplicationId}");
            }


            // C. Get Group Info.  Should be 3 - the producer and the Odd  and Even groups
            StreamGroupInfo[] groupInfo = await consumers[0].GetStreamApplications();
            Assert.AreEqual(3, groupInfo.Length, "C10:");


            // D. Test the GetApplicationPendingMessages method
            Assert.AreEqual(0, await stream.GetApplicationPendingMessageCount(), "D10:");
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
    /// Validates the PendingMessage GetPendingMessage count method works correctly
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task GetPendingMessageCountForGroup()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(10),
            AcknowledgeOnDelivery  = false,
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);


            int messageLimit     = 3;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Create 4 consumers in 2 groups - Odd / Even
            string          group         = "odd";
            int             consumerCount = 4;
            List<SLRStream> consumers     = new();
            for (int i = 0; i < consumerCount; i++)
            {
                group                  = i % 2 == 0 ? "Even" : "Odd";
                config.ApplicationName = group;
                SLRStream consumer = await _slrStreamEngine.GetSLRStreamAsync(config);
                consumers.Add(consumer);
                Assert.IsNotNull(consumer, $"B10: Consumer: {consumer.ApplicationId}");
            }


            // C. Consume some messages for one of the consumers so the Pending ack count goes up.
            StreamEntry[] messages = await consumers[0].ReadStreamGroupAsync(messageLimit);
            Assert.AreEqual(messageLimit, messages.Length, "C10:");


            // D. Get Group Info.  Should be 3 - the producer and the Odd  and Even groups
            StreamGroupInfo[] groupInfo = await consumers[0].GetStreamApplications();
            Assert.AreEqual(3, groupInfo.Length, "D10:");


            // E. Test the GetApplicationPendingMessages method
            Assert.AreEqual(3, await consumers[0].GetApplicationPendingMessageCount(), "E10:");
            Assert.AreEqual(0, await consumers[1].GetApplicationPendingMessageCount(), "E20:");
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
    /// Validates that we 1) Can read pending messages, 2) When acknowledged they are removed from pending.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task PendingMessages()
    {
        SLRStream stream    = null;
        int       timeLimit = 5;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstPend"),
            ApplicationName        = _uniqueKeys.GetKey("AppPend"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(timeLimit),
            AcknowledgeOnDelivery  = false,
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            int messageLimit     = 6;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B. Read messages but they are not acknowledged!
            StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit);
            Assert.AreEqual(messageLimit, messages.Length, "B10:");


            // C. Validate there are the required number of pending messages
            Assert.AreEqual(messageLimit, await stream.GetApplicationPendingMessageCount(), "C10:");


            // D. Now read the pending messages.  Should be same # as messages created
            Thread.Sleep(timeLimit + 1);
            StreamAutoClaimResult claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit * 2);
            Assert.AreEqual(messageLimit, claimed.ClaimedEntries.Length, "D10:");

            // E. Validate there are the required number of pending messages
            Thread.Sleep(timeLimit);
            Assert.AreEqual(messageLimit, await stream.GetApplicationPendingMessageCount(), "E10:");

            // F. Acknowledge them
            foreach (StreamEntry streamEntry in messages)
            {
                await stream.AddPendingAcknowledgementAsync(streamEntry);
            }

            await stream.FlushPendingAcknowledgementsAsync();


            // G. Validate there are zero pending messages
            Assert.AreEqual(0, await stream.GetApplicationPendingMessageCount(), "G10:");


            // Validate there are no more messages to be read from pending
            Thread.Sleep(timeLimit);
            claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit * 2);
            Assert.AreEqual(0, claimed.ClaimedEntries.Length, "G20:");


            // Validate there are no more messages to be read from normal
            messages = await stream.ReadStreamGroupAsync(messageLimit);
            Assert.AreEqual(0, messages.Length, "G30:");
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
    /// Producers that do not have a consumer component should not be part of consuemr group info.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task Producer_IsNotPartOfConsumerGroup()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerOnly,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(50),
            AcknowledgeOnDelivery  = false,
        };

        try
        {
            // A.  Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messageLimit     = 3;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Create 4 consumers in 2 groups - Odd / Even
            string          group         = "odd";
            int             consumerCount = 4;
            List<SLRStream> consumers     = new();
            config.StreamType = EnumSLRStreamTypes.ConsumerGroupOnly;
            for (int i = 0; i < consumerCount; i++)
            {
                group                  = i % 2 == 0 ? "Even" : "Odd";
                config.ApplicationName = group;
                SLRStream consumer = await _slrStreamEngine.GetSLRStreamAsync(config);
                consumers.Add(consumer);
                Assert.IsNotNull(consumer, $"B10: Consumer: {consumer.ApplicationId}");
            }


            // C. Get Group Info.  Should be 3 - the producer is not a consumer so should not be included
            StreamGroupInfo[] groupInfo = await consumers[0].GetStreamApplications();
            Assert.AreEqual(2, groupInfo.Length, "D10:");
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


    // Creates a producer.  2 application groups with 3 consumers in each, with each at various stages of processing messages.
    // Confirms that the stats are correct.  Specifically, the First Message ID that has not been processed by ALL of the consumer groups yet.
    [Test]
    public async Task GetStreamVitals_Success()
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
            // Dictionary to keep track of if we have confirmed or not a message being consumed by a consumer
            Dictionary<int, bool> producedMessages = new();


            // A.  Produce
            stream = await _slrStreamEngine.GetSLRStreamAsync(config);
            Assert.IsNotNull(stream, "A10:");

            int messageLimit     = 60;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
                producedMessages.Add(messagesProduced, false);
            }


            // B.  Build Consumers
            config.StreamType = EnumSLRStreamTypes.ConsumerGroupOnly;
            List<SLRStream> streams = new();

            // Add the initial producer / Consumer stream
            streams.Add(stream);


            // Build consumer groups
            int groups            = 2;
            int consumersPerGroup = 3;
            for (int i = 0; i < groups; i++)
            {
                string appName = "Group_" + (i + 1).ToString();
                config.ApplicationName = appName;

                // Build consumers
                for (int j = 0; j < consumersPerGroup; j++)
                {
                    //string consumerName = "Consumer_" + (i+1) + "_" + (j + 1).ToString();
                    SLRStream newStream = await _slrStreamEngine.GetSLRStreamAsync(config);
                    streams.Add(newStream);
                }
            }

            // Add in the consumer for the producer group
            int totalConsumers = (consumersPerGroup * groups) + 1;


            // C. Now we consume various messages via each consumer.
            StreamEntry[]                                 messages;
            Dictionary<RedisValue, ConsumerGroupLastRead> ProcessedMessages = new();

            // Consumer 0 (producer) = All messages
            // Consumer 1 = 1st 10 messages.
            // Consumer 2 = 1st 25 messages
            // Consumer 3 = 1st 40 messages
            // Consumer 4 (Group 2) = 1st 7 messages
            // Consumer 5 (group 2) = 1st 20 messages
            // Consumer 6 (group 2) = 1st 30 messages

            int minimalIdToDelete = messageLimit;
            int loopCtr           = 0;

            // Note that the messages retrieved is per Group.  
            foreach (SLRStream slrStream in streams)
            {
                switch (loopCtr)
                {
                    // Group 1
                    case 0:
                        messages = await slrStream.ReadStreamGroupAsync(messageLimit);
                        break;

                    // Group "2"
                    case 1:
                        messages = await slrStream.ReadStreamGroupAsync(10);
                        break;

                    // Group "2"
                    case 2:
                        messages = await slrStream.ReadStreamGroupAsync(15);
                        break;

                    // Group "2"
                    case 3:
                        messages = await slrStream.ReadStreamGroupAsync(17);
                        break;

                    // Group "3"
                    case 4:
                        messages = await slrStream.ReadStreamGroupAsync(7);
                        break;

                    // Group "3"
                    case 5:
                        messages = await slrStream.ReadStreamGroupAsync(10);
                        break;

                    // Group "3"
                    case 6:
                        messages = await slrStream.ReadStreamGroupAsync(13);
                        break;
                    default:
                        messages = new StreamEntry[0];
                        break;
                }

                if (messages.Length < minimalIdToDelete)
                    minimalIdToDelete = messages.Length;

                // add to internal dictionary, keeping track of who has read the message
                int j = 0;
                foreach (StreamEntry streamEntry in messages)
                {
                    if (!ProcessedMessages.TryGetValue(streamEntry.Id, out ConsumerGroupLastRead value))
                    {
                        value = new ConsumerGroupLastRead(j, streamEntry.Id, 7);
                        value.SetTrue(loopCtr);
                        ProcessedMessages.Add(value.MessageId, value);
                    }
                    else
                    {
                        value.SetTrue(loopCtr);
                    }

                    j++;
                }

                loopCtr++;
            }


            // D. Lets get the Info
            List<ConsumerGroupLastRead> readList = ProcessedMessages.OrderBy(v => v.Value.Index).Select(v => v.Value).ToList();
            SLRStreamVitals             vitals   = await streams[0].GetStreamVitals();

            Assert.AreEqual(3, vitals.Statistic_NumberOfApplicationGroups, "D400:");


            // E.  Lets validate some info 
            // Group 2's last processed message ID should be: 42
            Assert.IsTrue(readList[41].GetStatus(3), "E500:");
            Assert.IsFalse(readList[42].GetStatus(3), "E510:");

            // Group 3's Last process message ID should be: 30
            Assert.IsTrue(readList[29].GetStatus(6), "E520:");
            Assert.IsFalse(readList[30].GetStatus(6), "E530:");


            // Last Fully Processed Message should be Group 3's
            Assert.AreEqual(readList[29].MessageId, vitals.FirstFullyUnprocessedMessageID);

            // First Message
            Assert.AreEqual(readList[0].MessageId, vitals.OldestMessageId);
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


    [Test]

    //[Repeat(20)]
    public async Task RemoveMessages()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName            = _uniqueKeys.GetKey("TstTrim"),
            ApplicationName       = _uniqueKeys.GetKey("RemoveMsg"),
            StreamType            = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            AcknowledgeOnDelivery = true,
        };

        try
        {
            stream = await SetupTestProducer(config);


            // Need to produce a lot of message to ensure we have some to delete.
            int messageLimit     = 3000;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }

            // C. Validate
            SLRStreamVitals vitals    = await stream.GetStreamVitals();
            long            origCount = vitals.StreamInfo.Length;
            Assert.AreEqual(messageLimit, vitals.StreamInfo.Length, "C300:");

            // Read messages
            int           received = 0;
            StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit);
            received += messages.Length;
            Assert.AreEqual(messagesProduced, received, "C310:");


            // D. Delete first X messages
            int suggestedDeleteNumber = 300;
            int delectedCount         = await stream.RemoveFullyProcessedEntries(messages[suggestedDeleteNumber].Id);
            Assert.AreEqual(suggestedDeleteNumber, delectedCount, "D400:");


            // Get Vitals
            vitals = await stream.GetStreamVitals();
            Assert.Less(vitals.StreamInfo.Length, origCount, "D410:");
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
    /// Confirms that a consumer can take over a PEL from another consumer.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task ConsumerTakesOverPELOfAnotherConsumer()
    {
        SLRStream stream      = null;
        int       pendingTime = 5;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(pendingTime),
            AcknowledgeOnDelivery  = false,
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);


            int messageLimit     = 10;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }


            // B.  Create 3 consumers for the application
            int             consumerCount = 3;
            List<SLRStream> consumers     = new();
            for (int i = 0; i < consumerCount; i++)
            {
                SLRStream consumer = await _slrStreamEngine.GetSLRStreamAsync(config);
                consumers.Add(consumer);
                Assert.IsNotNull(consumer, $"B10: Consumer: {consumer.ApplicationId}");
            }


            // C. Consume 2 messages for the first consumer so the Pending ack count goes up.
            int           pendingCount = 2;
            DateTime      startTime    = DateTime.Now;
            StreamEntry[] messages     = await consumers[0].ReadStreamGroupAsync(pendingCount);
            Assert.AreEqual(pendingCount, messages.Length, "C10:");


            // D. Test the GetApplicationPendingMessages method, All Consumers should show pending messages
            Assert.AreEqual(pendingCount, await consumers[0].GetApplicationPendingMessageCount(), "D10:");
            Assert.AreEqual(pendingCount, await consumers[1].GetApplicationPendingMessageCount(), "D20:");
            Assert.AreEqual(pendingCount, await consumers[2].GetApplicationPendingMessageCount(), "D20:");


            // E. Try to read the messages again, but do not acknowledge.  All should show pending = 2
            Thread.Sleep(pendingTime);
            StreamAutoClaimResult claimed = await consumers[0].ReadStreamGroupPendingMessagesAsync(pendingCount);

            //StreamEntry[]         consumerMessagesPEL = await consumers[0].ReadStreamGroupAsync(pendingCount);
            Assert.AreEqual(pendingCount, claimed.ClaimedEntries.Length, "E10:");
            Assert.AreEqual(pendingCount, await consumers[0].GetApplicationPendingMessageCount(), "E20:");
            Assert.AreEqual(pendingCount, await consumers[1].GetApplicationPendingMessageCount(), "E30:");
            Assert.AreEqual(pendingCount, await consumers[2].GetApplicationPendingMessageCount(), "E40:");


            // F. Now Consumer 2 should try and get the pending messages from consumer 1.  First attempt should return zero as they have not met the threshold yet
            DateTime current = DateTime.Now;
            TimeSpan diff    = current - startTime;

            // Make sure we have not exceeded our time interval
            Assert.Less(config.ClaimMessagesOlderThan, diff,
                        "F05:  If this is consistently throwing you probably need to increase the config value timespan by more milliseconds");

            int consumer1 = 1;
            claimed = await consumers[consumer1].ReadStreamGroupPendingMessagesAsync(messageLimit);
            Assert.IsEmpty(claimed.ClaimedEntries, "F10:");

            // Try again
            Thread.Sleep(21);
            claimed = await consumers[consumer1].ReadStreamGroupPendingMessagesAsync(messageLimit);
            Assert.AreEqual(SLRStream.STREAM_POSITION_BEGINNING, claimed.NextStartId.ToString(), "F20:");
            Assert.AreEqual(pendingCount, claimed.ClaimedEntries.Length, "F30:");


            // G. Acknowledge the messages.  Should be no more pending.
            foreach (StreamEntry streamEntry in claimed.ClaimedEntries)
            {
                await consumers[consumer1].AddPendingAcknowledgementAsync(streamEntry);
            }

            await consumers[consumer1].FlushPendingAcknowledgementsAsync();
            Assert.AreEqual(0, await consumers[0].GetApplicationPendingMessageCount(), "G10:");
            Assert.AreEqual(0, await consumers[1].GetApplicationPendingMessageCount(), "G20:");
            Assert.AreEqual(0, await consumers[2].GetApplicationPendingMessageCount(), "G30:");


            // H.  Read 4 more messages, but 2 from consumer 1 and 2 from consumer 2.  DO NOT Acknowledge
            messages = await consumers[consumer1].ReadStreamGroupAsync(pendingCount);
            int           consumer2 = 2;
            StreamEntry[] messages2 = await consumers[consumer2].ReadStreamGroupAsync(pendingCount);
            Assert.AreEqual(pendingCount, messages.Length, "H10:");
            Assert.AreEqual(pendingCount, messages2.Length, "H20:");


            // I.  None of them have been confirmed.  Pending count should be 4.
            int totalPending = pendingCount * 2;
            Assert.AreEqual(totalPending, await consumers[0].GetApplicationPendingMessageCount(), "I10:");
            Assert.AreEqual(totalPending, await consumers[1].GetApplicationPendingMessageCount(), "I20:");
            Assert.AreEqual(totalPending, await consumers[2].GetApplicationPendingMessageCount(), "I30:");


            // J. Now consumer 0 should attempt to claim them.
            Thread.Sleep(21);
            claimed = await consumers[0].ReadStreamGroupPendingMessagesAsync(messageLimit);
            Assert.AreEqual(SLRStream.STREAM_POSITION_BEGINNING, claimed.NextStartId.ToString(), "J10:");
            Assert.AreEqual(totalPending, claimed.ClaimedEntries.Length, "J20:");

            // K. Acknowledge the messages.  Should be no more pending.
            foreach (StreamEntry streamEntry in claimed.ClaimedEntries)
            {
                await consumers[consumer1].AddPendingAcknowledgementAsync(streamEntry);
            }

            await consumers[consumer1].FlushPendingAcknowledgementsAsync();
            Assert.AreEqual(0, await consumers[0].GetApplicationPendingMessageCount(), "K10:");
            Assert.AreEqual(0, await consumers[1].GetApplicationPendingMessageCount(), "K20:");
            Assert.AreEqual(0, await consumers[2].GetApplicationPendingMessageCount(), "K30:");
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
    /// Confirms that a consumer can read its own PEL messages once the expiration has occurred.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task ConsumerCanReadOwnPELMessages()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(20),
            AcknowledgeOnDelivery  = false,
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);


            int messageLimit     = 10;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }

            // B.  Now lets read the messages. No Acknowledgement
            int           pendingCount = messageLimit;
            StreamEntry[] messages     = await stream.ReadStreamGroupAsync(pendingCount);
            Assert.AreEqual(pendingCount, messages.Length, "B10:");

            // C.  Sleep the required number of seconds
            Thread.Sleep(21);
            Assert.AreEqual(pendingCount, await stream.GetApplicationPendingMessageCount(), "C10:");

            // D.  Now read the pending messages
            StreamAutoClaimResult claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit);
            Assert.AreEqual(pendingCount, claimed.ClaimedEntries.Length, "D10:");
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
    /// 
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task MessageRetryCountIncreases_ForMessagesRetried()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(10),
            AcknowledgeOnDelivery  = false,
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);


            int messageLimit     = 10;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }

            // B.  Now lets read the messages. No Acknowledgement
            int           pendingCount = messageLimit;
            StreamEntry[] messages     = await stream.ReadStreamGroupAsync(pendingCount);
            Assert.AreEqual(pendingCount, messages.Length, "B10:");

            // C.  Sleep the required number of seconds
            Assert.AreEqual(pendingCount, await stream.GetApplicationPendingMessageCount(), "C10:");

            // D.  Now read the pending messages
            Thread.Sleep(21);
            StreamAutoClaimResult claimed = await stream.ReadStreamGroupPendingMessagesAsync(messageLimit);
            Assert.AreEqual(pendingCount, claimed.ClaimedEntries.Length, "D10:");
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
    /// Each consumer in a consumer group should get a unique consumer group ID.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task MultipleConsumers_GetUniqueApplicationID()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(50),
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            SLRStream streamB = await _slrStreamEngine.GetSLRStreamAsync(config);
            SLRStream streamC = await _slrStreamEngine.GetSLRStreamAsync(config);
            SLRStream streamD = await _slrStreamEngine.GetSLRStreamAsync(config);

            Assert.AreEqual(1, stream.ApplicationId, "A10:");
            Assert.AreEqual(2, streamB.ApplicationId, "A20:");
            Assert.AreEqual(3, streamC.ApplicationId, "A30:");
            Assert.AreEqual(4, streamD.ApplicationId, "A40:");
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
    /// When a stream is closed, its consumer ID is removed from Redis.
    /// </summary>
    /// <returns></returns>
    [Test]
    [TestCase(true)]
    [TestCase(false)]
    public async Task DeleteConsumer(bool forceDeletion)
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(0),
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            SLRStream streamB = await _slrStreamEngine.GetSLRStreamAsync(config);
            SLRStream streamC = await _slrStreamEngine.GetSLRStreamAsync(config);

            Assert.AreEqual(1, stream.ApplicationId, "A10:");
            Assert.AreEqual(2, streamB.ApplicationId, "A20:");
            Assert.AreEqual(3, streamC.ApplicationId, "A30:");


            // B. Produce some messages
            int messageLimit     = 10;
            int messagesProduced = 0;
            for (messagesProduced = 0; messagesProduced < messageLimit; messagesProduced++)
            {
                SLRMessage message = SLRMessage.CreateMessage($"i={messagesProduced}");
                await stream.SendMessageAsync(message);
            }

            // B.  Now lets read the messages on Stream C
            int           pendingCount = messageLimit;
            StreamEntry[] messages     = await streamC.ReadStreamGroupAsync(pendingCount);
            Assert.AreEqual(pendingCount, messages.Length, "B10:");


            // C. Now Delete Consumer C
            bool success = await streamB.DeleteConsumer(streamC.ApplicationId, forceDeletion);
            if (forceDeletion)
            {
                Assert.IsTrue(success, "C10:  The consumer should have been deleted, even though it had pending messages.");
                StreamConsumerInfo[] consumers = await streamB.GetConsumers();
                string               appID     = streamC.ApplicationId.ToString();
                foreach (StreamConsumerInfo streamConsumerInfo in consumers)
                {
                    Assert.AreNotEqual(streamConsumerInfo.Name, appID, "C20:");
                }
            }
            else
            {
                Assert.IsFalse(success, "C20:  Consumer Deletion should have failed since there were pending messages");
                long pendingStill = await streamB.GetApplicationPendingMessageCount();
                Assert.AreEqual(pendingCount, pendingStill, "C30:");
            }
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
    /// When a stream is closed, its consumer ID is removed from Redis.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task CloseConsumer_RemovesConsumerId()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(50),
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            SLRStream streamB = await _slrStreamEngine.GetSLRStreamAsync(config);
            SLRStream streamC = await _slrStreamEngine.GetSLRStreamAsync(config);
            SLRStream streamD = await _slrStreamEngine.GetSLRStreamAsync(config);

            Assert.AreEqual(1, stream.ApplicationId, "A10:");
            Assert.AreEqual(2, streamB.ApplicationId, "A20:");
            Assert.AreEqual(3, streamC.ApplicationId, "A30:");
            Assert.AreEqual(4, streamD.ApplicationId, "A40:");


            // B. Now close Consumer C
            await streamC.CloseStream();
            Assert.IsFalse(streamC.IsInitialized, "B10:");

            // C. Make sure it is not in Redis any longer
            string               appID     = streamC.ApplicationId.ToString();
            StreamConsumerInfo[] consumers = await streamB.GetConsumers();
            foreach (StreamConsumerInfo streamConsumerInfo in consumers)
            {
                Assert.AreNotEqual(streamConsumerInfo.Name, appID, "C10:");
            }
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
    /// When a stream is closed, its consumer ID is removed from Redis.
    /// </summary>
    /// <returns></returns>
    [Test]
    public async Task DeleteAllConsumers_ApplicationStillExistsOnStream()
    {
        SLRStream stream = null;
        SLRStreamConfig config = new()
        {
            StreamName             = _uniqueKeys.GetKey("TstCG"),
            ApplicationName        = _uniqueKeys.GetKey("AppCG"),
            StreamType             = EnumSLRStreamTypes.ProducerAndConsumerGroup,
            ClaimMessagesOlderThan = TimeSpan.FromMilliseconds(0),
        };

        try
        {
            // A.  Produce
            stream = await SetupTestProducer(config);

            SLRStream streamB = await _slrStreamEngine.GetSLRStreamAsync(config);
            SLRStream streamC = await _slrStreamEngine.GetSLRStreamAsync(config);

            Assert.AreEqual(1, stream.ApplicationId, "A10:");
            Assert.AreEqual(2, streamB.ApplicationId, "A20:");
            Assert.AreEqual(3, streamC.ApplicationId, "A30:");


            StreamConsumerInfo[] consumers = await stream.GetConsumers();
            Assert.AreEqual(3, consumers.Length, "A10:");


            // B. Now Delete All Consumers
            bool success = await streamB.DeleteConsumer(streamC.ApplicationId);
            Assert.IsTrue(success, "B10:  The consumer should have been deleted");

            success = await streamB.DeleteConsumer(streamB.ApplicationId);
            Assert.IsTrue(success, "B20:  The consumer should have been deleted");

            success = await stream.DeleteConsumer(stream.ApplicationId);
            Assert.IsTrue(success, "B30:  The consumer should have been deleted");


            // C. 
            consumers = await stream.GetConsumers();
            Assert.AreEqual(0, consumers.Length, "C10:");


            // D. Make sure application info is still on stream
            StreamGroupInfo[] apps = await stream.GetStreamApplications();
            Assert.AreEqual(1, apps.Length, "D10:");
            Assert.AreEqual(0, apps[0].ConsumerCount, "D20:");
            Assert.AreEqual(stream.ApplicationName, apps[0].Name, "D30:");
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
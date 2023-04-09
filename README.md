# SLRStreamProcessing AKA Redis.Stream

# Purpose
This library provides a high level interface to using Redis streams.  It builds upon the foundation provided by the StackExchange.Redis and StackExchange.Redis.Extensions libraries.  It provides easier message manipulation, better error handling, more streamlined reading and consuming of messages and some features those libraries are lacking.  

## SLRMessage
This is the core dto object used to send messages to Redis and receive messages back from Redis. By encapsulating the StreamEntry object from StackExchange.Redis into a new object, we can add additional properties that make creating and working with messages easier.
### Creating a message
There are several ways to create a message:
```
    # Create a raw text message
    string     text    = "hello";
    SLRMessage message1 = SLRMessage.CreateMessage(text);

    # Create a message with an object
    string     name     = "Fred Flinstone";
    string     password = "YabbaDabbaDoo";
    uint       age      = 54;
    UserSample user     = new(name, password, age);
    SLRMessage message2 = SLRMessage.CreateMessage<UserSample>(user);

    # Create a generic message
    SLRMessage message3 = new();
```
A text message is just that - raw text.  It should not be used for Json objects however.

An object message is an object that is encoded into Json and then saved in the message. 

A raw message  is just that, it contains neither a default text or object in it.  But it can store text and json in it's properties (So too can the text and object messages!)


Messages have properties that allow you to store multiple objects, strings, numbers besides the base text or object.  The number of properties per message is unlimited.
```
     
     # Create a message with UserAge property that stores a long
     string propName  = "UserAge";
     ulong  propValue = ulong.MaxValue;

     SLRMessage message = new SLRMessage();
     message.AddProperty(propName, propValue);


     # Create a message with a user object property
     string     name     = "Fred Flinstone";
     string     password = "YabbaDabbaDoo";
     uint       age      = 54;
     UserSample user     = new(name, password, age);

     # Create a raw message then add the the UserSample object to its properties
     SLRMessage message = new SLRMessage();
     message.AddPropertyObject<UserSample>("User", user);
```

## SLRStream
A SLRStream represents a connection to a Redis Stream.  It can be a producer or consumer of messages or it can be both at same time.  

### Acquire a stream object
Before you can acquire a connection to a stream you must build a SLRStreamConfig and set some of its properties.  See SLRStreamConfig section below for details.

Once you have the Stream Config, accessing the stream is easy.
```
    # Get Config
    SLRStreamConfig config = new()
    {
        StreamName = "myFirstStream, ApplicationName = "a.test.app", StreamType = EnumSLRStreamTypes.ProducerAndSimpleConsumer,
    };


    # Acquire the stream
    stream = await _slrStreamEngine.GetSLRStreamAsync(config);


    # Send a couple messages
    for (int i = 0; i < 2; i++)
    {
        SLRMessage message = SLRMessage.CreateMessage($"i={i}");
        await stream.SendMessageAsync(message);
    }


    # Read the messages back.  In this case we will read a maximum of 2 messages
    StreamEntry[] messages = await stream.ReadStreamAsync(2);
```


### Consuming Messages - Details
The above code created a simple Stream that could read and write messages to/from the stream.  Internally the library keeps track of 
what messages you have read and subsequent calls to ReadStream will only return new messages.  However, other sessions or if you restart this session know nothing about this and will start consuming from the very first message in the queue.  

Therefore it is upto the calling program to keep track of what messages it has consumed.  If you wish to have the queue keep track then you need to use a ConsumerGroup to read the queue.  Also, the queue will eventually fill up and stop working so the program must also remove messages periodically.


### Producer Streams
SimpleProducer streams are streams that are enabled to be written to by the calling program.  There is nothing special about these and very few settings to set.


### Simple Consumer Streams
These are streams that can read messages from the Redis Streams.  The Simple Consumer logic provides the most simplistic of features.  Features it provides are:
* It remeembers the last message read in the stream during this session.  Subsequent calls to read more messages will only get new messages since the last read.
 
I said they were simple!

Disadvantages:
* You have to store off the last message read, it is not automatically stored in Redis and thus does not survive application restarts*
* Multiple instances of the same application reading from the stream will each get every messages, thus it is up to the applications to figure out how to handle this situation.

Simple consumers have no message acknowledgement.  As soon as Redis has handed the message over to you, it is considered delivered and processed.  It is up to the application to keep track of the last message successfully processed and to move thru the messages in the stream.


### Consumer Group Streams
These are streams that have more advanced capabilities than simple Consumer Streams.  They provide:
* Each application must have a unique name (ApplicationName) that Redis then uses to determine what the last message read by an application is.
* This last message processed by an application is stored in Redis and survives application and session restarts!
* Multiple instances of the same application can be run in parallel.  Redis will ensure only 1 instance gets a given message.
* Messages can be auto-acknowledged or manually acknowledged, but they MUST be acknowledged.
* Unacknowledged messages after a period of time (Configurable by the app) become Pending and can be assumed by another consumer.*

#### Consumer Group Applications
Every stream that is going to be processed by consumer groups needs to have a group name that identifies it so that Redis knows which consumers are part of the group.  Redis refers to these as ConsumerGroups, SLRStreamProcessing calls this an Application.
In addition to the Application to identify who is processing the stream, each instance of the Application that is running needs an ID.  This can be anything, but SLRStreamProcessing assigns it a sequential number.  Redis calls this the ConsumerId. 

SLRStreamProcessing refers to this as the ApplicationId.  The SLRStreamProcessing engine auto-assigns this Id.  It simply looks for an available ID starting at zero.  This means that the latest instance, may not be the highest ApplicationId.  

#### Reading from a Consumer Group
To read from a Consumer Group your stream must be of a type that supports ConsumerGroups.  You set this in the config for the stream.
Reading messages is simply a matter of calling the ReadStreamGroupAsync method.  In the example below we are manually acknowledging messages.  Manual acknowledgement is a 2 step process

```
    // Configure the stream
    SLRStreamConfig config = new()
    {
        StreamName      = _uniqueKeys.GetKey("TstCG"),
        ApplicationName = _uniqueKeys.GetKey("AppCG"),
        StreamType      = EnumSLRStreamTypes.ConsumerGroupOnly,
    };

    // Read messages from Stream
    StreamEntry[] messages = await stream.ReadStreamGroupAsync(messageLimit * 2);
 

    // Build list of messages to Acknowledge 
    foreach (StreamEntry streamEntry in messages)
    {
        stream.AddPendingAcknowledgementAsync(streamEntry);
    }

    // Send the list of acknowledgement to Redis to actually acknowledge them
    await stream.FlushPendingAcknowledgementsAsync();
```

Note:  The AddPendingAcknowledgementAsync method will automatically call a FlushPendingAcknowledgementsAsync if the number of acknowledgements exceeds a user specified threshold.

To setup Auto-Acknowledgement:
```
    SLRStreamConfig config = new()
    {
        StreamName            = _uniqueKeys.GetKey("TstCG"),
        ApplicationName       = _uniqueKeys.GetKey("AppCG"),
        StreamType            = EnumSLRStreamTypes.ConsumerGroupOnly,
        AcknowledgeOnDelivery = true,
    };
```

#### Acknowledgements
Acknowledgements only apply to Consumers that are part of a ConsumerGroup. They can be manual or automatic.  It is recommended you use manual, as automatic assumes that as soon as Redis has delivered the message to you that is is delivered and processed and thus acknowledged.  If something happens during your processing of the message the other instances will not know that you did not compoletely processe the message and thus it should be re-delivered to another instance or even the same instance.  But this is upto the application designer's requirements.

Acknowledgement handling is set via the config file.  The default is Manual Acknowledgement.
```
    SLRStreamConfig config = new()
    {
        // Automatic Acknowledgement.
        AcknowledgeOnDelivery = true,

        // Manual Acknowledgement (Default)
        AcknowledgeOnDelivery = false,
    };
```


#### Handling failed acknowledgements.
For messages that are manually acknowledged, Redis Streams puts the message into a pending entry list for the consumer that is receiving the message.  Other instances can then periodically check to see if there are any messages that are in the pending entry list for other consumers of the same consumer group.  If there are they can take ownership of them and try to process them themselves.

There are 2 methods you can use to determine if there are pending messages.  Both methods are equally fast.  I have run performance runs of 100,000+ for each and they are both within a second of each other and sometimes one method wins and sometimes the other.  However, this only holds true if there are rarely actual pending messages (usually the case, but your mileage may vary).

You can call ReadStreamGroupPendingMessagesAsync which you will have to do anyway if there are pending messages, so this is my recommendation as it eliminates some additional logic you have to handle and eliminates a second call to Redis.  Or you can call GetApplicationPendingMessageCount, to get the number of pending messages and then call ReadStreamGroupPendingMessagesAsync to retrieve them.

```
    StreamAutoClaimResult claims = await streamA.ReadStreamGroupPendingMessagesAsync(10);
    if (claims.ClaimedEntries.Length > 0)
    {
        // Logic to process messages
    }
```


#### Closing ConsumeGroup Streams
It is highly advised to close ConsumerGroup Streams when you are done processing!  If you do not, then the list of consumers of a given application will continually grow.

Note.  If a ConsumerGroup is closed by an instance, it will only be deleted (removed from the list of consumers for the appication) IF there are no pending messages for the consumer.  If there are pending messages, then the consumer is not deleted.  This allows other instances OR a restart of a new instance of the application to claim these messages and process them.

Note:  When all consumers of a consumer group (Application) are closed the application information will remain available on the stream!  It's consumer count will be zero.


#### Deleting a consumer
You can force the deletion of a consumer by specifying its ConsumerID and the force flag.
```
    bool success = await streamB.DeleteConsumer(streamC.ApplicationId, forceDeletion);
```


### SLRStreamEngine

### SLRStreamConfig

### SLRStreamVitals


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


### Consumer Group Streams
These are streams that have more advanced capabilities than simple Consumer Streams.  They provide:
* Each application must have a unique name (ApplicationName) that Redis then uses to determine what the last message read by an application is.
* This last message processed by an application is stored in Redis and survives application and session restarts!
* Multiple instances of the same application can be run in parallel.  Redis will ensure only 1 instance gets a given message.
* Messages can be auto-acknowledged or manually acknowledged, but they MUST be acknowledged.
* Unacknowledged messages after a period of time (Configurable by the app) become Pending and can be assumed by another consumer.*


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

### SLRStreamEngine

### SLRStreamConfig

### SLRStreamVitals


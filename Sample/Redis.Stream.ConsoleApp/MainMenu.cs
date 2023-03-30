using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Spectre.Console;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis.Extensions.Core.Configuration;


namespace SlugEnt.SLRStreamProcessing.Sample;

public class MainMenu
{
    private readonly ILogger          _logger;
    private          IServiceProvider _serviceProvider;
    private          bool             _started;

    private ConnectionMultiplexer _redisMultiplexer;
    private SLRStreamEngine       _redisStreamEngine;

    //private DisplayFlightInfoStats _displayStats;



    public MainMenu(ILogger<MainMenu> logger, IServiceProvider serviceProvider)
    {
        _logger            = logger;
        _serviceProvider   = serviceProvider;
        _redisStreamEngine = _serviceProvider.GetService<SLRStreamEngine>();
        if (_redisStreamEngine == null)
            throw new ApplicationException("Unable to build a RedisStreamEngine");


        RedisConfiguration _redisConfiguration = new RedisConfiguration
        {
            Password = "redispw", Hosts = new[] { new RedisHost { Host = "localhost", Port = 6379 } }, ConnectTimeout = 700,
        };
        _redisStreamEngine.RedisConfiguration = _redisConfiguration;


        /*

        _flightInfoEngine = _serviceProvider.GetService<FlightInfoEngine>();
        if (_flightInfoEngine == null)
        {
            _logger.LogError($"Failed to load the FlightInfoEngine from ServiceProvider");
            return;
        }

        
        _displayStats = new DisplayFlightInfoStats(_flightInfoEngine);
        */
    }



    internal async Task Start()
    {
        // TODO TEMPorary only
        await ProcessStreams();


        bool keepProcessing = true;

        // Initialize the Engines
        //await _flightInfoEngine.InitializeAsync();

        while (keepProcessing)
        {
            if (Console.KeyAvailable)
            {
                keepProcessing = await MainMenuUserInput();
            }
            else
                Thread.Sleep(1000);


            Display();
        }
    }



    internal async Task Display()
    {
/*        if (_displayStats != null)
            _displayStats.Refresh();
*/
    }


    internal async Task<bool> MainMenuUserInput()
    {
        if (Console.KeyAvailable)
        {
            ConsoleKeyInfo keyInfo = Console.ReadKey();

            switch (keyInfo.Key)
            {
                case ConsoleKey.T: break;

                case ConsoleKey.S: break;

                case ConsoleKey.I:
/*                    Console.WriteLine("Enter the number of minutes between flight creations");
                    string interval = Console.ReadLine();
                    if (int.TryParse(interval, out int secondInterval))
                    {
                        _flightInfoEngine.SetFlightCreationInterval(secondInterval * 60);
                        Console.WriteLine($"Flights will now be created every {interval} minutes");
                    }
                    else
                        Console.WriteLine("Must enter a numeric integer value");
*/
                    break;

                case ConsoleKey.D:
//                    _flightInfoEngine.DeleteStreamAsync();
                    Console.WriteLine($"Deleted Stream for Engine FlightInfo");
                    Thread.Sleep(5000);
                    break;

                case ConsoleKey.R:
//                    _flightInfoEngine.Reset();
                    return false;

                    break;

                case ConsoleKey.X:
/*                    if (_flightInfoEngine != null)
                        await _flightInfoEngine.StopEngineAsync();
*/
                    return false;
            }
        }


        return true;
    }


    internal async Task ProcessStreams()
    {
        SLRStreamConfig config = new SLRStreamConfig()
        {
            StreamName = "A", ApplicationName = "testProgram", StreamType = EnumSLRStreamTypes.ProducerAndConsumerGroup, MaxPendingAcknowledgements = 25,
        };

        SLRStream streamA = await _redisStreamEngine.GetSLRStreamAsync(config);
        config.StreamType = EnumSLRStreamTypes.ProducerAndSimpleConsumer;
        config.StreamName = "B";
        SLRStream streamB = await _redisStreamEngine.GetSLRStreamAsync(config);
        config.StreamName = "C";
        SLRStream streamC = await _redisStreamEngine.GetSLRStreamAsync(config);


        //       streamA.DeleteStream();
        //       streamB.DeleteStream();
        //       streamC.DeleteStream();


        SampleUser userA       = new("Bob Jones", 25, false);
        SLRMessage messageUser = SLRMessage.CreateMessage<SampleUser>(userA);
        messageUser.AddProperty("Type", "user");
        messageUser.AddProperty("User", userA);

        //RedisMessage message = RedisMessage.CreateMessage("This is a message");
        //message.AddProperty("UserName", "Bob Jones");


        streamA.SendMessageAsync(messageUser);
        SLRMessage bMsg = SLRMessage.CreateMessage("from b");
        streamB.SendMessageAsync(bMsg);
        streamB.SendMessageAsync(bMsg);
        streamB.SendMessageAsync(bMsg);

        SLRMessage cMsg = SLRMessage.CreateMessage("from C");
        streamC.SendMessageAsync(cMsg);
        streamC.SendMessageAsync(cMsg);
        streamC.SendMessageAsync(cMsg);


        //StreamEntry[] messages = await streamA.ReadStreamAsync();
        StreamEntry[] messages = await streamA.ReadStreamGroupAsync();
        foreach (StreamEntry streamEntry in messages)
        {
            //SLRMessage redisMessage = new SLRMessage(streamEntry);
            Console.WriteLine($"  Message: {streamEntry.Id}");
            foreach (NameValueEntry streamEntryValue in streamEntry.Values)
            {
                Console.WriteLine($"    --> {streamEntryValue.Name}  :  {streamEntryValue.Value}");
            }

            streamA.AddPendingAcknowledgementAsync(streamEntry);

            //await streamA.AcknowledgeMessage(redisMessage);
        }

        // Now send any final acknowledgments.
        await streamA.FlushPendingAcknowledgementsAsync();
    }


    /// <summary>
    /// The thread the engine runs on.
    /// </summary>
    internal void ProcessingLoop()
    {
        //_flightInfoEngine.StartEngineAsync();

        bool continueProcessing = true;
        while (continueProcessing)
        {
            if (Console.KeyAvailable)
            {
                ConsoleKeyInfo keyInfo = Console.ReadKey();
                if (keyInfo.Key == ConsoleKey.X)
                {
                    return;
                }
            }


            // Processing logic


            // Update Display
        }
    }
}
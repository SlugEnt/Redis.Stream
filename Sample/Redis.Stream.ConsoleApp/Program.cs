// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Events;
using StackExchange.Redis;
using System.Diagnostics;
using System.Net;
using System.Reflection;


namespace Redis.Stream.Sample;

public class Program
{
    static async Task Main(string[] args)
    {
        Serilog.ILogger Logger;
        Log.Logger = new LoggerConfiguration()
#if DEBUG
                     .MinimumLevel.Debug()
                     .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
#else
						 .MinimumLevel.Information()
			             .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
#endif
                     .Enrich.FromLogContext()
                     .WriteTo.Console()
                     .WriteTo.Debug()
                     .CreateLogger();

        Log.Debug("Starting " + Assembly.GetEntryAssembly().FullName);


        var      host     = CreateHostBuilder(args).Build();
        MainMenu mainMenu = host.Services.GetService<MainMenu>();
        await host.StartAsync();

        await mainMenu.Start();

        return;
    }


    /// <summary>
    /// Creates the Host
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
            {
                services.AddTransient<MainMenu>();
                services.AddTransient<RedisStreamEngine>();
                services.AddTransient<RedisStream>();
            })
            .ConfigureLogging((_, logging) =>
            {
                logging.ClearProviders();
                logging.AddSerilog();
                logging.AddDebug();
                logging.AddConsole();
            });
}


/*
return;

NameValueEntry[]               vals    = new NameValueEntry[30];
Dictionary<string, RedisValue> dicvals = new();

for (int i = 0; i < vals.Length; i++)
{
    string         name       = "name" + i;
    int            valInt     = i * i + 10;
    RedisValue     redisValue = valInt;
    NameValueEntry value      = new NameValueEntry(name, redisValue);
    vals[i] = new NameValueEntry(name, i * i * 10);
    dicvals.Add(name, redisValue);
}

Stopwatch sw = Stopwatch.StartNew();
for (int i = 0; i < vals.Length; i++)
{
    string name = "name" + i;
    for (int j = 0; j < vals.Length; j++)
        if (vals[j].Name == name)
        {
            //Console.WriteLine($"Found {name}");
            j = vals.Length;
        }
}

sw.Stop();
TimeSpan arrayAccess = sw.Elapsed;

Stopwatch sw2 = Stopwatch.StartNew();

byte b = 1;

for (int i = 0; i < vals.Length; i++)
{
    string name = "nametijgotr otk " + i;
    if (dicvals.TryGetValue(name, out RedisValue val))
        b = 0;

    //Console.WriteLine($"Found {name}");
}

sw2.Stop();
TimeSpan dictAccess = sw2.Elapsed;

Console.WriteLine($"Elapsed time for Int Array access {arrayAccess}");
Console.WriteLine($"Elapsed time for Dictionary Array access {dictAccess}");
Console.WriteLine("Done");
*/
//rstream.AddMsg("yep");
//string msg = rstream.GetMsg();
//Console.WriteLine(msg);
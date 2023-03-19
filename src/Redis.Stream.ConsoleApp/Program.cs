// See https://aka.ms/new-console-template for more information
using Redis.Stream;

Console.WriteLine("Hello, World!");

RedisStream rstream = new RedisStream();
rstream.AddMsg("yep");
string msg = rstream.GetMsg();
Console.WriteLine(msg);
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Redis.Stream
{
    public class RedisStream
    {
        private ConnectionMultiplexer _redisMultiplexer;
        private IDatabase             _db;


        public RedisStream(string streamName, string applicationName, ConnectionMultiplexer connectionMultiplexer)
        {
            StreamName      = streamName;
            ApplicationName = applicationName;
            ApplicationId   = "1";

            _redisMultiplexer = connectionMultiplexer;
            _db               = _redisMultiplexer.GetDatabase();
        }


    #region "Properties"

        /// <summary>
        /// The name of the stream
        /// </summary>
        public string StreamName { get; protected set; }

        /// <summary>
        /// Name of the Application that is processing.  
        /// </summary>
        public string ApplicationName { get; protected set; }

        /// <summary>
        /// Uniquely identifies a specific instance of the application.
        /// </summary>
        public string ApplicationId { get; protected set; }

        /// <summary>
        /// Application Name + Application Id
        /// </summary>
        public string ApplicationFullName
        {
            get { return ApplicationName + "." + ApplicationId; }
        }


        /// <summary>
        /// Number of Messages we have received.
        /// </summary>
        public long MessagesReceived { get; protected set; }

    #endregion



        public void DeleteStream() { _db.KeyDelete(StreamName); }


        public void AddStreamMsg(string userName, int userAge, long userAgeInSeconds)
        {
            var values = new NameValueEntry[]
            {
                new NameValueEntry("UserName", userName), new NameValueEntry("Age", userAge), new NameValueEntry("AgeInSeconds", userAgeInSeconds),
            };
            _db.StreamAdd(StreamName, values);
        }


        public void SendMessage(RedisMessage message)
        {
            NameValueEntry[] values = message.GetNameValueEntries();
            _db.StreamAdd(StreamName, values);
        }



        public async Task<StreamEntry[]> ReadStreamAsync(int numberOfMessagesToRetrieve = 3)
        {
            StreamPosition                    streamPosition    = new(StreamName, "0-0");
            StreamPosition[]                  streamsToRetrieve = new StreamPosition[] { streamPosition };
            StackExchange.Redis.RedisStream[] streams           = await _db.StreamReadAsync(streamsToRetrieve, numberOfMessagesToRetrieve);

            //                _db.StreamRead(new StreamPosition[] { new StreamPosition(StreamName, "0-0"), new StreamPosition("B", "0-0"), new StreamPosition("C", "0-0") }, 3);

            StackExchange.Redis.RedisStream stream = streams[0];

            Console.WriteLine($"Stream: {stream.Key} | ");
            MessagesReceived += stream.Entries.Length;
            return stream.Entries;

            /*
            foreach (StackExchange.Redis.RedisStream stream in streams)
            {
                Console.WriteLine($"Stream: {stream.Key} | ");
                MessagesReceived += stream.Entries.Length;
                return stream.Entries;
            
                
                foreach (StreamEntry streamEntry in stream.Entries)
                {
                    RedisMessage redisMessage = new RedisMessage(streamEntry);
                    Console.WriteLine($"  Message: {streamEntry.Id}");
                    foreach (NameValueEntry streamEntryValue in streamEntry.Values)
                    {
                        Console.WriteLine($"    --> {streamEntryValue.Name}  :  {streamEntryValue.Value}");
                    }
                }
              
            }*/
        }



        public void AddMsg(string msg) { _db.StringSet("Test", msg); }


        public string GetMsg() { return _db.StringGet("Test"); }
    }
}
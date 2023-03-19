using System;
using System.Collections.Generic;
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


        public RedisStream()
        {
            _redisMultiplexer = ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                Password = "redis23", EndPoints = { new DnsEndPoint("podmanc.slug.local", 6379) },
            });

            _db = _redisMultiplexer.GetDatabase();
        }


        public void AddMsg(string msg) { _db.StringSet("Test", msg); }


        public string GetMsg() { return _db.StringGet("Test"); }
    }
}
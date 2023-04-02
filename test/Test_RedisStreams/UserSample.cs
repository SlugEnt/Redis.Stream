using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test_RedisStreams
{
    internal class UserSample
    {
        public string Name { get; set; }
        public string Password { get; set; }
        public uint Age { get; set; }


        public UserSample(string name, string password, uint age)
        {
            Name     = name;
            Password = password;
            Age      = age;
        }
    }
}
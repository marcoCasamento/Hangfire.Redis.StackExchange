using System;

namespace Hangfire.Redis
{
    public class HangFireRedisException : Exception
    {
        public HangFireRedisException(string message) : base(message)
        {
        }
    }
}
using System;
using Hangfire.Redis.StackExchange;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisStorageOptionsFacts
    {

        [Fact]
        public void InvisibilityTimeout_HasDefaultValue()
        {
            var options = CreateOptions();
            Assert.Equal(TimeSpan.FromMinutes(30), options.InvisibilityTimeout);
        }

        private static RedisStorageOptions CreateOptions()
        {
            return new RedisStorageOptions();
        }
    }
}

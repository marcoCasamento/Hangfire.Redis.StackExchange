using Hangfire.Redis.Tests.Utils;
using StackExchange.Redis;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisTest
    {
        private readonly IDatabase _redis;

        public RedisTest()
        {
            _redis = RedisUtils.CreateClient();
        }


        [Fact, CleanRedis]
        public void RedisSampleTest()
        {
            var defaultValue = _redis.StringGet("samplekey");
            Assert.True(defaultValue.IsNull);
        }
    }
}
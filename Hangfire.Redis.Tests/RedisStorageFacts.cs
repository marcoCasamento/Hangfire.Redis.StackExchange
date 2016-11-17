using System;
using System.Linq;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisStorageFacts
    {
        [Fact, CleanRedis]
        public void GetStateHandlers_ReturnsAllHandlers()
        {
            var storage = CreateStorage();

            var handlers = storage.GetStateHandlers();

            var handlerTypes = handlers.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(FailedStateHandler), handlerTypes);
            Assert.Contains(typeof(ProcessingStateHandler), handlerTypes);
            Assert.Contains(typeof(SucceededStateHandler), handlerTypes);
            Assert.Contains(typeof(DeletedStateHandler), handlerTypes);
        }

        [Fact]
        public void DbFromConnectionStringIsUsed()
        {
            var storage = new RedisStorage(String.Format("{0},defaultDatabase=5", RedisUtils.GetHostAndPort()));
            Assert.Equal(storage.Db, 5);
        }

        private RedisStorage CreateStorage()
        {
            var options = new RedisStorageOptions() { Db = RedisUtils.GetDb() };
            return new RedisStorage(RedisUtils.GetHostAndPort(), options);
        }
    }
}

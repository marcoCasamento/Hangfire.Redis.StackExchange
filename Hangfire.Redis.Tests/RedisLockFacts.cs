using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisLockFacts
    {
        [Fact, CleanRedis]
        public void AcquireLock()
        {
            var storage = new RedisStorage();
            var cnn = storage.GetConnection();

            var testLock = cnn.AcquireDistributedLock("testLock", TimeSpan.FromSeconds(1));
            Assert.NotNull(testLock);
        }

        [Fact, CleanRedis]
        public void AcquireAndDisposeLock()
        {
            var storage = new RedisStorage();
            var cnn = storage.GetConnection();

            for (int i = 0; i < 5; i++)
            {
                var testLock = cnn.AcquireDistributedLock("testLock", TimeSpan.FromSeconds(1));
                testLock.Dispose();
                Assert.NotNull(testLock); 
            }
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisLockFacts
    {

        [Fact, CleanRedis]
        public void AcquireAndDisposeLock()
        {
            var db = RedisUtils.CreateClient();
            
            using (var testLock = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(1)))
                Assert.NotNull(testLock);
            using (var testLock = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(1)))
                Assert.NotNull(testLock);
        }

        [Fact, CleanRedis]
        public void ExtendsALock()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(100)))
            {
                Assert.NotNull(testLock);
                using (var testLock2 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(10)))
                    Assert.NotNull(testLock2);
            }
        }

        [Fact, CleanRedis]
        public void GetLockAfterAnotherTimeout()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(50)))
            {
                Thread.Sleep(60);
                using (var testLock2 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(10)))
                    Assert.NotNull(testLock2);
            }
        }

        [Fact, CleanRedis]
        public void TwoContendingLocks()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock1 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(100)))
                Assert.Throws<TimeoutException>(() => new RedisLock(db, "testLock", "otherOwner", TimeSpan.FromMilliseconds(10)));
        }

        [Fact, CleanRedis]
        public void NestLock()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock1 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(100)))
            {
                using (var testLock2 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(100)))
                { }

            }
        }

        [Fact, CleanRedis]
        public void NestLockDisposePreservesRoot()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock1 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(1000)))
            {
                using (var testLock2 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(1000)))
                { }

                Assert.Throws<TimeoutException>(() =>
                {
                    using (var testLock2 = new RedisLock(db, "testLock", "otherOwner", TimeSpan.FromMilliseconds(1))) { };
                });
            }
        }

        [Fact, CleanRedis]
        public void ReleaseNestedAfterTimeout()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock1 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(50)))
            {
                using (var testLock2 = new RedisLock(db, "testLock", "owner", TimeSpan.FromMilliseconds(1)))
                { }

                Thread.Sleep(60);
                using (var testLock2 = new RedisLock(db, "testLock", "otherOwner", TimeSpan.FromMilliseconds(1))) { };
                Assert.True(true);
            }
        }
    }
}

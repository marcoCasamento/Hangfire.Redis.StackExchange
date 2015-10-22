using Hangfire.Annotations;
using StackExchange.Redis;
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
            {
                Assert.Throws<TimeoutException>(() =>
                    {
                        using (var testLock2 = new RedisLock(db, "testLock", "otherOwner", TimeSpan.FromMilliseconds(1)))
                        { }
                    }
                );
            }
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

        [Fact, CleanRedis]
        public void MultiThreadLock()
        {
            var db = RedisUtils.CreateClient();

            int concurrentCount = 0;
            Task[] tasks = new Task[2];
            tasks[0] = Task.Factory.StartNew(() => MultiThreadLockWorker.DoWork(db, "testLock", TimeSpan.FromMilliseconds(300), TimeSpan.FromMilliseconds(300), ref concurrentCount));
            tasks[1] = Task.Factory.StartNew(() => MultiThreadLockWorker.DoWork(db, "testLock", TimeSpan.FromMilliseconds(5), TimeSpan.FromMilliseconds(10), ref concurrentCount));
            Task.WaitAll(tasks);
            Assert.Equal<int>(1, concurrentCount);
        }
    }

    internal class MultiThreadLockWorker
    {
        internal static void DoWork([NotNull]IDatabase redis, [NotNull]RedisKey key, [NotNull]TimeSpan timeOut, [NotNull]TimeSpan sleep, ref int concurrentCount)
        {
            try
            {
                using (var testLock = new RedisLock(redis, key, Thread.CurrentThread.ManagedThreadId.ToString(), timeOut))
                {
                    Thread.Sleep(sleep);
                    concurrentCount++;
                };
            }
            catch(TimeoutException)
            {
                
            }
            catch(Exception)
            {
                throw;
            }
        }
    }
}

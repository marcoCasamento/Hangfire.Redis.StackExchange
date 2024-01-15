using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Redis.StackExchange;
using Hangfire.Redis.Tests.Utils;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisLockFacts
    {
        [Fact, CleanRedis]
        public void AcquireInSequence()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock = RedisLock.Acquire(db, "testLock", TimeSpan.FromMilliseconds(1)))
                Assert.NotNull(testLock);
            using (var testLock = RedisLock.Acquire(db, "testLock", TimeSpan.FromMilliseconds(1)))
                Assert.NotNull(testLock);
        }

        [Fact, CleanRedis]
        public void AcquireNested()
        {
            var db = RedisUtils.CreateClient();

            using (var testLock1 = RedisLock.Acquire(db, "testLock", TimeSpan.FromMilliseconds(100)))
            {
                Assert.NotNull(testLock1);
                
                using (var testLock2 = RedisLock.Acquire(db, "testLock", TimeSpan.FromMilliseconds(100)))
                {
                    Assert.NotNull(testLock2);
                }
            }
        }

        [Fact, CleanRedis]
        public void AcquireFromMultipleThreads()
        {
            var db = RedisUtils.CreateClient();

            var sync = new ManualResetEventSlim();

            var thread1 = new Thread(state =>
            {
                using (var testLock1 = RedisLock.Acquire(db, "test", TimeSpan.FromMilliseconds(50)))
                {
                    // ensure nested lock release doesn't release parent lock
                    using (var testLock2 = RedisLock.Acquire(db, "test", TimeSpan.FromMilliseconds(50)))
                    {
                    }

                    sync.Set();
                    Thread.Sleep(200);
                }
            });

            var thread2 = new Thread(state =>
            {
                Assert.True(sync.Wait(1000));

                Assert.Throws<DistributedLockTimeoutException>(() =>
                {
                    using (var testLock2 = RedisLock.Acquire(db, "test", TimeSpan.FromMilliseconds(50)))
                    {
                    }
                });
            });

            thread1.Start();
            thread2.Start();
            thread1.Join();
            thread2.Join();
        }

        //private async Task NestedTask(IDatabase db)
        //{

        //    await Task.Yield();
        //    using var lestLock2 = RedisLock.Acquire(db, "test", TimeSpan.FromMilliseconds(10));
        //    Assert.NotNull(lestLock2);
        //}

        //Test below will fail due to the changed storage model of HeldLocks in RedisLock (it should be AsyncLocal instead of ThreadLocal)
        //It seems that something is wrong when run in hangfire but I don't have a repro. 
        //Giving that Hangfire as of 1.8.7 still doesn't use Tasks (milestone set for 2.0) I'm leaving HeldLocks in a ThreadLocal
        //and commenting out the failing test

        //[Fact, CleanRedis]
        //public async Task AcquireFromNestedTask()
        //{
        //    var db = RedisUtils.CreateClient();

        //    using var lock1 = RedisLock.Acquire(db, "test", TimeSpan.FromMilliseconds(50));
        //    Assert.NotNull(lock1);

        //    await Task.Delay(100);

        //    await Task.Run(() => NestedTask(db));
            
        //}

        [Fact, CleanRedis]
        public void SlidingExpirationTest()
        {
            var db = RedisUtils.CreateClient();

            var sync1 = new ManualResetEventSlim();
            var sync2 = new ManualResetEventSlim();

            var thread1 = new Thread(state =>
            {
                using (var testLock1 = RedisLock.Acquire(db, "testLock", TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(110)))
                {
                    Assert.NotNull(testLock1);

                    // sleep a bit more than holdDuration
                    Thread.Sleep(250);
                    sync1.Set();
                    sync2.Wait();
                }
            });

            var thread2 = new Thread(state =>
            {
                Assert.True(sync1.Wait(1000));

                Assert.Throws<DistributedLockTimeoutException>(() =>
                {
                    using (var testLock2 = RedisLock.Acquire(db, "testLock", TimeSpan.FromMilliseconds(100)))
                    {
                    }
                });
            });

            thread1.Start();
            thread2.Start();
            thread2.Join();
            sync2.Set();
            thread1.Join();
        }
    }
}
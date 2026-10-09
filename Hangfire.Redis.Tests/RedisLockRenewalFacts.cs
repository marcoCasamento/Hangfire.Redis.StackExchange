using System;
using System.Threading;
using System.Collections.Generic;
using Hangfire.Redis.StackExchange;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Redis.Tests.Utils;
using Moq;
using StackExchange.Redis;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisLockRenewalFacts
    {
        [Theory, CleanRedis]
        [InlineData(false)]
        [InlineData(true)]
        public void StopsRenewingAfterOwnershipIsLost(bool replaceOwner)
        {
            var redis = RedisUtils.CreateClient();
            var observer = new RenewalObserver(redis);
            var lease = RedisLock.Acquire(observer.Database, "renewal-loss", TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(200));
            try
            {
                if (replaceOwner) redis.StringSet("renewal-loss", "another-owner", TimeSpan.FromSeconds(30));
                else redis.KeyDelete("renewal-loss");
                Assert.True(observer.Lost.Wait(TimeSpan.FromSeconds(5)));
                Thread.Sleep(500); // Observe multiple timer periods, not just the first failed call.
                Assert.Equal(1, observer.LostCalls);
                if (replaceOwner) Assert.Equal("another-owner", redis.StringGet("renewal-loss"));
                else Assert.False(redis.KeyExists("renewal-loss"));
            }
            finally
            {
                observer.Stop(lease);
            }
        }

        [Fact]
        public void DoesNotOverlapRenewalsWhileThePreviousCommandIsPending()
        {
            var entered = new ManualResetEventSlim();
            var release = new ManualResetEventSlim();
            var overlap = new ManualResetEventSlim();
            var active = 0;
            var database = new Mock<IDatabase>();
            database.Setup(db => db.LockTake(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan>(), It.IsAny<CommandFlags>())).Returns(true);
            database.Setup(db => db.LockRelease(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                It.IsAny<CommandFlags>())).Returns(true);
            database.Setup(db => db.LockExtend(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan>(), It.IsAny<CommandFlags>())).Returns(() =>
                {
                    if (Interlocked.Increment(ref active) > 1) overlap.Set();
                    entered.Set();
                    try
                    {
                        release.Wait(TimeSpan.FromSeconds(5));
                        return true;
                    }
                    finally
                    {
                        Interlocked.Decrement(ref active);
                    }
                });
            var lease = RedisLock.Acquire(database.Object, "pending-renewal", TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(200));
            try
            {
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                Assert.False(overlap.Wait(TimeSpan.FromMilliseconds(500)));
            }
            finally
            {
                lease.Dispose();
                release.Set();
                Assert.True(SpinWait.SpinUntil(() => Volatile.Read(ref active) == 0, TimeSpan.FromSeconds(5)));
            }
        }

        [Fact, CleanRedis]
        public void DisposalStopsAnInFlightRenewalAfterItReturnsFalse()
        {
            var redis = RedisUtils.CreateClient();
            var entered = new ManualResetEventSlim();
            var release = new ManualResetEventSlim();
            var observer = new RenewalObserver(redis)
            {
                BeforeExtend = () =>
                {
                    entered.Set();
                    release.Wait(TimeSpan.FromSeconds(5));
                }
            };
            var lease = RedisLock.Acquire(observer.Database, "disposed-renewal", TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(200));
            try
            {
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                lease.Dispose(); // The real Redis key is released while renewal is in flight.
                release.Set();
                Assert.True(observer.Lost.Wait(TimeSpan.FromSeconds(5)));
                Thread.Sleep(500);
                Assert.Equal(1, observer.LostCalls);
                Assert.False(redis.KeyExists("disposed-renewal"));
            }
            finally
            {
                release.Set();
                observer.Stop(lease);
            }
        }

        [Theory, CleanRedis]
        [InlineData(40)]
        [InlineData(80)]
        public void ConcurrentLostLeasesStopRenewing(int count)
        {
            var redis = RedisUtils.CreateClient();
            var observers = new RenewalObserver[count];
            var leases = new IDisposable[count];
            try
            {
                for (var i = 0; i < count; i++)
                {
                    observers[i] = new RenewalObserver(redis);
                    var key = "lost-lease-" + i;
                    leases[i] = RedisLock.Acquire(observers[i].Database, key, TimeSpan.FromSeconds(1),
                        TimeSpan.FromMilliseconds(200));
                    redis.KeyDelete(key);
                }
                foreach (var observer in observers)
                    Assert.True(observer.Lost.Wait(TimeSpan.FromSeconds(5)));
                Thread.Sleep(500);
                foreach (var observer in observers)
                    Assert.Equal(1, observer.LostCalls);
            }
            finally
            {
                for (var i = 0; i < count; i++)
                    if (leases[i] != null) observers[i].Stop(leases[i]);
            }
        }

        [Theory, CleanRedis]
        [InlineData(false)]
        [InlineData(true)]
        public void LostRenewalOnlyFailsAJobThatIsStillProcessing(bool alreadySucceeded)
        {
            var redis = RedisUtils.CreateClient();
            var storage = new RedisStorage(RedisUtils.GetHostAndPort(),
                new RedisStorageOptions { Db = RedisUtils.GetDb() });
            var client = new BackgroundJobClient(storage);
            var job = Job.FromExpression(() => RenewalTestJob.Execute());
            var processing = new Mock<IState>();
            processing.Setup(state => state.Name).Returns(ProcessingState.StateName);
            processing.Setup(state => state.SerializeData()).Returns(new Dictionary<string, string>
            {
                { "ServerId", "renewal-test-server" }
            });
            var jobId = client.Create(job, processing.Object);
            using (var connection = storage.GetConnection())
            {
                var context = new PerformingContext(new PerformContext(storage, connection,
                    new BackgroundJob(jobId, job, DateTime.UtcNow), new Mock<IJobCancellationToken>().Object));
                var subscriber = new HangfireSubscriber();
                subscriber.OnPerforming(context);
                var observer = new RenewalObserver(redis);
                IDisposable lease = null;
                try
                {
                    lease = RedisLock.Acquire(observer.Database, "performing-renewal", TimeSpan.FromSeconds(1),
                        TimeSpan.FromMilliseconds(400));
                    if (alreadySucceeded)
                        Assert.True(client.ChangeState(jobId, new SucceededState(null, 0, 0)));
                    redis.KeyDelete("performing-renewal");
                    Assert.True(observer.Lost.Wait(TimeSpan.FromSeconds(5)));
                    var expected = alreadySucceeded ? SucceededState.StateName : FailedState.StateName;
                    Assert.True(SpinWait.SpinUntil(() => connection.GetStateData(jobId)?.Name == expected,
                        TimeSpan.FromSeconds(5)));
                    Thread.Sleep(500);
                    Assert.Equal(expected, connection.GetStateData(jobId).Name);
                    Assert.Equal(1, observer.LostCalls);
                }
                finally
                {
                    subscriber.OnPerformed(null);
                    if (lease != null) observer.Stop(lease);
                }
            }
        }

        [Fact]
        public void RetriesATransientRenewalExceptionWithoutOverlappingCommands()
        {
            var first = new ManualResetEventSlim();
            var recovered = new ManualResetEventSlim();
            var calls = 0;
            var database = CreateRenewalDatabase(() =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    first.Set();
                    throw new TimeoutException("Simulated Redis timeout.");
                }
                recovered.Set();
                return true;
            });
            using (var lease = RedisLock.Acquire(database, "transient-renewal", TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(200)))
            {
                Assert.True(first.Wait(TimeSpan.FromSeconds(5)));
                Thread.Sleep(500);
                Assert.Equal(1, Volatile.Read(ref calls));
                Assert.True(recovered.Wait(TimeSpan.FromSeconds(5)));
            }
        }

        [Fact]
        public void StopsAfterTheExistingExceptionRetryBudgetOutsideAJob()
        {
            var exhausted = new ManualResetEventSlim();
            var calls = 0;
            var cleanup = 0;
            var database = CreateRenewalDatabase(() =>
            {
                if (Volatile.Read(ref cleanup) != 0) return true;
                if (Interlocked.Increment(ref calls) == 11) exhausted.Set();
                throw new TimeoutException("Simulated persistent Redis timeout.");
            });
            var lease = RedisLock.Acquire(database, "exhausted-renewal", TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(200));
            try
            {
                Assert.True(exhausted.Wait(TimeSpan.FromSeconds(45)));
                Thread.Sleep(3500); // Existing retry delay is three seconds, including the final failed attempt.
                Assert.Equal(11, Volatile.Read(ref calls));
            }
            finally
            {
                Volatile.Write(ref cleanup, 1);
                lease.Dispose();
            }
        }

        [Fact]
        public void DoesNotRetryAnInFlightExceptionAfterDisposal()
        {
            var entered = new ManualResetEventSlim();
            var release = new ManualResetEventSlim();
            var calls = 0;
            var cleanup = 0;
            var database = CreateRenewalDatabase(() =>
            {
                Interlocked.Increment(ref calls);
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(5));
                if (Volatile.Read(ref cleanup) != 0) return true;
                throw new TimeoutException("Simulated in-flight Redis timeout.");
            });
            var lease = RedisLock.Acquire(database, "disposed-exception", TimeSpan.FromSeconds(1),
                TimeSpan.FromMilliseconds(200));
            try
            {
                Assert.True(entered.Wait(TimeSpan.FromSeconds(5)));
                lease.Dispose();
                release.Set();
                Thread.Sleep(3500); // Observe beyond the retry interval.
                Assert.Equal(1, Volatile.Read(ref calls));
            }
            finally
            {
                Volatile.Write(ref cleanup, 1);
                release.Set();
                lease.Dispose();
            }
        }

        private static IDatabase CreateRenewalDatabase(Func<bool> renewal)
        {
            var database = new Mock<IDatabase>();
            database.Setup(db => db.LockTake(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan>(), It.IsAny<CommandFlags>())).Returns(true);
            database.Setup(db => db.LockRelease(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                It.IsAny<CommandFlags>())).Returns(true);
            database.Setup(db => db.LockExtend(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                It.IsAny<TimeSpan>(), It.IsAny<CommandFlags>())).Returns(renewal);
            return database.Object;
        }

        private sealed class RenewalObserver
        {
            private int _cleanup;
            private int _lostCalls;
            public readonly ManualResetEventSlim Lost = new ManualResetEventSlim();
            public IDatabase Database { get; }
            public Action BeforeExtend { get; set; }
            public int LostCalls => Volatile.Read(ref _lostCalls);

            public RenewalObserver(IDatabase redis)
            {
                var database = new Mock<IDatabase>();
                database.Setup(db => db.LockTake(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                    It.IsAny<TimeSpan>(), It.IsAny<CommandFlags>()))
                    .Returns((RedisKey key, RedisValue owner, TimeSpan duration, CommandFlags flags) =>
                        redis.LockTake(key, owner, duration, flags));
                database.Setup(db => db.LockRelease(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                    It.IsAny<CommandFlags>()))
                    .Returns((RedisKey key, RedisValue owner, CommandFlags flags) =>
                        redis.LockRelease(key, owner, flags));
                database.Setup(db => db.LockExtend(It.IsAny<RedisKey>(), It.IsAny<RedisValue>(),
                    It.IsAny<TimeSpan>(), It.IsAny<CommandFlags>()))
                    .Returns((RedisKey key, RedisValue owner, TimeSpan duration, CommandFlags flags) =>
                    {
                        // Only cleanup rescues an old, spinning implementation after the assertion failed.
                        if (Volatile.Read(ref _cleanup) != 0) return true;
                        BeforeExtend?.Invoke();
                        var extended = redis.LockExtend(key, owner, duration, flags);
                        if (!extended)
                        {
                            Interlocked.Increment(ref _lostCalls);
                            Lost.Set();
                        }
                        return extended;
                    });
                Database = database.Object;
            }

            public void Stop(IDisposable lease)
            {
                Volatile.Write(ref _cleanup, 1);
                lease.Dispose();
            }
        }
    }
    public static class RenewalTestJob
    {
        [AutomaticRetry(Attempts = 0)]
        public static void Execute()
        {
        }
    }
}

using System;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [CleanRedis]
    public class ExpiredJobsWatcherFacts
    {
        private static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(1);

        private readonly RedisStorage _storage;
        private readonly CancellationTokenSource _cts;

        public ExpiredJobsWatcherFacts()
        {
            var options = new RedisStorageOptions() {Db = RedisUtils.GetDb()};
            _storage = new RedisStorage(RedisUtils.GetHostAndPort(), options);
            _cts = new CancellationTokenSource();
            _cts.Cancel();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new ExpiredJobsWatcher(null, CheckInterval));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsZero()
        {
            Assert.Throws<ArgumentOutOfRangeException>("checkInterval",
                () => new ExpiredJobsWatcher(_storage, TimeSpan.Zero));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsNegative()
        {
            Assert.Throws<ArgumentOutOfRangeException>("checkInterval",
                () => new ExpiredJobsWatcher(_storage, TimeSpan.FromSeconds(-1)));
        }

        [Fact, CleanRedis]
        public void Execute_DeletesNonExistingJobs()
        {
            var redis = RedisUtils.CreateClient();

            Assert.Equal(0, redis.ListLength("{hangfire}:succeeded"));
            Assert.Equal(0, redis.ListLength("{hangfire}:deleted"));

            // Arrange
            redis.ListRightPush("{hangfire}:succeded", "my-job");
            redis.ListRightPush("{hangfire}:deleted", "other-job");

            var watcher = CreateWatcher();

            // Act
            watcher.Execute(_cts.Token);

            // Assert
            Assert.Equal(0, redis.ListLength("{hangfire}:succeeded"));
            Assert.Equal(0, redis.ListLength("{hangfire}:deleted"));
        }

        [Fact, CleanRedis]
        public void Execute_DoesNotDeleteExistingJobs()
        {
            var redis = RedisUtils.CreateClient();
            // Arrange
            redis.ListRightPush("{hangfire}:succeeded", "my-job");
            redis.HashSet("{hangfire}:job:my-job", "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));

            redis.ListRightPush("{hangfire}:deleted", "other-job");
            redis.HashSet("{hangfire}:job:other-job", "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow.AddDays(-1)));

            var watcher = CreateWatcher();

            // Act
            watcher.Execute(_cts.Token);

            // Assert
            Assert.Equal(1, redis.ListLength("{hangfire}:succeeded"));
            Assert.Equal(1, redis.ListLength("{hangfire}:deleted"));
        }

#pragma warning disable 618
        private IServerComponent CreateWatcher()
#pragma warning restore 618
        {
            return new ExpiredJobsWatcher(_storage, CheckInterval);
        }
    }
}
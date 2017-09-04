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
            var options = new RedisStorageOptions() { Db = RedisUtils.GetDb() };
            _storage = new RedisStorage(RedisUtils.GetHostAndPort(), options);
			_cts = new CancellationTokenSource();
			_cts.Cancel();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new ExpiredJobsWatcher(null, CheckInterval));

            Assert.Equal("storage", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsZero()
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(
                () => new ExpiredJobsWatcher(_storage, TimeSpan.Zero));

            Assert.Equal("checkInterval", exception.ParamName);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenCheckIntervalIsNegative()
        {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(
                () => new ExpiredJobsWatcher(_storage, TimeSpan.FromSeconds(-1)));

            Assert.Equal("checkInterval", exception.ParamName);
        }

        [Fact, CleanRedis]
        public void Execute_DeletesNonExistingJobs()
        {
            var redis = RedisUtils.CreateClient();
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
        
        private IServerComponent CreateWatcher()
        {
            return new ExpiredJobsWatcher(_storage, CheckInterval);
        }
    }
}

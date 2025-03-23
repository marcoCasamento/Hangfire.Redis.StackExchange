using System;
using Hangfire.Common;
using Hangfire.Redis.StackExchange;
using Hangfire.Redis.Tests.Utils;
using Moq;
using Xunit;
using StackExchange.Redis;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisFetchedJobFacts
    {
        private const string JobId = "id";
        private const string Queue = "queue";

        private readonly RedisStorage _storage;
        private readonly Mock<IDatabase> _redis;

        public RedisFetchedJobFacts()
        {
            _redis = new Mock<IDatabase>();

            var options = new RedisStorageOptions() { Db = RedisUtils.GetDb() };
            _storage = new RedisStorage(RedisUtils.GetHostAndPort(), options);
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
           Assert.Throws<ArgumentNullException>("storage",
               () => new RedisFetchedJob(null, _redis.Object, JobId, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenRedisIsNull()
        {
            Assert.Throws<ArgumentNullException>("redis",
                () => new RedisFetchedJob(_storage, null, JobId, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenJobIdIsNull()
        {
            Assert.Throws<ArgumentNullException>("jobId",
                () => new RedisFetchedJob(_storage, _redis.Object, null, Queue, DateTime.UtcNow));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenQueueIsNull()
        {
            Assert.Throws<ArgumentNullException>("queue",
                () => new RedisFetchedJob(_storage, _redis.Object, JobId, null, DateTime.UtcNow));
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesJobFromTheFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                
                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(0, redis.ListLength("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesOnlyJobWithTheSpecifiedId()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "job-id");
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "another-job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                redis.HashSet("{hangfire}:job:another-job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.ListLength("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("another-job-id", (string)redis.ListRightPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_DoesNotRemoveIfFetchedDoesntMatch()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "job-id");

                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:job-id", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "job-id", "my-queue", fetchedAt + TimeSpan.FromSeconds(1));

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.Equal(1, redis.ListLength("{hangfire}:queue:my-queue:dequeued"));
                Assert.Equal("job-id", (string)redis.ListRightPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HashExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromQueue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HashSet("{hangfire}:job:my-job", "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", null);

                // Act
                fetchedJob.RemoveFromQueue();

                // Assert
                Assert.False(redis.HashExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobBackToQueue()
        {
            UseRedis(redis => 
            {
                // Arrange
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal("my-job", (string)redis.ListRightPop("{hangfire}:queue:my-queue"));
                Assert.Null((string)redis.ListLeftPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_PushesAJobToTheRightSide()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.ListRightPush("{hangfire}:queue:my-queue", "another-job");
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));


                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert - RPOP
                Assert.Equal("my-job", (string)redis.ListRightPop("{hangfire}:queue:my-queue"));
                Assert.Null((string)redis.ListRightPop("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesAJobFromFetchedList()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));

                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.Equal(0, redis.ListLength("{hangfire}:queue:my-queue:dequeued"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheFetchedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                var fetchedAt = DateTime.UtcNow;
                redis.HashSet("{hangfire}:job:my-job", "Fetched", JobHelper.SerializeDateTime(fetchedAt));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", fetchedAt);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HashExists("{hangfire}:job:my-job", "Fetched"));
            });
        }

        [Fact, CleanRedis]
        public void Requeue_RemovesTheCheckedFlag()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.HashSet("{hangfire}:job:my-job", "Checked", JobHelper.SerializeDateTime(DateTime.UtcNow));
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", null);

                // Act
                fetchedJob.Requeue();

                // Assert
                Assert.False(redis.HashExists("{hangfire}:job:my-job", "Checked"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_WithNoComplete_RequeuesAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", DateTime.UtcNow);

                // Act
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(1, redis.ListLength("{hangfire}:queue:my-queue"));
            });
        }

        [Fact, CleanRedis]
        public void Dispose_AfterRemoveFromQueue_DoesNotRequeueAJob()
        {
            UseRedis(redis =>
            {
                // Arrange
				redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                redis.ListRightPush("{hangfire}:queue:my-queue:dequeued", "my-job");
                var fetchedJob = new RedisFetchedJob(_storage, redis, "my-job", "my-queue", DateTime.UtcNow);

                // Act
                fetchedJob.RemoveFromQueue();
                fetchedJob.Dispose();

                // Assert
                Assert.Equal(0, redis.ListLength("{hangfire}:queue:my-queue"));
            });
        }

        private static void UseRedis(Action<IDatabase> action)
        {
			var redis = RedisUtils.CreateClient();
            action(redis);
        }
    }
}

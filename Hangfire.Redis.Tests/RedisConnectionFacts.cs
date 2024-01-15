using StackExchange.Redis;
using System;
using System.Collections.Generic;
using Hangfire.Redis.StackExchange;
using Hangfire.Redis.Tests.Utils;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [Collection("Sequential")]
    public class RedisConnectionFacts
    {
        private readonly RedisStorage _storage;

        public RedisConnectionFacts()
        {
            var options = new RedisStorageOptions() {Db = RedisUtils.GetDb()};
            _storage = new RedisStorage(RedisUtils.GetHostAndPort(), options);
        }

        [Fact, CleanRedis]
        public void GetStateData_ThrowsAnException_WhenJobIdIsNull()
        {
            UseConnection(
                connection => Assert.Throws<ArgumentNullException>("jobId",
                    () => connection.GetStateData(null)));
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsNull_WhenJobDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetStateData("random-id");
                Assert.Null(result);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsCorrectResult()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet(
                    "{hangfire}:job:my-job:state",
                    new Dictionary<string, string>
                    {
                        {"State", "Name"},
                        {"Reason", "Reason"},
                        {"Key", "Value"}
                    }.ToHashEntries());

                var result = connection.GetStateData("my-job");

                Assert.NotNull(result);
                Assert.Equal("Name", result.Name);
                Assert.Equal("Reason", result.Reason);
                Assert.Equal("Value", result.Data["Key"]);
            });
        }

        [Fact, CleanRedis]
        public void GetStateData_ReturnsNullReason_IfThereIsNoSuchKey()
        {
            UseConnections((redis, connection) =>
            {
                redis.HashSet(
                    "{hangfire}:job:my-job:state",
                    new Dictionary<string, string>
                    {
                        {"State", "Name"}
                    }.ToHashEntries());

                var result = connection.GetStateData("my-job");

                Assert.NotNull(result);
                Assert.Null(result.Reason);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.GetAllItemsFromSet(null)));
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ReturnsEmptyCollection_WhenSetDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllItemsFromSet("some-set");

                Assert.NotNull(result);
                Assert.Empty(result);
            });
        }

        [Fact, CleanRedis]
        public void GetAllItemsFromSet_ReturnsAllItems()
        {
            UseConnections((redis, connection) =>
            {
                // Arrange
                redis.SortedSetAdd("{hangfire}:some-set", "1", 0);
                redis.SortedSetAdd("{hangfire}:some-set", "2", 0);

                // Act
                var result = connection.GetAllItemsFromSet("some-set");

                // Assert
                Assert.Equal(2, result.Count);
                Assert.Contains("1", result);
                Assert.Contains("2", result);
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => connection.SetRangeInHash(null, new Dictionary<string, string>()));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(connection =>
            {
                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => connection.SetRangeInHash("some-hash", null));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_SetsAllGivenKeyPairs()
        {
            UseConnections((redis, connection) =>
            {
                connection.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                });

                var hash = redis.HashGetAll("{hangfire}:some-hash").ToStringDictionary();
                Assert.Equal("Value1", hash["Key1"]);
                Assert.Equal("Value2", hash["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(connection =>
                Assert.Throws<ArgumentNullException>(() => connection.GetAllEntriesFromHash(null)));
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ReturnsNullValue_WhenHashDoesNotExist()
        {
            UseConnection(connection =>
            {
                var result = connection.GetAllEntriesFromHash("some-hash");
                Assert.Null(result);
            });
        }

        [Fact, CleanRedis]
        public void GetAllEntriesFromHash_ReturnsAllEntries()
        {
            UseConnections((redis, connection) =>
            {
                // Arrange
                redis.HashSet("{hangfire}:some-hash", new Dictionary<string, string>
                {
                    {"Key1", "Value1"},
                    {"Key2", "Value2"}
                }.ToHashEntries());

                // Act
                var result = connection.GetAllEntriesFromHash("some-hash");

                // Assert
                Assert.NotNull(result);
                Assert.Equal("Value1", result["Key1"]);
                Assert.Equal("Value2", result["Key2"]);
            });
        }
        
        [Fact]
        public void GetUtcDateTime_ReturnsValidDateTime()
        {
            UseConnections((redis, connection) =>
            {
                var result = connection.GetUtcDateTime();

                Assert.NotEqual(default, result);
            });
        }

        [Fact]
        public void SetCount_ReturnZeroIfSetDoesNotExists()
        {
            UseConnections((redis, connection) =>
            {
                var result = connection.GetSetCount("some-set");

                Assert.Equal(0, result);
            });
        }

        [Fact, CleanRedis]
        public void SetCount_ReturnNumberOfItems()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("{hangfire}:some-set", "1", 0);
                redis.SortedSetAdd("{hangfire}:some-set", "2", 0);

                var result = connection.GetSetCount("some-set");

                Assert.Equal(2, result);
            });
        }

        [Fact, CleanRedis]
        public void SetContains_ReturnTrueIfContained()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("{hangfire}:some-set", "1", 0);

                var result = connection.GetSetContains("some-set", "1");

                Assert.True(result);
            });
        }

        [Fact, CleanRedis]
        public void SetContains_ReturnFalseIfNotContained()
        {
            UseConnections((redis, connection) =>
            {
                redis.SortedSetAdd("{hangfire}:some-set", "1", 0);

                var result = connection.GetSetContains("some-set", "0");

                Assert.False(result);
            });
        }

        private void UseConnections(Action<IDatabase, RedisConnection> action)
        {
            var redis = RedisUtils.CreateClient();
            var subscription = new RedisSubscription(_storage, RedisUtils.CreateSubscriber());
            var server = RedisUtils.GetFirstServer();
            using (var connection = new RedisConnection(_storage, server, redis, subscription, new RedisStorageOptions().FetchTimeout))
            {
                action(redis, connection);
            }
        }

        private void UseConnection(Action<RedisConnection> action)
        {
            var redis = RedisUtils.CreateClient();
            var subscription = new RedisSubscription(_storage, RedisUtils.CreateSubscriber());
            var server = RedisUtils.GetFirstServer();

            using (var connection = new RedisConnection(_storage, server, redis, subscription, new RedisStorageOptions().FetchTimeout))
            {
                action(connection);
            }
        }
        
    }
}
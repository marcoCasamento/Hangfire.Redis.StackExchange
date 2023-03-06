using System;
using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.Redis.Tests.Utils;
using Hangfire.States;
using Moq;
using Xunit;
using StackExchange.Redis;

namespace Hangfire.Redis.Tests
{
    public class RedisWriteOnlyTransactionFacts
    {
        private readonly RedisStorage _storage;
        private readonly Mock<ITransaction> _transaction;

        public RedisWriteOnlyTransactionFacts()
        {
            var options = new RedisStorageOptions() { Db = RedisUtils.GetDb() };
            _storage = new RedisStorage(RedisUtils.GetHostAndPort(), options);

            _transaction = new Mock<ITransaction>();
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenStorageIsNull()
        {
            Assert.Throws<ArgumentNullException>("storage",
                () => new RedisWriteOnlyTransaction(null, _transaction.Object));
        }

        [Fact]
        public void Ctor_ThrowsAnException_WhenTransactionIsNull()
        {
            Assert.Throws<ArgumentNullException>("transaction",
                () => new RedisWriteOnlyTransaction(_storage, null));
        }

        [Fact, CleanRedis]
		public void ExpireJob_SetsExpirationDateForAllRelatedKeys()
		{
			UseConnection(redis =>
			{
				// Arrange
				redis.StringSet("{hangfire}:job:my-job", "job");
				redis.StringSet("{hangfire}:job:my-job:state", "state");
				redis.StringSet("{hangfire}:job:my-job:history", "history");

				// Act
				Commit(redis, x => x.ExpireJob("my-job", TimeSpan.FromDays(1)));

				// Assert
				var jobEntryTtl = redis.KeyTimeToLive("{hangfire}:job:my-job");
				var stateEntryTtl = redis.KeyTimeToLive("{hangfire}:job:my-job:state");
				var historyEntryTtl = redis.KeyTimeToLive("{hangfire}:job:my-job:state");

				Assert.True(TimeSpan.FromHours(23) < jobEntryTtl && jobEntryTtl < TimeSpan.FromHours(25));
				Assert.True(TimeSpan.FromHours(23) < stateEntryTtl && stateEntryTtl < TimeSpan.FromHours(25));
				Assert.True(TimeSpan.FromHours(23) < historyEntryTtl && historyEntryTtl < TimeSpan.FromHours(25));
			});
		}

		[Fact, CleanRedis]
		public void SetJobState_ModifiesJobEntry()
		{
			UseConnection(redis =>
			{
				// Arrange
				var state = new Mock<IState>();
				state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string>());
				state.Setup(x => x.Name).Returns("my-state");

				// Act
				Commit(redis, x => x.SetJobState("my-job", state.Object));

				// Assert
				var hash = redis.HashGetAll("{hangfire}:job:my-job").ToStringDictionary();
				Assert.Equal("my-state", hash["State"]);
			});
		}

		[Fact, CleanRedis]
		public void SetJobState_RewritesStateEntry()
		{
			UseConnection(redis =>
			{
				// Arrange
				redis.HashSet("{hangfire}:job:my-job:state", "OldName", "OldValue");

				var state = new Mock<IState>();
				state.Setup(x => x.SerializeData()).Returns(
					new Dictionary<string, string>
					{
						{ "Name", "Value" }
					});
				state.Setup(x => x.Name).Returns("my-state");
				state.Setup(x => x.Reason).Returns("my-reason");

				// Act
				Commit(redis, x => x.SetJobState("my-job", state.Object));

				// Assert
				var stateHash = redis.HashGetAll("{hangfire}:job:my-job:state").ToStringDictionary();
				Assert.False(stateHash.ContainsKey("OldName"));
				Assert.Equal("my-state", stateHash["State"]);
				Assert.Equal("my-reason", stateHash["Reason"]);
				Assert.Equal("Value", stateHash["Name"]);
			});
		}

		[Fact, CleanRedis]
		public void SetJobState_AppendsJobHistoryList()
		{
			UseConnection(redis =>
			{
				// Arrange
				var state = new Mock<IState>();
				state.Setup(x => x.Name).Returns("my-state");
				state.Setup(x => x.SerializeData()).Returns(new Dictionary<string, string>());

				// Act
				Commit(redis, x => x.SetJobState("my-job", state.Object));

				// Assert
				Assert.Equal(1, redis.ListLength("{hangfire}:job:my-job:history"));
			});
		}

		[Fact, CleanRedis]
		public void PersistJob_RemovesExpirationDatesForAllRelatedKeys()
		{
			UseConnection(redis =>
			{
				// Arrange
				redis.StringSet("{hangfire}:job:my-job", "job", TimeSpan.FromDays(1));
				redis.StringSet("{hangfire}:job:my-job:state", "state", TimeSpan.FromDays(1));
				redis.StringSet("{hangfire}:job:my-job:history", "history", TimeSpan.FromDays(1));

				// Act
				Commit(redis, x => x.PersistJob("my-job"));

				// Assert
				Assert.Null(redis.KeyTimeToLive("{hangfire}:job:my-job"));
				Assert.Null(redis.KeyTimeToLive("{hangfire}:job:my-job:state"));
				Assert.Null(redis.KeyTimeToLive("{hangfire}:job:my-job:history"));
			});
		}

		[Fact, CleanRedis]
		public void AddJobState_AddsJobHistoryEntry_AsJsonObject()
		{
			UseConnection(redis =>
			{
				// Arrange
				var state = new Mock<IState>();
				state.Setup(x => x.Name).Returns("my-state");
				state.Setup(x => x.Reason).Returns("my-reason");
				state.Setup(x => x.SerializeData()).Returns(
					new Dictionary<string, string> { { "Name", "Value" } });

				// Act
				Commit(redis, x => x.AddJobState("my-job", state.Object));

				// Assert
				var serializedEntry = redis.ListGetByIndex("{hangfire}:job:my-job:history", 0);
				Assert.NotEqual(RedisValue.Null, serializedEntry);
                

				var entry = SerializationHelper.Deserialize<Dictionary<string, string>>(serializedEntry);
				Assert.Equal("my-state", entry["State"]);
				Assert.Equal("my-reason", entry["Reason"]);
				Assert.Equal("Value", entry["Name"]);
				Assert.True(entry.ContainsKey("CreatedAt"));
			});
		}

		[Fact, CleanRedis]
		public void AddToQueue_AddsSpecifiedJobToTheQueue()
		{
			UseConnection(redis =>
			{
				Commit(redis, x => x.AddToQueue("critical", "my-job"));

				Assert.True(redis.SetContains("{hangfire}:queues", "critical"));
				Assert.Equal("my-job", (string)redis.ListGetByIndex("{hangfire}:queue:critical", 0));
			});
		}

		[Fact, CleanRedis]
		public void AddToQueue_PrependsListWithJob()
		{
			UseConnection(redis =>
			{
				redis.ListLeftPush("{hangfire}:queue:critical", "another-job");

				Commit(redis, x => x.AddToQueue("critical", "my-job"));

				Assert.Equal("my-job", (string)redis.ListGetByIndex("{hangfire}:queue:critical", 0));
			});
		}

		[Fact, CleanRedis]
		public void IncrementCounter_IncrementValueEntry()
		{
			UseConnection(redis =>
			{
				redis.StringSet("{hangfire}:entry", "3");

				Commit(redis, x => x.IncrementCounter("entry"));

				Assert.Equal("4", (string)redis.StringGet("{hangfire}:entry"));
				Assert.Null(redis.KeyTimeToLive("{hangfire}:entry"));
			});
		}

		[Fact, CleanRedis]
		public void IncrementCounter_WithExpiry_IncrementsValueAndSetsExpirationDate()
		{
			UseConnection(redis =>
			{
				redis.StringSet("{hangfire}:entry", "3");

				Commit(redis, x => x.IncrementCounter("entry", TimeSpan.FromDays(1)));

				var entryTtl = redis.KeyTimeToLive("{hangfire}:entry").Value;
				Assert.Equal("4", (string)redis.StringGet("{hangfire}:entry"));
				Assert.True(TimeSpan.FromHours(23) < entryTtl && entryTtl < TimeSpan.FromHours(25));
			});
		}

		[Fact, CleanRedis]
		public void DecrementCounter_DecrementsTheValueEntry()
		{
			UseConnection(redis =>
			{
				redis.StringSet("{hangfire}:entry", "3");

				Commit(redis, x => x.DecrementCounter("entry"));

				Assert.Equal("2", (string)redis.StringGet("{hangfire}:entry"));
				Assert.Null(redis.KeyTimeToLive("{hangfire}:entry"));
			});
		}

        [Fact, CleanRedis]
        public void DecrementCounter_WithExpiry_DecrementsTheValueAndSetsExpirationDate()
        {
            UseConnection(redis =>
            {
                redis.StringSet("{hangfire}:entry", "3");
				Commit(redis, x => x.DecrementCounter("entry", TimeSpan.FromDays(1)));
				var b = redis.KeyTimeToLive("{hangfire}:entry");
                var entryTtl = redis.KeyTimeToLive("{hangfire}:entry").Value;
                Assert.Equal("2", (string)redis.StringGet("{hangfire}:entry"));
                Assert.True(TimeSpan.FromHours(23) < entryTtl && entryTtl < TimeSpan.FromHours(25));
            });
        }

        [Fact, CleanRedis]
        public void AddToSet_AddsItemToSortedSet()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.AddToSet("my-set", "my-value"));

				Assert.True(redis.SortedSetRank("{hangfire}:my-set", "my-value").HasValue);
            });
        }

        [Fact, CleanRedis]
        public void AddToSet_WithScore_AddsItemToSortedSetWithScore()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.AddToSet("my-set", "my-value", 3.2));

				Assert.True(redis.SortedSetRank("{hangfire}:my-set", "my-value").HasValue);
                Assert.Equal(3.2, redis.SortedSetScore("{hangfire}:my-set", "my-value").Value, 3);
					
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromSet_RemoveSpecifiedItemFromSortedSet()
        {
            UseConnection(redis =>
            {
                redis.SortedSetAdd("{hangfire}:my-set", "my-value",0);

                Commit(redis, x => x.RemoveFromSet("my-set", "my-value"));

                Assert.False(redis.SortedSetRank("{hangfire}:my-set", "my-value").HasValue);
            });
        }

        [Fact, CleanRedis]
        public void InsertToList_PrependsListWithSpecifiedValue()
        {
            UseConnection(redis =>
            {
                redis.ListRightPush("{hangfire}:list", "value");

                Commit(redis, x => x.InsertToList("list", "new-value"));
				Assert.Equal("new-value", (string)redis.ListGetByIndex("{hangfire}:list", 0));
            });
        }

        [Fact, CleanRedis]
        public void RemoveFromList_RemovesAllGivenValuesFromList()
        {
            UseConnection(redis =>
            {
				redis.ListRightPush("{hangfire}:list", "value");
				redis.ListRightPush("{hangfire}:list", "another-value");
				redis.ListRightPush("{hangfire}:list", "value");

                Commit(redis, x => x.RemoveFromList("list", "value"));

                Assert.Equal(1, redis.ListLength("{hangfire}:list"));
                Assert.Equal("another-value", (string)redis.ListGetByIndex("{hangfire}:list", 0));
            });
        }

        [Fact, CleanRedis]
        public void TrimList_TrimsListToASpecifiedRange()
        {
            UseConnection(redis =>
            {
                redis.ListRightPush("{hangfire}:list", "1");
				redis.ListRightPush("{hangfire}:list", "2");
				redis.ListRightPush("{hangfire}:list", "3");
				redis.ListRightPush("{hangfire}:list", "4");

                Commit(redis, x => x.TrimList("list", 1, 2));

                Assert.Equal(2, redis.ListLength("{hangfire}:list"));
                Assert.Equal("2", (string)redis.ListGetByIndex("{hangfire}:list", 0));
				Assert.Equal("3", (string)redis.ListGetByIndex("{hangfire}:list", 1));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(redis =>
            {
                Assert.Throws<ArgumentNullException>("key",
                    () => Commit(redis, x => x.SetRangeInHash(null, new Dictionary<string, string>())));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_ThrowsAnException_WhenKeyValuePairsArgumentIsNull()
        {
            UseConnection(redis =>
            {
                Assert.Throws<ArgumentNullException>("keyValuePairs",
                    () => Commit(redis, x => x.SetRangeInHash("some-hash", null)));
            });
        }

        [Fact, CleanRedis]
        public void SetRangeInHash_SetsAllGivenKeyPairs()
        {
            UseConnection(redis =>
            {
                Commit(redis, x => x.SetRangeInHash("some-hash", new Dictionary<string, string>
                {
                    { "Key1", "Value1" },
                    { "Key2", "Value2" }
                }));

                var hash = redis.HashGetAll("{hangfire}:some-hash").ToStringDictionary();
                Assert.Equal("Value1", hash["Key1"]);
                Assert.Equal("Value2", hash["Key2"]);
            });
        }

        [Fact, CleanRedis]
        public void RemoveHash_ThrowsAnException_WhenKeyIsNull()
        {
            UseConnection(redis =>
            {
                Assert.Throws<ArgumentNullException>(
                    () => Commit(redis, x => x.RemoveHash(null)));
            });
        }

        [Fact, CleanRedis]
        public void RemoveHash_RemovesTheCorrespondingEntry()
        {
            UseConnection(redis =>
            {
                redis.HashSet("{hangfire}:some-hash", "key", "value");

                Commit(redis, x => x.RemoveHash("some-hash"));

                var hash = redis.HashGetAll("{hangfire}:some-hash");
                Assert.Empty(hash);
            });
        }

        private void Commit(IDatabase redis, Action<RedisWriteOnlyTransaction> action)
        {
            using (var transaction = new RedisWriteOnlyTransaction(_storage, redis.CreateTransaction()))
            {
                action(transaction);
                transaction.Commit();
            }
        }

        private static void UseConnection(Action<IDatabase> action)
        {
			var redis = RedisUtils.CreateClient();
            action(redis);
        }
    }
}

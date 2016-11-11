// Copyright © 2013-2015 Sergey Odinokov, Marco Casamento 
// This software is based on https://github.com/HangfireIO/Hangfire.Redis 

// Hangfire.Redis.StackExchange is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire.Redis.StackExchange is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire.Redis.StackExchange. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Linq;
using System.Collections.Generic;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Redis
{
    internal class RedisWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly ITransaction _transaction;

        public RedisWriteOnlyTransaction(ITransaction transaction)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");

            _transaction = transaction;
        }
        public override void AddRangeToSet([NotNull]string key, [NotNull]IList<string> items)
        {
            _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, items.Select(x => new SortedSetEntry(x, 0)).ToArray());
        }
        public override void ExpireHash([NotNull]string key, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }
        public override void ExpireList([NotNull]string key, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }
        public override void ExpireSet([NotNull]string key, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }
        public override void PersistHash([NotNull]string key)
        {
            _transaction.KeyPersistAsync(RedisStorage.Prefix + key);
        }
        public override void PersistList([NotNull]string key)
        {
            _transaction.KeyPersistAsync(RedisStorage.Prefix + key);
        }
        public override void PersistSet([NotNull]string key)
        {
            _transaction.KeyPersistAsync(RedisStorage.Prefix + key);
        }
        public override void RemoveSet([NotNull]string key)
        {
            _transaction.KeyDeleteAsync(RedisStorage.Prefix + key);
        }
        public override void Commit()
        {
            if (!_transaction.Execute()) 
            {
                // RedisTransaction.Commit returns false only when
                // WATCH condition has been failed. So, we should 
                // re-play the transaction.

                int replayCount = 1;
                const int maxReplayCount = 3;
                while (!_transaction.Execute())
                {
                    if (replayCount++ >= maxReplayCount)
                    {
						throw new HangFireRedisException("Transaction commit was failed due to WATCH condition failure. Retry attempts exceeded.");
                    }
                }
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
             _transaction.KeyExpireAsync(
                string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                expireIn);

			 _transaction.KeyExpireAsync(
                string.Format(RedisStorage.Prefix + "job:{0}:history", jobId),
                expireIn);

			 _transaction.KeyExpireAsync(
                string.Format(RedisStorage.Prefix + "job:{0}:state", jobId),
                expireIn);
        }

        public override void PersistJob(string jobId)
        {
             _transaction.KeyPersistAsync(string.Format(RedisStorage.Prefix + "job:{0}", jobId));
             _transaction.KeyPersistAsync(string.Format(RedisStorage.Prefix + "job:{0}:history", jobId));
             _transaction.KeyPersistAsync(string.Format(RedisStorage.Prefix + "job:{0}:state", jobId));
        }

        public override void SetJobState(string jobId, IState state)
        {
            _transaction.HashSetAsync(
                string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                "State",
                state.Name);
			_transaction.KeyDeleteAsync(string.Format(RedisStorage.Prefix + "job:{0}:state", jobId));

            var storedData = new Dictionary<string, string>(state.SerializeData());
            storedData.Add("State", state.Name);

            if (state.Reason != null)
            {
                storedData.Add("Reason", state.Reason);
            }
			_transaction.HashSetAsync(string.Format(RedisStorage.Prefix + "job:{0}:state", jobId),storedData.ToHashEntries());

            AddJobState(jobId, state);
        }

        public override void AddJobState(string jobId, IState state)
        {
            var storedData = new Dictionary<string, string>(state.SerializeData());
            storedData.Add("State", state.Name);
            storedData.Add("Reason", state.Reason);
            storedData.Add("CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow));

            _transaction.ListRightPushAsync(
                string.Format(RedisStorage.Prefix + "job:{0}:history", jobId),
                JobHelper.ToJson(storedData));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            _transaction.SetAddAsync(RedisStorage.Prefix + "queues", queue);
            _transaction.ListLeftPushAsync(string.Format(RedisStorage.Prefix + "queue:{0}", queue), jobId);
            _transaction.PublishAsync(RedisSubscription.Channel, jobId);
        }

        public override void IncrementCounter(string key)
        {
             _transaction.StringIncrementAsync(RedisStorage.Prefix + key);
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
			_transaction.StringIncrementAsync(RedisStorage.Prefix + key);
			_transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }

        public override void DecrementCounter(string key)
        {
            _transaction.StringDecrementAsync(RedisStorage.Prefix + key);
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            _transaction.StringDecrementAsync(RedisStorage.Prefix + key);
            _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }

        public override void AddToSet(string key, string value)
        {
            _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, value, 0);
        }

        public override void AddToSet(string key, string value, double score)
        {
             _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, value, score);
        }

        public override void RemoveFromSet(string key, string value)
        {
             _transaction.SortedSetRemoveAsync(RedisStorage.Prefix + key, value);
        }

        public override void InsertToList(string key, string value)
        {
             _transaction.ListLeftPushAsync(RedisStorage.Prefix + key, value);
        }

        public override void RemoveFromList(string key, string value)
        {
             _transaction.ListRemoveAsync(RedisStorage.Prefix + key, value);
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
             _transaction.ListTrimAsync(RedisStorage.Prefix + key, keepStartingFrom, keepEndingAt);
        }

        public override void SetRangeInHash(
            string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

			 _transaction.HashSetAsync(RedisStorage.GetRedisKey(key), keyValuePairs.ToHashEntries());
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            _transaction.KeyDeleteAsync(RedisStorage.GetRedisKey(key));
        }


		public override void Dispose()
		{
			//Don't have to dispose anything
		}
	}
}

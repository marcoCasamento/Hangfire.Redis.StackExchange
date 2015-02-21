// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Linq;
using System.Collections.Generic;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;


namespace Hangfire.Redis
{
    internal class RedisWriteOnlyTransaction : IWriteOnlyTransaction
    {
        private readonly ITransaction _transaction;

        public RedisWriteOnlyTransaction(ITransaction transaction)
        {
            if (transaction == null) throw new ArgumentNullException("transaction");

            _transaction = transaction;
        }

		//public void Dispose()
		//{
		//	_transaction.Dispose();
		//}

        public void Commit()
        {
            if (!_transaction.Execute()) 
            {
                // RedisTransaction.Commit returns false only when
                // WATCH condition has been failed. So, we should 
                // re-play the transaction.

                int replayCount = 1;
                const int maxReplayCount = 3;
				//TODO: check that multiple call to "execute" are legal
                while (!_transaction.Execute()) //.Replay())
                {
                    if (replayCount++ >= maxReplayCount)
                    {
						throw new HangFireRedisException("Transaction commit was failed due to WATCH condition failure. Retry attempts exceeded.");
                    }
                }
            }
        }

        public async void ExpireJob(string jobId, TimeSpan expireIn)
        {
            await _transaction.KeyExpireAsync(
                String.Format(RedisStorage.Prefix + "job:{0}", jobId),
                expireIn);

			await _transaction.KeyExpireAsync(
                String.Format(RedisStorage.Prefix + "job:{0}:history", jobId),
                expireIn);

			await _transaction.KeyExpireAsync(
                String.Format(RedisStorage.Prefix + "job:{0}:state", jobId),
                expireIn);
        }

        public async void PersistJob(string jobId)
        {
            await _transaction.KeyPersistAsync(String.Format(RedisStorage.Prefix + "job:{0}", jobId));
            await _transaction.KeyPersistAsync(String.Format(RedisStorage.Prefix + "job:{0}:history", jobId));
            await _transaction.KeyPersistAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId));
        }

        public void SetJobState(string jobId, IState state)
        {
            _transaction.HashSetAsync(
                String.Format(RedisStorage.Prefix + "job:{0}", jobId),
                "State",
                state.Name);
			_transaction.KeyDeleteAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId));

            var storedData = new Dictionary<string, string>(state.SerializeData());
            storedData.Add("State", state.Name);

            if (state.Reason != null)
            {
                storedData.Add("Reason", state.Reason);
            }
			_transaction.HashSetAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", jobId),storedData.ToHashEntries());

            AddJobState(jobId, state);
        }

        public void AddJobState(string jobId, IState state)
        {
            var storedData = new Dictionary<string, string>(state.SerializeData());
            storedData.Add("State", state.Name);
            storedData.Add("Reason", state.Reason);
            storedData.Add("CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow));

            _transaction.ListRightPushAsync(
                String.Format(RedisStorage.Prefix + "job:{0}:history", jobId),
                JobHelper.ToJson(storedData));
        }

        public async void AddToQueue(string queue, string jobId)
        {
			await _transaction.SetAddAsync(RedisStorage.Prefix + "queues", queue);

			//TODO: Check that ListRightPushAsync semantically means "enqueue"
            await _transaction.ListRightPushAsync(
                String.Format(RedisStorage.Prefix + "queue:{0}", queue), jobId);
        }

        public async void IncrementCounter(string key)
        {
            await _transaction.StringIncrementAsync(RedisStorage.Prefix + key);
        }

        public void IncrementCounter(string key, TimeSpan expireIn)
        {
			_transaction.StringIncrementAsync(RedisStorage.Prefix + key);
			_transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }

        public async void DecrementCounter(string key)
        {
            await _transaction.StringDecrementAsync(RedisStorage.Prefix + key);
        }

        public async void DecrementCounter(string key, TimeSpan expireIn)
        {
            await _transaction.StringDecrementAsync(RedisStorage.Prefix + key);
            await _transaction.KeyExpireAsync(RedisStorage.Prefix + key, expireIn);
        }

        public void AddToSet(string key, string value)
        {
            _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, value, 0);
        }

        public async void AddToSet(string key, string value, double score)
        {
            await _transaction.SortedSetAddAsync(RedisStorage.Prefix + key, value, score);
        }

        public async void RemoveFromSet(string key, string value)
        {
            await _transaction.SortedSetRemoveAsync(RedisStorage.Prefix + key, value);
        }

        public async void InsertToList(string key, string value)
        {
			//TODO check that ListRightPushAsync semantically equals x.EnqueueItemOnList
            await _transaction.ListRightPushAsync(RedisStorage.Prefix + key, value);
        }

        public async void RemoveFromList(string key, string value)
        {
            await _transaction.ListRemoveAsync(RedisStorage.Prefix + key, value);
        }

        public async void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            await _transaction.ListTrimAsync(RedisStorage.Prefix + key, keepStartingFrom, keepEndingAt);
        }

        public async void SetRangeInHash(
            string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

			await _transaction.HashSetAsync(RedisStorage.GetRedisKey(key), keyValuePairs.ToHashEntries());
        }

        public void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            _transaction.KeyDeleteAsync(RedisStorage.GetRedisKey(key));
        }


		public void Dispose()
		{
			//Don't have to dispose anything
		}
	}
}

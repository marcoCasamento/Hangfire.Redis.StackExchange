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
using System.Collections.Generic;
using System.Linq;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Redis.StackExchange
{
    internal class RedisWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly RedisStorage _storage;
        private readonly ITransaction _transaction;
        private readonly List<IDisposable> _lockToDispose = new List<IDisposable>();
        public RedisWriteOnlyTransaction([NotNull] RedisStorage storage, [NotNull] ITransaction transaction)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (transaction == null) throw new ArgumentNullException(nameof(transaction));

            _storage = storage;
            _transaction = transaction;
        }

        public override void AddRangeToSet([NotNull] string key, [NotNull] IList<string> items)
        {
            _transaction.SortedSetAddAsync(_storage.GetRedisKey(key), items.Select(x => new SortedSetEntry(x, 0)).ToArray());
        }
        public override void AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            var distributedLock = _storage.GetConnection().AcquireDistributedLock(resource, timeout);
            _lockToDispose.Add(distributedLock);
        }
        public override void ExpireHash([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(_storage.GetRedisKey(key), expireIn);
        }

        public override void ExpireList([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(_storage.GetRedisKey(key), expireIn);
        }

        public override void ExpireSet([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.KeyExpireAsync(_storage.GetRedisKey(key), expireIn);
        }

        public override void PersistHash([NotNull] string key)
        {
            _transaction.KeyPersistAsync(_storage.GetRedisKey(key));
        }

        public override void PersistList([NotNull] string key)
        {
            _transaction.KeyPersistAsync(_storage.GetRedisKey(key));
        }

        public override void PersistSet([NotNull] string key)
        {
            _transaction.KeyPersistAsync(_storage.GetRedisKey(key));
        }

        public override void RemoveSet([NotNull] string key)
        {
            _transaction.KeyDeleteAsync(_storage.GetRedisKey(key));
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

        public override void ExpireJob([NotNull] string jobId, TimeSpan expireIn)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            _transaction.KeyExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);
            _transaction.KeyExpireAsync(_storage.GetRedisKey($"job:{jobId}:history"), expireIn);
            _transaction.KeyExpireAsync(_storage.GetRedisKey($"job:{jobId}:state"), expireIn);
        }

        public override void PersistJob([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            _transaction.KeyPersistAsync(_storage.GetRedisKey($"job:{jobId}"));
            _transaction.KeyPersistAsync(_storage.GetRedisKey($"job:{jobId}:history"));
            _transaction.KeyPersistAsync(_storage.GetRedisKey($"job:{jobId}:state"));
        }

        public override void SetJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            _transaction.HashSetAsync(_storage.GetRedisKey($"job:{jobId}"), "State", state.Name);
            _transaction.KeyDeleteAsync(_storage.GetRedisKey($"job:{jobId}:state"));

            var storedData = new Dictionary<string, string>(state.SerializeData())
            {
                {"State", state.Name}
            };

            if (state.Reason != null)
                storedData.Add("Reason", state.Reason);

            _transaction.HashSetAsync(_storage.GetRedisKey($"job:{jobId}:state"), storedData.ToHashEntries());

            AddJobState(jobId, state);
        }

        public override void AddJobState([NotNull] string jobId, [NotNull] IState state)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));
            if (state == null) throw new ArgumentNullException(nameof(state));

            var storedData = new Dictionary<string, string>(state.SerializeData())
            {
                {"State", state.Name},
                {"Reason", state.Reason},
                {"CreatedAt", JobHelper.SerializeDateTime(DateTime.UtcNow)}
            };

            _transaction.ListRightPushAsync(
                _storage.GetRedisKey($"job:{jobId}:history"),
                SerializationHelper.Serialize(storedData));
        }

        public override void AddToQueue([NotNull] string queue, [NotNull] string jobId)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            _transaction.SetAddAsync(_storage.GetRedisKey("queues"), queue);
            if (_storage.LifoQueues != null && _storage.LifoQueues.Contains(queue, StringComparer.OrdinalIgnoreCase))
            {
                _transaction.ListRightPushAsync(_storage.GetRedisKey($"queue:{queue}"), jobId);
            }
            else
            {
                _transaction.ListLeftPushAsync(_storage.GetRedisKey($"queue:{queue}"), jobId);
            }

            _transaction.PublishAsync(_storage.SubscriptionChannel, jobId);
        }

        public override void IncrementCounter([NotNull] string key)
        {
            _transaction.StringIncrementAsync(_storage.GetRedisKey(key));
        }

        public override void IncrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.StringIncrementAsync(_storage.GetRedisKey(key));
            _transaction.KeyExpireAsync(_storage.GetRedisKey(key), expireIn);
        }

        public override void DecrementCounter([NotNull] string key)
        {
            _transaction.StringDecrementAsync(_storage.GetRedisKey(key));
        }

        public override void DecrementCounter([NotNull] string key, TimeSpan expireIn)
        {
            _transaction.StringDecrementAsync(_storage.GetRedisKey(key));
            _transaction.KeyExpireAsync(_storage.GetRedisKey(key), expireIn);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value)
        {
            AddToSet(key, value, 0);
        }

        public override void AddToSet([NotNull] string key, [NotNull] string value, double score)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            _transaction.SortedSetAddAsync(_storage.GetRedisKey(key), value, score);
        }
        public override string CreateJob([NotNull] Job job, [NotNull] IDictionary<string, string> parameters, DateTime createdAt, TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            var jobId = Guid.NewGuid().ToString();

            var invocationData = InvocationData.SerializeJob(job);

            // Do not modify the original parameters.
            var storedParameters = new Dictionary<string, string>(parameters)
            {
                { "Queue", invocationData.Queue },
                { "Type", invocationData.Type },
                { "Method", invocationData.Method },
                { "ParameterTypes", invocationData.ParameterTypes },
                { "Arguments", invocationData.Arguments },
                { "CreatedAt", JobHelper.SerializeDateTime(createdAt) }
            };
            _ = _transaction.HashSetAsync(_storage.GetRedisKey($"job:{jobId}"), storedParameters.ToHashEntries());
            _ = _transaction.KeyExpireAsync(_storage.GetRedisKey($"job:{jobId}"), expireIn);

            if (! _transaction.Execute())
                throw new HangfireRedisTransactionException("Transaction Execution failure");
            return jobId;
        }
        public override void RemoveFromSet([NotNull] string key, [NotNull] string value)
        {
            if (value == null) throw new ArgumentNullException(nameof(value));

            _transaction.SortedSetRemoveAsync(_storage.GetRedisKey(key), value);
        }

        public override void InsertToList([NotNull] string key, string value)
        {
            _transaction.ListLeftPushAsync(_storage.GetRedisKey(key), value);
        }

        public override void RemoveFromList([NotNull] string key, string value)
        {
            _transaction.ListRemoveAsync(_storage.GetRedisKey(key), value);
        }

        public override void TrimList([NotNull] string key, int keepStartingFrom, int keepEndingAt)
        {
            _transaction.ListTrimAsync(_storage.GetRedisKey(key), keepStartingFrom, keepEndingAt);
        }

        public override void SetRangeInHash(
            [NotNull] string key, [NotNull] IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            _transaction.HashSetAsync(_storage.GetRedisKey(key), keyValuePairs.ToHashEntries());
        }
        public override void SetJobParameter([NotNull] string jobId, [NotNull] string name, [CanBeNull] string value)
        {
            _storage.GetConnection().SetJobParameter(jobId, name, value);
        }
        public override void RemoveHash([NotNull] string key)
        {
            _transaction.KeyDeleteAsync(_storage.GetRedisKey(key));
        }

        public override void RemoveFromQueue([NotNull] IFetchedJob fetchedJob)
        {
            fetchedJob.RemoveFromQueue();
        }
        public override void Dispose()
        {
            //Don't have to dispose anything
            foreach (var lokc in _lockToDispose)
            {
                lokc.Dispose();
            }
        }
    }
}
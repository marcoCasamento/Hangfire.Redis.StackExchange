// Copyright � 2013-2015 Sergey Odinokov, Marco Casamento 
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
using System.Globalization;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using StackExchange.Redis;

namespace Hangfire.Redis
{
    internal class RedisConnection : JobStorageConnection
    {
        readonly RedisSubscription _subscription;
        readonly string _jobStorageIdentity;
        readonly TimeSpan _fetchTimeout = TimeSpan.FromMinutes(3);
        
        public RedisConnection(IDatabase redis, RedisSubscription subscription, string jobStorageIdentity, TimeSpan fetchTimeout)
        {
            _subscription = subscription;            
            _jobStorageIdentity = jobStorageIdentity;
            _fetchTimeout = fetchTimeout;

            Redis = redis;
        }

        public IDatabase Redis { get; private set; }

        public static void RemoveServer(IDatabase redis, string serverId)
        {
            var transaction = redis.CreateTransaction();

            transaction.SetRemoveAsync(
                RedisStorage.Prefix + "servers",
                serverId);

            transaction.KeyDeleteAsync(
                new RedisKey[]
                {
                    string.Format(RedisStorage.Prefix + "server:{0}", serverId),
                    string.Format(RedisStorage.Prefix + "server:{0}:queues", serverId)
                });

            transaction.Execute();
        }

        public override IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return new RedisLock(Redis, RedisStorage.Prefix + resource, _jobStorageIdentity + Thread.CurrentThread.ManagedThreadId, timeout);
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            var transaction = Redis.CreateTransaction();

            transaction.SetAddAsync(RedisStorage.Prefix + "servers", serverId);

            transaction.HashSetAsync(
                string.Format(RedisStorage.Prefix + "server:{0}", serverId),
                new Dictionary<string, string>
                    {
                        { "WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture) },
                        { "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) },
                    }.ToHashEntries());

            foreach (var queue in context.Queues)
            {
                var queue1 = queue;
                transaction.ListRightPushAsync(
                    string.Format(RedisStorage.Prefix + "server:{0}:queues", serverId),
                    queue1);
            }

            transaction.Execute();

        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters,
            DateTime createdAt,
            TimeSpan expireIn)
        {
            var jobId = Guid.NewGuid().ToString();

            var invocationData = InvocationData.Serialize(job);

            // Do not modify the original parameters.
            var storedParameters = new Dictionary<string, string>(parameters);
            storedParameters.Add("Type", invocationData.Type);
            storedParameters.Add("Method", invocationData.Method);
            storedParameters.Add("ParameterTypes", invocationData.ParameterTypes);
            storedParameters.Add("Arguments", invocationData.Arguments);
            storedParameters.Add("CreatedAt", JobHelper.SerializeDateTime(createdAt));

            var transaction = Redis.CreateTransaction();

            transaction.HashSetAsync(
                    string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                    storedParameters.ToHashEntries());

            transaction.KeyExpireAsync(
                string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                expireIn);

            // TODO: check return value
            transaction.Execute();

            return jobId;
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RedisWriteOnlyTransaction(Redis.CreateTransaction());
        }

        public override void Dispose()
        {
            // nothing to dispose
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            string jobId = null;
            string queueName = null;
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (int i = 0; i < queues.Length; i++)
                {
                    queueName = queues[i];
                    var queueKey = RedisStorage.Prefix + string.Format("queue:{0}", queueName);
                    var fetchedKey = RedisStorage.Prefix + string.Format("queue:{0}:dequeued", queueName);
                    jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);
                    if (jobId != null) break;
                }

                if (jobId == null)
                {
                    _subscription.WaitForJob(_fetchTimeout, cancellationToken);
                }
            } while (jobId == null);

            // The job was fetched by the server. To provide reliability,
            // we should ensure, that the job will be performed and acquired
            // resources will be disposed even if the server will crash 
            // while executing one of the subsequent lines of code.

            // The job's processing is splitted into a couple of checkpoints.
            // Each checkpoint occurs after successful update of the 
            // job information in the storage. And each checkpoint describes
            // the way to perform the job when the server was crashed after
            // reaching it.

            // Checkpoint #1-1. The job was fetched into the fetched list,
            // that is being inspected by the FetchedJobsWatcher instance.
            // Job's has the implicit 'Fetched' state.

            Redis.HashSet(
                string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow));

            // Checkpoint #2. The job is in the implicit 'Fetched' state now.
            // This state stores information about fetched time. The job will
            // be re-queued when the JobTimeout will be expired.

            return new RedisFetchedJob(Redis, jobId, queueName);
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            var result = Redis.HashGetAll(RedisStorage.GetRedisKey(key)).ToStringDictionary();

            return result.Count != 0 ? result : null;
        }

        public override List<string> GetAllItemsFromList(string key)
        {

            return Redis.ListRange(RedisStorage.Prefix + key).ToStringArray().ToList();
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            HashSet<string> result = new HashSet<string>();
            foreach (var item in Redis.SortedSetScan(RedisStorage.GetRedisKey(key)))
            {
                result.Add(item.Element);
            }

            return result;
        }

        public override long GetCounter(string key)
        {
            return Convert.ToInt64(Redis.StringGet(RedisStorage.Prefix + key));
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            return Redis.SortedSetRangeByScore(RedisStorage.Prefix + key, fromScore, toScore, skip: 0, take: 1)
                .FirstOrDefault();
        }

        public override long GetHashCount(string key)
        {
            return Redis.HashLength(RedisStorage.Prefix + key);
        }
        public override TimeSpan GetHashTtl(string key)
        {
            return Redis.KeyTimeToLive(RedisStorage.Prefix + key) ?? TimeSpan.Zero;
        }
        public override JobData GetJobData(string id)
        {
            var storedData = Redis.HashGetAll(string.Format(RedisStorage.Prefix + "job:{0}", id));

            if (storedData.Length == 0) return null;

            string type = null;
            string method = null;
            string parameterTypes = null;
            string arguments = null;
            string createdAt = null;

            if (storedData.ContainsKey("Type"))
            {
                type = storedData.First(x => x.Name == "Type").Value;
            }
            if (storedData.ContainsKey("Method"))
            {
                method = storedData.First(x => x.Name == "Method").Value;
            }
            if (storedData.ContainsKey("ParameterTypes"))
            {
                parameterTypes = storedData.First(x => x.Name == "ParameterTypes").Value;
            }
            if (storedData.ContainsKey("Arguments"))
            {
                arguments = storedData.First(x => x.Name == "Arguments").Value;
            }
            if (storedData.ContainsKey("CreatedAt"))
            {
                createdAt = storedData.First(x => x.Name == "CreatedAt").Value;
            }

            Job job = null;
            JobLoadException loadException = null;

            var invocationData = new InvocationData(type, method, parameterTypes, arguments);

            try
            {
                job = invocationData.Deserialize();
            }
            catch (JobLoadException ex)
            {
                loadException = ex;
            }

            return new JobData
            {
                Job = job,
                State = storedData.ContainsKey("State") ? (string)storedData.First(x => x.Name == "State").Value : null,
                CreatedAt = JobHelper.DeserializeNullableDateTime(createdAt) ?? DateTime.MinValue,
                LoadException = loadException
            };
        }

        public override string GetJobParameter(string id, string name)
        {
            return Redis.HashGet(
                string.Format(RedisStorage.Prefix + "job:{0}", id),
                name);
        }

        public override long GetListCount(string key)
        {
            return Redis.ListLength(RedisStorage.Prefix + key);
        }
        public override TimeSpan GetListTtl(string key)
        {
            return Redis.KeyTimeToLive(RedisStorage.Prefix + key) ?? TimeSpan.Zero;
        }
        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            return Redis.ListRange(RedisStorage.Prefix + key, startingFrom, endingAt).ToStringArray().ToList();
        }
        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            return Redis.SortedSetRangeByRank(RedisStorage.Prefix + key, startingFrom, endingAt).ToStringArray().ToList();
        }
        public override long GetSetCount(string key)
        {
            return Redis.SortedSetLength(RedisStorage.Prefix + key);
        }
        public override TimeSpan GetSetTtl(string key)
        {
            return Redis.KeyTimeToLive(RedisStorage.Prefix + key) ?? TimeSpan.Zero;
        }
        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            var entries = Redis.HashGetAll(
                RedisStorage.Prefix + string.Format("job:{0}:state", jobId));

            if (entries.Length == 0) return null;

            var stateData = entries.ToStringDictionary();

            stateData.Remove("State");
            stateData.Remove("Reason");

            return new StateData
            {
                Name = entries.First(x => x.Name == "State").Value,
                Reason = entries.ContainsKey("Reason") ? (string)entries.First(x => x.Name == "Reason").Value : null,
                Data = stateData
            };
        }

        public override string GetValueFromHash(string key, string name)
        {
            return Redis.HashGet(RedisStorage.Prefix + key, name);
        }
        public override void Heartbeat(string serverId)
        {
            Redis.HashSet(
                string.Format(RedisStorage.Prefix + "server:{0}", serverId),
                "Heartbeat",
                JobHelper.SerializeDateTime(DateTime.UtcNow));
        }

        public override void RemoveServer(string serverId)
        {
            RemoveServer(Redis, serverId);
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            var serverNames = Redis.SetMembers(RedisStorage.Prefix + "servers");
            var heartbeats = new Dictionary<string, Tuple<DateTime, DateTime?>>();

            var utcNow = DateTime.UtcNow;


            foreach (var serverName in serverNames)
            {
                var name = serverName;
                var srv = Redis.HashGet(string.Format(RedisStorage.Prefix + "server:{0}", name), new RedisValue[] { "StartedAt", "Heartbeat" });
                heartbeats.Add(name,
                                new Tuple<DateTime, DateTime?>(
                                JobHelper.DeserializeDateTime(srv[0]),
                                JobHelper.DeserializeNullableDateTime(srv[1])));
            }

            var removedServerCount = 0;
            foreach (var heartbeat in heartbeats)
            {
                var maxTime = new DateTime(
                    Math.Max(heartbeat.Value.Item1.Ticks, (heartbeat.Value.Item2 ?? DateTime.MinValue).Ticks));

                if (utcNow > maxTime.Add(timeOut))
                {
                    RemoveServer(Redis, heartbeat.Key);
                    removedServerCount++;
                }
            }

            return removedServerCount;
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            Redis.HashSet(
                string.Format(RedisStorage.Prefix + "job:{0}", id),
                name,
                value);
        }
        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            Redis.HashSet(RedisStorage.GetRedisKey(key), keyValuePairs.ToHashEntries());
        }
    }
}

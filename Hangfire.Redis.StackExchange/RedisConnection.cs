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
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Storage;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace Hangfire.Redis
{
    internal class RedisConnection : IStorageConnection
    {
        private static readonly TimeSpan FetchTimeout = TimeSpan.FromSeconds(1);
		
        public RedisConnection(IDatabase redis)
        {
			
            Redis = redis;
        }

        public IDatabase Redis { get; private set; }

        public void Dispose()
        {
            //Don't need to be disposable anymore
        }

        public IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new RedisWriteOnlyTransaction(Redis.CreateTransaction());
        }

        public IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            string jobId = null;
            string queueName;
            var queueIndex = 0;
			//System.Diagnostics.Debug.WriteLine("queues Lenght {0}, ManagedThreadId {1}", queues.Length, Thread.CurrentThread.ManagedThreadId);
            do
            {
                cancellationToken.ThrowIfCancellationRequested();

                queueIndex = (queueIndex + 1) % queues.Length;
                queueName = queues[queueIndex];

                var queueKey = RedisStorage.Prefix + String.Format("queue:{0}", queueName);
                var fetchedKey = RedisStorage.Prefix + String.Format("queue:{0}:dequeued", queueName);

				//jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);

				//if (jobId == null)
				//{
				//	AutoResetEvent are = new AutoResetEvent(false);
				//	Redis.Multiplexer.GetSubscriber().Subscribe(RedisStorage.Prefix + "queues",
				//		(channel, val) =>
				//		{
				//			jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);
				//			are.Set();
				//		}
				//		);
				//	are.WaitOne();
				//}

				jobId = Redis.ListRightPopLeftPush(queueKey, fetchedKey);
				if (jobId == null)
					Thread.Sleep(1000);

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
                String.Format(RedisStorage.Prefix + "job:{0}", jobId),
                "Fetched",
                JobHelper.SerializeDateTime(DateTime.UtcNow));

            // Checkpoint #2. The job is in the implicit 'Fetched' state now.
            // This state stores information about fetched time. The job will
            // be re-queued when the JobTimeout will be expired.

            return new RedisFetchedJob(Redis, jobId, queueName);
        }

        public IDisposable AcquireDistributedLock(string resource, TimeSpan timeout)
        {
            return Redis.AcquireLock(RedisStorage.Prefix + resource, Environment.MachineName + Guid.NewGuid(), timeout);
        }

        public string CreateExpiredJob(
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
                    String.Format(RedisStorage.Prefix + "job:{0}", jobId),
                    storedParameters.ToHashEntries());

                transaction.KeyExpireAsync(
                    String.Format(RedisStorage.Prefix + "job:{0}", jobId),
                    expireIn);

                // TODO: check return value
                transaction.Execute();
            
            return jobId;
        }

        public JobData GetJobData(string id)
        {
			var storedData = Redis.HashGetAll(String.Format(RedisStorage.Prefix + "job:{0}", id));

            if (storedData.Length == 0) return null;

            string type = null;
            string method = null;
            string parameterTypes = null;
            string arguments = null;
            string createdAt = null;

            if (storedData.ContainsKey("Type"))
            {
                type = storedData.First(x=> x.Name == "Type").Value;
            }
            if (storedData.ContainsKey("Method"))
            {
                method = storedData.First(x=> x.Name == "Method").Value;
            }
            if (storedData.ContainsKey("ParameterTypes"))
            {
                parameterTypes = storedData.First(x=> x.Name == "ParameterTypes").Value;
            }
            if (storedData.ContainsKey("Arguments"))
            {
                arguments = storedData.First(x=> x.Name == "Arguments").Value;
            }
            if (storedData.ContainsKey("CreatedAt"))
            {
                createdAt = storedData.First(x=> x.Name == "CreatedAt").Value;
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
                State = storedData.ContainsKey("State") ? (string)storedData.First(x=> x.Name == "State").Value : null,
                CreatedAt = JobHelper.DeserializeNullableDateTime(createdAt) ?? DateTime.MinValue,
                LoadException = loadException
            };
        }

        public StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException("jobId");

            var entries = Redis.HashGetAll(
                RedisStorage.Prefix + String.Format("job:{0}:state", jobId));

            if (entries.Length == 0) return null;

            var stateData = entries.ToStringDictionary();
			
            stateData.Remove("State");
            stateData.Remove("Reason");

            return new StateData
            {
                Name = entries.First(x=> x.Name == "State").Value,
				Reason = entries.ContainsKey("Reason") ? (string)entries.First(x=> x.Name == "Reason").Value : null,
                Data = stateData
            };
        }

        public void SetJobParameter(string id, string name, string value)
        {
            Redis.HashSet(
                String.Format(RedisStorage.Prefix + "job:{0}", id),
                name,
                value);
        }

        public string GetJobParameter(string id, string name)
        {
            return Redis.HashGet(
                String.Format(RedisStorage.Prefix + "job:{0}", id),
                name);
        }

        public HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException("key");
            
			HashSet<string> result = new HashSet<string>();
			foreach (var item in Redis.SortedSetScan(RedisStorage.GetRedisKey(key)))
			{
				result.Add(item.Element);
			}
            
            return result;
        }

        public string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            return Redis.SortedSetRangeByScore(RedisStorage.Prefix + key, fromScore, toScore, skip: 0, take: 1)
                .FirstOrDefault();
        }

        public void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException("key");
            if (keyValuePairs == null) throw new ArgumentNullException("keyValuePairs");

            Redis.HashSet(RedisStorage.GetRedisKey(key), keyValuePairs.ToHashEntries());
        }

        public Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException("key");

			var result = Redis.HashGetAll(RedisStorage.GetRedisKey(key)).ToStringDictionary();
				
            return result.Count != 0 ? result : null;
        }

        public void AnnounceServer(string serverId, ServerContext context)
        {
			var transaction = Redis.CreateTransaction();
            
            transaction.SetAddAsync(RedisStorage.Prefix + "servers", serverId);

            transaction.HashSetAsync(
                String.Format(RedisStorage.Prefix + "server:{0}", serverId),
                new Dictionary<string, string>
                    {
                        { "WorkerCount", context.WorkerCount.ToString(CultureInfo.InvariantCulture) },
                        { "StartedAt", JobHelper.SerializeDateTime(DateTime.UtcNow) },
                    }.ToHashEntries());

            foreach (var queue in context.Queues)
            {
                var queue1 = queue;
                transaction.ListRightPushAsync(
					String.Format(RedisStorage.Prefix + "server:{0}:queues", serverId),
                    queue1);
            }

			transaction.Execute();
            
        }

        public void RemoveServer(string serverId)
        {
            RemoveServer(Redis, serverId);
        }

        public static void RemoveServer(IDatabase redis, string serverId)
        {
			var transaction = redis.CreateTransaction();

			transaction.SetRemoveAsync(
                RedisStorage.Prefix + "servers",
                serverId);

            transaction.KeyDeleteAsync(
				new RedisKey[]
				{
					String.Format(RedisStorage.Prefix + "server:{0}", serverId),
					String.Format(RedisStorage.Prefix + "server:{0}:queues", serverId)
				});

			transaction.Execute();
        }

        public void Heartbeat(string serverId)
        {
            Redis.HashSet(
                String.Format(RedisStorage.Prefix + "server:{0}", serverId),
                "Heartbeat",
                JobHelper.SerializeDateTime(DateTime.UtcNow));
        }

        public int RemoveTimedOutServers(TimeSpan timeOut)
        {
            var serverNames = Redis.SetMembers(RedisStorage.Prefix + "servers");
            var heartbeats = new Dictionary<string, Tuple<DateTime, DateTime?>>();

            var utcNow = DateTime.UtcNow;


            foreach (var serverName in serverNames)
            {
                var name = serverName;
                var srv = Redis.HashGet(String.Format(RedisStorage.Prefix + "server:{0}", name), new RedisValue[] { "StartedAt", "Heartbeat" });
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
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Hangfire.Logging;
using Hangfire.Server;
using StackExchange.Redis;

namespace Hangfire.Redis.StackExchange
{
#pragma warning disable 618
    internal class ExpiredJobsWatcher : IServerComponent
#pragma warning restore 618
    {
        private static readonly ILog Logger = LogProvider.For<ExpiredJobsWatcher>();

        private readonly RedisStorage _storage;
        private readonly TimeSpan _checkInterval;

        private static readonly string[] ProcessedKeys =
        {
            "succeeded",
            "deleted"
        };

        public ExpiredJobsWatcher(RedisStorage storage, TimeSpan checkInterval)
        {
            if (storage == null)
                throw new ArgumentNullException(nameof(storage));
            if (checkInterval.Ticks <= 0)
                throw new ArgumentOutOfRangeException(nameof(checkInterval), "Check interval should be positive.");

            _storage = storage;
            _checkInterval = checkInterval;
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            using (var connection = (RedisConnection) _storage.GetConnection())
            {
                var redis = connection.Redis;

                foreach (var key in ProcessedKeys)
                {
                    var redisKey = _storage.GetRedisKey(key);

                    var count = redis.ListLength(redisKey);
                    if (count == 0) continue;

                    Logger.InfoFormat("Removing expired records from the '{0}' list...", key);

                    const int batchSize = 100;
                    var keysToRemove = new List<string>();

                    for (var last = count - 1; last >= 0; last -= batchSize)
                    {
                        var first = Math.Max(0, last - batchSize + 1);

                        var jobIds = redis.ListRange(redisKey, first, last).ToStringArray();
                        if (jobIds.Length == 0) continue;

                        var pipeline = redis.CreateBatch();
                        var tasks = new Task[jobIds.Length];

                        for (var i = 0; i < jobIds.Length; i++)
                        {
                            tasks[i] = pipeline.KeyExistsAsync(_storage.GetRedisKey($"job:{jobIds[i]}"));
                        }

                        pipeline.Execute();
                        Task.WaitAll(tasks);

                        keysToRemove.AddRange(jobIds.Where((t, i) => !((Task<bool>) tasks[i]).Result));
                    }

                    if (keysToRemove.Count == 0) continue;

                    Logger.InfoFormat("Removing {0} expired jobs from '{1}' list...", keysToRemove.Count, key);

                    using (var transaction = connection.CreateWriteTransaction())
                    {
                        foreach (var jobId in keysToRemove)
                        {
                            transaction.RemoveFromList(key, jobId);
                        }

                        transaction.Commit();
                    }
                }
            }

            cancellationToken.WaitHandle.WaitOne(_checkInterval);
        }
    }
}
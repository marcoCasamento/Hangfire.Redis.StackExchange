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
using System.Threading;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.Logging;
using StackExchange.Redis;

namespace Hangfire.Redis
{
    internal class FetchedJobsWatcher : IServerComponent
    {
        private readonly TimeSpan _invisibilityTimeout;
        private static readonly ILog Logger = LogProvider.GetLogger(typeof(FetchedJobsWatcher));

        private readonly RedisStorage _storage;
        private readonly FetchedJobsWatcherOptions _options;

        public FetchedJobsWatcher(RedisStorage storage, TimeSpan invisibilityTimeout)
            : this(storage, invisibilityTimeout, new FetchedJobsWatcherOptions())
        {
        }

        public FetchedJobsWatcher(
            RedisStorage storage,
            TimeSpan invisibilityTimeout,
            FetchedJobsWatcherOptions options)
        {
            if (storage == null) throw new ArgumentNullException("storage");
            if (options == null) throw new ArgumentNullException("options");
            if (invisibilityTimeout.Ticks <= 0)
            {
                throw new ArgumentOutOfRangeException("invisibilityTimeout", "Invisibility timeout duration should be positive.");
            }

            _storage = storage;
            _invisibilityTimeout = invisibilityTimeout;
            _options = options;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            using (var connection = (RedisConnection)_storage.GetConnection())
            {
                var queues = connection.Redis.SetMembers(
                    RedisStorage.Prefix + "queues");

                foreach (var queue in queues)
                {
                    ProcessQueue(queue, connection);
                }
            }

            cancellationToken.WaitHandle.WaitOne(_options.SleepTimeout);
        }

        private void ProcessQueue(string queue, RedisConnection connection)
        {
            // Allowing only one server at a time to process the timed out
            // jobs from the specified queue.
            Logger.DebugFormat(
                "Acquiring the lock for the fetched list of the '{0}' queue...", queue);

            using (new RedisLock(connection.Redis, string.Format(RedisStorage.Prefix + "queue:{0}:dequeued:lock", queue), _storage.Identity + Thread.CurrentThread.ManagedThreadId, _options.FetchedLockTimeout))
            {
                Logger.DebugFormat(
                    "Looking for timed out jobs in the '{0}' queue...", queue);

                var jobIds = connection.Redis.ListRange(
                    string.Format(RedisStorage.Prefix + "queue:{0}:dequeued", queue));

                var requeued = 0;

                foreach (var jobId in jobIds)
                {
                    if (RequeueJobIfTimedOut(connection, jobId, queue))
                    {
                        requeued++;
                    }
                }

                if (requeued == 0)
                {
                    Logger.DebugFormat("No timed out jobs were found in the '{0}' queue", queue);
                }
                else
                {
                    Logger.InfoFormat(
                        "{0} timed out jobs were found in the '{1}' queue and re-queued.",
                        requeued,
                        queue);
                }
            }
        }

        private bool RequeueJobIfTimedOut(RedisConnection connection, string jobId, string queue)
        {
            var flags = connection.Redis.HashGet(
                string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                new RedisValue[]{"Fetched","Checked"});

            var fetched = flags[0];
            var @checked = flags[1];

            if (string.IsNullOrEmpty(fetched) && string.IsNullOrEmpty(@checked))
            {
                // If the job does not have these flags set, then it is
                // in the implicit 'Fetched' state. This state has no 
                // information about the time it was fetched. So we
                // can not do anything with the job in this state, because
                // there are two options:

                // 1. It is going to move to the implicit 'Fetched' state
                //    in a short time.
                // 2. It will stay in the 'Fetched' state forever due to
                //    its processing server is dead.

                // To ensure its server is dead, we'll move the job to
                // the implicit 'Checked' state with the current timestamp
                // and will not do anything else at this pass of the watcher.
                // If job's state will still be 'Checked' on the later passes
                // and after the CheckedTimeout expired, then the server
                // is dead, and we'll re-queue the job.

                connection.Redis.HashSet(
                    string.Format(RedisStorage.Prefix + "job:{0}", jobId),
                    "Checked",
                    JobHelper.SerializeDateTime(DateTime.UtcNow));

                // Checkpoint #1-2. The job is in the implicit 'Checked' state.
                // It will be re-queued after the CheckedTimeout will be expired.
            }
            else
            {
                if (TimedOutByFetchedTime(fetched) || TimedOutByCheckedTime(fetched, @checked))
                {
                    var fetchedJob = new RedisFetchedJob(connection.Redis, jobId, queue);
                    fetchedJob.Dispose();

                    return true;
                }
            }

            return false;
        }

        private bool TimedOutByFetchedTime(string fetchedTimestamp)
        {
            return !string.IsNullOrEmpty(fetchedTimestamp) &&
                   (DateTime.UtcNow - JobHelper.DeserializeDateTime(fetchedTimestamp) > _invisibilityTimeout);
        }

        private bool TimedOutByCheckedTime(string fetchedTimestamp, string checkedTimestamp)
        {
            // If the job has the 'fetched' flag set, then it is
            // in the implicit 'Fetched' state, and it can not be timed
            // out by the 'checked' flag.
            if (!string.IsNullOrEmpty(fetchedTimestamp))
            {
                return false;
            }

            return !string.IsNullOrEmpty(checkedTimestamp) &&
                   (DateTime.UtcNow - JobHelper.DeserializeDateTime(checkedTimestamp) > _options.CheckedTimeout);
        }
    }
}
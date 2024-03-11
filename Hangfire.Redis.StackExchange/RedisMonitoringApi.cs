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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using StackExchange.Redis;

namespace Hangfire.Redis.StackExchange
{
    public class RedisMonitoringApi : JobStorageMonitor
    {
        private readonly RedisStorage _storage;
        private readonly IDatabase _database;

		public RedisMonitoringApi([NotNull] RedisStorage storage, [NotNull] IDatabase database)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
			if (database == null) throw new ArgumentNullException(nameof(database));

            _storage = storage;
			_database = database;
        }

        public override long ScheduledCount()
        {
            return UseConnection(redis =>
                redis.SortedSetLength(_storage.GetRedisKey("schedule")));
        }
        public override IDictionary<DateTime, long> DeletedByDatesCount()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "deleted"));
        }
        public override IDictionary<DateTime, long> HourlyDeletedJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "deleted"));
        }
        public override long EnqueuedCount([NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis => redis.ListLength(_storage.GetRedisKey($"queue:{queue}")));
        }

        public override long FetchedCount([NotNull] string queue)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis => redis.ListLength(_storage.GetRedisKey($"queue:{queue}:dequeued")));
        }

        public override long ProcessingCount()
        {
            return UseConnection(redis => redis.SortedSetLength(_storage.GetRedisKey("processing")));
        }
        
        public override long SucceededListCount()
        {
            return UseConnection(redis => redis.ListLength(_storage.GetRedisKey("succeeded")));
        }

        public override long FailedCount()
        {
            return UseConnection(redis => redis.SortedSetLength(_storage.GetRedisKey("failed")));
        }
        
        public override long DeletedListCount()
        {
            return UseConnection(redis => redis.ListLength(_storage.GetRedisKey("deleted")));
        }

        public IDatabase GetDataBase()
        {
            return _database;
        }

        public override JobList<ProcessingJobDto> ProcessingJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis
                    .SortedSetRangeByRank(_storage.GetRedisKey("processing"), from, from + count - 1)
                    .ToStringArray();

                return new JobList<ProcessingJobDto>(GetJobsWithProperties(redis,
                    jobIds,
                    null,
                    new[] { "StartedAt", "ServerName", "ServerId", "State" },
                    false,
                    (job, jobData, state, historyData, loadException) => new ProcessingJobDto
                    {
                        ServerId = state[2] ?? state[1],
                        Job = job,
                        StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InProcessingState = ProcessingState.StateName.Equals(
                            state[3], StringComparison.OrdinalIgnoreCase),
                    })
					.Where(x=> x.Value?.ServerId != null)
					.OrderBy(x => x.Value.StartedAt).ToList());
            });
        }

        public override JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var scheduledJobs = redis
                    .SortedSetRangeByRankWithScores(_storage.GetRedisKey("schedule"), from, from + count - 1)
                    .ToList();

                if (scheduledJobs.Count == 0)
                {
                    return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobs = new ConcurrentDictionary<string, List<string>>();
                var states = new ConcurrentDictionary<string, List<string>>();
                var jobIds = new Dictionary<string, string>();

				var pipeline = redis.CreateBatch();
				var tasks = new Task[scheduledJobs.Count * 2];
				int i = 0;
                foreach (var scheduledJob in scheduledJobs)
                {
                    var jobInfo = scheduledJob.Element.ToString().Split(':');
                    var jobId = jobInfo[jobInfo.Length-1];
                    jobIds.Add(scheduledJob.Element, jobId);
					tasks[i] = pipeline.HashGetAsync(
								_storage.GetRedisKey($"job:{jobId}"),
								new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" })
						.ContinueWith(x => jobs.TryAdd(scheduledJob.Element, x.Result.ToStringArray().ToList()));
					i++;
					tasks[i] = pipeline.HashGetAsync(
								_storage.GetRedisKey($"job:{jobId}:state"),
								new RedisValue[] { "State", "ScheduledAt" })
						.ContinueWith(x => states.TryAdd(scheduledJob.Element, x.Result.ToStringArray().ToList()));
					i++;
                }

				pipeline.Execute();
				Task.WaitAll(tasks);

                return new JobList<ScheduledJobDto>(scheduledJobs
                    .Select(job => new KeyValuePair<string, ScheduledJobDto>(
                        jobIds[job.Element],
                        new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long) job.Score),
                            Job = TryToGetJob(jobs[job.Element][0], jobs[job.Element][1], jobs[job.Element][2], jobs[job.Element][3], out var loadException),
                            ScheduledAt =
                                states[job.Element].Count > 1
                                    ? JobHelper.DeserializeNullableDateTime(states[job.Element][1])
                                    : null,
                            InScheduledState =
                                ScheduledState.StateName.Equals(states[job.Element][0], StringComparison.OrdinalIgnoreCase)
                        }))
                    .ToList());
            });
        }

        public override IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(redis => GetTimelineStats(redis, "succeeded"));
        }

        public override IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(redis => GetTimelineStats(redis, "failed"));
        }

        public override IList<ServerDto> Servers()
        {
            return UseConnection(redis =>
            {
                var serverNames = redis
                    .SetMembers(_storage.GetRedisKey("servers"))
                    .Select(x => (string)x)
                    .ToList();

                if (serverNames.Count == 0)
                {
                    return new List<ServerDto>();
                }

                var servers = new List<ServerDto>();

                foreach (var serverName in serverNames)
                {
                    var queue = redis.ListRange(
                        _storage.GetRedisKey($"server:{serverName}:queues"))
                        .ToStringArray().ToList();

                    var server = redis.HashGet(
                        _storage.GetRedisKey($"server:{serverName}"),
                        new RedisValue[] { "WorkerCount", "StartedAt", "Heartbeat" })
                        .ToStringArray().ToList();
                    if (server[0] == null)
                        continue;   // skip removed server

                    servers.Add(new ServerDto
                    {
                        Name = serverName,
                        WorkersCount = int.Parse(server[0]),
                        Queues = queue,
                        StartedAt = JobHelper.DeserializeDateTime(server[1]),
                        Heartbeat = JobHelper.DeserializeNullableDateTime(server[2])
                    });
                }

                return servers;
            });
        }

        public override JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var failedJobIds = redis
                    .SortedSetRangeByRank(_storage.GetRedisKey("failed"), from, from + count - 1, Order.Descending)
					.ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new[] { "FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "State", "Reason" },
                    false,
                    (job, jobData, state, historyData, loadException) => new FailedJobDto
                    {
                        Job = job,
                        Reason = state[5],
                        FailedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        ExceptionType = state[1],
                        ExceptionMessage = state[2],
                        ExceptionDetails = state[3],
                        InFailedState = FailedState.StateName.Equals(state[4], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public override JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var succeededJobIds = redis
                    .ListRange(_storage.GetRedisKey("succeeded"), from, from + count - 1)
					.ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new[] { "SucceededAt", "PerformanceDuration", "Latency", "State", "Result" },
                    false,
                    (job, jobData, state, historyData, loadException) => new SucceededJobDto
                    {
                        Job = job,
                        Result = state[4],
                        SucceededAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        TotalDuration = state[1] != null && state[2] != null
                            ? (long?) long.Parse(state[1]) + (long?) long.Parse(state[2])
                            : null,
                        InSucceededState = SucceededState.StateName.Equals(state[3], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public override JobList<DeletedJobDto> DeletedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var deletedJobIds = redis
                    .ListRange(_storage.GetRedisKey("deleted"), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    deletedJobIds,
                    null,
                    new[] { "DeletedAt", "State" },
                    false,
                    (job, jobData, state, historyData, loadException) => new DeletedJobDto
                    {
                        Job = job,
                        DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public override IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return UseConnection(redis =>
            {
                var queues = redis
                    .SetMembers(_storage.GetRedisKey("queues"))
					.Select(x=> (string)x)
                    .ToList();

                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);

                foreach (var queue in queues)
                {
                    string[] firstJobIds = null;
                    long length = 0;
                    long fetched = 0;


					var pipeline = redis.CreateBatch();
					Task[] tasks = new Task[3];
					tasks[0] = pipeline.ListRangeAsync(
                            _storage.GetRedisKey($"queue:{queue}"), -5, -1)
							.ContinueWith(x => firstJobIds = x.Result.ToStringArray());

                    tasks[1] = pipeline.ListLengthAsync(_storage.GetRedisKey($"queue:{queue}"))
						.ContinueWith(x => length = x.Result);

                    tasks[2] = pipeline.ListLengthAsync(_storage.GetRedisKey($"queue:{queue}:dequeued"))
						.ContinueWith(x => fetched = x.Result);

					pipeline.Execute();
					Task.WaitAll(tasks);

                    var jobs = GetJobsWithProperties(
                        redis,
                        firstJobIds,
                        new[] { "State" },
                        new[] { "EnqueuedAt", "State" },
                        false,
                        (job, jobData, state, historyData, loadException) => new EnqueuedJobDto
                        {
                            Job = job,
                            State = jobData[0],
                            EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                            InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                        });

                    result.Add(new QueueWithTopEnqueuedJobsDto
                    {
                        Name = queue,
                        FirstJobs = jobs,
                        Length = length,
                        Fetched = fetched
                    });
                }

                return result;
            });
        }

        public override JobList<EnqueuedJobDto> EnqueuedJobs([NotNull] string queue, int from, int count)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis =>
            {
                var jobIds = redis
                    .ListRange(_storage.GetRedisKey($"queue:{queue}"), from, from + count - 1)
                    .ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State" },
                    new[] { "EnqueuedAt", "State" },
                    false,
                    (job, jobData, state, historyData, loadException) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public override JobList<FetchedJobDto> FetchedJobs([NotNull] string queue, int from, int count)
        {
            if (queue == null) throw new ArgumentNullException(nameof(queue));

            return UseConnection(redis =>
            {
                var jobIds = redis
                    .ListRange(_storage.GetRedisKey($"queue:{queue}:dequeued"), from, from + count - 1)
                    .ToStringArray();
				RedisValue[] rk = new RedisValue[1];

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State", "Fetched" },
                    null, 
                    false,
                    (job, jobData, state, historyData, loadException) => new FetchedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        FetchedAt = JobHelper.DeserializeNullableDateTime(jobData[1])
                    });
            });
        }
        public override JobList<AwaitingJobDto> AwaitingJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var awaitingJobIds = redis
                    .ListRange(_storage.GetRedisKey("awaiting"), from, from + count - 1)
                    .ToStringArray();
                
                return GetJobsWithProperties(
                    redis,
                    awaitingJobIds,
                    null,
                    new[] { "Expiration", "Options", "State", "NextState", "ParentId"}, 
                    true,
                    (job, jobData, stateData, historyData, loadException) => new AwaitingJobDto
                    {
                        Job = job,
                        AwaitingAt = historyData.ContainsKey("CreatedAt") ? JobHelper.DeserializeNullableDateTime(historyData["CreatedAt"]) : null, 
                        InAwaitingState = AwaitingState.StateName.Equals(stateData[2], StringComparison.OrdinalIgnoreCase),
                        InvocationData = new InvocationData(jobData[0], jobData[1], jobData[2], jobData[3]), //should the third argument of GetJobsWithProperties pass something, add the number of elements to the index of the array of jobData in this line
                        StateData = new Dictionary<string, string>
                        {
                            { "Expiration", stateData[0] },
                            { "Options", stateData[1] },
                            { "State", stateData[2] },
                            { "NextState", stateData[3] },
                            { "ParentId", stateData[4] }
                        },
                        LoadException = new JobLoadException($"Error deserializing job", loadException),
                        ParentStateName = _storage.GetConnection().GetStateData(stateData[4]).Name
                    });
            });
        }
        public override IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "succeeded"));
        }

        public override IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "failed"));
        }

        public override JobDetailsDto JobDetails([NotNull] string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            return UseConnection(redis =>
            {
                var job = redis
                    .HashGetAll(_storage.GetRedisKey($"job:{jobId}"))
                    .ToStringDictionary();

                if (job.Count == 0) return null;

                var hiddenProperties = new[] { "Type", "Method", "ParameterTypes", "Arguments", "State", "CreatedAt", "Fetched" };

                var history = redis
                    .ListRange(_storage.GetRedisKey($"job:{jobId}:history"))
					.ToStringArray()
                    .Select(SerializationHelper.Deserialize<Dictionary<string, string>>)
                    .ToList();

                // history is in wrong order, fix this
                history.Reverse();

                var stateHistory = new List<StateHistoryDto>(history.Count);
                foreach (var entry in history)
                {
                    var stateData = new Dictionary<string, string>(entry, StringComparer.OrdinalIgnoreCase);
                    var dto = new StateHistoryDto
                    {
                        StateName = stateData["State"],
                        Reason = stateData.ContainsKey("Reason") ? stateData["Reason"] : null,
                        CreatedAt = JobHelper.DeserializeDateTime(stateData["CreatedAt"]),
                    };

                    // Each history item contains all of the information,
                    // but other code should not know this. We'll remove
                    // unwanted keys.
                    stateData.Remove("State");
                    stateData.Remove("Reason");
                    stateData.Remove("CreatedAt");

                    dto.Data = stateData;
                    stateHistory.Add(dto);
                }

                // For compatibility
                if (!job.ContainsKey("Method")) job.Add("Method", null);
                if (!job.ContainsKey("ParameterTypes")) job.Add("ParameterTypes", null);

                return new JobDetailsDto
                {
                    Job = TryToGetJob(job["Type"], job["Method"], job["ParameterTypes"], job["Arguments"], out var loadException),
                    CreatedAt =
                        job.ContainsKey("CreatedAt")
                            ? JobHelper.DeserializeDateTime(job["CreatedAt"])
                            : (DateTime?) null,
                    Properties =
                        job.Where(x => !hiddenProperties.Contains(x.Key)).ToDictionary(x => x.Key, x => x.Value),
                    History = stateHistory
                };
            });
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats([NotNull] IDatabase redis, [NotNull] string type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd-HH}")).ToArray();
            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out value))
                {
                    value = 0;
                }

                result.Add(dates[i], value);
            }

            return result;
        }

        private Dictionary<DateTime, long> GetTimelineStats([NotNull] IDatabase redis, [NotNull] string type)
        {
            if (type == null) throw new ArgumentNullException(nameof(type));

            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }
            
            var keys = dates.Select(x => _storage.GetRedisKey($"stats:{type}:{x:yyyy-MM-dd}")).ToArray();

            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < dates.Count; i++)
            {
                long value;
                if (!long.TryParse(valuesMap[valuesMap.Keys.ElementAt(i)], out value))
                {
                    value = 0;
                }
                result.Add(dates[i], value);
            }

            return result;
        }

        private JobList<T> GetJobsWithProperties<T>(
            [NotNull] IDatabase redis,
            [NotNull] string[] jobIds,
            string[] properties,
            string[] stateProperties,
            bool loadHistory,
            [NotNull] Func<Job, IReadOnlyList<string>, IReadOnlyList<string>, Dictionary<string, string>, Exception, T> selector)
        {
            if (jobIds == null) throw new ArgumentNullException(nameof(jobIds));
            if (selector == null) throw new ArgumentNullException(nameof(selector));

            if (jobIds.Length == 0) return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var states = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var histories = new Dictionary<string, Task<RedisValue>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);

            properties ??= new string[0];

            var extendedProperties = properties
                .Concat(new[] { "Type", "Method", "ParameterTypes", "Arguments" })
                .ToRedisValues();
            
            var pipeline = redis.CreateBatch();
			var tasks = new List<Task>(jobIds.Length * 2);
            foreach (var jobId in jobIds.Distinct())
            {
				var jobTask = pipeline.HashGetAsync(
                        _storage.GetRedisKey($"job:{jobId}"),
                        extendedProperties);
				tasks.Add(jobTask);
                jobs.Add(jobId, jobTask);

				if (stateProperties != null)
				{
                    var taskStateJob = pipeline.HashGetAsync(
                        _storage.GetRedisKey($"job:{jobId}:state"), 
                        stateProperties.ToRedisValues());
					tasks.Add(taskStateJob);
                    states.Add(jobId, taskStateJob);
				}

                if (loadHistory)
                {
                    var taskHistoryJob = pipeline.ListGetByIndexAsync(_storage.GetRedisKey($"job:{jobId}:history"), -1); 
                    tasks.Add(taskHistoryJob);
                    histories.Add(jobId, taskHistoryJob);
                }
            }

            pipeline.Execute();
			Task.WaitAll(tasks.ToArray());

			var jobList = new JobList<T>(jobIds
                .Select(jobId => new
                {
                    JobId = jobId,
                    JobData = jobs[jobId].Result.ToStringArray(),
                    Job = TryToGetJob(
                        jobs[jobId].Result[properties.Length],
                        jobs[jobId].Result[properties.Length + 1],
                        jobs[jobId].Result[properties.Length + 2],
                        jobs[jobId].Result[properties.Length + 3],
                        out Exception loadException),
                    LastHistoryEntry = !histories.ContainsKey(jobId) ? null : SerializationHelper.Deserialize<Dictionary<string, string>>(histories[jobId].ToString()),
                    LoadException = loadException,
                    State = stateProperties != null ? states[jobId].Result.ToStringArray() : null
                })
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.JobData.Any(y => y != null) 
                        ? selector(x.Job, x.JobData, x.State, x.LastHistoryEntry, x.LoadException) 
                        : default)));
			return jobList;
        }

        public override StatisticsDto GetStatistics()
        {
            return UseConnection(redis =>
            {
                var stats = new StatisticsDto();

                var queues = redis.SetMembers(_storage.GetRedisKey("queues"));

				var pipeline = redis.CreateBatch();
				var tasks = new Task[queues.Length + 8];

                tasks[0] = pipeline.SetLengthAsync(_storage.GetRedisKey("servers"))
					.ContinueWith(x=> stats.Servers = x.Result);

                tasks[1] = pipeline.SetLengthAsync(_storage.GetRedisKey("queues"))
                    .ContinueWith(x => stats.Queues = x.Result);

                tasks[2] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("schedule"))
					.ContinueWith(x => stats.Scheduled = x.Result);

                tasks[3] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("processing"))
					.ContinueWith(x => stats.Processing = x.Result);

                tasks[4] = pipeline.StringGetAsync(_storage.GetRedisKey("stats:succeeded"))
                    .ContinueWith(x => stats.Succeeded = long.Parse(x.Result.HasValue ?  (string)x.Result: "0"));

                tasks[5] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("failed"))
					.ContinueWith(x => stats.Failed = x.Result);

                tasks[6] = pipeline.StringGetAsync(_storage.GetRedisKey("stats:deleted"))
					.ContinueWith(x => stats.Deleted = long.Parse(x.Result.HasValue ?  (string)x.Result : "0"));

                tasks[7] = pipeline.SortedSetLengthAsync(_storage.GetRedisKey("recurring-jobs"))
                    .ContinueWith(x => stats.Recurring = x.Result);

				var i = 8;
                foreach (var queue in queues)
                {
                    tasks[i] = pipeline.ListLengthAsync(_storage.GetRedisKey($"queue:{queue}"))
						.ContinueWith(x => { lock (stats) { stats.Enqueued += x.Result; } });
					i++;
                }

				pipeline.Execute();
				Task.WaitAll(tasks);

                return stats;
            });
        }

        private T UseConnection<T>(Func<IDatabase, T> action)
        {
			return action(_database);
        }

        private static Job TryToGetJob(
            string type, string method, string parameterTypes, string arguments, out Exception loadException)
        {
            try
            {
                loadException = null;
                return new InvocationData(
                type,
                method,
                parameterTypes,
                arguments).DeserializeJob();
            }
            catch (Exception e)
            {
                loadException = e;
                return null;
            }
        }

        public override long AwaitingCount()
        {
            return UseConnection(redis => redis.SortedSetLength(_storage.GetRedisKey("awaiting")));
        }

        
    }
}

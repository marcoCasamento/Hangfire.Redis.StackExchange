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
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using Hangfire.Annotations;
using StackExchange.Redis;
using System.Threading.Tasks;

namespace Hangfire.Redis
{
    internal class RedisMonitoringApi : IMonitoringApi
    {
        private readonly IDatabase _database;

		public RedisMonitoringApi([NotNull] IDatabase database)
        {
			if (database == null) throw new ArgumentNullException("database");

			_database = database;
        }

        public long ScheduledCount()
        {
            return UseConnection(redis => 
                redis.SortedSetLength(RedisStorage.Prefix+"schedule"));
        }

        public long EnqueuedCount(string queue)
        {
            return UseConnection(redis => 
                redis.ListLength(String.Format(RedisStorage.Prefix + "queue:{0}", queue)));
        }

        public long FetchedCount(string queue)
        {
            return UseConnection(redis => 
                redis.ListLength(String.Format(RedisStorage.Prefix + "queue:{0}:dequeued", queue)));
        }

        public long FailedCount()
        {
            return UseConnection(redis => redis.SortedSetLength(RedisStorage.Prefix + "failed"));
        }

        public long ProcessingCount()
        {
            return UseConnection(redis => redis.SortedSetLength(RedisStorage.Prefix + "processing"));
        }

        public long DeletedListCount()
        {
            return UseConnection(redis => redis.ListLength(RedisStorage.Prefix + "deleted"));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(
            int from, int count)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.SortedSetRangeByRank(
                    RedisStorage.Prefix + "processing",
                    from,
                    from + count - 1).ToStringArray();

                return new JobList<ProcessingJobDto>(GetJobsWithProperties(redis,
                    jobIds,
                    null,
                    new[] { "StartedAt", "ServerName", "ServerId", "State" },
                    (job, jobData, state) => new ProcessingJobDto
                    {
                        ServerId = state[2] ?? state[1],
                        Job = job,
                        StartedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InProcessingState = ProcessingState.StateName.Equals(
                            state[3], StringComparison.OrdinalIgnoreCase),
                    })
					.Where(x=> x.Value.ServerId != null)
					.OrderBy(x => x.Value.StartedAt).ToList());
            });
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var scheduledJobs = redis.SortedSetRangeByRankWithScores(
                    RedisStorage.Prefix + "schedule",
                    from,
                    from + count - 1).ToList();

                if (scheduledJobs.Count == 0)
                {
                    return new JobList<ScheduledJobDto>(new List<KeyValuePair<string, ScheduledJobDto>>());
                }

                var jobs = new Dictionary<string, List<string>>();
                var states = new Dictionary<string, List<string>>();;

				var pipeline = redis.CreateBatch();
				var tasks = new Task[scheduledJobs.Count * 2];
				int i = 0;
                foreach (var scheduledJob in scheduledJobs)
                {
                    var job = scheduledJob;
					tasks[i] = pipeline.HashGetAsync(
								String.Format(RedisStorage.Prefix + "job:{0}", job.Element),
								new RedisValue[] { "Type", "Method", "ParameterTypes", "Arguments" })
						.ContinueWith(x => jobs.Add(job.Element, x.Result.ToStringArray().ToList()));
					i++;
					tasks[i] = pipeline.HashGetAsync(
								String.Format(RedisStorage.Prefix + "job:{0}:state", job.Element),
								new RedisValue[] { "State", "ScheduledAt" })
						.ContinueWith(x => states.Add(job.Element, x.Result.ToStringArray().ToList()));
					i++;
                }
				
				pipeline.Execute();
				Task.WaitAll(tasks);

                return new JobList<ScheduledJobDto>(scheduledJobs
                    .Select(job => new KeyValuePair<string, ScheduledJobDto>(
                        job.Element,
                        new ScheduledJobDto
                        {
                            EnqueueAt = JobHelper.FromTimestamp((long) job.Score),
                            Job = TryToGetJob(jobs[job.Element][0], jobs[job.Element][1], jobs[job.Element][2], jobs[job.Element][3]),
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

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(redis => GetTimelineStats(redis, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(redis => GetTimelineStats(redis, "failed"));
        }

        public IList<ServerDto> Servers()
        {
            return UseConnection(redis =>
            {
                var serverNames = redis.SetMembers(RedisStorage.Prefix + "servers").Select(x => (string)x).ToList();

                if (serverNames.Count == 0)
                {
                    return new List<ServerDto>();
                }

                var servers = new Dictionary<string, List<string>>();
                var queues = new Dictionary<string, List<string>>();

                foreach (var serverName in serverNames)
                {
                    var name = serverName;
                    servers.Add(name,
                        redis.HashGet(String.Format(RedisStorage.Prefix + "server:{0}", name), new RedisValue[] { "WorkerCount", "StartedAt", "Heartbeat" })
                            .ToStringArray().ToList()
                        );
                    queues.Add(name,
                        redis.ListRange(String.Format(RedisStorage.Prefix + "server:{0}:queues", name))
                            .ToStringArray().ToList()
                        );

                }


                return serverNames.Select(x => new ServerDto
                {
                    Name = x,
                    WorkersCount = int.Parse(servers[x][0]),
                    Queues = queues[x],
                    StartedAt = JobHelper.DeserializeDateTime(servers[x][1]),
                    Heartbeat = JobHelper.DeserializeNullableDateTime(servers[x][2])
                }).ToList();
            });
        }

        public JobList<FailedJobDto> FailedJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var failedJobIds = redis.SortedSetRangeByRank(
                    RedisStorage.Prefix + "failed",
                    from,
                    from + count - 1)
					.ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    failedJobIds,
                    null,
                    new[] { "FailedAt", "ExceptionType", "ExceptionMessage", "ExceptionDetails", "State", "Reason" },
                    (job, jobData, state) => new FailedJobDto
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

        public JobList<SucceededJobDto> SucceededJobs(int from, int count)
        {
            return UseConnection(redis =>
            {
                var succeededJobIds = redis.ListRange(
                    RedisStorage.Prefix + "succeeded",
                    from,
                    from + count - 1)
					.ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    succeededJobIds,
                    null,
                    new[] { "SucceededAt", "PerformanceDuration", "Latency", "State", "Result" },
                    (job, jobData, state) => new SucceededJobDto
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

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return UseConnection(redis =>
            {
                var deletedJobIds = redis.ListRange(
                    RedisStorage.Prefix + "deleted",
                    from,
                    from + count - 1).ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    deletedJobIds,
                    null,
                    new[] { "DeletedAt", "State" },
                    (job, jobData, state) => new DeletedJobDto
                    {
                        Job = job,
                        DeletedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InDeletedState = DeletedState.StateName.Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            return UseConnection(redis =>
            {
                var queues = redis.SetMembers(RedisStorage.Prefix + "queues")
					.Select(x=> (string)x).ToList();
                var result = new List<QueueWithTopEnqueuedJobsDto>(queues.Count);

                foreach (var queue in queues)
                {
                    string[] firstJobIds = null;
                    long length = 0;
                    long fetched = 0;

					
					var pipeline = redis.CreateBatch();
					Task[] tasks = new Task[3];
					tasks[0] = pipeline.ListRangeAsync(
                            String.Format(RedisStorage.Prefix + "queue:{0}", queue), -5, -1)
							.ContinueWith(x => firstJobIds = x.Result.ToStringArray());

                    tasks[1] = pipeline.ListLengthAsync(String.Format(RedisStorage.Prefix + "queue:{0}", queue))
						.ContinueWith(x => length = x.Result);

                    tasks[2] = pipeline.ListLengthAsync(String.Format(RedisStorage.Prefix + "queue:{0}:dequeued", queue))
						.ContinueWith(x => fetched = x.Result);

					pipeline.Execute();
					Task.WaitAll(tasks);

                    var jobs = GetJobsWithProperties(
                        redis,
                        firstJobIds,
                        new[] { "State" },
                        new[] { "EnqueuedAt", "State" },
                        (job, jobData, state) => new EnqueuedJobDto
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

        public JobList<EnqueuedJobDto> EnqueuedJobs(
            string queue, int from, int perPage)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.ListRange(
                    String.Format(RedisStorage.Prefix + "queue:{0}", queue),
                    from,
                    from + perPage - 1).ToStringArray();

                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State" },
                    new[] { "EnqueuedAt", "State" },
                    (job, jobData, state) => new EnqueuedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        EnqueuedAt = JobHelper.DeserializeNullableDateTime(state[0]),
                        InEnqueuedState = jobData[0].Equals(state[1], StringComparison.OrdinalIgnoreCase)
                    });
            });
        }

        public JobList<FetchedJobDto> FetchedJobs(
            string queue, int from, int perPage)
        {
            return UseConnection(redis =>
            {
                var jobIds = redis.ListRange(
                    String.Format(RedisStorage.Prefix + "queue:{0}:dequeued", queue),
                    from, from + perPage - 1).ToStringArray();
				RedisValue[] rk = new RedisValue[1];
				
                return GetJobsWithProperties(
                    redis,
                    jobIds,
                    new[] { "State", "Fetched" },
                    null,
                    (job, jobData, state) => new FetchedJobDto
                    {
                        Job = job,
                        State = jobData[0],
                        FetchedAt = JobHelper.DeserializeNullableDateTime(jobData[1])
                    });
            });
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(redis => GetHourlyTimelineStats(redis, "failed"));
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            return UseConnection(redis =>
            {
                var job = redis.HashGetAll(String.Format(RedisStorage.Prefix + "job:{0}", jobId)).ToStringDictionary();
                if (job.Count == 0) return null;

                var hiddenProperties = new[] { "Type", "Method", "ParameterTypes", "Arguments", "State", "CreatedAt" };

                var historyList = redis.ListRange(String.Format(RedisStorage.Prefix + "job:{0}:history", jobId))
					.Select(x=> (string)x).ToList();

                var history = historyList
                    .Select(JobHelper.FromJson<Dictionary<string, string>>)
                    .ToList();

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
                    Job = TryToGetJob(job["Type"], job["Method"], job["ParameterTypes"], job["Arguments"]),
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

        private Dictionary<DateTime, long> GetHourlyTimelineStats(
            IDatabase redis, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keys = dates.Select(x => String.Format(RedisStorage.Prefix + "stats:{0}:{1}", type, x.ToString("yyyy-MM-dd-HH"))).ToArray();
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

        private Dictionary<DateTime, long> GetTimelineStats(
            IDatabase redis, string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var startDate = endDate.AddDays(-7);
            var dates = new List<DateTime>();

            while (startDate <= endDate)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var stringDates = dates.Select(x => x.ToString("yyyy-MM-dd")).ToList();
            var keys = stringDates.Select(x => String.Format(RedisStorage.Prefix + "stats:{0}:{1}", type, x)).ToArray();

            var valuesMap = redis.GetValuesMap(keys);

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < stringDates.Count; i++)
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
            IDatabase redis,
            string[] jobIds,
            string[] properties,
            string[] stateProperties,
            Func<Job, List<string>, List<string>, T> selector)
        {
            if (jobIds.Length == 0) return new JobList<T>(new List<KeyValuePair<string, T>>());

            var jobs = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);
            var states = new Dictionary<string, Task<RedisValue[]>>(jobIds.Length, StringComparer.OrdinalIgnoreCase);

            properties = properties ?? new string[0];

            var pipeline = redis.CreateBatch();
			var tasks = new List<Task>(jobIds.Length * 2);
            foreach (var jobId in jobIds)
            {
                var id = jobId;
				var jobTask=pipeline.HashGetAsync(
                                String.Format(RedisStorage.Prefix + "job:{0}", id),
								properties.Union(new[] { "Type", "Method", "ParameterTypes", "Arguments" }).Select(x=> (RedisValue)x).ToArray());
				tasks.Add(jobTask);
                jobs.Add(jobId, jobTask);
				if (stateProperties != null)
				{
                    var taskStateJob = pipeline.HashGetAsync(String.Format(RedisStorage.Prefix + "job:{0}:state", id), stateProperties.Select(x => (RedisValue)x).ToArray());
					tasks.Add(taskStateJob);
                    states.Add(jobId, taskStateJob);
				}
            }

            pipeline.Execute();
			Task.WaitAll(tasks.ToArray());
			
			var jobList = new JobList<T>(jobIds
                .Select(x => new
                {
                    JobId = x,
                    Job = jobs[x].Result.ToStringArray().ToList(),
                    Method = TryToGetJob(
                        jobs[x].Result[properties.Length],
                        jobs[x].Result[properties.Length + 1],
                        jobs[x].Result[properties.Length + 2],
                        jobs[x].Result[properties.Length + 3]),
                    State = states.ContainsKey(x) ? states[x].Result.ToStringArray().ToList() : null
                })
                .Select(x => new KeyValuePair<string, T>(
                    x.JobId,
                    x.Job.TrueForAll(y => y == null)
                        ? default(T)
                        : selector(x.Method, x.Job, x.State)))
                .ToList());
			return jobList;
        }

        public long SucceededListCount()
        {
            return UseConnection(redis => redis.ListLength(RedisStorage.Prefix + "succeeded"));
        }

        public StatisticsDto GetStatistics()
        {
            return UseConnection(redis =>
            {
                var stats = new StatisticsDto();

                var queues = redis.SetMembers(RedisStorage.Prefix + "queues", CommandFlags.HighPriority);

				var pipeline = redis.CreateBatch();
				var tasks = new Task[queues.Length + 8];

                tasks[0] = pipeline.SetLengthAsync(RedisStorage.Prefix + "servers", CommandFlags.HighPriority)
					.ContinueWith(x=> stats.Servers = x.Result);

                tasks[1] = pipeline.SetLengthAsync(RedisStorage.Prefix + "queues", CommandFlags.HighPriority)
                    .ContinueWith(x => stats.Queues = x.Result);

            tasks[2] = pipeline.SortedSetLengthAsync(RedisStorage.Prefix + "schedule", flags: CommandFlags.HighPriority)
					.ContinueWith(x => stats.Scheduled = x.Result);

                tasks[3] = pipeline.SortedSetLengthAsync(RedisStorage.Prefix + "processing", flags: CommandFlags.HighPriority)
					.ContinueWith(x => stats.Processing = x.Result);

                tasks[4] = pipeline.StringGetAsync(RedisStorage.Prefix + "stats:succeeded", CommandFlags.HighPriority)
                    .ContinueWith(x => stats.Succeeded = long.Parse(x.Result.HasValue ?  (string)x.Result: "0"));

                tasks[5] = pipeline.SortedSetLengthAsync(RedisStorage.Prefix + "failed", flags: CommandFlags.HighPriority)
					.ContinueWith(x => stats.Failed = x.Result);

                tasks[6] = pipeline.StringGetAsync(RedisStorage.Prefix + "stats:deleted", CommandFlags.HighPriority)
					.ContinueWith(x => stats.Deleted = long.Parse(x.Result.HasValue ?  (string)x.Result : "0"));

                tasks[7] = pipeline.SortedSetLengthAsync(RedisStorage.Prefix + "recurring-jobs", flags: CommandFlags.HighPriority)
                    .ContinueWith(x => stats.Recurring = x.Result);
				var i = 8;
                foreach (var queue in queues)
                {
                    var queueName = queue;
                    tasks[i] = pipeline.ListLengthAsync(String.Format(RedisStorage.Prefix + "queue:{0}", queueName), CommandFlags.HighPriority)
						.ContinueWith(x => stats.Enqueued += x.Result);
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
            string type, string method, string parameterTypes, string arguments)
        {
            try
            {
                return new InvocationData(
                    type,
                    method,
                    parameterTypes,
                    arguments).Deserialize();
            }
            catch (Exception)
            {
                return null;
            }
        }
    }
}

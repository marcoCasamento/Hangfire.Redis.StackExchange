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

namespace Hangfire.Redis.StackExchange
{
    public class RedisStorageOptions
    {
        public const string DefaultPrefix = "{hangfire}:";

        public RedisStorageOptions()
        {
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            FetchTimeout = TimeSpan.FromMinutes(3);
            ExpiryCheckInterval = TimeSpan.FromHours(1);
            Db = 0;
            Prefix = DefaultPrefix;
            SucceededListSize = 499;
            DeletedListSize = 499;
            LifoQueues = new string[0];
            UseTransactions = true;
        }

        /// <summary>
        /// It's a part of mechanism to requeue the job if the server processing it died for some reason
        /// </summary>
        /// <see cref="FetchedJobsWatcher"/>
        public TimeSpan InvisibilityTimeout { get; set; }

        /// <summary>
        /// It's a fallback for fetching jobs if pub/sub mechanism fails. This software use redis pub/sub to be notified
        /// when a new job has been enqueued. Redis pub/sub however doesn't guarantee delivery so, should the
        /// RedisConnection lose a message, within the FetchTimeout the queue will be scanned from scratch
        /// and any waiting jobs will be processed.
        /// </summary>
        public TimeSpan FetchTimeout { get; set; }

        /// <summary>
        /// Time that should pass between expired jobs cleanup.
        /// RedisStorage uses non-expiring keys, so to clean up the store there is a thread running a IServerComponent
        /// that take care of deleting expired jobs from redis.
        /// </summary>
        public TimeSpan ExpiryCheckInterval { get; set; }
        public string Prefix { get; set; }
        public int Db { get; set; }
        public int SucceededListSize { get; set; }
        public int DeletedListSize { get; set; }
        public string[] LifoQueues { get; set; }
        public bool UseTransactions { get; set; }
    }
}
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
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Annotations;
using Hangfire.Logging;
using StackExchange.Redis;
using System.Text;
using System.Net;

namespace Hangfire.Redis
{
    public class RedisStorage : JobStorage
    {
        internal static readonly string Prefix = "hangfire:";

		private readonly ConnectionMultiplexer _connectionMultiplexer;
		private  TimeSpan _invisibilityTimeout;

        public RedisStorage()
            : this("localhost:6379")
        {
        }

        public RedisStorage(string connectionString)
			: this(connectionString, (int)0)
        {
        }

		
		public RedisStorage(string connectionString, int db)
			: this(connectionString, db, TimeSpan.FromMinutes(30))
        {
        }

		public RedisStorage(string connectionString, int db, TimeSpan invisibilityTimeout)
		{
			if (connectionString == null) throw new ArgumentNullException("connectionString");
			if (invisibilityTimeout == null) throw new ArgumentNullException("invisibilityTimeout");

			_connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
			_invisibilityTimeout = invisibilityTimeout;
			var endpoint = (DnsEndPoint)_connectionMultiplexer.GetEndPoints()[0];
			ConnectionString = string.Format("{0}:{1}", endpoint.Host, endpoint.Port);
			Db = db;

		}


        public string ConnectionString { get; private set; }
        public int Db { get; private set; }
 		public ConnectionMultiplexer RConnectionMultiplexer { get { return _connectionMultiplexer; } }

        public override IMonitoringApi GetMonitoringApi()
        {
            return new RedisMonitoringApi(_connectionMultiplexer.GetDatabase(Db));
        }

        public override IStorageConnection GetConnection()
        {
            return new RedisConnection(_connectionMultiplexer.GetDatabase(Db));
        }

        public override IEnumerable<IServerComponent> GetComponents()
        {
            yield return new FetchedJobsWatcher(this, _invisibilityTimeout);
        }

        public override IEnumerable<IStateHandler> GetStateHandlers()
        {
            yield return new FailedStateHandler();
            yield return new ProcessingStateHandler();
            yield return new SucceededStateHandler();
            yield return new DeletedStateHandler();
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for Redis job storage:");
	
			logger.InfoFormat("ConnectionString: {0}\nDN: {1}", ConnectionString, Db);
        }

        public override string ToString()
        {
			return String.Format("redis://{0}/{1}", ConnectionString, Db);
        }

        internal static string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return Prefix + key;
        }
    }
}
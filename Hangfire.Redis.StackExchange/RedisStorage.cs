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
using Hangfire.Server;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Annotations;
using Hangfire.Logging;
using StackExchange.Redis;
using System.Net;

namespace Hangfire.Redis
{
    public class RedisStorage : JobStorage
    {
        internal static string Prefix;
        internal static readonly string Identity = Guid.NewGuid().ToString();
		private readonly ConnectionMultiplexer _connectionMultiplexer;
		private  TimeSpan _invisibilityTimeout;

        public RedisStorage()
            : this("localhost:6379")
        {
        }

        public RedisStorage(string connectionString, RedisStorageOptions options = null)
		{
			if (connectionString == null) throw new ArgumentNullException("connectionString");
			if (options == null) options = new RedisStorageOptions();

			_connectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
            _invisibilityTimeout = options.InvisibilityTimeout;
			var endpoint = _connectionMultiplexer.GetEndPoints()[0];
			if (endpoint is IPEndPoint)
			{
				var ipEp = endpoint as IPEndPoint;
				ConnectionString = string.Format("{0}:{1}", TryGetHostName(ipEp.Address), ipEp.Port);
			}
			else 
			{
				var dnsEp = endpoint as DnsEndPoint;
				ConnectionString = string.Format("{0}:{1}", dnsEp.Host, dnsEp.Port);
			}

            Db = options.Db;
            if (Prefix != options.Prefix)
            {
                Prefix = options.Prefix;
            }
		}
		private static string TryGetHostName(IPAddress address)
		{
			string hostName = null;
			try
			{
				var hostEntry = Dns.GetHostEntry(address);
				hostName = hostEntry != null ? hostEntry.HostName : address.ToString();
			}
			catch 
			{
				//Whatever happens, just return address.ToString();
				hostName = address.ToString();
			}
			return hostName;
		}

        public string ConnectionString { get; private set; }
        public int Db { get; private set; }
        public override IMonitoringApi GetMonitoringApi()
        {
			return new RedisMonitoringApi(_connectionMultiplexer.GetDatabase(Db));
        }

        public override IStorageConnection GetConnection()
        {
            return new RedisConnection(_connectionMultiplexer.GetDatabase(Db), _connectionMultiplexer.GetSubscriber());
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
			return string.Format("redis://{0}/{1}", ConnectionString, Db);
        }

        internal static string GetRedisKey([NotNull] string key)
        {
            if (key == null) throw new ArgumentNullException("key");

            return Prefix + key;
        }
    }
}
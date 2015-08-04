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
namespace Hangfire.Redis
{
    public static class RedisBootstrapperConfigurationExtensions
    {
        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at localhost:6379 and use the '0' db to store 
        /// the data.
        /// </summary>
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration)
        {
            var storage = new RedisStorage();
			GlobalConfiguration.Configuration.UseStorage(storage);
            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at the specified host and port and store the
        /// data in db with number '0'.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="hostAndPort">Host and port, for example 'localhost:6379'</param>
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            string hostAndPort)
        {
            var storage = new RedisStorage(hostAndPort);
            GlobalConfiguration.Configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage
        /// available at the specified host and port, and store the
        /// data in the given database number.
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="hostAndPort">Host and port, for example, 'localhost:6379'</param>
        /// <param name="db">Database number to store the data, for example '0'</param>
        /// <returns></returns>
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            string hostAndPort,
            int db)
        {
            var storage = new RedisStorage(hostAndPort, db);
			GlobalConfiguration.Configuration.UseStorage(storage);

            return storage;
        }

        /// <summary>
        /// Tells the bootstrapper to use Redis as a job storage with
        /// the given options, available at the specified host and port,
        /// and store the data in the given database number. 
        /// </summary>
        /// <param name="configuration">Configuration</param>
        /// <param name="hostAndPort">Host and port, for example 'localhost:6379'</param>
        /// <param name="db">Database number to store the data, for example '0'</param>
        /// <param name="options">Advanced storage options</param>
        public static RedisStorage UseRedisStorage(
            this IBootstrapperConfiguration configuration,
            string connectionString,
            int db,
            TimeSpan invisibilityTimeout)
        {
			var storage = new RedisStorage(connectionString, db, invisibilityTimeout);
			GlobalConfiguration.Configuration.UseStorage(storage);

            return storage;
        }
    }
}

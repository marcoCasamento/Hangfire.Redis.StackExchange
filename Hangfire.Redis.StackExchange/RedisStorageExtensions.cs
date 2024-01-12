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
using Hangfire.Annotations;
using StackExchange.Redis;

namespace Hangfire.Redis.StackExchange
{
    public static class RedisStorageExtensions
    {
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(
            [NotNull] this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var storage = new RedisStorage();
            GlobalJobFilters.Filters.Add(new HangfireSubscriber());
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] IConnectionMultiplexer connectionMultiplexer,
            RedisStorageOptions options = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (connectionMultiplexer == null) throw new ArgumentNullException(nameof(connectionMultiplexer));
            var storage = new RedisStorage(connectionMultiplexer, options);
            GlobalJobFilters.Filters.Add(new HangfireSubscriber());
            return configuration.UseStorage(storage);
        }


        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] string nameOrConnectionString, 
            RedisStorageOptions options = null)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));
            var storage = new RedisStorage(nameOrConnectionString, options);
            GlobalJobFilters.Filters.Add(new HangfireSubscriber());
            return configuration.UseStorage(storage);
        }
    }
}
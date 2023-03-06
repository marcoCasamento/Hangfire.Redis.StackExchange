using System;
using Hangfire.Annotations;
using Hangfire.Redis;
using StackExchange.Redis;

namespace Hangfire
{
    public static class RedisStorageExtensions
    {
        public static IGlobalConfiguration<RedisStorage> UseRedisStorage(
            [NotNull] this IGlobalConfiguration configuration)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            var storage = new RedisStorage();
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
            return configuration.UseStorage(storage);
        }
    }
}
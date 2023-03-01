using StackExchange.Redis;
using System;

namespace Hangfire.Redis.Tests
{
    public static class RedisUtils
    {
        private const string HostVariable = "Hangfire_Redis_Host";
        private const string PortVariable = "Hangfire_Redis_Port";
        private const string DbVariable = "Hangfire_Redis_Db";

        private const string DefaultHost = "127.0.0.1";
        private const int DefaultPort = 6379;
        private const int DefaultDb = 1;
        static Lazy<ConnectionMultiplexer> connection = null;

        static RedisUtils()
        {
            connection = new Lazy<ConnectionMultiplexer>(() =>
                {
                    ConfigurationOptions options = new ConfigurationOptions
                    {
                        AllowAdmin = true,
                        SyncTimeout = 5000,
                        ConnectRetry = 5
                    };
                    options.EndPoints.Add(GetHostAndPort());
                    return ConnectionMultiplexer.Connect(options);
                }
            );
        }

        public static IDatabase CreateClient()
        {
            return connection.Value.GetDatabase(DefaultDb);
        }

        public static ISubscriber CreateSubscriber()
        {
            return connection.Value.GetSubscriber();
        }

        public static string GetHostAndPort()
        {
            return String.Format("{0}:{1}", GetHost(), GetPort());
        }

        public static string GetHost()
        {
            return Environment.GetEnvironmentVariable(HostVariable)
                   ?? DefaultHost;
        }

        public static int GetPort()
        {
            var portValue = Environment.GetEnvironmentVariable(PortVariable);
            return portValue != null ? int.Parse(portValue) : DefaultPort;
        }

        public static int GetDb()
        {
            var dbValue = Environment.GetEnvironmentVariable(DbVariable);
            return dbValue != null ? int.Parse(dbValue) : DefaultDb;
        }
    }
}
using StackExchange.Redis;
using System;

namespace Hangfire.Redis.Tests
{
    public static class RedisUtils
    {
        private const string HostVariable = "Hangfire_Redis_Host";
        private const string PortVariable = "Hangfire_Redis_Port";
        private const string DbVariable = "Hangfire_Redis_Db";

        private const string DefaultHost = "localhost";
        private const int DefaultPort = 6379;
        private const int DefaultDb = 1;
		static ConnectionMultiplexer connection = null;
        public static IDatabase CreateClient()
        {
			ConfigurationOptions options = new ConfigurationOptions();
			options.EndPoints.Add(GetHostAndPort());
			options.AllowAdmin = true;
			if (connection == null)
				connection = ConnectionMultiplexer.Connect(options);
			
			return connection.GetDatabase(DefaultDb);
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

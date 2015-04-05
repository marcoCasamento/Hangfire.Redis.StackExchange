using StackExchange.Redis;
using System;
using System.Runtime.ExceptionServices;
using System.Threading;

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
					ExceptionDispatchInfo lastError = null;
					ConfigurationOptions options = new ConfigurationOptions();
					options.EndPoints.Add(GetHostAndPort());
					options.AllowAdmin = true;
					for (int i = 0; i < 5; i++)
					{
						try
						{
							var cnn = ConnectionMultiplexer.Connect(options);
							if (cnn.IsConnected)
								return cnn;
						}
						catch (Exception ex)
						{
							lastError = ExceptionDispatchInfo.Capture(ex);
							Console.WriteLine(ex.Message);
							Thread.Sleep(10);
						}
					}
					lastError.Throw();
					return null;
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

using System;
using Hangfire;
using Hangfire.Redis;
using Hangfire.Redis.StackExchange;
namespace Hangfire.Redis.StackExchange.ConsoleSample
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                GlobalConfiguration.Configuration
                    .UseColouredConsoleLogProvider()
                    .UseRedisStorage("127.0.0.1:6379");
                var client = new BackgroundJobServer();

                // Run once
                BackgroundJob.Enqueue(() => System.Console.WriteLine("Background Job: Hello, world!"));

                BackgroundJob.Enqueue(() => Test());

                // Run every minute
                RecurringJob.AddOrUpdate(() => Test(), Cron.Minutely);

                System.Console.WriteLine("Press Enter to exit...");
                System.Console.ReadLine();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
        public static int x = 0;
        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void Test()
        {
            System.Console.WriteLine($"{x++} Cron Job: Hello, world!");
        }
    }
}

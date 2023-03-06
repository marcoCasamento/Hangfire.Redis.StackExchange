using System;

namespace Hangfire.Redis
{
    internal class FetchedJobsWatcherOptions
    {
        public FetchedJobsWatcherOptions()
        {
            FetchedLockTimeout = TimeSpan.FromMinutes(1);
            CheckedTimeout = TimeSpan.FromMinutes(1);
            SleepTimeout = TimeSpan.FromMinutes(1);
        }

        public TimeSpan FetchedLockTimeout { get; set; }
        public TimeSpan CheckedTimeout { get; set; }
        public TimeSpan SleepTimeout { get; set; }
    }
}
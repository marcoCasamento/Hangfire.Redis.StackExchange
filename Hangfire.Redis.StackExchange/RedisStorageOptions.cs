using System;

namespace Hangfire.Redis
{
    public class RedisStorageOptions
    {
        public const string DefaultPrefix = "{hangfire}:";

        public RedisStorageOptions()
        {
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            FetchTimeout = TimeSpan.FromMinutes(3);
            ExpiryCheckInterval = TimeSpan.FromHours(1);
            Db = 0;
            Prefix = DefaultPrefix;
            SucceededListSize = 499;
            DeletedListSize = 499;
            LifoQueues = new string[0];
            UseTransactions = true;
        }

        public TimeSpan InvisibilityTimeout { get; set; }
        public TimeSpan FetchTimeout { get; set; }
        public TimeSpan ExpiryCheckInterval { get; set; }
        public string Prefix { get; set; }
        public int Db { get; set; }

        public int SucceededListSize { get; set; }
        public int DeletedListSize { get; set; }
        public string[] LifoQueues { get; set; }
        public bool UseTransactions { get; set; }
    }
}

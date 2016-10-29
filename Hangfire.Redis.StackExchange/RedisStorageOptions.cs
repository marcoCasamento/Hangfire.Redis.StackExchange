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
    public class RedisStorageOptions 
    {
        public const string DefaultPrefix = "hangfire:";
        
        public RedisStorageOptions() 
        {
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            FetchTimeout = TimeSpan.FromMinutes(3);
            Db = 0;
            Prefix = DefaultPrefix;
            SucceededListSize = 499;
            DeletedListSize = 499;
        }

        public TimeSpan InvisibilityTimeout { get; set; }
        public TimeSpan FetchTimeout { get; set; }
        public string Prefix { get; set; }
        public int Db { get; set; }

        public int SucceededListSize { get; set; }
        public int DeletedListSize { get; set; }
    }
}

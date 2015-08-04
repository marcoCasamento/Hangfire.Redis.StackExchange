﻿// This file is part of Hangfire.
// Copyright © 2013-2014 Sergey Odinokov.
// 
// Hangfire is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as 
// published by the Free Software Foundation, either version 3 
// of the License, or any later version.
// 
// Hangfire is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public 
// License along with Hangfire. If not, see <http://www.gnu.org/licenses/>.

using StackExchange.Redis;
using System;



namespace Hangfire.Redis
{
    public class RedisStorageOptions 
    {
        public const string DefaultPrefix = "hangfire:";

        public RedisStorageOptions() 
        {
            ConnectionPoolSize = 50;
            InvisibilityTimeout = TimeSpan.FromMinutes(30);
            Db = 0;
            Prefix = DefaultPrefix;
        }

        public int ConnectionPoolSize { get; set; }
        public TimeSpan InvisibilityTimeout { get; set; }
        public string Prefix { get; set; }
        public int Db { get; set; }
    }
}

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

using Hangfire.Annotations;
using StackExchange.Redis;
using System;
using System.Diagnostics;
using System.Threading;

namespace Hangfire.Redis
{
    
    internal class RedisLock : IDisposable
    {
        readonly IDatabase _redis;
        readonly RedisKey _key;
        readonly RedisValue _owner;
        private bool isRoot = true;
        public RedisLock([NotNull]IDatabase redis, [NotNull]RedisKey key, [NotNull]RedisValue owner, [NotNull]TimeSpan timeOut)
        {
            _redis = redis;
            _key = key;
            _owner = owner;

            //The comparison below uses timeOut as a max timeSpan in waiting Lock
            int i = 0;
            DateTime lockExpirationTime = DateTime.UtcNow +timeOut;
            while (DateTime.UtcNow < lockExpirationTime)
            {
                if (_redis.LockTake(key, owner, timeOut))
                    return;
                //assumes that a second call made by the same owner means an extension request
                var lockOwner = _redis.LockQuery(key);
                
                if (lockOwner.Equals(owner))
                {
                    //extends the lock only for the remaining time
                    var ttl = redis.KeyTimeToLive(key) ?? TimeSpan.Zero;
                    var extensionTTL = lockExpirationTime - DateTime.UtcNow;
                    if (extensionTTL > ttl)
                    {
                        Debug.WriteLine("Extending lock {0} - {1} by {2}", _key, _owner, (extensionTTL));
                        _redis.LockExtend(key, owner, extensionTTL);
                    }
                    isRoot = false;
                    return;
                }
                SleepBackOffMultiplier(i);
                i++;
            }
            throw new TimeoutException(string.Format("Lock on {0} with owner identifier {1} Exceeded timeout of {2}", key, owner.ToString(), timeOut));
        }

        public void Dispose()
        {
            if (isRoot && !_redis.LockRelease(_key, _owner))
                Debug.WriteLine("Lock {0} - {1} already timed out", _key, _owner);
        }

        private static void SleepBackOffMultiplier(int i)
        {
            //exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            Thread.Sleep(nextTry);
        }
    }
}

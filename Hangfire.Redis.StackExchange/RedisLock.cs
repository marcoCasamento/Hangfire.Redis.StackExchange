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
using Hangfire.Storage;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace Hangfire.Redis
{
    internal class RedisLock : IDisposable
    {
        private static readonly TimeSpan DefaultHoldDuration = TimeSpan.FromSeconds(30);
        private static readonly string OwnerId = Guid.NewGuid().ToString();

#if NET45
        /// <summary>
        /// Drop-in implementation of AsyncLocal for NET 4.5
        /// </summary>
        private class AsyncLocal<T>
        {
            private readonly string __name = Guid.NewGuid().ToString();

            public T Value
            {
                get
                {
                    var value = System.Runtime.Remoting.Messaging.CallContext.LogicalGetData(__name);
                    return value == null ? default(T) : (T)value;
                }
                set
                {
                    System.Runtime.Remoting.Messaging.CallContext.LogicalSetData(__name, value);
                }
            }
        }
#endif
        
        private static AsyncLocal<ISet<RedisKey>> _heldLocks = new AsyncLocal<ISet<RedisKey>>();

        private static ISet<RedisKey> HeldLocks
        {
            get
            {
                var value = _heldLocks.Value;
                if (value == null)
                    _heldLocks.Value = value = new HashSet<RedisKey>();
                return value;
            }
        }

        private readonly IDatabase _redis;
        private readonly RedisKey _key;
        private readonly bool _holdsLock;
        private volatile bool _isDisposed = false;
        private readonly Timer _slidingExpirationTimer;

        private RedisLock([NotNull] IDatabase redis, RedisKey key, bool holdsLock, TimeSpan holdDuration)
        {
            _redis = redis;
            _key = key;
            _holdsLock = holdsLock;

            if (holdsLock)
            {
                HeldLocks.Add(_key);

                // start sliding expiration timer at half timeout intervals
                var halfLockHoldDuration = TimeSpan.FromTicks(holdDuration.Ticks / 2);
                _slidingExpirationTimer = new Timer(ExpirationTimerTick, holdDuration, halfLockHoldDuration, halfLockHoldDuration);
            }
        }

        private void ExpirationTimerTick(object state)
        {
            if (!_isDisposed)
            {
                _redis.LockExtend(_key, OwnerId, (TimeSpan)state);
            }
        }

        public void Dispose()
        {
            if (_holdsLock)
            {
                _isDisposed = true;
                _slidingExpirationTimer.Dispose();

                if (!_redis.LockRelease(_key, OwnerId))
                {
                    Debug.WriteLine("Lock {0} already timed out", _key);
                }

                HeldLocks.Remove(_key);
            }
        }

        public static IDisposable Acquire([NotNull] IDatabase redis, RedisKey key, TimeSpan timeOut)
        {
            return Acquire(redis, key, timeOut, DefaultHoldDuration);
        }

        internal static IDisposable Acquire([NotNull] IDatabase redis, RedisKey key, TimeSpan timeOut, TimeSpan holdDuration)
        {
            if (redis == null)
                throw new ArgumentNullException(nameof(redis));

            if (HeldLocks.Contains(key))
            {
                // lock is already held
                return new RedisLock(redis, key, false, holdDuration);
            }

            // The comparison below uses timeOut as a max timeSpan in waiting Lock
            var i = 0;
            var lockExpirationTime = DateTime.UtcNow + timeOut;
            do
            {
                if (redis.LockTake(key, OwnerId, holdDuration))
                {
                    // we have successfully acquired the lock
                    return new RedisLock(redis, key, true, holdDuration);
                }
                
                SleepBackOffMultiplier(i++, (int)(lockExpirationTime - DateTime.UtcNow).TotalMilliseconds);
            }
            while (DateTime.UtcNow < lockExpirationTime);

            throw new DistributedLockTimeoutException($"Failed to acquire lock on {key} within given timeout ({timeOut})");
        }

        private static void SleepBackOffMultiplier(int i, int maxWait)
        {
            if (maxWait <= 0) return;

            // exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            nextTry = Math.Min(nextTry, maxWait);

            Thread.Sleep(nextTry);
        }
    }
}

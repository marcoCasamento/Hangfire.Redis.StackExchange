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

using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Hangfire.Redis
{
    public static class RedisDatabaseExtensions
	{
		public static HashEntry[] ToHashEntries(this IEnumerable<KeyValuePair<string, string>> keyValuePairs)
		{
			var hashEntry = new HashEntry[keyValuePairs.Count()];
			int i = 0;
			foreach (var kvp in keyValuePairs)
			{
				hashEntry[i] = new HashEntry(kvp.Key, kvp.Value);
				i++;
			}
			return hashEntry;
		}
		
		//public static Dictionary<string, string> ToStringDictionary(this HashEntry[] entries)
		//{
		//	var dictionary = new Dictionary<string, string>(entries.Length);
		//	foreach (var entry in entries)
		//		dictionary[entry.Name] = entry.Value;
		//	return dictionary;
		//}
		
		public static bool ContainsKey(this HashEntry[] hashEntries, RedisValue key)
		{
			return hashEntries.Any(x=> x.Name == key);
		}

        public static Dictionary<string, string> GetValuesMap(this IDatabase redis, string[] keys)
		{
			var redisKeyArr = keys.Select(x => (RedisKey)x).ToArray();
			var valuesArr = redis.StringGet(redisKeyArr);
			Dictionary<string, string> result = new Dictionary<string, string>(valuesArr.Length);
			for (int i = 0; i < valuesArr.Length; i++)
			{
				result.Add(redisKeyArr[i], valuesArr[i]);
			}
			return result;
		}


    }

	public sealed class _RedisLock : IDisposable
	{
		readonly IDatabase _redis;
		readonly RedisKey _key;

        public _RedisLock(IDatabase redis, RedisKey key, TimeSpan? timeOut)
        {
            _redis = redis;
            _key = key;

            RetryUntilTrue(
                () =>
                {
                    //This pattern is taken from the redis command for SETNX http://redis.io/commands/setnx

                    //Calculate a unix time for when the lock should expire
                    TimeSpan realSpan = timeOut ?? new TimeSpan(365, 0, 0, 0); //if nothing is passed in the timeout hold for a year
                    DateTime expireTime = DateTime.UtcNow.Add(realSpan);
                    
                    //string lockString = (ToUnixTimeMs(expireTime) + 1).ToString();
                    string lockString = (expireTime.Ticks + 1).ToString();

                    //Try to set the lock, if it does not exist this will succeed and the lock is obtained
                    var nx = redis.StringSet(key, lockString, realSpan, When.NotExists);
                    if (nx)
                        return true;
                    Debug.WriteLine("key exists and is not expired");
                    //If we've gotten here then a key for the lock is present. This could be because the lock is
                    //correctly acquired or it could be because a client that had acquired the lock crashed (or didn't release it properly).
                    //Therefore we need to get the value of the lock to see when it should expire
                    string lockExpireString = redis.StringGet(key);
                    long lockExpireTime;
                    if (!long.TryParse(lockExpireString, out lockExpireTime))
                        return false;

                    //If the expire time is greater than the current time then we can't let the lock go yet
                    //if (lockExpireTime > ToUnixTimeMs(DateTime.UtcNow))
                    if (lockExpireTime > DateTime.UtcNow.Ticks)
                    {
                        Debug.WriteLine("key exists and is not expired - 3 {0:yyyyMMdd hh:mm:ss.ms} vs {1:yyyyMMdd hh:mm:ss.ms}", new DateTime(lockExpireTime), DateTime.UtcNow);
                        return false;
                    }
                    //If the expire time is less than the current time then it wasn't released properly and we can attempt to 
                    //acquire the lock. This is done by setting the lock to our timeout string AND checking to make sure
                    //that what is returned is the old timeout string in order to account for a possible race condition.
                    Debug.WriteLine("assuming key is either expired or undefined");
                    return redis.StringGetSet(key, lockString) == lockExpireString;
                },
                timeOut
            );
        }
        public void Dispose()
		{
            //Discard value, it's virtual impossible that the supplied token is incorrect
            var trn = _redis.CreateTransaction();
            var task = trn.KeyDeleteAsync(_key);
            trn.Execute();
            task.Wait();
            
		}

        private static void RetryUntilTrue(Func<bool> action, TimeSpan? timeOut)
        {
            var i = 0;
            var firstAttempt = DateTime.UtcNow;

            while (timeOut == null || DateTime.UtcNow - firstAttempt < timeOut.Value)
            {
                i++;
                if (action())
                {
                    return;
                }
                SleepBackOffMultiplier(i);
            }

            throw new TimeoutException(string.Format("Exceeded timeout of {0}", timeOut.Value));
        }
        private static void SleepBackOffMultiplier(int i)
        {
            //exponential/random retry back-off.
            var rand = new Random(Guid.NewGuid().GetHashCode());
            var nextTry = rand.Next(
                (int)Math.Pow(i, 2), (int)Math.Pow(i + 1, 2) + 1);

            Thread.Sleep(nextTry);
        }

        public const long UnixEpoch = 621355968000000000L;
        public static long ToUnixTimeMs(DateTime dateTime)
        {
            var universal = ToDateTimeSinceUnixEpoch(dateTime);
            return (long)universal.TotalMilliseconds;
        }
        private static TimeSpan ToDateTimeSinceUnixEpoch(DateTime dateTime)
        {
            var dtUtc = dateTime;
            if (dateTime.Kind != DateTimeKind.Utc)
            {
                dtUtc = dateTime.Kind == DateTimeKind.Unspecified && dateTime > DateTime.MinValue
                    ? DateTime.SpecifyKind(dateTime.Subtract(LocalTimeZone.GetUtcOffset(dateTime)), DateTimeKind.Utc)
                    : ToStableUniversalTime(dateTime);
            }

            var universal = dtUtc.Subtract(UnixEpochDateTimeUtc);
            return universal;
        }

        internal static TimeZoneInfo LocalTimeZone = GetLocalTimeZoneInfo();
        private static readonly DateTime MinDateTimeUtc = new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private static readonly DateTime UnixEpochDateTimeUtc = new DateTime(UnixEpoch, DateTimeKind.Utc);
        public static DateTime ToStableUniversalTime(DateTime dateTime)
        {
            if (dateTime.Kind == DateTimeKind.Utc)
                return dateTime;
            if (dateTime == DateTime.MinValue)
                return MinDateTimeUtc;

            return TimeZoneInfo.ConvertTimeToUtc(dateTime);
        }
        
        public static TimeZoneInfo GetLocalTimeZoneInfo()
        {
            try
            {
                return TimeZoneInfo.Local;
            }
            catch (Exception)
            {
                return TimeZoneInfo.Utc; //Fallback for Mono on Windows.
            }
        }
    }
}

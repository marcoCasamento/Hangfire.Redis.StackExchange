using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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

		public static RedisLock AcquireLock(this IDatabase redis, RedisKey key, RedisValue value, TimeSpan timeout)
		{
			DateTime enterTime = DateTime.UtcNow;
			while (DateTime.UtcNow.Subtract(enterTime)<timeout)
			{
				//TODO: Should update the timeout on lock
				if (redis.LockTake(key, value, timeout))
				{
					return new RedisLock(redis, key, value);
				}
			}
			throw new Exception(String.Format("Unable to take the lock key={0} value={1} in {2}, key already exists ?", key, value, timeout));
			
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

	public sealed class RedisLock : IDisposable
	{
		readonly IDatabase _redis;
		readonly RedisKey _key;
		readonly RedisValue _value;
		public RedisLock(IDatabase redis, RedisKey key, RedisValue value)
		{
			_redis = redis;
			_key = key;
			_value = value;
		}
		public void Dispose()
		{
			//Discard value, it's virtual impossible that the supplied token is incorrect
			_redis.LockRelease(_key, _value);
		}
	}
}

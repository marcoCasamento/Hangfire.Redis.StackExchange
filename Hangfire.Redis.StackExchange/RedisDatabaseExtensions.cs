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
			for (int i = 0; i < 5; i++)
			{
				if (redis.LockTake(key, value, timeout))
				{

					return new RedisLock(redis, key, value);
				}
				Thread.Sleep(10);
			}
			throw new Exception(String.Format("Unable to take the lock key={0} value={1} after 5 tentatives, key already exists ?", key, value));
			
		}

		public static string ListRightGetFromAndRightPushTo(this IDatabase redis, RedisKey listKeyFrom, RedisKey listKeyTo, TimeSpan timeOut)
		{
			RedisValue lockValueFrom = String.Format("{0}:From:{1}", listKeyFrom, Guid.NewGuid());
			RedisValue lockValueTo = String.Format("{0}:To:{1}", listKeyTo, Guid.NewGuid());
			System.Diagnostics.Debug.WriteLine("**ListRightGetFrom {0} AndRightPushTo {1}", listKeyFrom, listKeyTo);
			AutoResetEvent are = new AutoResetEvent(false);
			var sub = redis.Multiplexer.GetSubscriber();
			sub.Subscribe(listKeyTo.ToString(), (c, v) =>
				{
					redis.ListRightPush(listKeyTo, v);
					are.Set();
				});
			
			var poppedValue = redis.ListRightPop(listKeyTo);
			if (poppedValue.HasValue)
			{
				sub.Publish(listKeyTo.ToString(), poppedValue);
				are.WaitOne(timeOut);
				return poppedValue;
			}
			return null;
			
		}

		public static RedisValue ListRightGetFromAndRightPushTo(this IDatabase redis, RedisKey listKeyFrom, RedisKey listKeyTo)
		{
			System.Diagnostics.Debug.WriteLine("ListRightGetFrom {0} AndRightPushTo {1}", listKeyFrom, listKeyTo);
			var listValue = redis.ListRightPop(listKeyFrom);
			if (listValue.HasValue)
				redis.ListRightPush(listKeyTo, listValue);
			return listValue;
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

	public class RedisLock : IDisposable
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

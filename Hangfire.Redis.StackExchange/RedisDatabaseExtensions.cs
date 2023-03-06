using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;

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

        public static RedisValue[] ToRedisValues(this IEnumerable<string> values)
        {
            if (values == null)
                throw new ArgumentNullException(nameof(values));

            return values.Select(x => (RedisValue) x).ToArray();
        }

        //public static Dictionary<string, string> ToStringDictionary(this HashEntry[] entries)
        //{
        //	var dictionary = new Dictionary<string, string>(entries.Length);
        //	foreach (var entry in entries)
        //		dictionary[entry.Name] = entry.Value;
        //	return dictionary;
        //}

        public static Dictionary<string, string> GetValuesMap(this IDatabase redis, string[] keys)
        {
            var redisKeyArr = keys.Select(x => (RedisKey) x).ToArray();
            var valuesArr = redis.StringGet(redisKeyArr);
            Dictionary<string, string> result = new Dictionary<string, string>(valuesArr.Length);
            for (int i = 0; i < valuesArr.Length; i++)
            {
                result.Add(redisKeyArr[i], valuesArr[i]);
            }

            return result;
        }
    }
}
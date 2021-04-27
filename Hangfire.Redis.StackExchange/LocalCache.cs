using System;
using System.Collections.Concurrent;
using System.Linq;

namespace Hangfire.Redis
{
    public sealed class LocalCache
    {
        private static readonly Lazy<LocalCache> _instance = new Lazy<LocalCache>(() => new LocalCache());
        public static LocalCache Instance => _instance.Value;
        
        private const int DefaultMax = 1000;
        private const int DefaultExpiredMinutes = 10;
        private readonly ConcurrentDictionary<string, long> _cache;
        private readonly ConcurrentQueue<string> _queue;

        private long _checkTicks; 
        private static readonly object syncRoot = new object();

        public LocalCache()
        {
            _cache = new ConcurrentDictionary<string, long>();
            _queue = new ConcurrentQueue<string>();
            _checkTicks = 0;
        }

        /// <summary>
        /// 如果已存在则不添加并返回false，如果不存在则添加，并清理超出默认队列长度的旧Key
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool TryAdd(string key)
        {
            if (_cache.ContainsKey(key)) return false;
            lock (syncRoot)
            {
                if (_cache.ContainsKey(key)) return false;

                var expiredTicks = DateTime.Now.AddMinutes(DefaultExpiredMinutes).Ticks;
                if (_cache.TryAdd(key, expiredTicks))
                {
                    _queue.Enqueue(key);
                }
                else
                {
                    return false;
                }

                if (_queue.Count > DefaultMax && _queue.TryDequeue(out var lastKey))
                {
                    _cache.TryRemove(lastKey, out _);
                }

                CleanIfExpired();
                return true;
            }
        }

        private void CleanIfExpired()
        {
            if (_queue.IsEmpty && _cache.IsEmpty) return;
            if (_queue.Count < 2 && _cache.Count < 2) return; // 数量为1则表示新增加的数据，不做过期处理

            var lastExpiredTicks = DateTime.Now.AddMinutes(-DefaultExpiredMinutes).Ticks;
            if (_checkTicks > lastExpiredTicks) return;

            var expiredKeys = _cache.Where(x => x.Value < _checkTicks).Select(x => x.Key);
            _checkTicks = lastExpiredTicks; // 放在check和筛选后
            if (!expiredKeys.Any()) return;

            foreach (var expiredKey in expiredKeys)
            {
                _cache.TryRemove(expiredKey, out _);
                _queue.TryDequeue(out _); // 出队相应数量的数据
            }
        }

    }
}
using System;
using System.Threading;
using StackExchange.Redis;

namespace Hangfire.Redis
{
    internal class RedisSubscription : IDisposable
    {
        internal static string Channel = string.Format("{0}JobFetchChannel", RedisStorage.Prefix);

        private readonly ManualResetEvent _mre = new ManualResetEvent(false);
        private readonly ISubscriber _subscriber;

        public RedisSubscription(ISubscriber subscriber)
        {
            _subscriber = subscriber;
            _subscriber.Subscribe(Channel, (channel, value) => _mre.Set());
        }
        
                
        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            _mre.Reset();
            WaitHandle.WaitAny(new[] { _mre, cancellationToken.WaitHandle }, timeout);
        }

        public void Dispose()
        {
            _subscriber.Unsubscribe(Channel);
            _mre.Dispose();
        }
    }
}

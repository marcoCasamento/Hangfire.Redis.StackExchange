using System;
using System.Threading;
using Hangfire.Server;
using StackExchange.Redis;
using Hangfire.Annotations;

namespace Hangfire.Redis
{
#pragma warning disable 618
    internal class RedisSubscription : IServerComponent
#pragma warning restore 618
    {
        private readonly AutoResetEvent _resetEvent = new AutoResetEvent(false);

        public RedisSubscription([NotNull] RedisStorage storage, [NotNull] ISubscriber subscriber)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (subscriber == null) throw new ArgumentNullException(nameof(subscriber));


            Channel = storage.GetRedisKey("JobFetchChannel");

            subscriber.Subscribe(Channel, (channel, value) => _resetEvent.Set());
        }

        public string Channel { get; }

        public void WaitForJob(TimeSpan timeout, CancellationToken cancellationToken)
        {
            WaitHandle.WaitAny(new[] {_resetEvent, cancellationToken.WaitHandle}, timeout);
        }

        void IServerComponent.Execute(CancellationToken cancellationToken)
        {
            cancellationToken.WaitHandle.WaitOne();
        }
    }
}
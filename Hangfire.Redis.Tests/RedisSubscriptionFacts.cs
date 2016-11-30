using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Hangfire.Redis.Tests
{
    [CleanRedis]
    public class RedisSubscriptionFacts
    {
        private readonly CancellationTokenSource _cts;
        public RedisSubscriptionFacts()
        {
            _cts = new CancellationTokenSource();
        }
        [Fact]
        public void Ctor_ThrowAnException_WhenSubscriberIsNull()
        {
            var exception = Assert.Throws<ArgumentNullException>(
                () => new RedisSubscription(null));
            Assert.Equal("subscriber", exception.ParamName);
        }

        public void WaitForJob_WaitForTheTimeout()
        {
            //Arrange
            Stopwatch sw = new Stopwatch();
            var subscription = new RedisSubscription(RedisUtils.CreateSubscriber());
            var timeout = TimeSpan.FromMilliseconds(100);
            sw.Start();

            //Act
            subscription.WaitForJob(timeout, _cts.Token);

            //Assert
            sw.Stop();
            Assert.InRange(sw.ElapsedMilliseconds, 100, 120);
        }
    }
    
}

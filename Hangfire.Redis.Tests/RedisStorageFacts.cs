using System;
using System.Linq;
using Hangfire.Logging;
using Hangfire.Redis.Tests.Utils;
using Moq;
using Xunit;

namespace Hangfire.Redis.Tests
{
    public class RedisStorageFacts
    {
        [Fact, CleanRedis]
        public void GetStateHandlers_ReturnsAllHandlers()
        {
            var storage = CreateStorage();

            var handlers = storage.GetStateHandlers();

            var handlerTypes = handlers.Select(x => x.GetType()).ToArray();
            Assert.Contains(typeof(FailedStateHandler), handlerTypes);
            Assert.Contains(typeof(ProcessingStateHandler), handlerTypes);
            Assert.Contains(typeof(SucceededStateHandler), handlerTypes);
            Assert.Contains(typeof(DeletedStateHandler), handlerTypes);
        }

        [Fact]
        public void DbFromConnectionStringIsUsed()
        {
            var storage = new RedisStorage(String.Format("{0},defaultDatabase=5", RedisUtils.GetHostAndPort()));
            Assert.Equal(5, storage.Db);
        }

        [Fact]
        public void PasswordFromToStringIsNotShown()
        {
            string password = Guid.NewGuid().ToString("N");
            var storage = new RedisStorage(String.Format("{0},password={1}", RedisUtils.GetHostAndPort(), password));
            Assert.DoesNotContain(password, storage.ToString());
        }

        [Fact]
        public void PasswordFromWriteOptionsToLogIsNotShown()
        {
            string password = Guid.NewGuid().ToString("N");
            var storage = new RedisStorage(String.Format("{0},password={1}", RedisUtils.GetHostAndPort(), password));

            string loggedMessage = null;

            var logMock = new Mock<ILog>();

            logMock.Setup(p => p.Log(LogLevel.Debug, null, null)).Returns(true); // logger.IsDebugEnabled()
            logMock.Setup(p => p.Log(
                    LogLevel.Debug,
                    It.Is<Func<string>>(f => f != null && f.Invoke().StartsWith("ConnectionString: ")),
                    null))
                .Callback((LogLevel lvl, Func<string> msg, Exception ex) => { loggedMessage = msg.Invoke(); })
                .Returns(true)
                .Verifiable();

            storage.WriteOptionsToLog(logMock.Object);

            logMock.Verify();
            Assert.DoesNotContain(password, loggedMessage);
        }

        private RedisStorage CreateStorage()
        {
            var options = new RedisStorageOptions() { Db = RedisUtils.GetDb() };
            return new RedisStorage(RedisUtils.GetHostAndPort(), options);
        }
    }
}
using System.Reflection;
using Xunit.Sdk;

namespace Hangfire.Redis.Tests.Utils
{
    public class CleanRedisAttribute : BeforeAfterTestAttribute
    {
        public override void Before(MethodInfo methodUnderTest)
        {
            var client = RedisUtils.CreateClient();
            client.Multiplexer.GetServer(RedisUtils.GetHostAndPort()).FlushDatabase(RedisUtils.GetDb());
        }

        public override void After(MethodInfo methodUnderTest)
        {
        }
    }
}
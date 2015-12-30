using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Hangfire.Redis.Tests.Utils
{
    public static class StaticFakeJobs
    {
        public static string Work(int identifier, int waitTime)
        {
            Thread.Sleep(waitTime);
            var jobResult = String.Format("{0} - {1} Job done after waiting {2} ms", DateTime.Now.ToString("hh:mm:ss fff"), identifier, waitTime);
            Console.WriteLine(jobResult);
            return jobResult; 
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Hangfire.Redis
{
	public class HangFireRedisException : Exception
	{
		public HangFireRedisException(string message) : base (message)
		{

		}
	}
}

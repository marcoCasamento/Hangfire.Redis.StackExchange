# Hangfire.Redis.StackExchange

HangFire Redis storage based on [HangFire.Redis](https://github.com/HangfireIO/Hangfire.Redis/) but using lovely [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis) client.

[![Build status](https://ci.appveyor.com/api/projects/status/mrg1hivw1fnrvw2o?svg=true)](https://ci.appveyor.com/project/marcoCasamento/hangfire-redis-stackexchange)
[![Nuget Badge](https://buildstats.info/nuget/Hangfire.Redis.StackExchange)](https://www.nuget.org/packages/Hangfire.Redis.StackExchange/)

| Package Name                                  | NuGet.org                                                                                                                                                             |
|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Hangfire.Redis.StackExchange`                | [![Nuget Badge](https://img.shields.io/nuget/v/Hangfire.Redis.StackExchange.svg)](https://www.nuget.org/packages/Hangfire.Redis.StackExchange/)                       |
| `Hangfire.Redis.StackExchange.StrongName`     | [![Nuget Badge](https://img.shields.io/nuget/v/Hangfire.Redis.StackExchange.StrongName.svg)](https://www.nuget.org/packages/Hangfire.Redis.StackExchange.StrongName/) |

#### Highlights
- Support for Hangfire Batches ([feature of Hangfire Pro](http://hangfire.io/blog/2015/04/17/hangfire-pro-1.2.0-released.html))
- Efficient use of Redis resources thanks to ConnectionMultiplexer
- Support for Redis Prefix, allow multiple Hangfire Instances against a single Redis DB
- Allow customization of Succeeded and Failed lists size

> Despite the name, `Hangfire.Redis.StackExchange.StrongName` is **not signed** because `Hangfire.Core` is not yet signed.

## Tutorial: Hangfire on Redis on ASP.NET Core MVC

### Getting Started

To use Hangfire against Redis in an ASP.NET Core MVC projects, first you will need to install at least these two packages: `Hangfire.AspNetCore` and `Hangfire.Redis.StackExchange`.

In `Startup.cs`, these are the **bare minimum** codes that you will need to write for enabling Hangfire on Redis:

```cs
public class Startup
{
    public static ConnectionMultiplexer Redis;

    public Startup(IHostingEnvironment env)
    {
        // Other codes / configurations are omitted for brevity.
        Redis = ConnectionMultiplexer.Connect(Configuration.GetConnectionString("Redis"));
    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddHangfire(configuration =>
        {
            configuration.UseRedisStorage(Redis);
        });
    }

    public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
    {
        app.UseHangfireServer();
    }
}
```

> **Attention:** If you are using `Microsoft.Extensions.Caching.Redis` package, you will need to use `Hangfire.Redis.StackExchange.StrongName` instead, because the former package requires `StackExchange.Redis.StrongName` instead of `StackExchange.Redis`!

### Configurations

#### configuration.UseRedisStorage(...)

This method accepts two parameters:

- The first parameter accepts either your Redis connection string or a `ConnectionMultiplexer` object. By [recommendation of the official StackExchange.Redis documentation](https://stackexchange.github.io/StackExchange.Redis/Basics), it is actually recommended to create one `ConnectionMultiplexer` for multiple reuse.

- The second parameter accepts `RedisStorageOptions` object. As of version 1.7.0, these are the properties you can set into the said object:

```cs
namespace Hangfire.Redis
{
    public class RedisStorageOptions
    {
        public const string DefaultPrefix = "{hangfire}:";

        public RedisStorageOptions();

        public TimeSpan InvisibilityTimeout { get; set; }
        public TimeSpan FetchTimeout { get; set; }
        public string Prefix { get; set; }
        public int Db { get; set; }
        public int SucceededListSize { get; set; }
        public int DeletedListSize { get; set; }
    }
}
```

> It is **highly recommended** to set the **Prefix** property, to avoid overlap with other projects that targets the same Redis store!

#### app.UseHangfireServer(...)

This method accepts `BackgroundJobServerOptions` as the first parameter:

```cs
namespace Hangfire
{
    public class BackgroundJobServerOptions
    {
        public BackgroundJobServerOptions();

        public string ServerName { get; set; }
        public int WorkerCount { get; set; }
        public string[] Queues { get; set; }
        public TimeSpan ShutdownTimeout { get; set; }
        public TimeSpan SchedulePollingInterval { get; set; }
        public TimeSpan HeartbeatInterval { get; set; }
        public TimeSpan ServerTimeout { get; set; }
        public TimeSpan ServerCheckInterval { get; set; }
        public IJobFilterProvider FilterProvider { get; set; }
        public JobActivator Activator { get; set; }
    }
}
```

Of these options, several interval options may be manually set (to longer intervals) to reduce CPU load:

- `SchedulePollingInterval` is by default set to [15 seconds](http://docs.hangfire.io/en/latest/background-methods/calling-methods-with-delay.html).

- `HeartbeatInterval` is by default set to [30 seconds](https://github.com/HangfireIO/Hangfire/blob/master/src/Hangfire.Core/Server/ServerHeartbeat.cs). 

- `ServerTimeout` and `ServerCheckInterval` is by default set to [5 minutes](https://github.com/HangfireIO/Hangfire/blob/master/src/Hangfire.Core/Server/ServerWatchdog.cs).

### Dashboard

Written below is a short snippet on how to implement Hangfire dashboard in ASP.NET Core MVC applications, with limited access to cookie-authenticated users of `Administrator` role. [Read more in official documentation](http://docs.hangfire.io/en/latest/configuration/using-dashboard.html).

```cs
public class AdministratorHangfireDashboardAuthorizationFilter : IDashboardAuthorizationFilter
{
    public bool Authorize(DashboardContext context)
    {
        var user = context.GetHttpContext().User;
        return user.Identity.IsAuthenticated && user.IsInRole("Administrator");
    }
}
```

```cs
public void Configure(IApplicationBuilder app, IHostingEnvironment env, ILoggerFactory loggerFactory)
{
    app.UseCookieAuthentication(...);

    // This middleware must be placed AFTER the authentication middlewares!
    app.UseHangfireDashboard(options: new DashboardOptions
    {
        Authorization = new[] { new AdministratorHangfireDashboardAuthorizationFilter() }
    });
}
```

### Jobs via ASP.NET Core Dependency Injection Services

For cleaner and more managable application code, it is possible to define your jobs in a class that is [registered via dependency injection](https://docs.microsoft.com/en-us/aspnet/core/fundamentals/dependency-injection).

```cs
public class MyHangfireJobs
{
    public async Task SendGetRequest()
    {
        var client = new HttpClient();
        await client.GetAsync("https://www.accelist.com");
    }
}
```

```cs
public void ConfigureServices(IServiceCollection services)
{
    services.AddTransient<MyHangfireJobs>();
}
```

Using this technique, the registered jobs service will be able to obtain other services as dependency via constructor parameters, such as Entity Framework Core `DbContext`; which enables the development of powerful jobs with relative ease.

Then later you can execute the jobs using generic expression:

```cs
BackgroundJob.Enqueue<MyHangfireJobs>(jobs => jobs.SendGetRequest());

BackgroundJob.Schedule<MyHangfireJobs>(jobs => jobs.SendGetRequest(), DateTimeOffset.UtcNow.AddDays(1));

RecurringJob.AddOrUpdate<MyHangfireJobs>("RecurringSendGetRequest", jobs => jobs.SendGetRequest(), Cron.Hourly());
```

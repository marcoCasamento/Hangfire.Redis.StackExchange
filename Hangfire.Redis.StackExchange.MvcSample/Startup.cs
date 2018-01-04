using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Hangfire;
using Hangfire.Redis;
using Hangfire.Redis.StackExchange;
namespace Hangfire.Redis.StackExchange.MvcSample
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();
            services.AddHangfire(t => t.UseRedisStorage(Configuration[key: "ConnectionStrings:Database"]));
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IHostingEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }
            else
            {
                app.UseExceptionHandler("/Home/Error");
            }

            app.UseStaticFiles();
            // Add Hangfire Server and Dashboard support
            app.UseHangfireServer();
            app.UseHangfireDashboard();

            // Run once
            BackgroundJob.Enqueue(() => Console.WriteLine("Background Job: Hello, world!"));

            BackgroundJob.Enqueue(() => Test());

            // Run every minute
            RecurringJob.AddOrUpdate(() => Test(), Cron.Minutely);
            app.UseMvc(routes =>
            {
                routes.MapRoute(
                    name: "default",
                    template: "{controller=Home}/{action=Index}/{id?}");
            });
        }
        public static int X;

        [AutomaticRetry(Attempts = 2, LogEvents = true, OnAttemptsExceeded = AttemptsExceededAction.Delete)]
        public static void Test() => Console.WriteLine($"{X++} Cron Job: Hello, world!");
    }
}

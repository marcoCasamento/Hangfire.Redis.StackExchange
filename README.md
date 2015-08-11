# Hangfire.Redis.StackExchange

![Build Status](https://ci.appveyor.com/api/projects/status/32r7s2skrgm9ubva/branch/master?svg=true)

HangFire Redis storage based on [HangFire.Redis](https://github.com/HangfireIO/Hangfire.Redis/) but using lovely [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis) client.

#### Highlights
- Support for Hangfire Batches ([feature of Hangfire Pro](http://hangfire.io/blog/2015/04/17/hangfire-pro-1.2.0-released.html))
- Efficient use of Redis resources thanks to ConnectionMultiplexer
- Support for Redis Prefix, allow multiple Hangfire Instances against a single Redis DB
- Allow customization of Succeeded and Failed lists size
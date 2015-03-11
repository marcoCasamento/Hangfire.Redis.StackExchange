# Hangfire.Redis.StackExchange

| Windows / .NET | Linux / Mono
| --- | ---
|  [![Build Status](https://ci.appveyor.com/api/projects/status/32r7s2skrgm9ubva/branch/master?svg=true)](https://ci.appveyor.com/project/marcoCasamento/hangfire-redis-stackexchange) | [![Build Status](https://travis-ci.org/marcoCasamento/Hangfire.Redis.StackExchange.svg?branch=master)](https://travis-ci.org/marcoCasamento/Hangfire.Redis.StackExchange)

HangFire Redis storage based on [HangFire.Redis](https://github.com/HangfireIO/Hangfire.Redis/) but using lovely StackExchange.Redis client.

This project is based upon [HangFire.Redis](https://github.com/HangfireIO/Hangfire.Redis/), 
that, however, get abandoned due to [switch to pro (paid) version](http://odinserj.net/2014/11/15/hangfire-pro/).  
Also, that original version use [ServiceStack.Redis](https://servicestack.net/redis) 
that with the v4.0 switched to a paid licensing model too, 
so I wrote down my implementation of HangFire Storage for Redis 
leveraging the excellent and **Free** [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)  



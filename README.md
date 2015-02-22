# Hangfire.Redis.StackExchange [![Build Status](https://travis-ci.org/marcoCasamento/Hangfire.Redis.StackExchange.svg?branch=master)](https://travis-ci.org/marcoCasamento/Hangfire.Redis.StackExchange)
HangFire Redis storage based on original (and now unsupported) Hangfire.Redis but using lovely StackExchange.Redis client

This project is based upon https://github.com/HangfireIO/Hangfire.Redis/, it's born out of a need, that project get abandoned due to [switch to pro (paid) version](http://odinserj.net/2014/11/15/hangfire-pro/).  
The original version was based [ServiceStack.Redis](https://servicestack.net/redis) that with the v4.0 switched to a paid licensing model too, so I wrote down an implementation of Hangfire Storage for Redis but based on the excellent and **Free** [StackExchange.Redis](https://github.com/StackExchange/StackExchange.Redis)  

As for the license I'm still not sure under which license I can release it, given the fact that is based on the old LGPL version from [Sergey Odinokov](https://github.com/odinserj), any help on choosing a good fit it's really appreciated, I just want that anyone can use/modify it for whatever kind of software and don't sue me under ANY cirumstance :-)


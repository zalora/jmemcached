# JMemcached [![](https://jitpack.io/v/zalora/jmemcached.svg)](https://jitpack.io/#zalora/jmemcached)

JMemcached is a Java based Memcached frontend originally written by Ryan Daum: https://github.com/rdaum/jmemcache-daemon

## How we are using it

We are using Infinispan as a "clustered Memcached", but found a weird [bug](https://issues.jboss.org/browse/ISPN-7086)
in the Infinispan Memcached-Server Implementation, which makes it unusable for our purpose. As the core Infinispan
works fine, we simply added a new frontend to access Infinispan, which is JMemcached.

## Original Author

We were trying to reach out to the original author to ask for permission to pickup his abandoned project, but were not
really successful, so if you are the author or know him, we'd be happy to get in touch: wolfram.huesken@zalora.com

# JMemcached

JMemcached is a Java based Memcached frontend originally written by Ryan Daum: https://github.com/rdaum/jmemcache-daemon

## How we are using it

We are using Infinispan as a "clustered Memcached", but found a weird bug in the Infinispan Server Implementation, which
makes it unusable for our purpose. As the core Infinispan works fine, we simply added a new frontend to access Infinispan,
which is JMemcached.

At the moment both projects are separated, so it could be used by other projects or people facing the same problem as we do.

## Original Author

We were trying to reach out to the original author to ask for permission to pickup an abandonned project, but were not
really successful, so if you are or know him, we'd be happy to get in touch: wolfram.huesken@zalora.com

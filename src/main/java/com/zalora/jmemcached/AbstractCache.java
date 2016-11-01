package com.zalora.jmemcached;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.valueOf;

/**
 * Abstract implementation of a cache handler for the memcache daemon; provides some convenience methods and
 * a general framework for implementation
 * @author Ryan Daum
 */
public abstract class AbstractCache<CACHE_ELEMENT extends CacheElement> implements Cache<CACHE_ELEMENT> {

    protected final AtomicLong started = new AtomicLong();

    protected final AtomicInteger getCmds = new AtomicInteger();
    protected final AtomicInteger setCmds = new AtomicInteger();
    protected final AtomicInteger getHits = new AtomicInteger();
    protected final AtomicInteger getMisses = new AtomicInteger();
    protected final AtomicLong casCounter = new AtomicLong(1);

    public AbstractCache() {
        initStats();
    }

    /**
     * @return the current time in seconds (from epoch), used for expiries, etc.
     */
    public static int Now() {
        return (int) (System.currentTimeMillis());
    }

    protected abstract Set<String> keys();

    public abstract long getCurrentItems();

    public abstract long getLimitMaxBytes();

    public abstract long getCurrentBytes();

    public final int getGetCmds() {
        return getCmds.get();
    }

    public final int getSetCmds() {
        return setCmds.get();
    }

    public final int getGetHits() {
        return getHits.get();
    }

    public final int getGetMisses() {
        return getMisses.get();
    }

    /**
     * Return runtime statistics
     *
     * @param arg additional arguments to the stats command
     * @return the full command response
     */
    public final Map<String, Set<String>> stat(String arg) {
        Map<String, Set<String>> result = new HashMap<String, Set<String>>();

        // Stats we know
        multiSet(result, "version", MemCacheDaemon.memcachedVersion);
        multiSet(result, "cmd_get", valueOf(getGetCmds()));
        multiSet(result, "cmd_set", valueOf(getSetCmds()));
        multiSet(result, "get_hits", valueOf(getGetHits()));
        multiSet(result, "get_misses", valueOf(getGetMisses()));
        multiSet(result, "time", valueOf(System.currentTimeMillis() / 1000));
        multiSet(result, "uptime", valueOf((System.currentTimeMillis() - this.started.longValue()) / 1000));
        multiSet(result, "curr_items", valueOf(this.getCurrentItems()));
        multiSet(result, "limit_maxbytes", valueOf(this.getLimitMaxBytes()));
        multiSet(result, "current_bytes", valueOf(this.getCurrentBytes()));
        multiSet(result, "free_bytes", valueOf(Runtime.getRuntime().freeMemory()));

        // TODO we could collect these stats
        multiSet(result, "bytes_read", "0");
        multiSet(result, "bytes_written", "0");
        multiSet(result, "total_connections", "0");
        multiSet(result, "bytes", "0");
        multiSet(result, "total_items", "0");

        // Fake stats added for PHP
        multiSet(result, "rusage_user", "0");
        multiSet(result, "rusage_system", "0");
        multiSet(result, "connection_structures", "0");
        multiSet(result, "libevent", "undefined");
        multiSet(result, "libevent", "64");
        multiSet(result, "curr_connections", "1");
        multiSet(result, "reserved_fds", "20");
        multiSet(result, "cmd_flush", "0");
        multiSet(result, "cmd_touch", "0");
        multiSet(result, "delete_misses", "0");
        multiSet(result, "delete_hits", "0");
        multiSet(result, "incr_misses", "0");
        multiSet(result, "incr_hits", "0");
        multiSet(result, "decr_misses", "0");
        multiSet(result, "decr_hits", "0");
        multiSet(result, "cas_misses", "0");
        multiSet(result, "cas_hits", "0");
        multiSet(result, "cas_badval", "0");
        multiSet(result, "touch_hits", "0");
        multiSet(result, "touch_misses", "0");
        multiSet(result, "conn_yields", "0");
        multiSet(result, "hash_power_level", "0");
        multiSet(result, "hash_bytes", "0");
        multiSet(result, "hash_is_expanding", "0");
        multiSet(result, "malloc_fails", "0");
        multiSet(result, "auth_cmds", "0");
        multiSet(result, "auth_errors", "0");
        multiSet(result, "expired_unfetched", "0");
        multiSet(result, "evicted_unfetched", "0");
        multiSet(result, "evictions", "0");
        multiSet(result, "reclaimed", "0");
        multiSet(result, "crawler_reclaimed", "0");
        multiSet(result, "crawler_items_checked", "0");
        multiSet(result, "lrutail_reflocked", "0");
        multiSet(result, "accepting_conns", "1");
        multiSet(result, "listen_disabled_num", "0");
        multiSet(result, "time_in_listen_disabled_us", "0");

        // Not really the same thing precisely, but meaningful nonetheless
        multiSet(result, "pid", valueOf(Thread.currentThread().getId()));
        multiSet(result, "threads", valueOf(Runtime.getRuntime().availableProcessors()));

        return result;
    }

    private void multiSet(Map<String, Set<String>> map, String key, String val) {
        Set<String> cur = map.get(key);
        if (cur == null) {
            cur = new HashSet<String>();
        }
        cur.add(val);
        map.put(key, cur);
    }

    /**
     * Initialize all statistic counters
     */
    protected void initStats() {
        started.set(System.currentTimeMillis());
    }

    public abstract void asyncEventPing();

}

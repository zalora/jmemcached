package com.zalora.jmemcached;

import java.io.Serializable;
import com.zalora.jmemcached.storage.SizedItem;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * @author Ryan Daum
 */
public interface CacheElement extends Serializable, SizedItem {

    public final static long THIRTY_DAYS = 2592000000L;

    int size();

    int hashCode();

    long getExpire();

    long getFlags();

    ChannelBuffer getData();

    void setData(ChannelBuffer data);

    String getKey();

    long getCasUnique();

    void setCasUnique(long casUnique);

    boolean isBlocked();

    void block(long blockedUntil);

    long getBlockedUntil();

    CacheElement append(LocalCacheElement element);

    CacheElement prepend(LocalCacheElement element);

    LocalCacheElement.IncrDecrResult add(int mod);

}

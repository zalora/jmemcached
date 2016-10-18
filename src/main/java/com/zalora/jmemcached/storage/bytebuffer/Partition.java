package com.zalora.jmemcached.storage.bytebuffer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import com.zalora.jmemcached.LocalCacheElement;

import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.nio.charset.Charset;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Ryan Daum
 */
public final class Partition {
    private static final int NUM_BUCKETS = 32768;

    ReentrantReadWriteLock storageLock = new ReentrantReadWriteLock();

    ChannelBuffer[] buckets = new ChannelBuffer[NUM_BUCKETS];

    ByteBufferBlockStore blockStore;

    int numberItems;

    private final Charset UTF8 = Charset.forName("UTF8");

    Partition(ByteBufferBlockStore blockStore) {
        this.blockStore = blockStore;
    }

    public Region find(String key) {
        int bucket = findBucketNum(key);

        if (buckets[bucket] == null) return null;
        ChannelBuffer regions = buckets[bucket].slice();

        regions.readerIndex(0);
        while (regions.readableBytes() > 0) {
            int totsize = regions.readInt();
            int rsize = regions.readInt();
            int rusedBlocks = regions.readInt();
            int rstartBlock = regions.readInt();
            long expiry = regions.readLong();
            long timestamp = regions.readLong();
            int rkeySize = regions.readInt();

            if (rkeySize == key.length()) {
                ChannelBuffer rkey = regions.readSlice(rkeySize);

                if (rkey.equals(key.getBytes(UTF8))) return new Region(rsize, rusedBlocks, rstartBlock, blockStore.get(rstartBlock, rsize), expiry, timestamp);
            } else {
                regions.skipBytes(rkeySize);
            }
        }

        return null;
    }

    public boolean has(String key) {
        int bucket = findBucketNum(key);

        if (buckets[bucket] == null) return false;
        ChannelBuffer regions = buckets[bucket].slice();


        regions.readerIndex(0);
        while (regions.readableBytes() > 0) {
            int totsize = regions.readInt();
            regions.skipBytes(28);
            int rkeySize = regions.readInt();

            if (rkeySize == key.length()) {
                ChannelBuffer rkey = regions.readSlice(rkeySize);

                if (rkey.equals(key.getBytes(UTF8))) return true;
            } else {
                regions.skipBytes(rkeySize);
            }
        }

        return false;
    }

    private int findBucketNum(String key) {
        int hash = BlockStorageCacheStorage.hash(key.hashCode());
        return hash & (buckets.length - 1);
    }

    public void remove(String key, Region region) {
        int bucket = findBucketNum(key);

        ChannelBuffer newRegion = ChannelBuffers.dynamicBuffer(128);
        ChannelBuffer regions = buckets[bucket].slice();
        if (regions == null) return;

        regions.readerIndex(0);
        while (regions.readableBytes() > 0) {
            // read key portion then region portion
            int pos = regions.readerIndex();
            int totsize = regions.readInt();
            regions.skipBytes(28);
            int rkeySize = regions.readInt();
            ChannelBuffer rkey = regions.readBytes(rkeySize);

            if (rkeySize != key.length() || !rkey.equals(key.getBytes(UTF8))) {
                newRegion.writeBytes(regions.slice(pos, regions.readerIndex()));
            }
        }

        buckets[bucket] = newRegion;

        numberItems--;
    }

    public Region add(String key, LocalCacheElement e) throws UnsupportedEncodingException {
        Region region = blockStore.alloc(e.bufferSize(), e.getExpire(), System.currentTimeMillis());
        e.writeToBuffer(region.slice);
        int bucket = findBucketNum(key);

        ChannelBuffer outbuf = ChannelBuffers.directBuffer(32 + key.length());
        outbuf.writeInt(region.size);
        outbuf.writeInt(region.usedBlocks);
        outbuf.writeInt(region.startBlock);
        outbuf.writeLong(region.expiry);
        outbuf.writeLong(region.timestamp);
        outbuf.writeInt(key.length());
        outbuf.writeBytes(key.getBytes(UTF8));

        ChannelBuffer regions = buckets[bucket];
        if (regions == null) {
            regions = ChannelBuffers.dynamicBuffer(128);
            buckets[bucket] = regions;
        }

        regions.writeInt(outbuf.capacity());
        regions.writeBytes(outbuf);

        numberItems++;

        return region;
    }

    public void clear() {
        for (ChannelBuffer bucket : buckets) {
            if (bucket != null)
                bucket.clear();
        }
        blockStore.clear();
        numberItems = 0;
    }

    public Collection<String> keys() {
        Set<String> keys = new HashSet<String>();

        for (ChannelBuffer regionsa : buckets) {
            if (regionsa != null) {
                ChannelBuffer regions = regionsa.slice();
                regions.readerIndex(0);
                while (regions.readableBytes() > 0) {
                    // read key portion then region portion
                    int totsize = regions.readInt();
                    regions.skipBytes(28);
                    int rkeySize = regions.readInt();
                    ChannelBuffer rkey = regions.readBytes(rkeySize);

                    keys.add(rkey.toString(UTF8));
                }
            }
        }
        return keys;
    }

    public int getNumberItems() {
        return numberItems;
    }
}

package com.zalora.jmemcached.storage.bytebuffer;

import com.zalora.jmemcached.LocalCacheElement;
import com.zalora.jmemcached.storage.CacheStorage;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;

/**
 * Implementation of the cache using the block buffer storage back end.
 */
public final class BlockStorageCacheStorage implements CacheStorage<String, LocalCacheElement> {

    Partition[] partitions;

    volatile int ceilingBytes;
    volatile int maximumItems;
    volatile int numberItems;
    final long maximumSizeBytes;

    public BlockStorageCacheStorage(int blockStoreBuckets, int ceilingBytesParam, int blockSizeBytes, long maximumSizeBytes, int maximumItemsVal, BlockStoreFactory factory) {
        this.partitions = new Partition[blockStoreBuckets];

        long bucketSizeBytes = maximumSizeBytes / blockStoreBuckets;
        for (int i = 0; i < blockStoreBuckets; i++) {
            this.partitions[i] = new Partition(factory.manufacture(bucketSizeBytes, blockSizeBytes));
        }

        this.numberItems = 0;
        this.ceilingBytes = 0;
        this.maximumItems = 0;
        this.maximumSizeBytes = maximumSizeBytes;
    }

    private Partition pickPartition(String key) {
        return partitions[hash(key.hashCode()) & (partitions.length - 1)];
    }

    public final long getMemoryCapacity() {
        long capacity = 0;
        for (Partition byteBufferBlockStore : partitions) {
            capacity += byteBufferBlockStore.blockStore.getStoreSizeBytes();
        }
        return capacity;
    }

    public final long getMemoryUsed() {
        long memUsed = 0;
        for (Partition byteBufferBlockStore : partitions) {
            memUsed += (byteBufferBlockStore.blockStore.getStoreSizeBytes() - byteBufferBlockStore.blockStore.getFreeBytes());
        }
        return memUsed;
    }

    public final int capacity() {
        return maximumItems;
    }

    public final void close() throws IOException {
        // first clear all items
        clear();

        // then ask the block store to close
        for (Partition byteBufferBlockStore : partitions) {
            byteBufferBlockStore.blockStore.close();
        }
        this.partitions = null;
    }

    public final LocalCacheElement putIfAbsent(String key, LocalCacheElement item) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            // not there? add it
            if (region == null) {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    numberItems++;
                    partition.add(key, item);
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

                return null;
            } else {
                // there? return its value
                return region.toValue();
            }
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    public final boolean remove(Object okey, Object value) {
        if (!(okey instanceof String) || (!(value instanceof LocalCacheElement))) return false;

        String key = (String) okey;
        Partition partition = pickPartition(key);

        try {
            partition.storageLock.readLock().lock();
            Region region = partition.find(key);
            if (region == null) return false;
            else {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    partition.blockStore.free(region);
                    partition.remove(key, region);
                    numberItems++;
                    return true;
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

            }
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final boolean replace(String key, LocalCacheElement original, LocalCacheElement replace) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            // not there? that's a fail
            if (region == null) return false;

            // there, check for equivalence of value
            LocalCacheElement el = null;
            el = region.toValue();
            if (!el.equals(original)) {
                return false;
            } else {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    partition.remove(key, region);
                    partition.add(key, replace);
                    return true;
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }
            }

        } finally {
            partition.storageLock.readLock().unlock();
        }
        return false;
    }

    public final LocalCacheElement replace(String key, LocalCacheElement replace) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            // not there? that's a fail
            if (region == null) return null;

            // there,
            LocalCacheElement el = null;
            el = region.toValue();
            partition.storageLock.readLock().unlock();
            partition.storageLock.writeLock().lock();
            try {
                partition.remove(key, region);
                partition.add(key, replace);
                return el;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } finally {
                partition.storageLock.readLock().lock();
                partition.storageLock.writeLock().unlock();
            }
        } finally {
            partition.storageLock.readLock().unlock();
        }

        return replace;
    }

    public final int size() {
        return numberItems;
    }

    public final boolean isEmpty() {
        return numberItems == 0;
    }

    public final boolean containsKey(Object okey) {
        if (!(okey instanceof String)) return false;

        String key = (String) okey;
        Partition partition = pickPartition(key);

        try {
            partition.storageLock.readLock().lock();
            return partition.has(key);
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final boolean containsValue(Object o) {
        throw new UnsupportedOperationException("operation not supported");
    }

    public final LocalCacheElement get(Object okey) {
        if (!(okey instanceof String)) return null;

        String key = (String) okey;
        Partition partition = pickPartition(key);

        try {
            partition.storageLock.readLock().lock();
            Region region = partition.find(key);
            if (region == null) return null;
            return region.toValue();
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final LocalCacheElement put(final String key, final LocalCacheElement item) {
        Partition partition = pickPartition(key);

        partition.storageLock.readLock().lock();
        try {
            Region region = partition.find(key);

            partition.storageLock.readLock().unlock();
            partition.storageLock.writeLock().lock();
            try {
                LocalCacheElement old = null;
                if (region != null) {
                    old = region.toValue();
                }
                if (region != null) partition.remove(key, region);
                partition.add(key, item);
                numberItems++;
                return old;
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            } finally {
                partition.storageLock.readLock().lock();
                partition.storageLock.writeLock().unlock();
            }


        } finally {
            partition.storageLock.readLock().unlock();
        }
        return item;
    }

    public final LocalCacheElement remove(Object okey) {
        if (!(okey instanceof String)) return null;

        String key = (String) okey;
        Partition partition = pickPartition(key);

        try {
            partition.storageLock.readLock().lock();
            Region region = partition.find(key);
            if (region == null) return null;
            else {
                partition.storageLock.readLock().unlock();
                partition.storageLock.writeLock().lock();
                try {
                    LocalCacheElement old = null;
                    old = region.toValue();
                    partition.blockStore.free(region);
                    partition.remove(key, region);
                    numberItems--;
                    return old;
                } finally {
                    partition.storageLock.readLock().lock();
                    partition.storageLock.writeLock().unlock();
                }

            }
        } finally {
            partition.storageLock.readLock().unlock();
        }
    }

    public final void putAll(Map<? extends String, ? extends LocalCacheElement> map) {
        // absent, lock the store and put the new value in
        for (Entry<? extends String, ? extends LocalCacheElement> entry : map.entrySet()) {
            String key = entry.getKey();
            LocalCacheElement item;
            item = entry.getValue();
            put(key, item);
        }
    }

    public final void clear() {
        for (Partition partition : partitions) {
            partition.storageLock.writeLock().lock();
            numberItems += partition.keys().size() * - 1;
            try {
                partition.clear();
            } finally {
                partition.storageLock.writeLock().unlock();
            }
        }

    }

    public Set<String> keySet() {
        Set<String> keys = new HashSet<String>();
        for (Partition partition : partitions) {
            keys.addAll(partition.keys());
        }

        return keys;
    }

    public Collection<LocalCacheElement> values() {
        throw new UnsupportedOperationException("operation not supported");
    }

    public Set<Entry<String, LocalCacheElement>> entrySet() {
        throw new UnsupportedOperationException("operation not supported");
    }

    protected static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }


}

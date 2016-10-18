package com.zalora.jmemcached.storage.bytebuffer;

/**
 * @author Ryan Daum
 */
public interface BlockStoreFactory<BS extends ByteBufferBlockStore> {
    BS manufacture(long sizeBytes, int blockSizeBytes);
}

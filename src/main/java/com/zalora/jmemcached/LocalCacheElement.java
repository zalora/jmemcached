package com.zalora.jmemcached;

import lombok.Getter;
import java.nio.charset.Charset;
import org.jboss.netty.buffer.ChannelBuffer;
import java.io.UnsupportedEncodingException;
import org.jboss.netty.buffer.ChannelBuffers;
import com.zalora.jmemcached.util.BufferUtils;

/**
 * Represents information about a cache entry
 *
 * @author Ryan Daum
 */
public final class LocalCacheElement implements CacheElement {

    private ChannelBuffer data;

    @Getter
    private long expire;

    @Getter
    private int flags;

    @Getter
    private String key;

    private long casUnique = 0L;
    private boolean blocked = false;
    private long blockedUntil;

    public LocalCacheElement() {
    }

    public LocalCacheElement(String key) {
        this.key = key;
    }

    public LocalCacheElement(String key, int flags, long expire, long casUnique) {
        this.key = key;
        this.flags = flags;
        this.expire = expire;
        this.casUnique = casUnique;
    }

    /**
     * @return the current time in seconds
     */
    public static int Now() {
        return (int) (System.currentTimeMillis() / 1000);
    }

    public static LocalCacheElement key(String key) {
        return new LocalCacheElement(key);
    }

    public static LocalCacheElement readFromBuffer(ChannelBuffer in) {
        int bufferSize = in.readInt();
        long expiry = in.readLong();
        int keyLength = in.readInt();
        ChannelBuffer key = in.slice(in.readerIndex(), keyLength);
        in.skipBytes(keyLength);
        LocalCacheElement localCacheElement = new LocalCacheElement(key.toString(Charset.forName("UTF-8")));

        localCacheElement.expire = expiry;
        localCacheElement.flags = in.readInt();

        int dataLength = in.readInt();
        localCacheElement.data = in.slice(in.readerIndex(), dataLength);
        in.skipBytes(dataLength);

        localCacheElement.casUnique = in.readInt();
        localCacheElement.blocked = in.readByte() == 1;
        localCacheElement.blockedUntil = in.readLong();

        return localCacheElement;
    }

    public int size() {
        return getData().capacity();
    }

    public LocalCacheElement append(LocalCacheElement appendElement) {
        int newLength = size() + appendElement.size();
        LocalCacheElement appendedElement = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ChannelBuffer appended = ChannelBuffers.buffer(newLength);
        ChannelBuffer existing = getData();
        ChannelBuffer append = appendElement.getData();

        appended.writeBytes(existing);
        appended.writeBytes(append);

        appended.readerIndex(0);

        existing.readerIndex(0);
        append.readerIndex(0);

        appendedElement.setData(appended);
        appendedElement.setCasUnique(appendedElement.getCasUnique() + 1);

        return appendedElement;
    }

    public LocalCacheElement prepend(LocalCacheElement prependElement) {
        int newLength = size() + prependElement.size();

        LocalCacheElement prependedElement = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        ChannelBuffer prepended = ChannelBuffers.buffer(newLength);
        ChannelBuffer prepend = prependElement.getData();
        ChannelBuffer existing = getData();

        prepended.writeBytes(prepend);
        prepended.writeBytes(existing);

        existing.readerIndex(0);
        prepend.readerIndex(0);

        prepended.readerIndex(0);

        prependedElement.setData(prepended);
        prependedElement.setCasUnique(prependedElement.getCasUnique() + 1);

        return prependedElement;
    }

    public IncrDecrResult add(int mod) {
        // TODO handle parse failure!
        int modVal = BufferUtils.atoi(getData()) + mod; // change value
        if (modVal < 0) {
            modVal = 0;

        } // check for underflow

        ChannelBuffer newData = BufferUtils.itoa(modVal);

        LocalCacheElement replace = new LocalCacheElement(getKey(), getFlags(), getExpire(), 0L);
        replace.setData(newData);
        replace.setCasUnique(replace.getCasUnique() + 1);

        return new IncrDecrResult(modVal, replace);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalCacheElement that = (LocalCacheElement) o;

        if (blocked != that.blocked) return false;
        if (blockedUntil != that.blockedUntil) return false;
        if (casUnique != that.casUnique) return false;
        if (expire != that.expire) return false;
        if (flags != that.flags) return false;
        if (data != null ? !data.equals(that.data) : that.data != null) return false;
        if (key != null ? !key.equals(that.key) : that.key != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (expire ^ (expire >>> 32));
        result = 31 * result + flags;
        result = 31 * result + (data != null ? data.hashCode() : 0);
        result = 31 * result + (key != null ? key.hashCode() : 0);
        result = 31 * result + (int) (casUnique ^ (casUnique >>> 32));
        result = 31 * result + (blocked ? 1 : 0);
        result = 31 * result + (int) (blockedUntil ^ (blockedUntil >>> 32));
        return result;
    }

    public ChannelBuffer getData() {
        data.readerIndex(0);
        return data;
    }

    public void setData(ChannelBuffer data) {
        data.readerIndex(0);
        this.data = data;
    }

    public long getCasUnique() {
        return casUnique;
    }

    public void setCasUnique(long casUnique) {
        this.casUnique = casUnique;
    }

    public boolean isBlocked() {
        return blocked;
    }

    public long getBlockedUntil() {
        return blockedUntil;
    }

    public void block(long blockedUntil) {
        this.blocked = true;
        this.blockedUntil = blockedUntil;
    }

    public int bufferSize() {
        return 4 + 8 + 4 + key.length() + 4 + 4 + 4 + key.length() + 8 + 1 + 8;
    }

    public void writeToBuffer(ChannelBuffer out) throws UnsupportedEncodingException {
        out.writeInt(bufferSize());
        out.writeLong(expire);
        out.writeInt(key.length());
        out.writeBytes(key.getBytes("UTF-8"));
        out.writeInt(flags);
        out.writeInt(data.capacity());
        out.writeBytes(data);
        out.writeLong(casUnique);
        out.writeByte(blocked ? 1 : 0);
        out.writeLong(blockedUntil);
    }

    public static class IncrDecrResult {
        int oldValue;
        LocalCacheElement replace;

        public IncrDecrResult(int oldValue, LocalCacheElement replace) {
            this.oldValue = oldValue;
            this.replace = replace;
        }
    }

}

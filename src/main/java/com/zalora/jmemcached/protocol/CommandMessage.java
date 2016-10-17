package com.zalora.jmemcached.protocol;

import java.util.*;
import java.io.Serializable;
import java.nio.charset.Charset;
import com.zalora.jmemcached.CacheElement;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * The payload object holding the parsed message.
 */
public final class CommandMessage<CACHE_ELEMENT extends CacheElement> implements Serializable {

    public Op op;
    public CACHE_ELEMENT element;
    public List<String> keys;
    public boolean noreply;
    public long cas_key;
    public int time = 0;
    public int opaque;
    public boolean addKeyToResponse = false;

    public int incrExpiry;
    public int incrAmount;

    private CommandMessage(Op op) {
        this.op = op;
        element = null;
    }

    public void setKey(ChannelBuffer key) {
        this.keys = new ArrayList<String>();
        this.keys.add(key.toString(Charset.forName("UTF-8")));
    }

    public void setKeys(List<ChannelBuffer> keys) {
        this.keys = new ArrayList<String>(keys.size());
        for (ChannelBuffer key : keys) {
            this.keys.add(key.toString(Charset.forName("UTF-8")));
        }
    }

    public static CommandMessage command(Op operation) {
        return new CommandMessage(operation);
    }

}
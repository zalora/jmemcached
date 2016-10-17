package com.zalora.jmemcached.protocol.text;

import com.zalora.jmemcached.Cache;
import com.zalora.jmemcached.protocol.SessionStatus;
import com.zalora.jmemcached.protocol.MemcachedCommandHandler;

import java.nio.charset.Charset;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.DefaultChannelGroup;

public final class MemcachedPipelineFactory implements ChannelPipelineFactory {

    public static final Charset USASCII = Charset.forName("US-ASCII");

    private Cache cache;
    private String version;
    private boolean verbose;
    private int idleTime;

    private DefaultChannelGroup channelGroup;
    private final MemcachedResponseEncoder memcachedResponseEncoder = new MemcachedResponseEncoder();
    private final MemcachedCommandHandler memcachedCommandHandler;


    public MemcachedPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, int frameSize, DefaultChannelGroup channelGroup) {
        this.cache = cache;
        this.version = version;
        this.verbose = verbose;
        this.idleTime = idleTime;
        this.channelGroup = channelGroup;
        memcachedCommandHandler = new MemcachedCommandHandler(this.cache, this.version, this.verbose, this.idleTime, this.channelGroup);
    }

    public final ChannelPipeline getPipeline() throws Exception {
        SessionStatus status = new SessionStatus().ready();

        return Channels.pipeline(
            new MemcachedCommandDecoder(status),
            memcachedCommandHandler,
            memcachedResponseEncoder
        );
    }

}

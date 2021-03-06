package com.zalora.jmemcached.protocol.binary;

import com.zalora.jmemcached.Cache;
import com.zalora.jmemcached.protocol.MemcachedCommandHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.DefaultChannelGroup;

/**
 * @author Ryan Daum
 */
public class MemcachedBinaryPipelineFactory implements ChannelPipelineFactory {

    private final MemcachedBinaryCommandDecoder decoder = new MemcachedBinaryCommandDecoder();
    private final MemcachedCommandHandler memcachedCommandHandler;
    private final MemcachedBinaryResponseEncoder memcachedBinaryResponseEncoder = new MemcachedBinaryResponseEncoder();

    public MemcachedBinaryPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
        memcachedCommandHandler = new MemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup);
    }

    public ChannelPipeline getPipeline() throws Exception {
        return Channels.pipeline(
                decoder,
                memcachedCommandHandler,
                memcachedBinaryResponseEncoder
        );
    }
}

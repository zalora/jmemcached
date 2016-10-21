package com.zalora.jmemcached.protocol.text;

import com.zalora.jmemcached.Cache;
import com.zalora.jmemcached.CacheElement;
import com.zalora.jmemcached.protocol.Op;
import com.zalora.jmemcached.protocol.ResponseMessage;
import com.zalora.jmemcached.util.BufferUtils;
import org.jboss.netty.channel.*;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.Set;
import java.util.Map;

import com.zalora.jmemcached.protocol.exceptions.ClientException;

import static java.lang.String.valueOf;

/**
 * Response encoder for the memcached text protocol. Produces strings destined for the StringEncoder
 */
public final class MemcachedResponseEncoder<CACHE_ELEMENT extends CacheElement> extends SimpleChannelUpstreamHandler {

    final Logger logger = LoggerFactory.getLogger(MemcachedResponseEncoder.class);

    public static final ChannelBuffer CRLF = ChannelBuffers.copiedBuffer("\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer SPACE = ChannelBuffers.copiedBuffer(" ", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer VALUE = ChannelBuffers.copiedBuffer("VALUE ", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer EXISTS = ChannelBuffers.copiedBuffer("EXISTS\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer NOT_FOUND = ChannelBuffers.copiedBuffer("NOT_FOUND\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer NOT_STORED = ChannelBuffers.copiedBuffer("NOT_STORED\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer STORED = ChannelBuffers.copiedBuffer("STORED\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer DELETED = ChannelBuffers.copiedBuffer("DELETED\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer END = ChannelBuffers.copiedBuffer("END\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer OK = ChannelBuffers.copiedBuffer("OK\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer ERROR = ChannelBuffers.copiedBuffer("ERROR\r\n", MemcachedPipelineFactory.USASCII);
    private static final ChannelBuffer CLIENT_ERROR = ChannelBuffers.copiedBuffer("CLIENT_ERROR\r\n", MemcachedPipelineFactory.USASCII);

    /**
     * Handle exceptions in protocol processing. Exceptions are either client or internal errors.  Report accordingly.
     *
     * @param ctx
     * @param e
     * @throws Exception
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        try {
            throw e.getCause();
        } catch (ClientException ce) {
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(CLIENT_ERROR);
        } catch (Throwable tr) {
            logger.error("error", tr);
            if (ctx.getChannel().isOpen())
                ctx.getChannel().write(ERROR);
        }
    }

    @Override
    public void messageReceived(ChannelHandlerContext channelHandlerContext, MessageEvent messageEvent) throws Exception {
        ResponseMessage<CACHE_ELEMENT> command = (ResponseMessage<CACHE_ELEMENT>) messageEvent.getMessage();
        Op cmd = command.cmd.op;
        Channel channel = messageEvent.getChannel();

        switch (cmd) {
            case GET:
            case GETS:
                CacheElement[] results = command.elements;

                ChannelBuffer[] buffers = new ChannelBuffer[results.length * (9 + (cmd == Op.GETS ? 2 : 0)) + 1];
                int i = 0;
                for (CacheElement result : results) {
                    if (result != null) {
                        buffers[i++] = VALUE;
                        buffers[i++] = ChannelBuffers.copiedBuffer(result.getKey().getBytes(Charset.forName("UTF-8")));
                        buffers[i++] = SPACE;
                        buffers[i++] = BufferUtils.ltoa(result.getFlags());
                        buffers[i++] = SPACE;
                        buffers[i++] = BufferUtils.itoa(result.size());
                        if (cmd == Op.GETS) {
                            buffers[i++] = SPACE;
                            buffers[i++] = BufferUtils.ltoa(result.getCasUnique());
                        }
                        buffers[i++] = CRLF;
                        buffers[i++] = result.getData();
                        buffers[i++] = CRLF;
                    }
                }
                buffers[i] = END;

                Channels.write(channel, ChannelBuffers.wrappedBuffer(buffers));
                break;
            case APPEND:
            case PREPEND:
            case ADD:
            case SET:
            case REPLACE:
            case CAS:
                if (!command.cmd.noreply)
                    Channels.write(channel, storeResponse(command.response));
                break;
            case DELETE:
                if (!command.cmd.noreply) {
                    Channels.write(channel, deleteResponseString(command.deleteResponse));
                }
                break;
            case DECR:
            case INCR:
                if (!command.cmd.noreply)
                    Channels.write(channel, incrDecrResponseString(command.incrDecrResponse));
                break;
            case STATS:
                for (Map.Entry<String, Set<String>> stat : command.stats.entrySet()) {
                    for (String statVal : stat.getValue()) {
                        StringBuilder builder = new StringBuilder();
                        builder.append("STAT ");
                        builder.append(stat.getKey());
                        builder.append(" ");
                        builder.append(String.valueOf(statVal));
                        builder.append("\r\n");

                        Channels.write(channel, ChannelBuffers.copiedBuffer(builder.toString(), MemcachedPipelineFactory.USASCII));
                    }
                }

                Channels.write(channel, END.duplicate());
                break;
            case VERSION:
                Channels.write(channel, ChannelBuffers.copiedBuffer("VERSION " + command.version + "\r\n", MemcachedPipelineFactory.USASCII));
                break;
            case QUIT:
                Channels.disconnect(channel);
                break;
            case FLUSH_ALL:
                if (!command.cmd.noreply) {
                    ChannelBuffer ret = command.flushSuccess ? OK.duplicate() : ERROR.duplicate();
                    Channels.write(channel, ret);
                }
                break;
            case VERBOSITY:
                break;
            default:
                Channels.write(channel, ERROR.duplicate());
                logger.error("error; unrecognized command: " + cmd);
        }
    }

    private ChannelBuffer deleteResponseString(Cache.DeleteResponse deleteResponse) {
        if (deleteResponse == Cache.DeleteResponse.DELETED) return DELETED.duplicate();
        else return NOT_FOUND.duplicate();
    }

    private ChannelBuffer incrDecrResponseString(Integer ret) {
        if (ret == null) {
            return NOT_FOUND.duplicate();
        } else {
            return ChannelBuffers.copiedBuffer(valueOf(ret) + "\r\n", MemcachedPipelineFactory.USASCII);
        }
    }

    /**
     * Find the string response message which is equivalent to a response to a set/add/replace message
     * in the cache
     *
     * @param storeResponse the response code
     * @return the string to output on the network
     */
    private ChannelBuffer storeResponse(Cache.StoreResponse storeResponse) {
        switch (storeResponse) {
            case EXISTS:
                return EXISTS.duplicate();
            case NOT_FOUND:
                return NOT_FOUND.duplicate();
            case NOT_STORED:
                return NOT_STORED.duplicate();
            case STORED:
                return STORED.duplicate();
        }
        throw new RuntimeException("unknown store response from cache: " + storeResponse);
    }

}

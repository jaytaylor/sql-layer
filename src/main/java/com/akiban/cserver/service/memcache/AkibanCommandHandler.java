/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.cserver.service.memcache;

import java.io.ByteArrayOutputStream;

import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiOutputter;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.service.session.Session;
import com.akiban.cserver.service.session.SessionImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.akiban.cserver.api.HapiProcessor;
import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.protocol.Command;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;


/**
 * Processes CommandMessage and generate ResponseMessage, shared among all channels.
 *
 * Inspried by: com.thimbleware.jmemcached.protocol.MemcachedCommandHandler
 */
@ChannelHandler.Sharable
final class AkibanCommandHandler extends SimpleChannelUpstreamHandler
{
    private static final String MODULE = AkibanCommandHandler.class.toString();
    private static final String OUTPUTSTREAM_CACHE = "OUTPUTSTREAM_CACHE";
    static interface FormatGetter {
        HapiOutputter getFormat();
    }
    private final ThreadLocal<Session> session = new ThreadLocal<Session>() {
        @Override
        protected Session initialValue() {
            return new SessionImpl();
        }
    };
    /**
     * State variables that are universal for entire service.
     * The handler *must* be declared with a ChannelPipelineCoverage of "all".
     */
    private final HapiProcessor hapiProcessor;
    private final DefaultChannelGroup channelGroup;
    private static final Log LOG = LogFactory.getLog(MemcacheService.class);
    private final FormatGetter formatGetter;

    public AkibanCommandHandler(HapiProcessor hapiProcessor, DefaultChannelGroup channelGroup, FormatGetter formatGetter)
    {
        this.hapiProcessor = hapiProcessor;
        this.channelGroup = channelGroup;
        this.formatGetter = formatGetter;
    }

    /**
     * On open we manage some statistics, and add this connection to the channel group.
     */
    @Override
    public void channelOpen(ChannelHandlerContext context, ChannelStateEvent event) throws Exception {
        channelGroup.add(context.getChannel());
    }

    /**
     * Track stats and then remove from channel group
     */
    @Override
    public void channelClosed(ChannelHandlerContext context, ChannelStateEvent event) throws Exception {
        channelGroup.remove(context.getChannel());
    }
    
    /**
     * Eat the exception, probably an improperly closed client.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception
    {
        Throwable exception = e.getCause();
        LOG.error("Command handler caught exception", exception);
        Channels.write(ctx.getChannel(), ExceptionHelper.forException(exception));
    }

    /**
     * Turn CommandMessages into executions against the CS and then pass on downstream message
     */
    @Override
    public void messageReceived(ChannelHandlerContext context, MessageEvent event) throws Exception {
        if (!(event.getMessage() instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            context.sendUpstream(event);
            return;
        }

        @SuppressWarnings("unchecked")
        CommandMessage<CacheElement> command = (CommandMessage<CacheElement>) event.getMessage();
        Command cmdOp = command.cmd;

        if(LOG.isDebugEnabled()) {
            StringBuilder msg = new StringBuilder();
            msg.append(command.cmd);
            if(command.element != null) {
                msg.append(" ").append(command.element.getKeystring());
            }
            for(int i = 0; i < command.keys.size(); ++i) {
                msg.append(" ").append(command.keys.get(i));
            }
            LOG.debug(msg.toString());
        }

        LOG.trace(cmdOp.name());
        Channel channel = event.getChannel();
        switch(cmdOp) {
            case GET:
            case GETS:
                handleGets(context, command, channel);
                break;
            case QUIT:
                handleQuit(channel);
                break;
            case VERSION:
                handleVersion(context, command, channel);
                break;

            case SET:
            case CAS:
            case ADD:
            case REPLACE:
            case APPEND:
            case PREPEND:
            case INCR:
            case DECR:
            case DELETE:
            case STATS:
            case FLUSH_ALL:
                throw new UnsupportedOperationException(cmdOp.name());

            default:
                throw new UnknownCommandException("unknown command:" + cmdOp);
        }
    }

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel) {
        ResponseMessage responseMessage = new ResponseMessage(command);
        responseMessage.version = MemCacheDaemon.memcachedVersion;
        Channels.fireMessageReceived(context, responseMessage, channel.getRemoteAddress());
    }

    protected void handleGets(ChannelHandlerContext context, CommandMessage<CacheElement> command, Channel channel)
    throws HapiRequestException
    {
        String key = command.keys.get(0);

        byte[] result_bytes;
        result_bytes = getBytesForGets(session.get(), key, hapiProcessor, formatGetter.getFormat());
        
        CacheElement[] results = null;
        if(result_bytes != null) {
            LocalCacheElement element = new LocalCacheElement(key);
            element.setData(result_bytes);
            results = new CacheElement[] { element };
        }

        ResponseMessage<CacheElement> resp = new ResponseMessage<CacheElement>(command).withElements(results);
        Channels.fireMessageReceived(context, resp, channel.getRemoteAddress());
    }

    static byte[] getBytesForGets(Session sessionLocal, String key,
                                        HapiProcessor hapiProcessor, HapiOutputter outputter)
            throws HapiRequestException
    {
        HapiGetRequest request = ParsedHapiGetRequest.parse(key);
        ByteArrayOutputStream outputStream = getOutputStream(sessionLocal);
        hapiProcessor.processRequest(sessionLocal, request, outputter, outputStream);
        return outputStream.toByteArray();
    }

    private static ByteArrayOutputStream getOutputStream(Session session) {
        ByteArrayOutputStream outputStream = session.get(MODULE, OUTPUTSTREAM_CACHE);
        if (outputStream == null) {
            outputStream = new ByteArrayOutputStream(1024);
            session.put(MODULE, OUTPUTSTREAM_CACHE, outputStream);
        }
        else {
            outputStream.reset();
        }
        return outputStream;
    }
}

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

package com.akiban.server.service.memcache;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import com.akiban.server.service.session.SessionService;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.akiban.server.AkServer;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.service.session.Session;
import com.akiban.util.tap.Tap;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.protocol.Command;
import com.thimbleware.jmemcached.protocol.CommandMessage;
import com.thimbleware.jmemcached.protocol.ResponseMessage;
import com.thimbleware.jmemcached.protocol.exceptions.UnknownCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.thimbleware.jmemcached.protocol.text.MemcachedPipelineFactory.USASCII;
/**
 * Processes CommandMessage and generate ResponseMessage, shared among all
 * channels.
 * 
 * Inspried by: com.thimbleware.jmemcached.protocol.MemcachedCommandHandler
 */
@ChannelHandler.Sharable
final class AkibanCommandHandler extends SimpleChannelUpstreamHandler {
    private static final String VERSION_STRING = getVersionString();
    private static final Session.Key<ByteArrayOutputStream> OUTPUTSTREAM_CACHE = Session.Key.named("OUTPUTSTREAM_CACHE");
    private final static Tap.InOutTap HAPI_GETS_TAP = Tap.createTimer("hapi: get_string");

    private static class UnsupportedMemcachedException extends
            UnsupportedOperationException {
        private final Command command;

        UnsupportedMemcachedException(Command command) {
            super(command.name());
            this.command = command;
        }
    }

    private static String getVersionString() {
        String version = String.format(
                "Akiban Server version <%s> using jmemcached %s",
                AkServer.VERSION_STRING, MemCacheDaemon.memcachedVersion);
        return version.replaceAll("[\r\n]", " ");
    }

    private static ChannelBuffer forException(Throwable e) {
        StringBuilder sb = new StringBuilder("SERVER_ERROR ");
        if (e instanceof HapiRequestException) {
            HapiRequestException hre = (HapiRequestException) e;
            sb.append(hre.getReasonCode().name());
            if (e.getMessage() != null) {
                sb.append(": ").append(
                        hre.getSimpleMessage().replaceAll("[\r\n]", " "));
            }

            if (hre.getReasonCode().warrantsErrorLogging()) {
                LOG.error("Bad HapiRequestException", hre);
            } else {
                LOG.info("HapiRequestException, probably due to user error",
                        hre);
            }
        } else if (e instanceof UnsupportedMemcachedException) {
            UnsupportedMemcachedException ume = (UnsupportedMemcachedException) e;
            sb.append("unsupported memcache request: ").append(
                    ume.command.name());
            LOG.trace("Unsupported memcache request", ume);
        } else if (e instanceof UnknownCommandException) {
            LOG.trace("unknown memcache command", e);
            sb.append(e.getMessage().replaceAll("[\\r\\n]", " "));
        } else if (e != null) {
            sb.append("unknown exception ").append(
                    e.getClass().getCanonicalName());
            if (e.getMessage() != null) {
                sb.append(": ").append(
                        e.getMessage().replaceAll("[\\r\\n]", " "));
            }
            LOG.error("Unknown exception", e);
        } else {
            sb.append("null exception!");
            LOG.error("null exception!", new Exception("current stack trace"));
        }
        sb.append("\r\n");
        return ChannelBuffers.wrappedBuffer(sb.toString().getBytes(USASCII));
    }

    static interface FormatGetter {
        HapiOutputter getFormat();
    }

    static interface CommandCallback {
        void connectionOpened();

        void connectionClosed();

        void requestProcessed();

        void requestFailed();
    }

    private final ThreadLocal<Session> session;
    /**
     * State variables that are universal for entire service. The handler *must*
     * be declared with a ChannelPipelineCoverage of "all".
     */
    private final HapiProcessor hapiProcessor;
    private final DefaultChannelGroup channelGroup;
    private static final Logger LOG = LoggerFactory
            .getLogger(MemcacheService.class);
    private final FormatGetter formatGetter;
    private final CommandCallback callback;

    public AkibanCommandHandler(HapiProcessor hapiProcessor,
            DefaultChannelGroup channelGroup, FormatGetter formatGetter,
            CommandCallback callback,
            final SessionService sessionService) {
        this.hapiProcessor = hapiProcessor;
        this.channelGroup = channelGroup;
        this.formatGetter = formatGetter;
        this.callback = callback;
        this.session = new ThreadLocal<Session>() {
            @Override
            protected Session initialValue() {
                return sessionService.createSession();
            }
        };
    }

    /**
     * On open we manage some statistics, and add this connection to the channel
     * group.
     */
    @Override
    public void channelOpen(ChannelHandlerContext context,
            ChannelStateEvent event) throws Exception {
        callback.connectionOpened();
        channelGroup.add(context.getChannel());
    }

    /**
     * Track stats and then remove from channel group
     */
    @Override
    public void channelClosed(ChannelHandlerContext context,
            ChannelStateEvent event) throws Exception {
        session.get().close();
        session.remove();
        callback.connectionClosed();
        channelGroup.remove(context.getChannel());
    }

    /**
     * Eat the exception, probably an improperly closed client.
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception {
        Throwable exception = e.getCause();
        if (exception.getCause() == null
                && exception.getClass().equals(IOException.class)
                && "Connection reset by peer".equals(exception.getMessage())) {
            LOG.trace("netty exception on client shutdown", exception);
        } else {
            callback.requestFailed();
            Channels.write(ctx.getChannel(), forException(exception));
        }
    }

    /**
     * Turn CommandMessages into executions against the CS and then pass on
     * downstream message
     */
    @Override
    public void messageReceived(ChannelHandlerContext context,
            MessageEvent event) throws Exception {
        if (!(event.getMessage() instanceof CommandMessage)) {
            // Ignore what this encoder can't encode.
            context.sendUpstream(event);
            return;
        }

        @SuppressWarnings("unchecked")
        CommandMessage<CacheElement> command = (CommandMessage<CacheElement>) event
                .getMessage();
        Command cmdOp = command.cmd;

        LOG.trace(cmdOp.name());
        Channel channel = event.getChannel();
        switch (cmdOp) {
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
            throw new UnsupportedMemcachedException(cmdOp);

        default:
            throw new UnknownCommandException("unknown command:" + cmdOp);
        }
    }

    protected void handleQuit(Channel channel) {
        channel.disconnect();
    }

    protected void handleVersion(ChannelHandlerContext context,
            CommandMessage<CacheElement> command, Channel channel) {
        ResponseMessage responseMessage = new ResponseMessage(command);
        responseMessage.version = VERSION_STRING;
        Channels.fireMessageReceived(context, responseMessage, channel
                .getRemoteAddress());
    }

    protected void handleGets(ChannelHandlerContext context,
            CommandMessage<CacheElement> command, Channel channel)
            throws HapiRequestException {
        HAPI_GETS_TAP.in();
        if (LOG.isTraceEnabled()) {
            StringBuilder msg = new StringBuilder();
            msg.append(command.cmd);
            if (command.element != null) {
                msg.append(" ").append(command.element.getKeystring());
            } else {
                msg.append(" null_command_element");
            }
            for (int i = 0; i < command.keys.size(); ++i) {
                msg.append(" ").append(command.keys.get(i));
            }
            LOG.trace(msg.toString());
        }

        final CacheElement[] results = handleGetKeys(command.keys, session
                .get(), hapiProcessor, formatGetter.getFormat());
        ResponseMessage<CacheElement> resp = new ResponseMessage<CacheElement>(
                command).withElements(results);
        HAPI_GETS_TAP.out();
        Channels.fireMessageReceived(context, resp, channel.getRemoteAddress());
        callback.requestProcessed();
    }

    static CacheElement[] handleGetKeys(List<String> keys, Session session,
            HapiProcessor processor, HapiOutputter outputter)
            throws HapiRequestException {
        if (keys.size() == 0) {
            return new CacheElement[0];
        }

        final boolean ignoreLastKey = keys.get(keys.size() - 1).length() == 0;
        final CacheElement[] results = new CacheElement[ignoreLastKey ? keys
                .size() - 1 : keys.size()];

        int index = 0;
        for (String key : keys) {
            if (index == results.length) {
                assert ignoreLastKey : String.format(
                        "index=%d, results.length=%d", index, results.length);
                break;
            }
            final byte[] result_bytes = getBytesForGets(session, key,
                    processor, outputter);
            LocalCacheElement element = new LocalCacheElement(key);
            element.setData(result_bytes);
            results[index++] = element;
        }
        return results;
    }

    static byte[] getBytesForGets(Session sessionLocal, String key,
            HapiProcessor hapiProcessor, HapiOutputter outputter)
            throws HapiRequestException {
        HapiGetRequest request = ParsedHapiGetRequest.parse(key);
        ByteArrayOutputStream outputStream = getOutputStream(sessionLocal);
        hapiProcessor.processRequest(sessionLocal, request, outputter,
                outputStream);
        return outputStream.toByteArray();
    }

    private static ByteArrayOutputStream getOutputStream(Session session) {
        ByteArrayOutputStream outputStream = session.get(OUTPUTSTREAM_CACHE);
        if (outputStream == null) {
            outputStream = new ByteArrayOutputStream(1024);
            session.put(OUTPUTSTREAM_CACHE, outputStream);
        } else {
            outputStream.reset();
        }
        return outputStream;
    }
}

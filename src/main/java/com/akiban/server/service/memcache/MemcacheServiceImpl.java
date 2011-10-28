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

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.jmx.JmxRegistryService;
import com.akiban.server.service.session.SessionService;
import com.google.inject.Inject;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import javax.management.ObjectName;
import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryCommandDecoder;
import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryResponseEncoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedCommandDecoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedFrameDecoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedResponseEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.Index;
import com.akiban.server.api.HapiGetRequest;
import com.akiban.server.api.HapiOutputter;
import com.akiban.server.api.HapiProcessor;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.error.ServiceStartupException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.util.Tap;

public class MemcacheServiceImpl implements MemcacheService,
        Service<MemcacheService>, JmxManageable {
    private static final Logger LOG = LoggerFactory
            .getLogger(MemcacheServiceImpl.class);

    private final static Tap.PointTap HAPI_CONNECTION_OPEN_TAP = Tap.createCount("hapi: connection open");
    private final static Tap.PointTap HAPI_CONNECTION_CLOSE_TAP = Tap.createCount("hapi: connection close");
    private final static Tap.PointTap HAPI_EXCEPTION_TAP = Tap.createCount("hapi: exception");
    private MemcacheMXBean manageBean;

    private final AkibanCommandHandler.CommandCallback callback = new AkibanCommandHandler.CommandCallback() {

        @Override
        public void requestProcessed() {
        }

        @Override
        public void connectionOpened() {
            HAPI_CONNECTION_OPEN_TAP.hit();
        }

        @Override
        public void connectionClosed() {
            HAPI_CONNECTION_CLOSE_TAP.hit();
        }

        @Override
        public void requestFailed() {
            HAPI_EXCEPTION_TAP.hit();
        }
    };

    private final AkibanCommandHandler.FormatGetter formatGetter = new AkibanCommandHandler.FormatGetter() {
        @Override
        public HapiOutputter getFormat() {
            return manageBean.getOutputFormat().getOutputter();
        }
    };

    // Service vars
    private final ConfigurationService config;
    private final JmxRegistryService jmxRegistry;
    private final SessionService sessionService;
    private final DXLService dxl;

    // Daemon vars
    private final int text_frame_size = 32768 * 1024;
    private DefaultChannelGroup allChannels;
    private ServerSocketChannelFactory channelFactory;
    int port;

    @Inject
    public MemcacheServiceImpl(ConfigurationService config, JmxRegistryService jmxRegistry, SessionService sessionService, DXLService dxl) {
        this.config = config;
        this.jmxRegistry = jmxRegistry;
        this.sessionService = sessionService;
        this.dxl = dxl;
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
            HapiOutputter outputter, OutputStream outputStream)
            throws HapiRequestException {
        final HapiProcessor processor = manageBean.getHapiProcessor()
                .getHapiProcessor(config, dxl);

        processor.processRequest(session, request, outputter, outputStream);
    }

    @Override
    public Index findHapiRequestIndex(Session session, HapiGetRequest request)
            throws HapiRequestException {
        final HapiProcessor processor = manageBean.getHapiProcessor()
                .getHapiProcessor(config, dxl);

        return processor.findHapiRequestIndex(session, request);
    }

    @Override
    public void start() throws ServiceStartupException {
        OutputFormat defaultOutput;
        {
            String defaultOutputName = config.getProperty("akserver.memcached.output.format");
            try {
                defaultOutput = OutputFormat.valueOf(defaultOutputName
                        .toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.warn("Default memcache outputter not found, using JSON: "
                        + defaultOutputName);
                defaultOutput = OutputFormat.JSON;
            }
        }

        HapiProcessorFactory defaultHapi;
        {
            String defaultHapiName = config.getProperty("akserver.memcached.processor");
            try {
                defaultHapi = HapiProcessorFactory.valueOf(defaultHapiName
                        .toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.warn("Default memcache processor not found, using SCANROWS: "
                        + defaultHapiName);
                defaultHapi = HapiProcessorFactory.SCANROWS;
            }
        }

        manageBean = new ManageBean(defaultHapi, defaultOutput, jmxRegistry, sessionService, config, dxl);
        try {
            final String portString = config.getProperty("akserver.memcached.port");

            LOG.debug("Starting memcache service on port {}", portString);

            this.port = Integer.parseInt(portString);
            final InetSocketAddress addr = new InetSocketAddress(port);
            final int idle_timeout = -1;
            final boolean binary = false;
            final boolean verbose = false;

            startDaemon(addr, idle_timeout, binary, verbose);
        } catch (RuntimeException e) {
            throw e;
        }
    }

    @Override
    public void stop() {
        stopDaemon();
    }
    
    @Override
    public void crash() {
        // Shutdown the network threads so a new instance can start up.
        stop();
    }
    

    //
    // start/stopDaemon inspired by com.thimbleware.jmemcached.MemCacheDaemon
    //
    private void startDaemon(final InetSocketAddress addr, final int idle_time,
            final boolean binary, final boolean verbose) {
        channelFactory = new NioServerSocketChannelFactory(Executors
                .newCachedThreadPool(), Executors.newCachedThreadPool());

        allChannels = new DefaultChannelGroup("memcacheServiceChannelGroup");
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);

        final ChannelPipelineFactory pipelineFactory;

        if (binary) {
            pipelineFactory = new BinaryPipelineFactory(this, verbose,
                    idle_time, allChannels, formatGetter, callback, sessionService);
        } else {
            pipelineFactory = new TextPipelineFactory(this, verbose, idle_time,
                    text_frame_size, allChannels, formatGetter, callback, sessionService);
        }

        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("sendBufferSize", 65536);
        bootstrap.setOption("receiveBufferSize", 65536);

        Channel serverChannel = bootstrap.bind(addr);
        allChannels.add(serverChannel);

        LOG.debug("Listening on {}", addr);
    }

    private void stopDaemon() {
        LOG.debug("Shutting down daemon");

        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();

        if (!future.isCompleteSuccess()) {
            LOG.error("Failed to close all network channels");
        }

        channelFactory.releaseExternalResources();
    }

    public MemcacheServiceImpl cast() {
        return this;
    }

    public Class<MemcacheService> castClass() {
        return MemcacheService.class;
    }

    private final static class TextPipelineFactory implements
            ChannelPipelineFactory {
        private int frameSize;
        private final AkibanCommandHandler commandHandler;
        private final MemcachedResponseEncoder responseEncoder;

        public TextPipelineFactory(HapiProcessor hapiProcessor,
                boolean verbose, int idleTime, int frameSize,
                DefaultChannelGroup channelGroup,
                AkibanCommandHandler.FormatGetter formatGetter,
                AkibanCommandHandler.CommandCallback callback,
                SessionService sessionService) {
            this.frameSize = frameSize;
            responseEncoder = new MemcachedResponseEncoder();
            commandHandler = new AkibanCommandHandler(hapiProcessor,
                    channelGroup, formatGetter, callback, sessionService);
        }

        public final ChannelPipeline getPipeline() throws Exception {
            SessionStatus status = new SessionStatus().ready();
            MemcachedFrameDecoder frameDecoder = new MemcachedFrameDecoder(
                    status, frameSize);
            MemcachedCommandDecoder commandDecoder = new MemcachedCommandDecoder(
                    status);
            return Channels.pipeline(frameDecoder, commandDecoder,
                    commandHandler, responseEncoder);
        }
    }

    private final static class BinaryPipelineFactory implements
            ChannelPipelineFactory {
        private final AkibanCommandHandler commandHandler;
        private final MemcachedBinaryCommandDecoder commandDecoder;
        private final MemcachedBinaryResponseEncoder responseEncoder;

        public BinaryPipelineFactory(HapiProcessor hapiProcessor,
                boolean verbose, int idleTime,
                DefaultChannelGroup channelGroup,
                AkibanCommandHandler.FormatGetter formatGetter,
                AkibanCommandHandler.CommandCallback callback,
                SessionService sessionService) {
            commandDecoder = new MemcachedBinaryCommandDecoder();
            responseEncoder = new MemcachedBinaryResponseEncoder();
            commandHandler = new AkibanCommandHandler(hapiProcessor,
                    channelGroup, formatGetter, callback, sessionService);
        }

        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(commandDecoder, commandHandler,
                    responseEncoder);
        }
    }

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("Memcache", manageBean, MemcacheMXBean.class);
    }

    @Override
    public void setHapiProcessor(HapiProcessorFactory processor) {
        manageBean.setHapiProcessor(processor);
    }

    private static class ManageBean implements MemcacheMXBean {
        private final JmxRegistryService jmxRegistry;
        private final SessionService sessionService;
        private final AtomicReference<WhichStruct<OutputFormat>> outputAs;
        private final AtomicReference<WhichStruct<HapiProcessorFactory>> processAs;
        private final ConfigurationService config;
        private final DXLService dxl;

        private static class WhichStruct<T> {
            final T whichItem;
            final ObjectName jmxName;

            private WhichStruct(T whichItem, ObjectName jmxName) {
                this.whichItem = whichItem;
                this.jmxName = jmxName;
            }
        }

        ManageBean(HapiProcessorFactory whichHapi, OutputFormat outputFormat, JmxRegistryService jmxRegistry,
                   SessionService sessionService, ConfigurationService config, DXLService dxl) {
            processAs = new AtomicReference<WhichStruct<HapiProcessorFactory>>(
                    null);
            outputAs = new AtomicReference<WhichStruct<OutputFormat>>(null);
            this.jmxRegistry = jmxRegistry;
            this.sessionService = sessionService;
            this.config = config;
            this.dxl = dxl;
            setHapiProcessor(whichHapi);
            setOutputFormat(outputFormat);
        }

        @Override
        public OutputFormat getOutputFormat() {
            return outputAs.get().whichItem;
        }

        @Override
        public void setOutputFormat(OutputFormat whichFormat)
                throws IllegalArgumentException {
            WhichStruct<OutputFormat> old = outputAs.get();
            if (old != null && old.whichItem.equals(whichFormat)) {
                return;
            }
            ObjectName objectName = null;
            if (whichFormat.getOutputter() instanceof JmxManageable) {
                JmxManageable asJmx = (JmxManageable) whichFormat
                        .getOutputter();
                objectName = jmxRegistry.register(asJmx);
            }
            WhichStruct<OutputFormat> newStruct = new WhichStruct<OutputFormat>(
                    whichFormat, objectName);

            old = outputAs.getAndSet(newStruct);

            if (old != null && old.jmxName != null) {
                jmxRegistry.unregister(old.jmxName);
            }
        }

        @Override
        public OutputFormat[] getAvailableOutputFormats() {
            return OutputFormat.values();
        }

        @Override
        public HapiProcessorFactory getHapiProcessor() {
            return processAs.get().whichItem;
        }

        @Override
        public void setHapiProcessor(HapiProcessorFactory whichProcessor) {
            WhichStruct<HapiProcessorFactory> old = processAs.get();
            if (old != null && old.whichItem.equals(whichProcessor)) {
                return;
            }
            ObjectName objectName = null;
            if (whichProcessor.getHapiProcessor(config, dxl) instanceof JmxManageable) {
                JmxManageable asJmx = (JmxManageable) whichProcessor
                        .getHapiProcessor(config, dxl);
                objectName = jmxRegistry.register(asJmx);
            }
            WhichStruct<HapiProcessorFactory> newStruct = new WhichStruct<HapiProcessorFactory>(
                    whichProcessor, objectName);

            old = processAs.getAndSet(newStruct);

            if (old != null && old.jmxName != null) {
                jmxRegistry.unregister(old.jmxName);
            }
        }

        @Override
        public HapiProcessorFactory[] getAvailableHapiProcessors() {
            return HapiProcessorFactory.values();
        }

        @Override
        public String chooseIndex(String request) {
            Session session = sessionService.createSession();
            try {
                HapiGetRequest getRequest = ParsedHapiGetRequest.parse(request);
                Index index = processAs.get().whichItem.getHapiProcessor(config, dxl)
                        .findHapiRequestIndex(session, getRequest);
                return index == null ? "null" : index.toString();
            } catch (HapiRequestException e) {
                throw new RuntimeException(e.getMessage());
            } finally {
                session.close();
            }
        }
    }
}

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
import com.akiban.server.service.Service;
import com.akiban.server.service.ServiceManager;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.ServiceStartupException;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.Store;
import com.akiban.util.Tap;

public class MemcacheServiceImpl implements MemcacheService,
        Service<MemcacheService>, JmxManageable {
    private static final Logger LOG = LoggerFactory
            .getLogger(MemcacheServiceImpl.class);

    private final static Tap HAPI_CONNECTION_TAP = Tap.add(new Tap.Count(
            "hapi: connection"));
    private final static Tap HAPI_EXCEPTION_TAP = Tap.add(new Tap.Count(
            "hapi: exception"));
    private final MemcacheMXBean manageBean;

    private final AkibanCommandHandler.CommandCallback callback = new AkibanCommandHandler.CommandCallback() {

        @Override
        public void requestProcessed() {
            ;
        }

        @Override
        public void connectionOpened() {
            HAPI_CONNECTION_TAP.in();
        }

        @Override
        public void connectionClosed() {
            HAPI_CONNECTION_TAP.out();
        }

        @Override
        public void requestFailed() {
            HAPI_EXCEPTION_TAP.in();
            HAPI_CONNECTION_TAP.out();
        }
    };

    private final AkibanCommandHandler.FormatGetter formatGetter = new AkibanCommandHandler.FormatGetter() {
        @Override
        public HapiOutputter getFormat() {
            return manageBean.getOutputFormat().getOutputter();
        }
    };

    // Service vars
    private final ServiceManager serviceManager;

    // Daemon vars
    private final int text_frame_size = 32768 * 1024;
    private final AtomicReference<Store> store = new AtomicReference<Store>();
    private DefaultChannelGroup allChannels;
    private ServerSocketChannelFactory channelFactory;
    int port;

    public MemcacheServiceImpl() {
        this.serviceManager = ServiceManagerImpl.get();

        ConfigurationService config = ServiceManagerImpl.get()
                .getConfigurationService();

        OutputFormat defaultOutput;
        {
            String defaultOutputName = config.getProperty(
                    "akserver.memcached.output.format", OutputFormat.JSON
                            .name());
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
            String defaultHapiName = config.getProperty(
                    "akserver.memcached.processor",
                    HapiProcessorFactory.SCANROWS.name());
            try {
                defaultHapi = HapiProcessorFactory.valueOf(defaultHapiName
                        .toUpperCase());
            } catch (IllegalArgumentException e) {
                LOG.warn("Default memcache processor not found, using SCANROWS: "
                        + defaultHapiName);
                defaultHapi = HapiProcessorFactory.SCANROWS;
            }
        }

        manageBean = new ManageBean(defaultHapi, defaultOutput);
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request,
            HapiOutputter outputter, OutputStream outputStream)
            throws HapiRequestException {
        final HapiProcessor processor = manageBean.getHapiProcessor()
                .getHapiProcessor();

        processor.processRequest(session, request, outputter, outputStream);
    }

    @Override
    public Index findHapiRequestIndex(Session session, HapiGetRequest request)
            throws HapiRequestException {
        final HapiProcessor processor = manageBean.getHapiProcessor()
                .getHapiProcessor();

        return processor.findHapiRequestIndex(session, request);
    }

    @Override
    public void start() throws ServiceStartupException {
        if (!store.compareAndSet(null, serviceManager.getStore())) {
            throw new ServiceStartupException("already started");
        }
        try {
            final String portString = serviceManager.getConfigurationService()
                    .getProperty("akserver.memcached.port");

            LOG.debug("Starting memcache service on port {}", portString);

            this.port = Integer.parseInt(portString);
            final InetSocketAddress addr = new InetSocketAddress(port);
            final int idle_timeout = -1;
            final boolean binary = false;
            final boolean verbose = false;

            startDaemon(addr, idle_timeout, binary, verbose);
        } catch (RuntimeException e) {
            store.set(null);
            throw e;
        }
    }

    @Override
    public void stop() {
        stopDaemon();
        store.set(null);
    }
    
    @Override
    public void crash() throws Exception {
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
                    idle_time, allChannels, formatGetter, callback);
        } else {
            pipelineFactory = new TextPipelineFactory(this, verbose, idle_time,
                    text_frame_size, allChannels, formatGetter, callback);
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
                AkibanCommandHandler.CommandCallback callback) {
            this.frameSize = frameSize;
            responseEncoder = new MemcachedResponseEncoder();
            commandHandler = new AkibanCommandHandler(hapiProcessor,
                    channelGroup, formatGetter, callback);
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
                AkibanCommandHandler.CommandCallback callback) {
            commandDecoder = new MemcachedBinaryCommandDecoder();
            responseEncoder = new MemcachedBinaryResponseEncoder();
            commandHandler = new AkibanCommandHandler(hapiProcessor,
                    channelGroup, formatGetter, callback);
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
        private final AtomicReference<WhichStruct<OutputFormat>> outputAs;
        private final AtomicReference<WhichStruct<HapiProcessorFactory>> processAs;

        private static class WhichStruct<T> {
            final T whichItem;
            final ObjectName jmxName;

            private WhichStruct(T whichItem, ObjectName jmxName) {
                this.whichItem = whichItem;
                this.jmxName = jmxName;
            }
        }

        ManageBean(HapiProcessorFactory whichHapi, OutputFormat outputFormat) {
            processAs = new AtomicReference<WhichStruct<HapiProcessorFactory>>(
                    null);
            outputAs = new AtomicReference<WhichStruct<OutputFormat>>(null);
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
                objectName = ServiceManagerImpl.get().getJmxRegistryService()
                        .register(asJmx);
            }
            WhichStruct<OutputFormat> newStruct = new WhichStruct<OutputFormat>(
                    whichFormat, objectName);

            old = outputAs.getAndSet(newStruct);

            if (old != null && old.jmxName != null) {
                ServiceManagerImpl.get().getJmxRegistryService().unregister(
                        old.jmxName);
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
            if (whichProcessor.getHapiProcessor() instanceof JmxManageable) {
                JmxManageable asJmx = (JmxManageable) whichProcessor
                        .getHapiProcessor();
                objectName = ServiceManagerImpl.get().getJmxRegistryService()
                        .register(asJmx);
            }
            WhichStruct<HapiProcessorFactory> newStruct = new WhichStruct<HapiProcessorFactory>(
                    whichProcessor, objectName);

            old = processAs.getAndSet(newStruct);

            if (old != null && old.jmxName != null) {
                ServiceManagerImpl.get().getJmxRegistryService().unregister(
                        old.jmxName);
            }
        }

        @Override
        public HapiProcessorFactory[] getAvailableHapiProcessors() {
            return HapiProcessorFactory.values();
        }

        @Override
        public String chooseIndex(String request) {
            Session session = ServiceManagerImpl.newSession();
            try {
                HapiGetRequest getRequest = ParsedHapiGetRequest.parse(request);
                Index index = processAs.get().whichItem.getHapiProcessor()
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

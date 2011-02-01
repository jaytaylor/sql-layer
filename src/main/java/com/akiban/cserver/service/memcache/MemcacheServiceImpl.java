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

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.cserver.api.HapiGetRequest;
import com.akiban.cserver.api.HapiProcessor;
import com.akiban.cserver.api.HapiRequestException;
import com.akiban.cserver.service.ServiceStartupException;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.session.Session;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import com.akiban.cserver.service.Service;
import com.akiban.cserver.service.ServiceManager;
import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.store.Store;
import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryCommandDecoder;
import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryResponseEncoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedCommandDecoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedFrameDecoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedResponseEncoder;

import javax.management.ObjectName;

import static com.akiban.cserver.service.memcache.MemcacheMXBean.*;

public class MemcacheServiceImpl implements MemcacheService, Service<MemcacheService>, JmxManageable {
    private static final Logger LOG = Logger.getLogger(MemcacheServiceImpl.class);

    private final MemcacheMXBean manageBean;
    private final AkibanCommandHandler.FormatGetter formatGetter = new AkibanCommandHandler.FormatGetter() {
        @Override
        public Outputter getFormat() {
            return manageBean.getOutputFormat().getOutputter();
        }
    };

    // Service vars
    private final ServiceManager serviceManager;
    private static final Log log = LogFactory.getLog(MemcacheServiceImpl.class);

    // Daemon vars
    private final int text_frame_size = 32768 * 1024;
    private final AtomicReference<Store> store = new AtomicReference<Store>();
    private DefaultChannelGroup allChannels;
    private ServerSocketChannelFactory channelFactory;
    int port;

    public MemcacheServiceImpl() {
        this.serviceManager = ServiceManagerImpl.get();

        ConfigurationService config = ServiceManagerImpl.get().getConfigurationService();
        String defaultOutputName = config.getProperty("cserver", "memcached.output.format", OutputFormat.JSON.name());
        OutputFormat defaultOutput;
        try {
            defaultOutput = OutputFormat.valueOf(defaultOutputName.toUpperCase());
        } catch (IllegalArgumentException e) {
            LOG.warn("Default memcache outputter not found, using JSON: " + defaultOutputName);
            defaultOutput = OutputFormat.JSON;
        }
        manageBean = new ManageBean(WhichHapi.FETCHROWS, defaultOutput);
    }

    @Override
    public void processRequest(Session session, HapiGetRequest request, Outputter outputter, OutputStream outputStream)
            throws HapiRequestException
    {
        final HapiProcessor processor = manageBean.getHapiProcessor().getHapiProcessor();

        processor.processRequest(session, request, outputter, outputStream);
    }

    @Override
    public void start() throws ServiceStartupException {
        if (!store.compareAndSet(null, serviceManager.getStore())) {
            throw new ServiceStartupException("already started");
        }
        try {
            final String portString = serviceManager.getConfigurationService()
                    .getProperty("cserver", "memcached.port");

            log.info("Starting memcache service on port " + portString);

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
        log.info("Stopping memcache service");
        stopDaemon();
        store.set(null);
    }

    //
    // start/stopDaemon inspired by com.thimbleware.jmemcached.MemCacheDaemon
    //
    private void startDaemon(final InetSocketAddress addr, final int idle_time,
            final boolean binary, final boolean verbose) {
        channelFactory = new NioServerSocketChannelFactory(
                Executors.newCachedThreadPool(),
                Executors.newCachedThreadPool());

        allChannels = new DefaultChannelGroup("memcacheServiceChannelGroup");
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);

        final ChannelPipelineFactory pipelineFactory;

        if (binary) {
            pipelineFactory = new BinaryPipelineFactory(this, verbose,
                    idle_time, allChannels, formatGetter);
        } else {
            pipelineFactory = new TextPipelineFactory(this, verbose,
                    idle_time, text_frame_size, allChannels, formatGetter);
        }

        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("sendBufferSize", 65536);
        bootstrap.setOption("receiveBufferSize", 65536);

        Channel serverChannel = bootstrap.bind(addr);
        allChannels.add(serverChannel);

        log.info("Listening on " + addr);
    }

    private void stopDaemon() {
        log.info("Shutting down daemon");

        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();

        if (!future.isCompleteSuccess()) {
            log.error("Failed to close all network channels");
        }

        channelFactory.releaseExternalResources();
    }

    public MemcacheServiceImpl cast() {
        return this;
    }

    public Class<MemcacheService> castClass() {
        return MemcacheService.class;
    }

    private final static class TextPipelineFactory implements ChannelPipelineFactory {
        private int frameSize;
        private final AkibanCommandHandler commandHandler;
        private final MemcachedResponseEncoder responseEncoder;

        public TextPipelineFactory(HapiProcessor hapiProcessor, boolean verbose, int idleTime,
                int frameSize, DefaultChannelGroup channelGroup,
                AkibanCommandHandler.FormatGetter formatGetter) {
            this.frameSize = frameSize;
            responseEncoder = new MemcachedResponseEncoder();
            commandHandler = new AkibanCommandHandler(hapiProcessor, channelGroup, formatGetter);
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

    private final static class BinaryPipelineFactory implements ChannelPipelineFactory {
        private final AkibanCommandHandler commandHandler;
        private final MemcachedBinaryCommandDecoder commandDecoder;
        private final MemcachedBinaryResponseEncoder responseEncoder;

        public BinaryPipelineFactory(HapiProcessor hapiProcessor, boolean verbose,
                int idleTime, DefaultChannelGroup channelGroup,
                AkibanCommandHandler.FormatGetter formatGetter) {
            commandDecoder = new MemcachedBinaryCommandDecoder();
            responseEncoder = new MemcachedBinaryResponseEncoder();
            commandHandler = new AkibanCommandHandler(hapiProcessor, channelGroup, formatGetter);
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

    private static class ManageBean implements MemcacheMXBean {
        private final AtomicReference<WhichStruct<OutputFormat>> outputAs;
        private final AtomicReference<WhichStruct<WhichHapi>> processAs;

        private static class WhichStruct<T> {
            final T whichItem;
            final ObjectName jmxName;

            private WhichStruct(T whichItem, ObjectName jmxName) {
                this.whichItem = whichItem;
                this.jmxName = jmxName;
            }
        }

        ManageBean(WhichHapi whichHapi, OutputFormat outputFormat) {
            processAs = new AtomicReference<WhichStruct<WhichHapi>>(null);
            outputAs = new AtomicReference<WhichStruct<OutputFormat>>(null);
            setHapiProcessor(whichHapi);
            setOutputFormat(outputFormat);
        }

        @Override
        public OutputFormat getOutputFormat() {
            return outputAs.get().whichItem;
        }

        @Override
        public void setOutputFormat(OutputFormat whichFormat) throws IllegalArgumentException {
            ObjectName objectName = null;
            if (whichFormat.getOutputter() instanceof JmxManageable) {
                JmxManageable asJmx = (JmxManageable)whichFormat.getOutputter();
                objectName = ServiceManagerImpl.get().getJmxRegistryService().register(asJmx);
            }
            WhichStruct<OutputFormat> newStruct = new WhichStruct<OutputFormat>(whichFormat, objectName);

            WhichStruct<OutputFormat> old = outputAs.getAndSet(newStruct);

            if (old != null && old.jmxName != null) {
                ServiceManagerImpl.get().getJmxRegistryService().unregister(old.jmxName);
            }
        }

        @Override
        public OutputFormat[] getAvailableOutputFormats() {
            return OutputFormat.values();
        }

        @Override
        public WhichHapi getHapiProcessor() {
            return processAs.get().whichItem;
        }

        @Override
        public void setHapiProcessor(WhichHapi whichProcessor) {
            ObjectName objectName = null;
            if (whichProcessor.getHapiProcessor() instanceof JmxManageable) {
                JmxManageable asJmx = (JmxManageable)whichProcessor.getHapiProcessor();
                objectName = ServiceManagerImpl.get().getJmxRegistryService().register(asJmx);
            }
            WhichStruct<WhichHapi> newStruct = new WhichStruct<WhichHapi>(whichProcessor, objectName);

            WhichStruct<WhichHapi> old = processAs.getAndSet(newStruct);

            if (old != null && old.jmxName != null) {
                ServiceManagerImpl.get().getJmxRegistryService().unregister(old.jmxName);
            }
        }

        @Override
        public WhichHapi[] getAvailableHapiProcessors() {
            return WhichHapi.values();
        }
    }
}

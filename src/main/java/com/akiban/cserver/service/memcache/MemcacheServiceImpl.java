package com.akiban.cserver.service.memcache;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.cserver.service.ServiceStartupException;
import com.akiban.cserver.service.session.SessionImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class MemcacheServiceImpl implements MemcacheService,
        Service<MemcacheService> {
    
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
    }

    @Override
    public String processRequest(String request) {
        ByteBuffer buffer = ByteBuffer.allocate(65536);
        Store storeLocal = store.get();
        if (storeLocal == null) {
            storeLocal = serviceManager.getStore(); // We should be able to run this even without the service started
        }
        return HapiProcessorImpl.processRequest(storeLocal, new SessionImpl(), request, buffer);
    }

    @Override
    public void start() throws ServiceStartupException {
        try {
            if (!store.compareAndSet(null, serviceManager.getStore())) {
                throw new ServiceStartupException("already started");
            }
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
            assert !ServiceStartupException.class.isInstance(e)
                    : "ServiceStartupException has been chnaged to a RuntimeException";
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
            pipelineFactory = new BinaryPipelineFactory(store.get(), verbose,
                    idle_time, allChannels);
        } else {
            pipelineFactory = new TextPipelineFactory(store.get(), verbose,
                    idle_time, text_frame_size, allChannels);
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

        public TextPipelineFactory(Store store, boolean verbose, int idleTime,
                int frameSize, DefaultChannelGroup channelGroup) {
            this.frameSize = frameSize;
            responseEncoder = new MemcachedResponseEncoder();
            commandHandler = new AkibanCommandHandler(store, channelGroup);
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

        public BinaryPipelineFactory(Store store, boolean verbose,
                int idleTime, DefaultChannelGroup channelGroup) {
            commandDecoder = new MemcachedBinaryCommandDecoder();
            responseEncoder = new MemcachedBinaryResponseEncoder();
            commandHandler = new AkibanCommandHandler(store, channelGroup);
        }

        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(commandDecoder, commandHandler,
                    responseEncoder);
        }
    }
}

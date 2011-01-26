package com.akiban.cserver.service.memcache;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import com.akiban.cserver.service.ServiceStartupException;
import com.akiban.cserver.service.config.ConfigurationService;
import com.akiban.cserver.service.jmx.JmxManageable;
import com.akiban.cserver.service.memcache.outputter.DummyByteOutputter;
import com.akiban.cserver.service.memcache.outputter.JsonByteOutputter;
import com.akiban.cserver.service.memcache.outputter.JsonOutputter;
import com.akiban.cserver.service.memcache.outputter.RawByteOutputter;
import com.akiban.cserver.service.memcache.outputter.RowDataStringOutputter;
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

public class MemcacheServiceImpl implements MemcacheService, Service<MemcacheService>, JmxManageable {
    private static final Logger LOG = Logger.getLogger(MemcacheServiceImpl.class);

    @SuppressWarnings("unused") // these are queried/set via JMX
    public enum OutputFormat {
        JSON(JsonByteOutputter.instance()),
        RAW(RawByteOutputter.instance()),
        DUMMY(DummyByteOutputter.instance()),
        PLAIN(RowDataStringOutputter.instance())
        ;
        private final Outputter<byte[]> outputter;

        OutputFormat(Outputter<byte[]> outputter) {
            this.outputter = outputter;
        }

        public Outputter<byte[]> getOutputter() {
            return outputter;
        }
    }

    private final AtomicReference<OutputFormat> outputAs;
    private final MemcacheMXBean manageBean = new ManageBean();
    private final AkibanCommandHandler.FormatGetter formatGetter = new AkibanCommandHandler.FormatGetter() {
        @Override
        public Outputter<byte[]> getFormat() {
            return outputAs.get().getOutputter();
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
        outputAs = new AtomicReference<OutputFormat>(defaultOutput);
    }

    @Override
    public <T> T processRequest(Session session, String request, Outputter<T> outputter) {
        ByteBuffer buffer = ByteBuffer.allocate(65536);
        StringBuilder sb = new StringBuilder();
        Store storeLocal = store.get();
        if (storeLocal == null) {
            storeLocal = serviceManager.getStore(); // We should be able to run this even without the service started
        }
        return HapiProcessorImpl.processRequest(storeLocal, session, request, buffer, outputter, sb);
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
            pipelineFactory = new BinaryPipelineFactory(store.get(), verbose,
                    idle_time, allChannels, formatGetter);
        } else {
            pipelineFactory = new TextPipelineFactory(store.get(), verbose,
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

        public TextPipelineFactory(Store store, boolean verbose, int idleTime,
                int frameSize, DefaultChannelGroup channelGroup,
                AkibanCommandHandler.FormatGetter formatGetter) {
            this.frameSize = frameSize;
            responseEncoder = new MemcachedResponseEncoder();
            commandHandler = new AkibanCommandHandler(store, channelGroup, formatGetter);
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
                int idleTime, DefaultChannelGroup channelGroup,
                AkibanCommandHandler.FormatGetter formatGetter) {
            commandDecoder = new MemcachedBinaryCommandDecoder();
            responseEncoder = new MemcachedBinaryResponseEncoder();
            commandHandler = new AkibanCommandHandler(store, channelGroup, formatGetter);
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

    private class ManageBean implements MemcacheMXBean {
        @Override
        public String getOutputFormat() {
            return outputAs.get().name();
        }

        @Override
        public void setOutputFormat(String whichFormat) throws IllegalArgumentException {
            OutputFormat newFormat = OutputFormat.valueOf(whichFormat);
            outputAs.set(newFormat);
        }

        @Override
        public String[] getAvailableOutputFormats() {
            OutputFormat[] formats = OutputFormat.values();
            String[] names = new String[ formats.length ];
            for (int i=0; i < formats.length; ++i) {
                names[i] = formats[i].name();
            }
            return names;
        }
    }
}

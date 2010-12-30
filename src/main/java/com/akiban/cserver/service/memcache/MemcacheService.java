package com.akiban.cserver.service.memcache;


import java.util.concurrent.Executors;
import java.net.InetSocketAddress;

import com.akiban.cserver.service.config.ConfigurationService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.store.Store;
import com.akiban.cserver.service.Service;

import com.thimbleware.jmemcached.protocol.SessionStatus;
import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryCommandDecoder;
import com.thimbleware.jmemcached.protocol.binary.MemcachedBinaryResponseEncoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedFrameDecoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedCommandDecoder;
import com.thimbleware.jmemcached.protocol.text.MemcachedResponseEncoder;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory; 


public class MemcacheService implements Service<MemcacheService>
{
    // Service vars
    private final Store store;
    private final int port;
    private static final Log log = LogFactory.getLog(MemcacheService.class);

    // Daemon vars
    private final int text_frame_size = 32768 * 1024;
    private volatile boolean running = false;
    private DefaultChannelGroup allChannels;
    private ServerSocketChannelFactory channelFactory;

    public MemcacheService(Store store, ConfigurationService config)
    {
        this.store = store;
        
        String portStr = config.getProperty("cserver", "memcached.port");
        this.port = Integer.parseInt(portStr);
    }

    @Override
    public void start() throws Exception
    {
        log.info("Starting memcache service");

        final InetSocketAddress addr = new InetSocketAddress(port);
        final int idle_timeout = -1;
        final boolean binary = false;
        final boolean verbose = false;

        startDaemon(addr, idle_timeout, binary, verbose);
    }
    
    @Override
    public void stop() throws Exception
    {
        log.info("Stopping memcache service");
        stopDaemon();
    }

    //
    // start/stopDaemon inspired by com.thimbleware.jmemcached.MemCacheDaemon
    //
    private void startDaemon(final InetSocketAddress addr, final int idle_time, final boolean binary, final boolean verbose) 
    {
        channelFactory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), 
                                                           Executors.newCachedThreadPool());

        allChannels = new DefaultChannelGroup("memcacheServiceChannelGroup");
        ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);

        ChannelPipelineFactory pipelineFactory;

        if(binary) {
            pipelineFactory = new BinaryPipelineFactory(store, verbose, idle_time, allChannels);
        }
        else {
            pipelineFactory = new TextPipelineFactory(store, verbose, idle_time, text_frame_size, allChannels);
        }
        
        bootstrap.setPipelineFactory(pipelineFactory);
        bootstrap.setOption("sendBufferSize", 65536);
        bootstrap.setOption("receiveBufferSize", 65536);

        Channel serverChannel = bootstrap.bind(addr);
        allChannels.add(serverChannel);

        log.info("Listening on " + addr);
        running = true;
    }

    private void stopDaemon()
    {
        log.info("Shutting down daemon");

        ChannelGroupFuture future = allChannels.close();
        future.awaitUninterruptibly();

        if(!future.isCompleteSuccess()) {
            log.error("Failed to close all network channels");
        }
        
        channelFactory.releaseExternalResources();

        running = false;
    }

    public MemcacheService cast()
    {
        return this;
    }

    public Class<MemcacheService> castClass()
    {
        return MemcacheService.class;
    }


    private final class TextPipelineFactory implements ChannelPipelineFactory
    {
        private int frameSize;
        private final AkibanCommandHandler commandHandler;
        private final MemcachedResponseEncoder responseEncoder;

        public TextPipelineFactory(Store store, boolean verbose, int idleTime, int frameSize, DefaultChannelGroup channelGroup) {
            this.frameSize = frameSize;
            responseEncoder = new MemcachedResponseEncoder();
            commandHandler = new AkibanCommandHandler(store, channelGroup);
        }

        public final ChannelPipeline getPipeline() throws Exception {
            SessionStatus status = new SessionStatus().ready();
            MemcachedFrameDecoder frameDecoder = new MemcachedFrameDecoder(status, frameSize);
            MemcachedCommandDecoder commandDecoder = new MemcachedCommandDecoder(status);
            return Channels.pipeline(frameDecoder, commandDecoder, commandHandler, responseEncoder);
        }
    }

    private final class BinaryPipelineFactory implements ChannelPipelineFactory
    {
        private final AkibanCommandHandler commandHandler;
        private final MemcachedBinaryCommandDecoder commandDecoder;
        private final MemcachedBinaryResponseEncoder responseEncoder;

        public BinaryPipelineFactory(Store store, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
            commandDecoder =  new MemcachedBinaryCommandDecoder();
            responseEncoder = new MemcachedBinaryResponseEncoder();
            commandHandler = new AkibanCommandHandler(store, channelGroup);
        }

        public ChannelPipeline getPipeline() throws Exception {
            return Channels.pipeline(commandDecoder, commandHandler, responseEncoder);
        }
    }
}

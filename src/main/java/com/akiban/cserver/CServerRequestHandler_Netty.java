package com.akiban.cserver;

import com.akiban.message.AkibanConnection;
import com.akiban.message.NettyAkibanConnectionImpl;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;
import com.akiban.network.AkibaNetworkHandler;
import com.akiban.network.CommEventNotifier;
import com.akiban.network.NetworkHandlerFactory;

import java.util.Map;
import java.util.TreeMap;

class CServerRequestHandler_Netty extends AbstractCServerRequestHandler
{
    // RequestHandler interface

    @Override
    public void handleRequest(ExecutionContext executionContext, AkibanConnection connection, Request request)
            throws Exception
    {
        // For the netty driver, execution of requests is handled by CServer.CServerRunnable.
        assert false : request;
    }

    // CServerRequestHandler_Netty interface

    public static AbstractCServerRequestHandler start(CServer chunkserver, String host, int port)
    {
        LOG.info(String.format("Starting CServerRequestHandler_Netty"));
        return new CServerRequestHandler_Netty(chunkserver, host, port);
    }

    public synchronized void stop()
    {
        LOG.info(String.format("Stopping CServerRequestHandler_Netty"));
        if (networkUp) {
            // for now I think this is the only way to make these threads
            // bail from their reads.
            for (final Thread thread : threadMap.values()) {
                thread.interrupt();
            }
            NetworkHandlerFactory.closeNetwork();
            networkUp = false;
        }
    }

    private CServerRequestHandler_Netty(CServer chunkserver, String host, int port)
    {
        super(chunkserver, host, port);
        NetworkHandlerFactory.initializeNetwork(host, Integer.toString(port), new ChannelNotifier());
        networkUp = true;
    }

    private final Map<Integer, Thread> threadMap = new TreeMap<Integer, Thread>();
    private volatile boolean networkUp;

    public class ChannelNotifier implements CommEventNotifier
    {

        @Override
        public void onConnect(AkibaNetworkHandler handler)
        {
            if (LOG.isInfoEnabled()) {
                LOG.info("Connection #" + handler.getId() + " created");
            }
            String threadName = "CServer_" + handler.getId();
            NettyAkibanConnectionImpl connection = NettyAkibanConnectionImpl.createConnection(handler);
            Thread thread = new Thread(chunkserver.newRunnable(connection), threadName);
            thread.setDaemon(true);
            thread.start();
            synchronized (threadMap) {
                threadMap.put(handler.getId(), thread);
            }
        }

        @Override
        public void onDisconnect(AkibaNetworkHandler handler)
        {
            final Thread thread;
            synchronized (threadMap) {
                thread = threadMap.remove(handler.getId());
            }
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                if (LOG.isInfoEnabled()) {
                    LOG.info("Connection #" + handler.getId() + " ended");
                }
            } else {
                LOG.error("CServer thread for connection #" + handler.getId()
                          + " was missing or dead");
            }
        }
    }
}

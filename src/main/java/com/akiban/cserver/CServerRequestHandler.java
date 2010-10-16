package com.akiban.cserver;

import com.akiban.message.AkibanConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;
import com.akiban.server.Server;

import java.io.IOException;

class CServerRequestHandler extends AbstractCServerRequestHandler
{
    // RequestHandler interface

    @Override
    public void handleRequest(ExecutionContext executionContext, AkibanConnection connection, Request request)
            throws Exception
    {
        chunkserver.executeRequest(executionContext, connection, request);
    }

    // AbstractCServerRequestHandler interface

    public synchronized void stop() throws IOException, InterruptedException
    {
        LOG.info(String.format("Stopping CServerRequestHandler"));
        server.stopServer();
    }

    // CServerRequestHandler interface

    public static AbstractCServerRequestHandler start(CServer chunkserver, String host, int port)
            throws IOException, InterruptedException
    {
        LOG.info(String.format("Starting CServerRequestHandler"));
        return new CServerRequestHandler(chunkserver, host, port);
    }

    // For use by this class

    private CServerRequestHandler(CServer chunkserver, String host, int port)
            throws IOException, InterruptedException
    {
        super(chunkserver, host, port);
        server = Server.startServer("CServer", host, port, chunkserver.executionContext(), this);
    }

    // State

    private final Server server;
}

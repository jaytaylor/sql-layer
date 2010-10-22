package com.akiban.cserver;

import com.akiban.ais.message.CServerContext;
import com.akiban.message.AkibanConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;
import com.akiban.server.Server;

import java.io.IOException;

public class CServerRequestHandler extends AbstractCServerRequestHandler
{
    // RequestHandler interface

    @Override
    public void handleRequest(AkibanConnection connection, Request request)
            throws Exception
    {
        ((CServerContext)executionContext).executeRequest(connection, request);
    }

    // AbstractCServerRequestHandler interface


    @Override
    public void start() throws IOException, InterruptedException
    {
        LOG.info(String.format("Starting CServerRequestHandler"));
        server = Server.startServer("CServer", host, port, tcpNoDelay, this);
    }

    public synchronized void stop() throws IOException, InterruptedException
    {
        LOG.info(String.format("Stopping CServerRequestHandler"));
        server.stopServer();
    }

    // CServerRequestHandler interface

    public CServerRequestHandler(String host, int port, boolean tcpNoDelay)
    {
        this.host = host;
        this.port = port;
        this.tcpNoDelay = tcpNoDelay;
    }

    // State

    private final String host;
    private final int port;
    private final boolean tcpNoDelay;
    private Server server;
}

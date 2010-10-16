package com.akiban.cserver;

import com.akiban.server.RequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

abstract class AbstractCServerRequestHandler implements RequestHandler
{
    // AbstractCServerRequestHandler interface

    public abstract void stop() throws IOException, InterruptedException;

    protected AbstractCServerRequestHandler(CServer chunkserver, String host, int port)
    {
        this.chunkserver = chunkserver;
        this.host = host;
        this.port = port;
    }

    // State

    protected static final Log LOG = LogFactory.getLog(AbstractCServerRequestHandler.class);

    protected final CServer chunkserver;
    protected String host;
    protected int port;
}

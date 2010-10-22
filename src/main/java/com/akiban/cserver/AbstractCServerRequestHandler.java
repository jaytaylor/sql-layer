package com.akiban.cserver;

import com.akiban.server.RequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public abstract class AbstractCServerRequestHandler extends RequestHandler
{
    // AbstractCServerRequestHandler interface

    public abstract void start() throws IOException, InterruptedException;
    
    public abstract void stop() throws IOException, InterruptedException;

    // State

    protected static final Log LOG = LogFactory.getLog(AbstractCServerRequestHandler.class);
}

package com.akiban.server;

import com.akiban.message.AkibanConnection;
import com.akiban.message.AkibanConnectionImpl;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

class RequestRunner implements Runnable
{
    // Runnable interface

    @Override
    public void run()
    {
        try {
            Request request;
            do {
                request = (Request) connection.receive();
                if (request != null) {
                    if (LOG.isInfoEnabled()) {
                        LOG.info(String.format("About to execute %s", request));
                    }
                    requestHandler.handleRequest(connection, request);
                    if (LOG.isInfoEnabled()) {
                        LOG.info(String.format("Finished execution of %s", request));
                    }
                }
            } while (request != null);
        } catch (SocketException e) {
            LOG.info("Caught exception on receive, (normal on close of client connection)", e);
            termination = e;
        } catch (Exception e) {
            LOG.error("Unexpected exception", e);
            termination = e;
            server.onException(this);
        } finally {
            server.onDisconnect(this);
            connection.close();
        }
    }

    // RequestRunner interface

    public Throwable termination()
    {
        return termination;
    }

    public RequestRunner(Server server, 
                         Socket clientSocket,
                         RequestHandler requestHandler) throws IOException
    {
        this.server = server;
        this.connection = new AkibanConnectionImpl(clientSocket);
        this.requestHandler = requestHandler;
    }

    private static final Log LOG = LogFactory.getLog(RequestRunner.class);

    private final Server server;
    private final AkibanConnection connection;
    private final RequestHandler requestHandler;
    private Throwable termination;
}

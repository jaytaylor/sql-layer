package com.akiban.cserver;

import com.akiban.message.*;
import com.akiban.server.RequestHandler;
import com.akiban.util.Command;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class CServerLifecycleTest
{
    private static final int N = 5;
    private static final int N_REQUESTS = 10;
    private static final boolean USE_NETTY = Boolean.parseBoolean(System.getProperty("usenetty", "false"));

    @Test
    public void testStartupShutdown() throws Exception
    {
        for (int i = 0; i < N; i++) {
            CServer cserver = startChunkServer();
            stopChunkServer(cserver);
        }
    }

    @Test
    public void testShutdownWithClosedConnections() throws Exception
    {
        if (!USE_NETTY) {
            for (int i = 0; i < N; i++) {
                CServer cserver = startChunkServer();
                AkibanConnection connection = new AkibanConnectionImpl(LOCALHOST, cserver.port());
                for (int r = 0; r < N_REQUESTS; r++) {
                    TestRequest request = new TestRequest(r, 10, 10);
                    TestResponse response = (TestResponse) connection.sendAndReceive(request);
                    Assert.assertEquals(r, response.id());
                }
                connection.close();
                stopChunkServer(cserver);
            }
        }
    }

    @Test
    public void testShutdownWithOpenConnections() throws Exception
    {
        if (!USE_NETTY) {
            for (int i = 0; i < N; i++) {
                CServer cserver = startChunkServer();
                AkibanConnection connection = new AkibanConnectionImpl(LOCALHOST, cserver.port());
                for (int r = 0; r < N_REQUESTS; r++) {
                    TestRequest request = new TestRequest(r, 10, 10);
                    TestResponse response = (TestResponse) connection.sendAndReceive(request);
                    Assert.assertEquals(r, response.id());
                }
                stopChunkServer(cserver);
            }
        }
    }

    private CServer startChunkServer()
            throws Exception
    {
        MessageRegistry.reset(); // In case a message registry is left over from a previous test in the same JVM. 
        CServer cserver = new CServer(false);
        cserver.start();
        Assert.assertTrue(listeningOnPort(cserver.port()));
        MessageRegistry.reset(); 
        initializeMessageRegistry();
        return cserver;
    }

    private void stopChunkServer(CServer cserver)
            throws Exception
    {
        cserver.stop();
        Assert.assertTrue(!listeningOnPort(cserver.port()));
        MessageRegistry.reset();
    }

    private void initializeMessageRegistry()
    {
        if (MessageRegistry.only() == null) {
            new TestMessageRegistry();
            MessageRegistry.only().registerModule("com.akiban.message");
            MessageRegistry.only().registerModule("com.akiban.cserver");
        }
    }

    private boolean listeningOnPort(int portInt) throws Command.Exception, IOException
    {
        String port = Integer.toString(portInt);
        Command command = Command.saveOutput("netstat", "-an");
        command.run();
        boolean up = false;
        for (String line : command.stdout()) {
            if (line.indexOf(port) > 0 && line.indexOf("LISTEN") > 0) {
                up = true;
            }
        }
        return up;
    }

    private static final String LOCALHOST = "127.0.0.1";

    public class TestMessageRegistry extends MessageRegistryBase
    {
        private TestMessageRegistry()
        {
            super(10);
            register(1, "com.akiban.cserver.TestRequest");
            register(2, "com.akiban.cserver.TestResponse");
        }
    }

    private static class TestRequestHandler implements RequestHandler
    {
        @Override
        public void handleRequest(ExecutionContext executionContext, AkibanConnection connection, Request request)
        {
            try {
                request.execute(connection, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

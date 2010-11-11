package com.akiban.server;

import com.akiban.message.*;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ServerTest
{
    @Before
    public void setUp() throws Exception
    {
        initializeNetwork();
    }

    @Test
    public void testPortInUse() throws Exception
    {
        Server server = Server.startServer(TEST, LOCALHOST, SERVER_PORT, true, new TestRequestHandler());
        try {
            Server.startServer(TEST, LOCALHOST, SERVER_PORT, true, new TestRequestHandler());
            Assert.assertTrue(false);
        } catch (InterruptedException e) {
            Assert.assertTrue(false);
        } catch (IOException e) {
            // expected
        }
        server.stopServer();
        Assert.assertTrue(server.stopped());
    }

    @Test
    public void testOneMessage() throws Exception
    {
        Server server = Server.startServer(TEST, LOCALHOST, SERVER_PORT, true, new TestRequestHandler());
        AkibanConnection connection = new AkibanConnectionImpl(LOCALHOST, SERVER_PORT);
        final int REQUEST_ID = 12345;
        TestRequest request = new TestRequest(REQUEST_ID, 10, 10);
        TestResponse response = (TestResponse) connection.sendAndReceive(request);
        Assert.assertEquals(REQUEST_ID, response.id());
        connection.close();
        server.stopServer();
        Assert.assertTrue(server.stopped());
    }

    @Test
    public void testSeveralMessages() throws Exception
    {
        final int N = 100;
        Server server = Server.startServer(TEST, LOCALHOST, SERVER_PORT, true, new TestRequestHandler());
        AkibanConnection connection = new AkibanConnectionImpl(LOCALHOST, SERVER_PORT);
        for (int r = 0; r < N; r++) {
            TestRequest request = new TestRequest(r, 10, 10);
            TestResponse response = (TestResponse) connection.sendAndReceive(request);
            Assert.assertEquals(r, response.id());
        }
        connection.close();
        server.stopServer();
        Assert.assertTrue(server.stopped());
    }

    @Test
    public void testMessagesMultiThreaded() throws Exception
    {
        final int THREADS = 10;
        final int MESSAGES_PER_THREAD = 10;
        Server server = Server.startServer(TEST, LOCALHOST, SERVER_PORT, true, new TestRequestHandler());
        final List<Exception> exceptions = new ArrayList<Exception>();
        final List<Thread> threads = new ArrayList<Thread>();
        for (int t = 0; t < THREADS; t++) {
            Thread thread = new Thread()
            {
                @Override
                public void run()
                {
                    try {
                        AkibanConnection connection = new AkibanConnectionImpl(LOCALHOST, SERVER_PORT);
                        for (int r = 0; r < MESSAGES_PER_THREAD; r++) {
                            TestRequest request = new TestRequest(r, 10, 10);
                            TestResponse response = (TestResponse) connection.sendAndReceive(request);
                            Assert.assertEquals(r, response.id());
                        }
                        connection.close();
                    } catch (Exception e) {
                        exceptions.add(e);
                    }
                }
            };
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Assert.assertTrue(exceptions.isEmpty());
        server.stopServer();
        Assert.assertTrue(server.stopped());
    }

    private void initializeNetwork()
    {
        MessageRegistryBase.reset();
        new TestMessageRegistry();
        MessageRegistry.only().registerModule("com.akiban.message");
        MessageRegistry.only().registerModule("com.akiban.server");
    }

    private static final String TEST = "TEST";
    private static final String LOCALHOST = "127.0.0.1";
    private static final int SERVER_PORT = 9999;

    public class TestMessageRegistry extends MessageRegistryBase
    {
        private TestMessageRegistry()
        {
            super(10);
            register(1, "com.akiban.server.TestRequest");
            register(2, "com.akiban.server.TestResponse");
        }
    }

    private static class TestRequestHandler extends RequestHandler
    {
        @Override
        public void handleRequest(AkibanConnection connection, Request request)
        {
            try {
                request.execute(connection, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

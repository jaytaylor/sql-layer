package com.akiban.cserver;

import com.akiban.cserver.service.ServiceManagerImpl;
import com.akiban.cserver.service.UnitTestServiceManagerFactory;
import com.akiban.message.AkibanConnection;
import com.akiban.message.AkibanConnectionImpl;
import com.akiban.message.MessageRegistry;
import com.akiban.message.MessageRegistryBase;
import com.akiban.util.Command;
import junit.framework.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

public class CServerLifecycleTest
{
    private static final int N = 5;
    private static final int N_REQUESTS = 10;

    private ServiceManagerImpl serviceManager;

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

    @Test
    public void testShutdownWithOpenConnections() throws Exception
    {
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

    private CServer startChunkServer()
        throws Exception
    {
        MessageRegistry.reset(); // In case a message registry is left over from a previous test in the same JVM.
        serviceManager = UnitTestServiceManagerFactory.createServiceManager();
        final Properties originalProperties = System.getProperties();
        try {
            final Properties testProperties = new Properties(System.getProperties());
            testProperties.setProperty("akiban.admin", "NONE");
            System.setProperties(testProperties);
            serviceManager.startServices();
        } finally {
            System.setProperties(originalProperties);
        }
        CServer cserver = serviceManager.getCServer();
        Assert.assertTrue(listeningOnPort(cserver.port()));
        MessageRegistry.reset();
        initializeMessageRegistry();
        return cserver;
    }

    private void stopChunkServer(CServer cserver)
        throws Exception
    {
        serviceManager.stopServices();
        serviceManager = null;
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
}
